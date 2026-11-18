"""Reverse proxy to the Colab runtime's Jupyter server.

Every proxied request (REST and WS upgrade) carries the header pair proven by
googlecolab/colab-vscode (src/colab/headers.ts):
    X-Colab-Runtime-Proxy-Token: <connectionInfo.token>
    X-Colab-Client-Agent: <agent string>
REST is rewritten to {connectionInfo.url}/{path}; WS upgrades are piped
bidirectionally with the same header injection on the upstream handshake.
"""

import asyncio
import logging
import re
from urllib.parse import urlsplit

import httpx
import websockets
from fastapi import Request, WebSocket
from fastapi.responses import JSONResponse, StreamingResponse

from . import config

log = logging.getLogger("proxy")

UPSTREAM_TIMEOUT = httpx.Timeout(180.0, connect=15.0)
_KERNEL_CHANNEL_RE = re.compile(r"^/?api/kernels/([^/]+)/channels/?$")


def _inject(headers: dict) -> dict:
    """Strip browser context (host/cookies/origin) and add the Colab header pair."""
    from .main import get_state  # late import to avoid a cycle

    st = get_state()
    out = {k: v for k, v in headers.items() if k.lower() not in config.HOP_BY_HOP}
    out["x-colab-runtime-proxy-token"] = st.conn["token"]
    out["x-colab-client-agent"] = "jupyter-gateway-colab"
    return out


def _upstream_url(st, subpath: str, query: str) -> str | None:
    if not st.conn:
        return None
    url = f"{st.conn['url']}/{subpath.lstrip('/')}"
    if query:
        url += f"?{query}"
    return url


def _no_runtime_response(st) -> JSONResponse:
    return JSONResponse(
        {
            "error": {
                "message": "No active Colab runtime. Provision one at /admin.",
                "gateway_status": st.status,
                "last_error": st.last_error,
            }
        },
        status_code=503,
    )


# ---- REST ----------------------------------------------------------------


async def proxy_rest(request: Request, subpath: str) -> StreamingResponse | JSONResponse:
    from .main import get_state

    st = get_state()
    st.invalidate_if_expired()
    if st.status != "ACTIVE" or not st.conn:
        return _no_runtime_response(st)
    url = _upstream_url(st, subpath, urlsplit(str(request.url)).query)

    headers = _inject(dict(request.headers))
    body = await request.body()
    client: httpx.AsyncClient = request.app.state.http

    req = client.build_request(
        request.method, url, headers=headers, content=body if body else None
    )
    try:
        resp = await client.send(req, stream=True)
    except httpx.HTTPError as e:
        log.warning("upstream error on %s %s: %s", request.method, url, e)
        return JSONResponse(
            {"error": {"message": f"Upstream Jupyter unreachable: {e}"}},
            status_code=502,
        )

    async def gen():
        try:
            async for chunk in resp.aiter_raw():
                yield chunk
        finally:
            await resp.aclose()

    out_headers = {
        k: v
        for k, v in resp.headers.items()
        if k.lower() not in config.HOP_BY_HOP
        and k.lower() not in ("content-encoding", "content-length")
    }
    return StreamingResponse(gen(), status_code=resp.status_code, headers=out_headers)


# ---- WebSocket ------------------------------------------------------------


async def proxy_ws(ws: WebSocket, subpath: str) -> None:
    from .main import get_state
    from . import gate

    if not gate.session_valid(ws.cookies.get(config.SESSION_COOKIE)):
        await ws.close(code=4401)
        return

    st = get_state()
    if st.status != "ACTIVE" or not st.conn:
        await ws.close(code=1013)
        return

    url = _upstream_url(st, subpath, urlsplit(str(ws.url)).query or "")
    if not url:
        await ws.close(code=1013)
        return
    url = url.replace("https://", "wss://", 1).replace("http://", "ws://", 1)

    await _pipe(ws, st, url, subpath)


async def _pipe(ws: WebSocket, st, url: str, subpath: str) -> None:
    """Open the upstream kernel WS with the token headers and pipe both ways.

    Rotation fallback (empirically validated by the battery): if the upstream
    socket closes while the browser socket is still open, reconnect with the
    freshest token; if the server refuses the new handshake on a kernel
    channel, restart the kernel over REST and try once more. colab-vscode
    injects the same header pair on every (re)connect.
    """
    is_kernel_channel = bool(_KERNEL_CHANNEL_RE.match(subpath))
    try:
        upstream = await _connect_upstream(st, url)
    except Exception as e:
        log.warning("upstream WS refused %s: %s", url, e)
        await ws.close(code=1014)
        return

    await ws.accept()
    to_up = asyncio.create_task(_browser_to_upstream(ws, upstream))
    from_up = asyncio.create_task(_upstream_to_browser(ws, upstream))

    done, _pending = await asyncio.wait(
        {to_up, from_up}, return_when=asyncio.FIRST_COMPLETED
    )
    for t in (to_up, from_up):
        t.cancel()

    if to_up in done and not to_up.exception():
        # Browser went away: nothing to preserve.
        await _close_up(upstream)
        return

    # Upstream dropped while the browser is still here -> rotation fallback.
    log.info("Upstream WS closed while browser connected (%s); reconnecting", subpath)
    upstream = None
    for attempt in range(3):
        try:
            if st.status == "ACTIVE":
                try:
                    await st.refresh_now()
                except Exception:
                    pass  # keep the cached token if GetRuntime hiccups
            fresh = _upstream_url(st, subpath, "") or url
            fresh = fresh.replace("https://", "wss://", 1).replace("http://", "ws://", 1)
            upstream = await _connect_upstream(st, fresh)
            break
        except Exception as e:
            log.warning("reconnect attempt %d failed: %s", attempt + 1, e)
            if attempt == 1 and is_kernel_channel:
                await _restart_kernel(st, subpath)
            await asyncio.sleep(1)
    if upstream is None:
        # 1012 = Service Restart: JupyterLab treats it as a lost kernel socket
        # and auto-reconnects (rebinding to the same kernel over the gateway).
        await ws.close(code=1012)
        return

    from_up = asyncio.create_task(_upstream_to_browser(ws, upstream))
    to_up = asyncio.create_task(_browser_to_upstream(ws, upstream))
    await asyncio.wait({to_up, from_up}, return_when=asyncio.FIRST_COMPLETED)
    for t in (to_up, from_up):
        t.cancel()
    await _close_up(upstream)


async def _close_up(upstream) -> None:
    try:
        await upstream.close()
    except Exception:
        pass


async def _connect_upstream(st, url: str):
    headers = {
        "x-colab-runtime-proxy-token": st.conn["token"],
        "x-colab-client-agent": "jupyter-gateway-colab",
    }
    return await websockets.connect(
        url,
        additional_headers=headers,
        max_size=None,        # kernel iopub messages can exceed 1 MB
        ping_interval=None,   # mirrors the reference client (no client pings)
        open_timeout=30,
        close_timeout=5,
    )


async def _restart_kernel(st, subpath: str) -> None:
    m = _KERNEL_CHANNEL_RE.match(subpath)
    if not m or not st.conn:
        return
    kernel_id = m.group(1)
    import httpx as _hx

    async with _hx.AsyncClient(timeout=UPSTREAM_TIMEOUT) as client:
        try:
            r = await client.post(
                f"{st.conn['url']}/api/kernels/{kernel_id}/restart",
                headers=_inject({}),
            )
            log.info("kernel restart fallback for %s: HTTP %s", kernel_id, r.status_code)
        except Exception as e:
            log.warning("kernel restart fallback failed: %s", e)


async def _browser_to_upstream(ws: WebSocket, upstream) -> None:
    while True:
        msg = await ws.receive()
        if msg["type"] == "websocket.disconnect":
            return
        text = msg.get("text")
        if text is not None:
            await upstream.send(text)
        else:
            data = msg.get("bytes")
            if data is not None:
                await upstream.send(data)


async def _upstream_to_browser(ws: WebSocket, upstream) -> None:
    while True:
        message = await upstream.recv()
        if isinstance(message, str):
            await ws.send_text(message)
        else:
            await ws.send_bytes(message)
