"""Reverse proxy to the Colab runtime's Jupyter server.

Every proxied request (REST and WS upgrade) carries the header pair proven by
googlecolab/colab-vscode (src/colab/headers.ts):
    X-Colab-Runtime-Proxy-Token: <connectionInfo.token>
    X-Colab-Client-Agent: <agent string>
REST is rewritten to {connectionInfo.url}/{path}.

WS upstream stack (verified live 2026-11-18 by the sibling platform build):
- the asyncio `websockets` library gets its sockets silently RESET by the
  Colab proxy — the upstream leg uses `websocket-client` (WebSocketApp) in a
  worker thread with client-side pings (the proxy sends none and drops idle
  sockets);
- kernel-channel sockets authenticate with query params
  session_id + token + colab-runtime-proxy-token AND the proxy-token header;
- kernels in `starting` state get their sockets dropped — wait for idle first.
"""

import asyncio
import logging
import re
import threading
import uuid
from urllib.parse import urlencode, urlsplit

import httpx
import websocket as ws_client
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

    headers = _inject(dict(request.headers))
    body = await request.body()
    client: httpx.AsyncClient = request.app.state.http

    resp = await _send_streaming(client, request.method, st, subpath, request, headers, body)
    if resp is None:
        return JSONResponse(
            {"error": {"message": "Upstream Jupyter unreachable"}}, status_code=502
        )

    # Stale endpoint detection: the Colab proxy's port mapping rotates when a
    # runtime is reassigned — the old host answers Go-style "404 page not
    # found" for every path. Buffer the (small) body, refresh connectionInfo
    # once and retry; otherwise stream normally.
    if resp.status_code == 404 and "json" not in (resp.headers.get("content-type") or "").lower():
        buffered = bytearray()
        async for chunk in resp.aiter_raw():
            buffered += chunk
            if len(buffered) > 65536:
                break
        await resp.aclose()
        if b"404 page not found" in buffered.lower():
            try:
                await st.refresh_now()
            except Exception:
                return Response(content=bytes(buffered), status_code=404)
            resp = await _send_streaming(client, request.method, st, subpath, request, headers, body)
            if resp is None:
                return JSONResponse(
                    {"error": {"message": "Upstream Jupyter unreachable after refresh"}},
                    status_code=502,
                )
            if resp.status_code == 404 and "json" not in (resp.headers.get("content-type") or "").lower():
                stale2 = bytearray()
                async for chunk in resp.aiter_raw():
                    stale2 += chunk
                    if len(stale2) > 65536:
                        break
                await resp.aclose()
                if b"404 page not found" in stale2.lower():
                    return Response(content=bytes(stale2), status_code=404)
                return Response(content=bytes(stale2), status_code=resp.status_code)
        else:
            return Response(content=bytes(buffered), status_code=resp.status_code)

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


async def _send_streaming(client, method, st, subpath, request, headers, body):
    url = _upstream_url(st, subpath, urlsplit(str(request.url)).query)
    req = client.build_request(method, url, headers=headers, content=body if body else None)
    try:
        return await client.send(req, stream=True)
    except httpx.HTTPError as e:
        log.warning("upstream error on %s %s: %s", method, url, e)
        return None


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

    # Accept the browser IMMEDIATELY (headless browsers abort slow handshakes);
    # the upstream connect + early-frame buffering happens in _pipe.
    await _pipe(ws, st, subpath)


class _UpstreamBridge:
    """websocket-client WebSocketApp in a worker thread, frames bridged into
    an asyncio queue (the verified-live replacement for asyncio websockets)."""

    def __init__(self, url: str, headers: dict) -> None:
        self.loop = asyncio.get_running_loop()
        self.frames: asyncio.Queue = asyncio.Queue(maxsize=512)
        self.closed = asyncio.Event()
        self.close_info: str = ""
        self._app: ws_client.WebSocketApp | None = None
        self._thread: threading.Thread | None = None
        self._url = url
        self._headers = headers

    def start(self) -> None:
        app = ws_client.WebSocketApp(
            self._url,
            header=[f"{k}: {v}" for k, v in self._headers.items()],
            on_open=self._on_open,
            on_data=self._on_data,
            on_error=self._on_error,
            on_close=self._on_close,
        )
        self._app = app
        self._thread = threading.Thread(
            target=app.run_forever,
            kwargs={"ping_interval": 20, "ping_timeout": 10},
            daemon=True,
        )
        self._thread.start()

    def _put(self, item) -> None:
        def _go():
            try:
                self.frames.put_nowait(item)
            except asyncio.QueueFull:
                self.close_info = "frame queue overflow"
                self._app and self._app.close()
        self.loop.call_soon_threadsafe(_go)

    def _on_open(self, app) -> None:
        self._put(("open", None))

    def _on_data(self, app, data, data_type, cont) -> None:
        # websocket-client delivers TEXT frames as str and BINARY as bytes.
        self._put(("msg", data))

    def _on_error(self, app, err) -> None:
        log.warning("upstream WS bridge error: %s", err)
        if self.close_info == "":
            self.close_info = f"error: {err}"

    def _on_close(self, app, code, msg) -> None:
        if self.close_info == "":
            self.close_info = f"close {code} {msg or ''}".strip()
        self._put(("closed", self.close_info))

    def send(self, text: str | None = None, data: bytes | None = None) -> bool:
        app = self._app
        if app is None:
            return False
        try:
            if data is not None:
                app.send(data, ws_client.ABNF.OPCODE_BINARY)
            else:
                app.send(text, ws_client.ABNF.OPCODE_TEXT)
            return True
        except Exception:
            return False

    async def open(self, timeout_s: float = 30) -> bool:
        item = await asyncio.wait_for(self.frames.get(), timeout=timeout_s)
        return item[0] == "open"

    async def recv(self) -> tuple[str, object]:
        return await self.frames.get()

    def stop(self) -> None:
        try:
            self._app and self._app.close()
        except Exception:
            pass


def _ws_upstream_url(st, subpath: str, query: str) -> str | None:
    """Upstream WS URL with the verified-live auth query params appended."""
    if not st.conn:
        return None
    params = {}
    if query:
        params = dict(pair.split("=", 1) for pair in query.split("&") if "=" in pair)
    params.setdefault("session_id", uuid.uuid4().hex)
    params["token"] = st.conn["token"]
    params["colab-runtime-proxy-token"] = st.conn["token"]
    url = f"{st.conn['url']}/{subpath.lstrip('/')}"
    return f"{url}?{urlencode(params)}"


class _DeferredSend:
    """Send sink that buffers browser frames until the upstream bridge is up."""

    def __init__(self) -> None:
        self.bridge: "_UpstreamBridge | None" = None
        self.buf: list = []

    def send(self, text: str | None = None, data: bytes | None = None) -> bool:
        if self.bridge is None:
            self.buf.append((text, data))
            return True
        return self.bridge.send(text=text, data=data)

    def attach(self, bridge) -> None:
        self.bridge = bridge
        for text, data in self.buf:
            if text is not None:
                bridge.send(text=text)
            elif data is not None:
                bridge.send(data=data)
        self.buf.clear()


async def _pipe(ws: WebSocket, st, subpath: str) -> None:
    """Pipe the browser socket to the runtime kernel channel.

    Accepts the browser immediately, then connects the upstream in the
    background (kernel start can take tens of seconds; slow handshakes abort
    in headless browsers). Early browser frames are buffered until the bridge
    is up.

    Rotation fallback (empirically validated by the battery): if the upstream
    socket closes while the browser socket is still open, reconnect with the
    freshest token (query params + header re-injected); if the server refuses
    the new handshake on a kernel channel, restart the kernel over REST and
    try once more; final failure -> close 1012 (the notebook UI retries with a
    fresh session).
    """
    query = urlsplit(str(ws.url)).query or ""
    is_kernel_channel = bool(_KERNEL_CHANNEL_RE.match(subpath))

    await ws.accept()
    deferred = _DeferredSend()
    to_up = asyncio.create_task(_browser_to_upstream(ws, deferred))

    # ---- initial upstream connect (background, browser already accepted) ----
    bridge = await _connect_bridge(st, subpath, query, is_kernel_channel)
    if to_up.done():
        await _teardown(bridge, to_up, None)
        return
    if bridge is None:
        to_up.cancel()
        await ws.close(code=1012)
        return
    deferred.attach(bridge)
    from_up = asyncio.create_task(_upstream_to_browser(ws, bridge))

    done, _pending = await asyncio.wait(
        {to_up, from_up}, return_when=asyncio.FIRST_COMPLETED
    )
    for t in (to_up, from_up):
        t.cancel()

    to_up_result = None
    if to_up in done and not to_up.cancelled():
        try:
            to_up_result = to_up.result()
        except Exception:
            to_up_result = "error"
    if to_up_result == "browser-gone":
        # Browser went away: nothing to preserve.
        bridge.stop()
        return

    # Upstream dropped while the browser is still here -> rotation fallback.
    log.info("Upstream WS closed while browser connected (%s): %s; reconnecting",
             subpath, bridge.close_info)
    bridge.stop()
    deferred.bridge = None
    bridge = None
    for attempt in range(3):
        try:
            if st.status == "ACTIVE":
                try:
                    await st.refresh_now()
                except Exception:
                    pass  # keep the cached token if GetRuntime hiccups
            if attempt == 1 and is_kernel_channel:
                await _restart_kernel(st, subpath)  # plain reconnect refused -> restart
            bridge = await _connect_bridge(st, subpath, query, is_kernel_channel, refresh=False)
            if bridge is not None:
                break
            log.warning("reconnect attempt %d failed", attempt + 1)
        except Exception as e:
            log.warning("reconnect attempt %d failed: %s", attempt + 1, e)
        await asyncio.sleep(1)
    if bridge is None:
        # 1012 = Service Restart: the notebook UI treats it as a lost kernel
        # socket and retries (new session against the same runtime).
        await ws.close(code=1012)
        return

    deferred.attach(bridge)
    from_up = asyncio.create_task(_upstream_to_browser(ws, bridge))
    to_up = asyncio.create_task(_browser_to_upstream(ws, deferred))
    await asyncio.wait({to_up, from_up}, return_when=asyncio.FIRST_COMPLETED)
    for t in (to_up, from_up):
        t.cancel()
    bridge.stop()


async def _teardown(bridge, *tasks) -> None:
    for t in tasks:
        if t is not None:
            t.cancel()
    if bridge:
        bridge.stop()


async def _connect_bridge(st, subpath: str, query: str, is_kernel_channel: bool = False,
                          refresh: bool = True) -> "_UpstreamBridge | None":
    """Connect the upstream bridge. Kernel channels retry while the kernel is
    `starting` (verified live: sockets to starting kernels DO connect; the
    earlier wait-for-idle design only added handshake latency)."""
    headers = {
        "x-colab-runtime-proxy-token": st.conn["token"],
        "x-colab-client-agent": "jupyter-gateway-colab",
    }
    attempts = 20 if is_kernel_channel else 2  # ~60 s for kernels, 1-shot otherwise
    refreshed = False
    for attempt in range(attempts):
        url = _ws_upstream_url(st, subpath, query)
        if not url:
            return None
        bridge = await _try_bridge(url, headers)
        if bridge is not None:
            return bridge
        if st.status != "ACTIVE" or not st.conn:
            return None  # runtime reclaimed mid-wait
        m = _KERNEL_CHANNEL_RE.match(subpath) if is_kernel_channel else None
        if m and await _kernel_exists(st, m.group(1)) is False:
            log.info("kernel %s no longer exists; giving up", m.group(1))
            return None
        if not refreshed and refresh:
            try:
                await st.refresh_now()  # stale endpoint / token rotation
                headers["x-colab-runtime-proxy-token"] = st.conn["token"]
                refreshed = True
            except Exception:
                pass
        await asyncio.sleep(3)
    return None


async def _kernel_exists(st, kernel_id: str) -> bool | None:
    import httpx as _hx

    try:
        async with _hx.AsyncClient(timeout=UPSTREAM_TIMEOUT) as client:
            r = await client.get(
                f"{st.conn['url']}/api/kernels/{kernel_id}", headers=_inject({})
            )
            if r.status_code == 404:
                return False
            return True
    except Exception:
        return None  # unknown; keep trying


async def _try_bridge(url: str, headers: dict) -> "_UpstreamBridge | None":
    url = url.replace("https://", "wss://", 1).replace("http://", "ws://", 1)
    bridge = _UpstreamBridge(url, headers)
    bridge.start()
    try:
        opened = await bridge.open(timeout_s=30)
    except asyncio.TimeoutError:
        bridge.stop()
        log.warning("upstream WS did not open within 30s: %s", url)
        return None
    if not opened:
        log.warning("upstream WS refused %s: %s", url, bridge.close_info)
        bridge.stop()
        return None
    return bridge


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


async def _browser_to_upstream(ws: WebSocket, sink: "_DeferredSend | _UpstreamBridge") -> str:
    while True:
        msg = await ws.receive()
        if msg["type"] == "websocket.disconnect":
            return "browser-gone"
        text = msg.get("text")
        if text is not None:
            ok = await asyncio.to_thread(sink.send, text=text)
        else:
            data = msg.get("bytes")
            if data is None:
                continue
            ok = await asyncio.to_thread(sink.send, data=data)
        if not ok:
            return "send-failed"


async def _upstream_to_browser(ws: WebSocket, bridge: "_UpstreamBridge") -> None:
    while True:
        kind, payload = await bridge.recv()
        if kind == "open":
            continue
        if kind == "closed":
            return
        if isinstance(payload, str):
            await ws.send_text(payload)
        else:
            await ws.send_bytes(payload)
