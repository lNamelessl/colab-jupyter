"""Jupyter Gateway for Colab — FastAPI app assembly.

Routes (explicit, registered BEFORE the catch-all proxy):
  GET  /health          unauthenticated health probe
  GET  /                -> /admin
  GET/POST /login       gateway password -> signed session cookie
  POST /logout
  GET  /connect         Google credential onboarding (loopback copy-paste flow)
  POST /connect/json    paste full credential JSON (GOOGLE_CREDENTIALS_JSON shape)
  POST /connect/code    paste the address-bar authorization code (4/0A...)
  GET  /admin           runtime lifecycle dashboard
  GET  /admin/state     JSON status (battery + dashboard polling)
  POST /admin/create    CreateRuntime LRO with a chosen eligible spec
  POST /admin/delete    DeleteRuntime (irreversible)
  POST /admin/refresh   force GetRuntime token refresh (battery hook)
  REST+WS catch-all     proxy to {connectionInfo.url} with X-Colab-Runtime-Proxy-Token
"""

import asyncio
import contextlib
import logging
import time
from urllib.parse import quote

import httpx
from fastapi import FastAPI, Request, WebSocket
from fastapi.responses import HTMLResponse, JSONResponse, RedirectResponse
from fastapi.templating import Jinja2Templates

from . import config, creds as creds_mod, gate
from .colab import ColabApiError, ColabClient, conn_fields
from .state import GatewayState

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s"
)
log = logging.getLogger("main")

app = FastAPI(title="Jupyter Gateway for Colab", docs_url=None, redoc_url=None, openapi_url=None)
templates = Jinja2Templates(directory="app/templates")

_state: GatewayState | None = None
_client: ColabClient | None = None


def get_state() -> GatewayState:
    assert _state is not None
    return _state


def get_app() -> FastAPI:
    return app


@app.on_event("startup")
async def startup() -> None:
    global _state, _client
    credstore = creds_mod.CredStore()
    credstore.load_env()
    client = ColabClient(credstore)
    _client = client
    _state = GatewayState(credstore, client)
    app.state.http = httpx.AsyncClient(timeout=httpx.Timeout(180.0, connect=15.0))
    await _state.adopt_on_boot()
    _state.start_refresher()
    log.info("Gateway up. status=%s", _state.status)


@app.on_event("shutdown")
async def shutdown() -> None:
    if _state:
        await _state.stop_refresher()
    if _client:
        with contextlib.suppress(Exception):
            await _client.aclose()
    with contextlib.suppress(Exception):
        await app.state.http.aclose()


# ---- gate helpers ---------------------------------------------------------


def _session_ok(request: Request) -> bool:
    return gate.session_valid(request.cookies.get(config.SESSION_COOKIE))


def _gate_redirect(request: Request):
    nxt = quote(str(request.url.path), safe="")
    return RedirectResponse(f"/login?next={nxt}", status_code=303)


def _set_session_cookie(resp):
    resp.set_cookie(
        config.SESSION_COOKIE,
        gate.make_session(),
        max_age=config.SESSION_TTL_S,
        httponly=True,
        samesite="lax",
        secure=True,
        path="/",
    )
    return resp


# ---- health / auth --------------------------------------------------------


@app.get("/health")
async def health() -> JSONResponse:
    st = get_state()
    return JSONResponse({"ok": True, "status": st.status})


@app.get("/")
async def root(request: Request):
    if not _session_ok(request):
        return _gate_redirect(request)
    return RedirectResponse("/admin", status_code=302)


@app.get("/login")
async def login_page(request: Request):
    if _session_ok(request):
        return RedirectResponse("/admin", status_code=302)
    return templates.TemplateResponse(
        request, "login.html", {"error": None, "next": request.query_params.get("next", "/admin")}
    )


@app.post("/login")
async def login_submit(request: Request):
    form = await request.form()
    password = str(form.get("password", ""))
    nxt = str(form.get("next", "/admin")) or "/admin"
    if nxt.startswith("/") is False:
        nxt = "/admin"
    if not gate.check_password(password):
        return templates.TemplateResponse(
            request, "login.html", {"error": "Wrong gateway password.", "next": nxt},
            status_code=401,
        )
    return _set_session_cookie(RedirectResponse(nxt, status_code=303))


@app.post("/logout")
async def logout():
    resp = RedirectResponse("/login", status_code=303)
    resp.delete_cookie(config.SESSION_COOKIE, path="/")
    return resp


# ---- credential onboarding (loopback copy-paste flow) ---------------------


@app.get("/connect")
async def connect_page(request: Request):
    if not _session_ok(request):
        return _gate_redirect(request)
    st = get_state()
    return templates.TemplateResponse(
        request,
        "connect.html",
        {
            "consent_url": creds_mod.consent_url(),
            "connected": st.creds.has(),
            "email": await st.creds.user_email() if st.creds.has() else None,
            "quota_default": config.QUOTA_PROJECT_ID,
            "error": None,
        },
    )


@app.post("/connect/json")
async def connect_json(request: Request):
    if not _session_ok(request):
        return _gate_redirect(request)
    st = get_state()
    form = await request.form()
    raw = str(form.get("credential_json", "")).strip()
    quota = str(form.get("quota_project_id", "")).strip()
    try:
        import json as _json

        data = _json.loads(raw)
        if quota and not data.get("quota_project_id"):
            data["quota_project_id"] = quota
        elif not data.get("quota_project_id") and config.QUOTA_PROJECT_ID:
            data["quota_project_id"] = config.QUOTA_PROJECT_ID
        st.creds.save(data)
        get_state().event("Google credential saved from pasted JSON")
        await _enable_colab_api(data.get("quota_project_id", ""))
        await _validate_credential()
    except creds_mod.CredentialError as e:
        return templates.TemplateResponse(
            request, "connect.html",
            {"consent_url": creds_mod.consent_url(), "connected": False, "email": None,
             "error": str(e)},
            status_code=400,
        )
    except Exception as e:
        return templates.TemplateResponse(
            request, "connect.html",
            {"consent_url": creds_mod.consent_url(), "connected": False, "email": None,
             "error": f"Invalid JSON: {e}"},
            status_code=400,
        )
    return RedirectResponse("/admin", status_code=303)


@app.post("/connect/code")
async def connect_code(request: Request):
    if not _session_ok(request):
        return _gate_redirect(request)
    form = await request.form()
    code = str(form.get("authorization_code", "")).strip()
    quota = str(form.get("quota_project_id", "")).strip()
    if not code:
        return templates.TemplateResponse(
            request, "connect.html",
            {"consent_url": creds_mod.consent_url(), "connected": False, "email": None,
             "error": "Paste the code from the address bar."},
            status_code=400,
        )
    st = get_state()
    try:
        async with httpx.AsyncClient(timeout=30) as client:
            data = await creds_mod.exchange_code(code, client, quota_project_id=quota)
        st.creds.save(data)
        st.event("Google credential saved from loopback authorization code")
        await _enable_colab_api(data.get("quota_project_id", ""))
        await _validate_credential()
    except creds_mod.CredentialError as e:
        return templates.TemplateResponse(
            request, "connect.html",
            {"consent_url": creds_mod.consent_url(), "connected": False, "email": None,
             "error": str(e)},
            status_code=400,
        )
    except Exception as e:
        return templates.TemplateResponse(
            request, "connect.html",
            {"consent_url": creds_mod.consent_url(), "connected": False, "email": None,
             "error": f"Exchange failed: {e}"},
            status_code=400,
        )
    return RedirectResponse("/admin", status_code=303)


async def _enable_colab_api(quota_project: str) -> None:
    """Best-effort: enable colaboratory.googleapis.com on the quota project
    (the Colab API rejects ADC user creds without an enabled quota project)."""
    if not quota_project:
        return
    st = get_state()
    try:
        tok = st.creds.access_token()
        async with httpx.AsyncClient(timeout=60) as client:
            r = await client.post(
                f"https://serviceusage.googleapis.com/v1/projects/{quota_project}"
                "/services/colaboratory.googleapis.com:enable",
                headers={"Authorization": f"Bearer {tok}"},
            )
        st.event(f"Colab API enable on {quota_project}: HTTP {r.status_code}")
    except Exception as e:
        st.event(f"Colab API enable attempt failed: {e}", "warn")


@app.post("/connect/disconnect")
async def connect_disconnect(request: Request):
    if not _session_ok(request):
        return _gate_redirect(request)
    st = get_state()
    st.creds.clear()
    st.status = "NO_CREDENTIAL"
    st.event("Google credential removed", "warn")
    return RedirectResponse("/connect", status_code=303)


async def _validate_credential() -> None:
    """Live-check the credential against the Colab API, then adopt runtimes."""
    st = get_state()
    try:
        await st.client.get_subscription()
    except ColabApiError as e:
        st.status = "ERROR"
        st.last_error = f"Credential validated but Colab API refused ({e.status}): {e.message}"
        st.event(st.last_error, "warn")
        raise creds_mod.CredentialError(st.last_error)
    except Exception as e:
        st.status = "ERROR"
        st.last_error = f"Credential check failed: {e}"
        st.event(st.last_error, "warn")
        raise creds_mod.CredentialError(st.last_error)
    st.event("Google credential validated against the Colab API")
    await st.adopt_on_boot()


# ---- runtime lifecycle dashboard ------------------------------------------


@app.get("/admin")
async def admin(request: Request):
    if not _session_ok(request):
        return _gate_redirect(request)
    st = get_state()
    specs = await _eligible_specs()
    return templates.TemplateResponse(
        request,
        "dashboard.html",
        {
            "st": st,
            "state_json": _state_json(st),
            "specs": specs,
            "email": await st.creds.user_email() if st.creds.has() else None,
            "connected": st.creds.has(),
        },
    )


@app.get("/nb")
async def nb_home(request: Request):
    if not _session_ok(request):
        return _gate_redirect(request)
    return templates.TemplateResponse(request, "nb_home.html", {})


@app.get("/nb/view/{path:path}")
async def nb_view(request: Request, path: str):
    if not _session_ok(request):
        return _gate_redirect(request)
    return templates.TemplateResponse(request, "nb_edit.html", {"path": path})


@app.get("/admin/state")
async def admin_state(request: Request):
    if not _session_ok(request):
        return JSONResponse({"error": "unauthenticated"}, status_code=401)
    return JSONResponse(_state_json(get_state()))


def _state_json(st: GatewayState) -> dict:
    return {
        "status": st.status,
        "credential": st.creds.has(),
        "runtime_id": (st.runtime_name or "").removeprefix("runtimes/"),
        "runtime_name": st.runtime_name,
        "spec": st.spec,
        "token_expire_time": st.conn.get("expire_time") if st.conn else None,
        "token_seconds_left": round(st.token_seconds_left()) if st.conn else None,
        "token_refreshes": st.token_refreshes,
        "last_error": st.last_error,
        "events": list(st.events)[:12],
    }


async def _eligible_specs() -> list[dict]:
    """ListRuntimeSpecs filtered to eligible:true, cached 5 minutes."""
    st = get_state()
    now = time.time()
    cached = getattr(app.state, "specs_cache", None)
    if cached and now - cached[0] < 300:
        return cached[1]
    try:
        raw = await st.client.list_runtime_specs()
    except Exception as e:
        st.event(f"ListRuntimeSpecs failed: {e}", "warn")
        return cached[1] if cached else []
    specs = []
    for s in raw:
        key = s.get("key") or {}
        if not s.get("eligible"):
            continue
        specs.append(
            {
                "variant": key.get("variant", ""),
                "accelerator": key.get("accelerator", "NONE"),
                "shape": key.get("shape", ""),
                "label": f"{key.get('accelerator', 'NONE')} "
                         f"{key.get('variant', '').replace('VARIANT_', '')} "
                         f"{key.get('shape', '').replace('SHAPE_', '')}".title().replace("Gpu", "GPU"),
            }
        )
    specs.sort(key=lambda s: (s["accelerator"] == "NONE", s["accelerator"], s["shape"]))
    app.state.specs_cache = (now, specs)
    return specs


@app.post("/admin/create")
async def admin_create(request: Request):
    if not _session_ok(request):
        return _gate_redirect(request)
    st = get_state()
    # One concurrent runtime assignment per account (free tier): creating while
    # a runtime is assigned leaves the new one dangling (GetRuntime never
    # returns connectionInfo). Reuse the active runtime instead.
    if st.status == "ACTIVE" and st.conn and st.token_seconds_left() > 0:
        msg = "Runtime already active — reuse it (delete first to switch specs)."
        if request.query_params.get("json"):
            return JSONResponse({"ok": False, "error": msg}, status_code=409)
        st.event("Create refused: runtime already active", "warn")
        return RedirectResponse("/admin", status_code=303)
    form = await request.form()
    chosen = str(form.get("spec", ""))
    specs = await _eligible_specs()
    spec = next((s for s in specs if f"{s['variant']}|{s['accelerator']}|{s['shape']}" == chosen), None)
    if spec is None and specs:
        spec = specs[0]  # default: first eligible (usually the free T4)
    if spec is None:
        st.status = "ERROR" if st.creds.has() else st.status
        st.last_error = "No eligible runtime specs available for this account"
        return RedirectResponse("/admin", status_code=303)
    result = await st.provision(
        {"variant": spec["variant"], "accelerator": spec["accelerator"], "shape": spec["shape"]}
    )
    if not result["ok"] and request.query_params.get("json"):
        return JSONResponse(result, status_code=502)
    return RedirectResponse("/admin", status_code=303)


@app.post("/admin/delete")
async def admin_delete(request: Request):
    if not _session_ok(request):
        return _gate_redirect(request)
    result = await get_state().delete()
    if not result["ok"] and request.query_params.get("json"):
        return JSONResponse(result, status_code=502)
    return RedirectResponse("/admin", status_code=303)


@app.post("/admin/wsdebug")
async def admin_wsdebug(request: Request):
    """TEMPORARY battery diagnostic: full kernel round-trip server-side via the
    same _UpstreamBridge the WS proxy uses. Returns what arrived."""
    if not _session_ok(request):
        return JSONResponse({"error": "unauthenticated"}, status_code=401)
    st = get_state()
    H = {"x-colab-runtime-proxy-token": st.conn["token"], "content-type": "application/json"}
    kernel_id = None
    kl = await app.state.http.get(f"{st.conn['url']}/api/kernels", headers=H)
    kid_list = kl.json()
    kid_list = kid_list if isinstance(kid_list, list) else kid_list.get("kernels", [])
    if kid_list:
        kernel_id = kid_list[0]["id"]
    else:
        kc = await app.state.http.post(
            f"{st.conn['url']}/api/kernels", headers=H, json={"name": "python3"}
        )
        if kc.status_code == 200:
            kernel_id = kc.json().get("id")
    if not kernel_id:
        return JSONResponse({"error": "no kernel", "list_status": kl.status_code,
                             "create_status": kc.status_code})
    result = {"kernel_id": kernel_id}
    from .proxy import _connect_bridge

    import json as _json

    query = f"session_id=wsdebug-{kernel_id[:8]}"
    bridge = await _connect_bridge(st, f"api/kernels/{kernel_id}/channels", query)
    if bridge is None:
        return JSONResponse({"error": "bridge refused", "kernel_id": kernel_id})
    result["opened"] = True
    msg = {
        "header": {"msg_id": "debug-1", "msg_type": "kernel_info_request",
                   "session": f"wsdebug-{kernel_id[:8]}", "username": "debug",
                   "version": "5.3", "date": "2026-11-18T00:00:00Z"},
        "parent_header": {}, "metadata": {},
        "content": {}, "channel": "shell", "buffers": [],
    }
    ok = bridge.send(text=_json.dumps(msg))
    result["send_ok"] = ok
    loop = asyncio.get_event_loop()
    deadline = loop.time() + 25
    while loop.time() < deadline:
        try:
            kind, payload = await asyncio.wait_for(bridge.recv(), timeout=10)
        except asyncio.TimeoutError:
            break
        snippet = payload if isinstance(payload, str) else repr(payload)[:60]
        result.setdefault("events", []).append(str(snippet)[:160])
        if isinstance(payload, str) and "kernel_info_reply" in payload:
            result["reply"] = "GOT_KERNEL_INFO_REPLY"
            break
    bridge.stop()
    return JSONResponse(result)


@app.post("/admin/refresh")
async def admin_refresh(request: Request):
    if not _session_ok(request):
        return _gate_redirect(request)
    st = get_state()
    ok = await st.refresh_now()
    if request.query_params.get("json"):
        return JSONResponse({"ok": ok, **_state_json(st)}, status_code=200 if ok else 502)
    return RedirectResponse("/admin", status_code=303)


# ---- proxy catch-alls (registered LAST) -----------------------------------

_PROXY_METHODS = ["GET", "POST", "PUT", "PATCH", "DELETE", "HEAD", "OPTIONS"]


@app.api_route("/jupyter/{path:path}", methods=_PROXY_METHODS)
async def jupyter_proxy(request: Request, path: str):
    from .proxy import proxy_rest

    return await proxy_rest(request, path)


@app.api_route("/{path:path}", methods=_PROXY_METHODS)
async def catchall_proxy(request: Request, path: str):
    from .proxy import proxy_rest

    return await proxy_rest(request, path)


@app.websocket("/jupyter/{path:path}")
async def jupyter_ws(ws: WebSocket, path: str):
    from .proxy import proxy_ws

    await proxy_ws(ws, path)


@app.websocket("/{path:path}")
async def catchall_ws(ws: WebSocket, path: str):
    from .proxy import proxy_ws

    await proxy_ws(ws, path)
