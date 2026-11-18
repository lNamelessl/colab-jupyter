"""Colab API v1beta client (colaboratory.googleapis.com).

Shapes verified against the vendored colab-api.json in googlecolab/colab-vscode:
- Runtime.name        = "runtimes/{runtime_id}"
- CreateRuntime body  = {"runtime": {"runtimeSpec": {variant, accelerator, shape}},
                         "requestId": <uuid>}  (LRO Operation returned)
- RuntimeSpec key     = {variant: VARIANT_CPU|VARIANT_GPU|VARIANT_TPU,
                         accelerator: "T4"/"L4"/"A100"/"NONE"/..., shape: SHAPE_STANDARD|SHAPE_HIGHMEM}
- ConnectionInfo      = {token, expireTime, url, endpoint?}
"""

import asyncio
import logging
import uuid
from datetime import datetime, timezone

import httpx

from . import config

log = logging.getLogger("colab")

API_TIMEOUT = httpx.Timeout(60.0, connect=15.0)
LRO_TIMEOUT_S = 480


class ColabApiError(Exception):
    def __init__(self, status: int, message: str) -> None:
        super().__init__(message)
        self.status = status
        self.message = message


class RuntimeGoneError(ColabApiError):
    """404 from GetRuntime — the runtime was reclaimed/deleted."""


def _rfc3339(value: str) -> float | None:
    try:
        dt = datetime.fromisoformat(value.replace("Z", "+00:00"))
        return dt.timestamp()
    except (ValueError, AttributeError, TypeError):
        return None


class ColabClient:
    def __init__(self, credstore) -> None:
        self._creds = credstore
        self._client = httpx.AsyncClient(
            base_url=config.COLAB_BASE, timeout=API_TIMEOUT
        )

    async def aclose(self) -> None:
        await self._client.aclose()

    async def _headers(self) -> dict:
        tok = await self._creds.access_token()
        return {"Authorization": f"Bearer {tok}", "Accept": "application/json"}

    async def _request(self, method: str, path: str, **kw) -> httpx.Response:
        r = await self._client.request(method, path, headers=await self._headers(), **kw)
        if r.status_code >= 400:
            detail = ""
            try:
                j = r.json()
                detail = j.get("error", {}).get("message") or str(j)[:300]
            except Exception:
                detail = r.text[:300]
            raise ColabApiError(r.status_code, detail)
        return r

    # ---- resources -----------------------------------------------------

    async def get_subscription(self) -> dict:
        return (await self._request("GET", "/v1beta/subscription")).json()

    async def list_runtime_specs(self) -> list[dict]:
        body = (await self._request("GET", "/v1beta/runtimespecs")).json()
        return body.get("runtimeSpecs", [])

    async def list_runtimes(self) -> list[dict]:
        body = (await self._request("GET", "/v1beta/runtimes")).json()
        return body.get("runtimes", [])

    async def get_runtime(self, runtime_name: str) -> dict:
        """Returns the Runtime resource. 404 -> RuntimeGoneError (reclaimed)."""
        name = runtime_name if "/" in runtime_name else f"runtimes/{runtime_name}"
        try:
            return (await self._request("GET", f"/v1beta/{name}")).json()
        except ColabApiError as e:
            if e.status == 404:
                raise RuntimeGoneError(404, f"Runtime {name} no longer exists") from e
            raise

    async def create_runtime(self, spec: dict, on_progress=None) -> dict:
        """CreateRuntime (LRO). spec = {variant, accelerator, shape} (flat Key).
        Returns the Runtime resource once the operation completes."""
        body = {
            "runtime": {"runtimeSpec": spec},
            "requestId": str(uuid.uuid4()),
        }
        op = (await self._request("POST", "/v1beta/runtimes", json=body)).json()
        if op.get("done"):
            self._raise_if_op_error(op)
            runtime = op.get("response") or {}
            return runtime if runtime.get("name") else {}

        op_name = op.get("name")
        if not op_name:
            raise ColabApiError(500, "CreateRuntime returned an operation without a name")
        log.info("LRO started: %s", op_name)
        deadline = asyncio.get_event_loop().time() + LRO_TIMEOUT_S
        while asyncio.get_event_loop().time() < deadline:
            await asyncio.sleep(3)
            current = await self._poll_operation(op_name)
            if current.get("done"):
                self._raise_if_op_error(current)
                runtime = current.get("response") or {}
                if runtime.get("name"):
                    return runtime
                # Standard LRO: response is the resource; if absent, GET it.
                rts = await self.list_runtimes()
                for r in rts:
                    if r.get("name", "").endswith(op_name.rsplit("/", 1)[-1]):
                        return r
                return {}
            if on_progress:
                try:
                    on_progress(current.get("metadata", {}))
                except Exception:
                    pass
        raise ColabApiError(504, f"LRO {op_name} timed out after {LRO_TIMEOUT_S}s")

    @staticmethod
    def _raise_if_op_error(op: dict) -> None:
        err = op.get("error")
        if err:
            details = err.get("details") or []
            reason = ""
            for d in details:
                if isinstance(d, dict):
                    reason = f" {d.get('reason', '')} {d.get('metadata', '')}".strip()
            raise ColabApiError(400, f"{err.get('message', 'operation failed')} {reason}".strip())

    async def _poll_operation(self, op_name: str) -> dict:
        try:
            return (await self._request("GET", f"/v1beta/{op_name}")).json()
        except ColabApiError as e:
            if e.status == 404:
                return (await self._request("GET", f"/v1/{op_name}")).json()
            raise

    async def delete_runtime(self, runtime_name: str) -> None:
        name = runtime_name if "/" in runtime_name else f"runtimes/{runtime_name}"
        try:
            await self._request("DELETE", f"/v1beta/{name}", params={"allowMissing": "true"})
        except ColabApiError as e:
            if e.status != 404:
                raise


def conn_fields(runtime: dict) -> dict | None:
    """{token, url, expire_epoch, expire_time} from a Runtime resource."""
    ci = runtime.get("connectionInfo") or {}
    token, url = ci.get("token"), ci.get("url")
    if not token or not url:
        return None
    return {
        "token": token,
        "url": url.rstrip("/"),
        "expire_epoch": _rfc3339(ci.get("expireTime", "")) or 0.0,
        "expire_time": ci.get("expireTime"),
    }
