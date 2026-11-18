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


def _friendly(detail: str) -> str:
    """Surface actionable hints for the known Colab API refusal shapes."""
    low = (detail or "").lower()
    if "quota project" in low or "service_disabled" in low:
        return (
            detail + " — set QUOTA_PROJECT_ID (or credential quota_project_id) "
            "to a Cloud project with the Colab API enabled"
        )
    if "has not been used in project" in low or "it is disabled" in low:
        return (
            detail + " — enable propagation can take a minute; retry or run "
            "gcloud services enable colaboratory.googleapis.com --project <id>"
        )
    return detail


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
        h = {"Authorization": f"Bearer {tok}", "Accept": "application/json"}
        # Quirk 57b: the Colab API rejects ADC user credentials without a quota
        # project — every call carries x-goog-user-project.
        quota = await self._quota()
        if quota:
            h["x-goog-user-project"] = quota
        return h

    async def _quota(self) -> str:
        try:
            return self._creds.quota_project()
        except Exception:
            return config.QUOTA_PROJECT_ID

    async def _request(self, method: str, path: str, **kw) -> httpx.Response:
        r = await self._client.request(method, path, headers=await self._headers(), **kw)
        if r.status_code >= 400:
            detail = ""
            try:
                j = r.json()
                detail = j.get("error", {}).get("message") or str(j)[:300]
            except Exception:
                detail = r.text[:300]
            raise ColabApiError(r.status_code, _friendly(detail))
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
        """CreateRuntime (LRO) + poll until live. Returns the Runtime resource.

        Readiness (sibling-verified live 2026-11-18): the LRO metadata carries
        the runtime name immediately; poll GetRuntime on that id until it
        returns connectionInfo. The operations.get/:wait endpoints 404 at this
        host, so GetRuntime polling is the only working readiness signal.
        Body shape verified live: {"runtimeSpec": {flat Key}} + requestId
        query param (idempotency).
        """
        request_id = str(uuid.uuid4())
        op = (
            await self._request(
                "POST",
                "/v1beta/runtimes",
                params={"requestId": request_id},
                json={"runtimeSpec": spec},
            )
        ).json()
        if op.get("done"):
            self._raise_if_op_error(op)
            resp = op.get("response") or {}
            rt = resp.get("runtime") if "runtime" in resp else resp
            if rt and rt.get("connectionInfo"):
                return rt

        runtime_name = (op.get("metadata") or {}).get("runtime") or ""
        if not runtime_name:
            raise ColabApiError(500, "CreateRuntime returned no metadata.runtime (cannot poll)")
        log.info("LRO started for %s; polling GetRuntime until connectionInfo", runtime_name)
        runtime_id = runtime_name.removeprefix("runtimes/")
        deadline = asyncio.get_event_loop().time() + LRO_TIMEOUT_S
        while asyncio.get_event_loop().time() < deadline:
            await asyncio.sleep(3)
            try:
                runtime = await self.get_runtime(runtime_id)
            except RuntimeGoneError:
                continue  # not visible yet
            except ColabApiError as e:
                if e.status == 404:
                    continue
                raise
            if runtime.get("connectionInfo"):
                if on_progress:
                    try:
                        on_progress({"ready": runtime_name})
                    except Exception:
                        pass
                return runtime
            if on_progress:
                try:
                    on_progress({"runtime": runtime_name})
                except Exception:
                    pass
        raise ColabApiError(504, f"CreateRuntime LRO timed out after {LRO_TIMEOUT_S}s")

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
