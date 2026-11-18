"""Gateway runtime state + background token refresher.

Lifecycle statuses:
  NO_CREDENTIAL - no Google credential onboarded yet (/connect)
  NO_RUNTIME    - credential present, no runtime assigned (provision available)
  PROVISIONING  - CreateRuntime LRO in flight
  ACTIVE        - runtime assigned, connectionInfo cached (proxy usable)
  RECLAIMED     - GetRuntime returned 404 (idle deletion / 12 h cap) - reprovision
  DELETING      - DeleteRuntime in flight
  ERROR         - last lifecycle action failed (message in last_error)
"""

import asyncio
import logging
import time
from collections import deque
from datetime import datetime, timezone

from . import config, colab as colab_mod

log = logging.getLogger("state")


class GatewayState:
    def __init__(self, credstore, client: colab_mod.ColabClient) -> None:
        self.creds = credstore
        self.client = client
        self.status = "NO_CREDENTIAL"
        self.last_error: str | None = None
        self.runtime_name: str | None = None
        self.spec: dict | None = None
        self.conn: dict | None = None  # {token,url,expire_epoch,expire_time}
        self.events: deque = deque(maxlen=60)
        self.token_refreshes = 0
        self.adopted_at: float | None = None
        self._task: asyncio.Task | None = None
        self._busy = asyncio.Lock()  # one lifecycle op at a time

    # ---- events --------------------------------------------------------

    def event(self, msg: str, level: str = "info") -> None:
        ts = datetime.now(timezone.utc).strftime("%H:%M:%S")
        self.events.appendleft({"ts": ts, "level": level, "msg": msg})
        getattr(log, "info" if level == "info" else "warning")(msg)

    # ---- adoption / refresh -------------------------------------------

    async def adopt_on_boot(self) -> None:
        """Reattach to any runtime already assigned to the account (server-side
        state survives gateway redeploys — the disconnect-proof property)."""
        if not self.creds.has():
            self.status = "NO_CREDENTIAL"
            return
        try:
            runtimes = await self.client.list_runtimes()
        except Exception as e:
            self.status = "ERROR"
            self.last_error = f"ListRuntimes failed: {e}"
            self.event(self.last_error, "warn")
            return
        best = None
        for r in runtimes:
            if r.get("connectionInfo") and r.get("name"):
                best = r  # last one wins; list order is not specified
        if best:
            self.runtime_name = best["name"]
            self.spec = best.get("runtimeSpec") or {}
            self.conn = colab_mod.conn_fields(best)
            self.status = "ACTIVE" if self.conn else "NO_RUNTIME"
            self.adopted_at = time.time()
            self.event(f"Adopted existing runtime {self.runtime_name} on boot")
        else:
            self.status = "NO_RUNTIME"
            self.event("No runtime assigned to this account")

    async def refresh_now(self) -> bool:
        """GetRuntime to obtain a fresh proxy token. Returns True on success.
        404 -> RECLAIMED (runtime was deleted server-side)."""
        if not self.runtime_name:
            return False
        try:
            runtime = await self.client.get_runtime(self.runtime_name)
        except colab_mod.RuntimeGoneError:
            self.conn = None
            self.status = "RECLAIMED"
            self.last_error = None
            self.event("Runtime no longer exists (reclaimed) — reprovision needed", "warn")
            return False
        except Exception as e:
            self.event(f"Token refresh failed: {e}", "warn")
            raise
        conn = colab_mod.conn_fields(runtime)
        if conn:
            self.conn = conn
            self.spec = runtime.get("runtimeSpec") or self.spec
            self.token_refreshes += 1
            self.status = "ACTIVE"
            self.event(
                f"Token refreshed (#{self.token_refreshes}), expires {conn['expire_time']}"
            )
            return True
        return False

    def token_seconds_left(self) -> float:
        if not self.conn:
            return -1.0
        return self.conn["expire_epoch"] - time.time()

    # ---- background refresher -----------------------------------------

    async def refresher_loop(self) -> None:
        """GetRuntime at expireTime - 5 min (colab-vscode ConnectionRefresher);
        30 s retry on transient failures; 404 -> RECLAIMED."""
        while True:
            try:
                await asyncio.sleep(15)
                if self.status != "ACTIVE" or not self.conn:
                    continue
                left = self.token_seconds_left()
                if left > config.REFRESH_BUFFER_S:
                    continue
                try:
                    await self.refresh_now()
                except Exception:
                    if self.token_seconds_left() < config.RETRY_BUFFER_S:
                        self.event("Refresh window exhausted; proxy tokens may 404", "warn")
                    await asyncio.sleep(config.RETRY_BUFFER_S)
            except asyncio.CancelledError:
                return
            except Exception as e:  # never let the loop die
                log.exception("refresher loop error: %s", e)
                await asyncio.sleep(15)

    def start_refresher(self) -> None:
        if self._task is None or self._task.done():
            self._task = asyncio.get_event_loop().create_task(self.refresher_loop())

    async def stop_refresher(self) -> None:
        if self._task:
            self._task.cancel()
            try:
                await self._task
            except asyncio.CancelledError:
                pass
            self._task = None

    # ---- lifecycle -----------------------------------------------------

    async def provision(self, spec: dict) -> dict:
        """CreateRuntime LRO end-to-end. Returns dict with ok/error."""
        if self._busy.locked():
            return {"ok": False, "error": "Another lifecycle operation is in progress"}
        async with self._busy:
            self.last_error = None
            prev = self.runtime_name
            had_conn = self.conn is not None
            self.status = "PROVISIONING"
            self.event(f"Provisioning runtime ({spec.get('accelerator')} {spec.get('shape')})")
            try:
                runtime = await self.client.create_runtime(
                    spec,
                    on_progress=lambda m: self.event(f"LRO metadata: {m}") if m else None,
                )
            except colab_mod.ColabApiError as e:
                # A failed create does not invalidate the previously adopted
                # runtime (still assigned server-side) — restore it.
                self.status = "ACTIVE" if (had_conn and self.conn) else ("RECLAIMED" if prev else "NO_RUNTIME")
                self.last_error = f"CreateRuntime failed ({e.status}): {e.message}"
                self.event(self.last_error, "warn")
                return {"ok": False, "error": self.last_error}
            except Exception as e:
                self.status = "ACTIVE" if (had_conn and self.conn) else ("RECLAIMED" if prev else "NO_RUNTIME")
                self.last_error = f"CreateRuntime failed: {e}"
                self.event(self.last_error, "warn")
                return {"ok": False, "error": self.last_error}
            self.runtime_name = runtime.get("name") or self.runtime_name
            self.spec = runtime.get("runtimeSpec") or spec
            self.conn = colab_mod.conn_fields(runtime)
            if self.conn:
                self.status = "ACTIVE"
                self.event(f"Runtime ACTIVE: {self.runtime_name}")
                return {"ok": True}
            # Provisioned but no connectionInfo yet: one GetRuntime usually fixes it.
            ok = await self.refresh_now()
            if not ok:
                self.status = "ERROR"
                self.last_error = "Runtime created but connectionInfo absent"
                return {"ok": False, "error": self.last_error}
            return {"ok": True}

    async def delete(self) -> dict:
        if self._busy.locked():
            return {"ok": False, "error": "Another lifecycle operation is in progress"}
        async with self._busy:
            name = self.runtime_name
            self.status = "DELETING"
            self.event(f"Deleting runtime {name}")
            try:
                if name:
                    await self.client.delete_runtime(name)
            except Exception as e:
                self.status = "ACTIVE"
                self.last_error = f"DeleteRuntime failed: {e}"
                self.event(self.last_error, "warn")
                return {"ok": False, "error": self.last_error}
            self.runtime_name = None
            self.conn = None
            self.spec = None
            self.status = "NO_RUNTIME"
            self.event("Runtime deleted")
            return {"ok": True}

    def invalidate_if_expired(self) -> None:
        """Proxy guard: token far past expiry -> surface RECLAIMED on next use."""
        if self.status == "ACTIVE" and self.conn and self.token_seconds_left() < -120:
            self.status = "ERROR"
            self.last_error = "Proxy token expired and refresh failed"
