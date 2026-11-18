"""Password session gate: signed-cookie sessions, stdlib only."""

import hashlib
import hmac
import logging
import os
import secrets
import time

from . import config

log = logging.getLogger("gate")

_secret: bytes | None = None


def _session_secret() -> bytes:
    """Persisted across redeploys on /data so sessions survive gateway restarts."""
    global _secret
    if _secret is not None:
        return _secret
    path = os.path.join(config.DATA_DIR, "session.key")
    try:
        os.makedirs(config.DATA_DIR, exist_ok=True)
        if os.path.exists(path):
            with open(path, "rb") as f:
                data = f.read().strip()
            if len(data) >= 32:
                _secret = data
                return _secret
        data = secrets.token_hex(32).encode()
        tmp = path + ".tmp"
        with open(tmp, "wb") as f:
            f.write(data)
        os.replace(tmp, path)
        _secret = data
    except OSError as e:  # unwritable /data: degrade to per-boot secret
        log.warning("Cannot persist session key to %s: %s", path, e)
        _secret = secrets.token_hex(32).encode()
    return _secret


def _sign(payload: str) -> str:
    return hmac.new(_session_secret(), payload.encode(), hashlib.sha256).hexdigest()


def make_session() -> str:
    payload = str(int(time.time()) + config.SESSION_TTL_S)
    return f"{payload}.{_sign(payload)}"


def session_valid(value: str | None) -> bool:
    if not value or "." not in value:
        return False
    payload, sig = value.rsplit(".", 1)
    if not hmac.compare_digest(sig, _sign(payload)):
        return False
    try:
        return int(payload) > time.time()
    except ValueError:
        return False


def check_password(candidate: str) -> bool:
    expected = config.GATEWAY_PASSWORD
    if not expected:
        # Fail closed but never lock the operator out of their own deploy:
        # a per-boot random password is printed to the deploy logs once.
        generated = getattr(check_password, "_ephemeral", None)
        if generated is None:
            generated = secrets.token_urlsafe(18)
            setattr(check_password, "_ephemeral", generated)
            log.warning(
                "GATEWAY_PASSWORD not set; ephemeral password for this boot: %s",
                generated,
            )
        expected = getattr(check_password, "_ephemeral")
    return hmac.compare_digest(candidate, expected)
