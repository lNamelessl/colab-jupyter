"""Google credential store: BYO refresh-token JSON, server-side refresh via
google-auth. Priority: GOOGLE_CREDENTIALS_JSON env > /data/google_credentials.json.
"""

import json
import logging
import os
import threading
from datetime import datetime, timedelta, timezone

import google.auth.transport.requests
from google.oauth2.credentials import Credentials

from . import config

log = logging.getLogger("creds")

REQUIRED_KEYS = ("refresh_token", "client_id", "client_secret")


class CredentialError(Exception):
    pass


class CredStore:
    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._creds: Credentials | None = None
        self._email: str | None = None

    # ---- storage -------------------------------------------------------

    def _file_path(self) -> str:
        return os.path.join(config.DATA_DIR, "google_credentials.json")

    def _load_file(self) -> dict | None:
        try:
            with open(self._file_path(), "r", encoding="utf-8") as f:
                data = json.load(f)
            return self._valid(data)
        except (OSError, json.JSONDecodeError):
            return None

    @staticmethod
    def _valid(data: dict | None) -> dict | None:
        if isinstance(data, dict) and all(k in data and data[k] for k in REQUIRED_KEYS):
            return {
                "refresh_token": data["refresh_token"],
                "client_id": data["client_id"],
                "client_secret": data["client_secret"],
            }
        return None

    def load_env(self) -> bool:
        """Adopt the GOOGLE_CREDENTIALS_JSON variable if present and valid."""
        raw = config.GOOGLE_CREDENTIALS_JSON.strip()
        if not raw:
            return False
        try:
            data = json.loads(raw)
        except json.JSONDecodeError:
            log.warning("GOOGLE_CREDENTIALS_JSON is set but not valid JSON")
            return False
        if not self._valid(data):
            log.warning(
                "GOOGLE_CREDENTIALS_JSON missing one of: %s", ", ".join(REQUIRED_KEYS)
            )
            return False
        self._build(data)
        log.info("Credential adopted from GOOGLE_CREDENTIALS_JSON")
        return True

    def save(self, data: dict) -> None:
        clean = self._valid(data)
        if not clean:
            raise CredentialError(
                "JSON must include refresh_token, client_id and client_secret"
            )
        os.makedirs(config.DATA_DIR, exist_ok=True)
        tmp = self._file_path() + ".tmp"
        with open(tmp, "w", encoding="utf-8") as f:
            json.dump(clean, f, indent=2)
        os.replace(tmp, self._file_path())
        self._build(clean)

    def clear(self) -> None:
        try:
            os.remove(self._file_path())
        except OSError:
            pass
        with self._lock:
            self._creds = None
            self._email = None

    def has(self) -> bool:
        with self._lock:
            if self._creds is not None:
                return True
        return self._load_file() is not None or self.load_env()

    # ---- tokens --------------------------------------------------------

    def _build(self, data: dict) -> None:
        with self._lock:
            self._creds = Credentials(
                token=None,
                refresh_token=data["refresh_token"],
                token_uri=config.TOKEN_URI,
                client_id=data["client_id"],
                client_secret=data["client_secret"],
                scopes=config.SCOPES,
            )
            self._email = None

    def _ensure(self) -> Credentials:
        with self._lock:
            if self._creds is not None:
                return self._creds
        data = self._load_file()
        if data is None:
            raise CredentialError(
                "No Google credential on this gateway. Open /connect to add one."
            )
        self._build(data)
        with self._lock:
            return self._creds  # type: ignore[return-value]

    def access_token(self) -> str:
        """Valid access token; refreshes when missing/expiring (<60 s left)."""
        creds = self._ensure()
        needs_refresh = creds.token is None or creds.expiry is None or (
            creds.expiry - datetime.now(timezone.utc) < timedelta(seconds=60)
        )
        if needs_refresh:
            with self._lock:
                creds.refresh(google.auth.transport.requests.Request())
        if creds.token is None:
            raise CredentialError("Failed to obtain access token")
        return creds.token

    async def user_email(self) -> str | None:
        """Best-effort identity badge for the dashboard."""
        if self._email:
            return self._email
        try:
            import httpx

            tok = self.access_token()
            async with httpx.AsyncClient(timeout=15) as client:
                r = await client.get(
                    "https://openidconnect.googleapis.com/v1/userinfo",
                    headers={"Authorization": f"Bearer {tok}"},
                )
                if r.status_code == 200:
                    self._email = r.json().get("email")
        except Exception as e:
            log.debug("userinfo lookup failed: %s", e)
        return self._email


# ---- OAuth loopback copy-paste flow -------------------------------------


def consent_url() -> str:
    """Consent URL with the localhost loopback redirect (the ONLY working
    redirect for this client — the sdk.cloud.google.com page is dead)."""
    from urllib.parse import urlencode

    params = urlencode(
        {
            "client_id": config.OAUTH_CLIENT_ID,
            "redirect_uri": config.OAUTH_REDIRECT_URI,
            "response_type": "code",
            "scope": " ".join(config.SCOPES),
            "access_type": "offline",
            "prompt": "consent",
        }
    )
    return f"{config.OAUTH_AUTH_URI}?{params}"


async def exchange_code(code: str, httpx_client) -> dict:
    """Exchange the address-bar authorization code for a refresh token.
    The redirect_uri MUST be echoed exactly as http://localhost:8080."""
    r = await httpx_client.post(
        config.TOKEN_URI,
        data={
            "code": code.strip(),
            "client_id": config.OAUTH_CLIENT_ID,
            "client_secret": config.OAUTH_CLIENT_SECRET,
            "redirect_uri": config.OAUTH_REDIRECT_URI,
            "grant_type": "authorization_code",
        },
        headers={"Content-Type": "application/x-www-form-urlencoded"},
    )
    body = {}
    try:
        body = r.json()
    except Exception:
        body = {"raw": r.text}
    if r.status_code != 200 or "refresh_token" not in body:
        raise CredentialError(
            f"Token exchange failed ({r.status_code}): "
            f"{body.get('error_description') or body.get('error') or body.get('raw', '')}"
        )
    return {
        "refresh_token": body["refresh_token"],
        "client_id": config.OAUTH_CLIENT_ID,
        "client_secret": config.OAUTH_CLIENT_SECRET,
    }
