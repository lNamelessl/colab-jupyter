import os

# Server
PORT = int(os.environ.get("PORT", "8000"))

# Gateway session gate
GATEWAY_PASSWORD = os.environ.get("GATEWAY_PASSWORD", "")

# BYO Google credential: full JSON (refresh_token + client_id + client_secret)
# either provided as a variable or pasted through /connect (persisted to /data).
GOOGLE_CREDENTIALS_JSON = os.environ.get("GOOGLE_CREDENTIALS_JSON", "")

DATA_DIR = os.environ.get("GATEWAY_DATA_DIR", "/data")

# Google OAuth loopback copy-paste flow (the gcloud-style installed-app client).
# NOTE: the legacy sdk.cloud.google.com applicationdefaultauthcode.html redirect
# is DEAD (redirect_uri_mismatch); only the localhost loopback redirect works.
OAUTH_CLIENT_ID = (
    "764086051850-6qr4p6gpi6hn506pt8ejuq83di341hur.apps.googleusercontent.com"
)
OAUTH_CLIENT_SECRET = "d-FL95Q19q7MQmFpd7hHD0Ty"
OAUTH_REDIRECT_URI = "http://localhost:8080"
OAUTH_AUTH_URI = "https://accounts.google.com/o/oauth2/v2/auth"
TOKEN_URI = "https://oauth2.googleapis.com/token"
SCOPES = [
    "openid",
    "https://www.googleapis.com/auth/cloud-platform",
    "https://www.googleapis.com/auth/userinfo.email",
    "https://www.googleapis.com/auth/colaboratory",
]

# Colab API v1beta
COLAB_BASE = "https://colaboratory.googleapis.com"

# The Colab API requires a quota project for ADC user credentials: every call
# must carry x-goog-user-project pointing at a Cloud project where the Colab
# API is enabled (else PERMISSION_DENIED / SERVICE_DISABLED "requires a quota
# project"). Resolution order: credential JSON's "quota_project_id" key >
# this env var > the shared template project below.
QUOTA_PROJECT_ID = (
    os.environ.get("QUOTA_PROJECT_ID", "").strip()
    or "project-05b09e41-482f-4259-8d9"
)

# Token refresh: GetRuntime when within this window of expireTime, retry 30 s
# on failure (mirrors colab-vscode ConnectionRefresher).
REFRESH_BUFFER_S = 5 * 60
RETRY_BUFFER_S = 30

# Proxy behaviour
PROXY_HEADERS = {
    "x-colab-runtime-proxy-token",  # injected per-request with the live token
    "x-colab-client-agent",
}
HOP_BY_HOP = {
    "connection",
    "keep-alive",
    "proxy-authenticate",
    "proxy-authorization",
    "te",
    "trailer",
    "transfer-encoding",
    "upgrade",
    "host",
    "content-length",
    "cookie",
    "origin",
    "referer",
}
SESSION_COOKIE = "gw_session"
SESSION_TTL_S = 14 * 24 * 3600
