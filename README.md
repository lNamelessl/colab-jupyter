# Jupyter Gateway for Colab

A self-hosted Jupyter UI backed by a **Google Colab GPU runtime**, deployed on Railway and
proxied through your own URL with gateway authentication.

The gateway provisions a Colab runtime via Google's **official Colab API (v1beta)** on your
own Google account, then reverse-proxies the runtime's Jupyter server through your Railway
domain — REST and kernel WebSockets alike — injecting the runtime's short-lived
`X-Colab-Runtime-Proxy-Token` on every request and auto-refreshing it 5 minutes before it
expires (the same mechanism Google's own colab-vscode client uses).

**The disconnect-proof property:** the notebook kernel and filesystem live on Google's
infrastructure, not in your browser and not in this container. Close the tab, shut the laptop,
redeploy the gateway — a long-running cell keeps executing server-side, and any browser can
re-attach to the same runtime through the gateway afterwards.

[![Deploy on Railway](https://railway.com/button.svg)](https://railway.com/deploy/colab-jupyter)

## What you get

- **JupyterLab in your browser at your own URL** — the stock Colab runtime Jupyter server,
  proxied under your Railway domain with a password gate in front of it.
- **Free-tier T4 GPU** (and any other spec your account is eligible for, discovered live from
  `ListRuntimeSpecs`) — run `import torch; torch.cuda.is_available()` → `True`.
- **Server-side persistence** — notebooks and files live on the runtime across gateway
  redeploys and browser closure.
- **Zero-rotation worry** — proxy tokens expire hourly; the gateway refreshes them
  automatically (GetRuntime at expiry − 5 min) and re-injects the fresh token on REST and WS.
- **Runtime lifecycle UI** — provision (with an eligible-accelerator picker), force token
  refresh, delete (irreversible, confirm-guarded), event log, one-click reprovision if Google
  reclaims the runtime (free-tier idle deletion / 12-hour lifetime cap).

## Connect your Google account (one-time)

The gateway needs a Google OAuth refresh token with the `colaboratory` scope. It uses the
gcloud-style **loopback copy-paste** flow — no OAuth client registration of your own needed:

1. Deploy, open your gateway URL, sign in with the gateway password
   (`GATEWAY_PASSWORD`, auto-generated per deploy — see the Variables tab).
2. Click **Connect your Google account**, then **Open Google consent page**, choose your
   account and approve the `cloud-platform` / `colaboratory` scopes.
3. **After approving, the browser lands on a "can't connect" page (localhost refused to
   connect) — that is expected.** Copy the `code=` value from the **address bar**
   (it starts with `4/0A`) and paste it into the gateway's connect page.
4. The gateway exchanges the code (echoing `redirect_uri=http://localhost:8080` exactly),
   validates the credential against the Colab API, and stores the refresh token on its
   private volume (`/data`).
   Alternatively, paste a full credential JSON
   (`{"refresh_token": "...", "client_id": "...", "client_secret": "..."}`) — the same shape
   as the `GOOGLE_CREDENTIALS_JSON` variable, which you can also set directly on the service
   instead of using the connect page.
5. Click **Provision runtime** (defaults to the eligible free-tier T4), wait 1–3 minutes, then
   **Open Jupyter**.

> The legacy `sdk.cloud.google.com/applicationdefaultauthcode.html` redirect is dead
> (`redirect_uri_mismatch`); only the `localhost:8080` loopback redirect works with this
> client. That's why the flow is copy-paste.

## Using the notebook

- Everything proxied is the **standard Jupyter API**: sessions, kernel WebSocket channels
  (`/api/kernels/{id}/channels`), and the Contents API for files (<50 MB per request).
- Create a notebook, pick the default kernel, run `import torch; torch.cuda.is_available()`.
- Files upload/download through the Jupyter UI as usual.
- **Long jobs:** checkpoint to Drive or GCS. Free-tier runtimes are reclaimed after ~12 hours
  and when idle — the gateway surfaces this as a "RECLAIMED" one-click reprovision, but files
  on the reclaimed runtime are gone.

## Configuration (Railway)

| Variable | Meaning |
|---|---|
| `GOOGLE_CREDENTIALS_JSON` (optional) | BYO refresh-token JSON; if unset, use the /connect page |
| `GATEWAY_PASSWORD` | gateway login password (auto-generated per deploy by the template) |
| `TZ` | optional timezone |

Volume `/data` stores the Google credential JSON and the session signing key, so logins and
the credential survive redeploys.

## Is this allowed?

The gateway uses the **official Colab API** to orchestrate runtimes on **your own account**
with **your own credential** — the sanctioned integration path (Google ships colab-vscode and
a hosted MCP endpoint for exactly this). There is **no synthetic keep-alive** anywhere in
this template: idle runtimes are reclaimed by Google by design, and the gateway never fakes
activity. Consumer Colab ToS and the compute-unit model apply to the linked account.

## Local development

```bash
docker build -t colab-jupyter .
docker run -p 8000:8000 -e GATEWAY_PASSWORD=dev -e GOOGLE_CREDENTIALS_JSON='{"refresh_token":"...","client_id":"...","client_secret":"..."}' colab-jupyter
```

## Architecture

```
browser ──(password gate)──> Railway gateway (FastAPI)
                               ├── REST proxy  /{path} & /jupyter/{path} → {connectionInfo.url}/{path}
                               │     + X-Colab-Runtime-Proxy-Token injection
                               ├── WS proxy    /api/kernels/{id}/channels (upgrade pass-through,
                               │     same header injection, rotation fallback: reconnect → kernel restart)
                               ├── token refresher task (GetRuntime at expiry − 5 min; 404 → RECLAIMED)
                               └── lifecycle UI (Create/Delete/ListRuntimeSpecs-eligible picker)
                                        │
                                        └──> Colab runtime (Google infra): Jupyter server + GPU
```

Licensed under the MIT License. Not affiliated with or endorsed by Google. Colab is a
trademark of Google LLC.
