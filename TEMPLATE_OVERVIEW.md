# Jupyter Gateway for Colab

A familiar Jupyter UI backed by a Google Colab GPU runtime, proxied through your own Railway
URL with a password gate. One-click deploy, connect your Google account with a loopback
copy-paste flow, provision a GPU runtime through Google's official Colab API, and open
JupyterLab — your notebooks execute on Google's infrastructure and keep running when you
close the browser or redeploy the gateway.

[![Deploy on Railway](https://railway.com/button.svg)](https://railway.com/deploy/colab-jupyter)

**The disconnect-proof pitch:** the kernel, the notebook files and the filesystem all live on
the Colab runtime (Google's servers), not in your browser tab and not in this container.
Close the laptop mid-training — the cell keeps running. Redeploy the gateway — the runtime is
untouched and the UI re-attaches. Any browser can reconnect to the same runtime.

What the template gives you:

- Stock JupyterLab served by the Colab runtime, reachable at your own URL with gateway auth
  in front (REST and kernel WebSockets both proxied with the runtime's short-lived
  `X-Colab-Runtime-Proxy-Token`, auto-refreshed 5 minutes before expiry).
- Free-tier T4 GPU eligibility discovery from the official API — the accelerator picker lists
  only the specs your account is actually eligible for.
- Runtime lifecycle UI: provision, force token refresh, delete with confirmation, event log,
  and a one-click reprovision when Google reclaims a runtime (free-tier idle deletion or the
  12-hour lifetime cap).
- Honest limits: free-tier runtimes are reclaimed after ~12 hours or when idle, so checkpoint
  long jobs to Drive or GCS. The gateway never fakes activity — no synthetic keep-alive.

# Deploy and Host

## About Hosting

One Railway service (Python 3.12 + FastAPI + httpx + websockets + google-auth, built from the
repo Dockerfile, runs non-root) plus one private volume at `/data` that holds your Google
credential JSON and the gateway session key. After deploy, the template asks for exactly one
variable: `GOOGLE_CREDENTIALS_JSON` — your Google refresh-token JSON. If you skip it, the
gateway serves a built-in connect page where you can complete the same OAuth loopback
copy-paste flow interactively (open the consent page, approve the `colaboratory` scope, copy
the `code=4/0A...` value from the localhost address bar, paste it back). `GATEWAY_PASSWORD`
is pre-wired to a per-deploy generated secret — find it on the service's Variables tab, sign
in, click **Provision runtime** (defaults to the free-tier T4 when eligible), wait 1–3
minutes for Google to assign the runtime, then click **Open Jupyter**. Everything you do
afterwards — notebooks, files, kernels, sessions — lives on the Colab runtime server-side.

## Why Deploy

Zero Colab templates existed on Railway or any other PaaS: every existing Jupyter template
gives you a plain CPU container with no GPU, and every GPU-notebook path costs per-hour VPS
money. This template pairs the official Colab API with a reverse proxy so you get a real
free-tier T4 behind your own authenticated URL — and the token plumbing is fully automatic
(hourly proxy-token rotation handled at expiry − 5 minutes with a reconnect fallback, the
same header injection Google's own colab-vscode client uses). Your work survives gateway
redeploys and browser closure because the runtime is server-side, which plain Jupyter
templates cannot offer at any price.

## Common Use Cases

- GPU notebook sessions in any browser: train or fine-tune a small model on the free T4,
  run CUDA/torch checks, profile kernels — with `torch.cuda.is_available()` → True.
- Long-running analyses that outlive your laptop: start a 40-minute cell, close the browser,
  come back to the finished output (execution continues on Google's servers).
- A stable, authenticated notebook URL for a course or a team: the gateway password gates
  access, and the runtime stays assigned to your Google account across redeploys.
- Prototyping with files: upload datasets and notebooks through the Jupyter Contents API
  (up to 50 MB per request, chunkable), download results — then checkpoint the important
  ones to Drive/GCS before the 12-hour free-tier lifetime hits.

## Dependencies for

### Deployment Dependencies

- **A Google account with Colab access** (free tier is fine — T4 eligible; H100/TPU specs
  require a paid plan, and the picker only shows what your account is eligible for). The
  gateway orchestrates runtimes on your own account via the official Colab API
  (`colaboratory.googleapis.com`, scope `colaboratory`) — sanctioned orchestration, no quota
  reselling, no synthetic keep-alive; consumer Colab ToS and compute-unit limits apply.
- **A Google OAuth refresh token** with the `colaboratory` scope: provide it as the
  `GOOGLE_CREDENTIALS_JSON` deploy variable (shape: refresh_token, client_id, client_secret)
  or complete the built-in /connect loopback copy-paste flow after deploy — no custom OAuth
  client registration required (the gateway uses the gcloud-style installed-app client with
  the localhost loopback redirect; the old sdk.cloud.google.com redirect is dead).
- **Railway-provisioned pieces** (automatic): one service from the repo Dockerfile, one
  volume mounted at `/data` (credential + session key persistence), one domain with the
  `/health` healthcheck wired (`GATEWAY_PASSWORD` pre-wired to a per-deploy generated
  secret; `TZ` optional).
- **In-runtime dependencies** are Google's: the Jupyter server, its Python image and the GPU
  driver come with the Colab runtime itself — nothing extra to configure.

MIT licensed. Not affiliated with or endorsed by Google.
