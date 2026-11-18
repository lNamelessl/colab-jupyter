#!/bin/sh
# Runs as root: fix volume ownership (Railway mounts volumes root-owned), then
# drop privileges to the non-root app user before exec'ing the server.
set -e

if [ -d /data ]; then
    chown -R app:app /data || true
fi

exec setpriv --reuid=app --regid=app --init-groups "$@"
