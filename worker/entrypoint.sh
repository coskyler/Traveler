#!/usr/bin/env sh
set -eu

: "${DATABASE_URL:?DATABASE_URL is required}"
: "${POSTGRES_TIMEOUT:=15}"

echo "Waiting for Postgres (${POSTGRES_TIMEOUT}s)..."

python - <<'PY'
import os, time, socket
from urllib.parse import urlparse

url = os.environ["DATABASE_URL"]
timeout = int(os.environ["POSTGRES_TIMEOUT"])

u = urlparse(url)
host = u.hostname or "localhost"
port = u.port or 5432

deadline = time.time() + timeout
while True:
    try:
        with socket.create_connection((host, port), timeout=2):
            print(f"Postgres is reachable at {host}:{port}")
            break
    except OSError:
        if time.time() >= deadline:
            raise SystemExit(f"Timed out waiting for Postgres at {host}:{port}")
        time.sleep(1)
PY

exec "$@"