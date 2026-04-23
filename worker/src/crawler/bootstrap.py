# src/crawler/bootstrap.py
import os
import time
import socket
from urllib.parse import urlparse

def wait_for_postgres() -> None:
    url = os.environ["DATABASE_URL"]
    timeout = 15

    u = urlparse(url)
    host = u.hostname or "localhost"
    port = u.port or 5432

    deadline = time.time() + timeout

    while True:
        try:
            with socket.create_connection((host, port), timeout=2):
                print(f"Postgres is reachable at {host}:{port}", flush=True)
                return
        except OSError:
            if time.time() >= deadline:
                raise SystemExit(f"Timed out waiting for Postgres at {host}:{port}")
            time.sleep(1)


def main() -> None:
    wait_for_postgres()

    os.execvp(
        "python",
        ["python", "-u", "-m", "crawler.worker"],
    )


if __name__ == "__main__":
    main()