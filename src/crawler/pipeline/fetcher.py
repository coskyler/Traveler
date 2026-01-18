import httpx
import random
from pydantic import BaseModel
import logging
log = logging.getLogger(__name__)

class FetchResult(BaseModel):
    ok: bool
    text: str | None = None
    message: str | None = None

_USER_AGENTS = [
    # chrome / windows
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36",

    # chrome / mac
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36",

    # firefox / windows
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:122.0) "
    "Gecko/20100101 Firefox/122.0",
]

def fetch(url):
    try:
        r = httpx.get(
            url,
            headers={
                "User-Agent": random.choice(_USER_AGENTS),
                "Accept": (
                    "text/html,application/xhtml+xml,application/xml;"
                    "q=0.9,image/avif,image/webp,*/*;q=0.8"
                ),
                "Accept-Language": "en-US,en;q=0.9",
                "Accept-Encoding": "gzip, deflate, br",
                "Connection": "keep-alive",
                "Upgrade-Insecure-Requests": "1",
            },
            follow_redirects=True,
            timeout=15,
        )

        return FetchResult(
            ok=True,
            text = r.text
        )
    except httpx.RequestError as e:
        log.error("Fetch failed: %s", e)
        return FetchResult(
            ok=False,
            message="Request error"
        )