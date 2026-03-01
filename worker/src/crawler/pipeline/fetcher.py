import httpx
import random
from urllib.parse import urlparse
import logging
from crawler.pipeline.types import FetchResult
import time

log = logging.getLogger(__name__)


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
_SOCIAL_ORIGINS = {
    "facebook.com",
    "instagram.com",
    "tripadvisor.com",
    "booking.com",
    "expedia.com",
    "viator.com",
    "getyourguide.com",
    "airbnb.com",
    "klook.com",
    "yelp.com",
    "google.com",
    "business.google.com",
    "toursbylocals.com",
    "peek.com",
    "fareharbor.com",
    "tiqets.com",
    "withlocals.com",
    "tripaneer.com",
    "eventbrite.com",
    "meetup.com",
    "showaround.com",
    "whatsapp.com",
    "messenger.com",
    "telegram.org",
    "wechat.com",
    "line.me",
    "tiktok.com",
    "x.com",
    "linkedin.com",
    "reddit.com",
    "youtube.com",
    "pinterest.com",
}


def _is_social_url(url: str) -> bool:
    for u in _SOCIAL_ORIGINS:
        if u in url:
            return True

    return False


def _is_valid_url(url: str) -> bool:
    p = urlparse(url)
    return p.scheme in ("http", "https") and bool(p.netloc) and "." in p.netloc


def fetch(url: str) -> FetchResult:
    if not url or not _is_valid_url(url):
        return FetchResult(ok=False, message="Invalid URL")
    
    if _is_social_url(url):
        return FetchResult(ok=False, message="Social URL")
    
    attempts = 3
    for attempt in range(attempts):
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
                timeout=20,
            )
            if r.status_code >= 500:
                r.raise_for_status()
            break
        except (httpx.TimeoutException, httpx.NetworkError, httpx.HTTPStatusError):
            if attempt < attempts - 1:
                print("Retrying fetch...")
                time.sleep(2 ** attempt)
            else:
                return FetchResult(ok=False, message="Operator request error")
        except httpx.HTTPError:
            return FetchResult(ok=False, message="Operator request error")
    
    if not r.is_success:
        return FetchResult(ok=False, message=f"Request error: {r.status_code}")

    content_type = (r.headers.get("content-type") or "").lower()
    if "html" not in content_type:
        return FetchResult(ok=False, message="Non-HTML response")


    return FetchResult(ok=True, url=str(r.url), text=r.text)
