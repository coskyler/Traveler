import os
import atexit
import queue
import threading
import time
from concurrent.futures import Future
from urllib.parse import urlparse

import httpx
from gologin import GoLogin
from playwright.sync_api import Error as PlaywrightError
from playwright.sync_api import TimeoutError as PlaywrightTimeoutError
from playwright.sync_api import sync_playwright

from crawler.pipeline.types import FetchResult

_playwright = None
_gologin = None
_browser = None
_context = None
_fetch_queue = queue.Queue()
_fetch_thread = None
_fetch_thread_lock = threading.Lock()
_STOP = object()


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


def _get_context():
    global _playwright, _gologin, _browser, _context

    if _browser and _browser.is_connected() and _context:
        return _context

    token = os.environ.get("GOLOGIN_TOKEN") or os.environ["GL_API_TOKEN"]
    profile_id = os.environ.get("GOLOGIN_PROFILE_ID") or os.environ["GL_PROFILE_ID"]
    _gologin = GoLogin(
        {
            "token": token,
            "profile_id": profile_id,
            "extra_params": ["--headless"],
        }
    )
    debugger_address = _gologin.start()
    _playwright = sync_playwright().start()
    _browser = _playwright.chromium.connect_over_cdp(f"http://{debugger_address}")
    _context = _browser.contexts[0]
    return _context


def _fetch_in_browser(url: str) -> FetchResult:
    attempts = 3
    status_code = None
    content_type = ""
    final_url = None
    text = None

    for attempt in range(attempts):
        page = None
        try:
            context = _get_context()
            page = context.new_page()
            response = page.goto(url, wait_until="domcontentloaded", timeout=20000)
            if response is None:
                return FetchResult(ok=False, message="Operator request error")

            status_code = response.status
            final_url = page.url
            content_type = (response.headers.get("content-type") or "").lower()
            if status_code >= 500:
                raise PlaywrightError(f"Server error: {status_code}")

            text = page.content()
            break
        except (PlaywrightTimeoutError, PlaywrightError):
            if attempt < attempts - 1:
                print("Retrying fetch...")
                time.sleep(2 ** attempt)
            else:
                return FetchResult(ok=False, message="Operator request error")
        finally:
            if page:
                try:
                    page.close()
                except PlaywrightError:
                    pass

    if status_code is None:
        return FetchResult(ok=False, message="Operator request error")

    if not 200 <= status_code < 300:
        return FetchResult(ok=False, message=f"Request error: {status_code}")

    if "html" not in content_type and "<html" not in text.lower():
        return FetchResult(ok=False, message="Non-HTML response")

    return FetchResult(ok=True, url=final_url, text=text)


def _close_browser_state():
    global _playwright, _gologin, _browser, _context

    try:
        if _browser and _browser.is_connected():
            _browser.close()
    except PlaywrightError:
        pass
    finally:
        _browser = None
        _context = None

    if _playwright:
        try:
            _playwright.stop()
        except PlaywrightError:
            pass
        finally:
            _playwright = None

    if _gologin:
        try:
            _gologin.stop()
        except Exception:
            pass
        finally:
            _gologin = None


def _browser_loop():
    try:
        while True:
            item = _fetch_queue.get()
            try:
                if item is _STOP:
                    return

                url, future = item
                try:
                    future.set_result(_fetch_in_browser(url))
                except BaseException as exc:
                    future.set_exception(exc)
            finally:
                _fetch_queue.task_done()
    finally:
        _close_browser_state()


def _ensure_browser_thread():
    global _fetch_thread

    with _fetch_thread_lock:
        if _fetch_thread and _fetch_thread.is_alive():
            return

        _fetch_thread = threading.Thread(
            target=_browser_loop,
            name="fetch-browser",
            daemon=True,
        )
        _fetch_thread.start()


def shutdown_browser():
    if _fetch_thread and _fetch_thread.is_alive():
        _fetch_queue.put(_STOP)
    else:
        _close_browser_state()


atexit.register(shutdown_browser)


def fetch(url: str) -> FetchResult:
    if not url or not _is_valid_url(url):
        return FetchResult(ok=False, message="Invalid URL")

    if _is_social_url(url):
        return FetchResult(ok=False, message="Social URL")

    _ensure_browser_thread()
    future = Future()
    _fetch_queue.put((url, future))
    return future.result()

def stealth_fetch(url: str) -> FetchResult:
    if not url or not _is_valid_url(url):
        return FetchResult(ok=False, message="Invalid URL")

    if _is_social_url(url):
        return FetchResult(ok=False, message="Social URL")

    headers = {
        "Authorization": f"Bearer {os.environ['BRIGHTDATA_FETCH_API_KEY']}",
        "Content-Type": "application/json",
    }
    data = {
        "zone": "webswarm_fetch",
        "url": url,
        "format": "raw",
    }

    response = None
    for attempt in range(2):
        try:
            response = httpx.post(
                "https://api.brightdata.com/request",
                json=data,
                headers=headers,
                timeout=30,
            )
            break
        except httpx.RequestError:
            if attempt == 0:
                time.sleep(2)

    if response is None:
        return FetchResult(ok=False, message="Operator request error")

    if not response.is_success:
        return FetchResult(ok=False, message=f"Request error: {response.status_code}")

    content_type = (response.headers.get("content-type") or "").lower()
    text = response.text
    if "html" not in content_type and "<html" not in text.lower():
        return FetchResult(ok=False, message="Non-HTML response")

    return FetchResult(ok=True, url=url, text=text)
