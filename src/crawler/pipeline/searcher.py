import httpx
import json
import os
from crawler.pipeline.types import OperatorInfo, SearchResult
from rapidfuzz import fuzz
from unidecode import unidecode
import tldextract
import re
from urllib.parse import quote_plus


def _norm(s: str) -> str:
    return re.sub(r"\s+", "", unidecode(s).lower())


def _score(operator: str, title: str, link: str, search_rank: int = 0) -> float:
    ext = tldextract.extract(link)
    domain = ext.domain

    operator_n = _norm(operator)
    title_n = _norm(title)
    domain_n = _norm(domain)

    s_title = fuzz.token_set_ratio(operator_n, title_n)
    s_domain = fuzz.partial_ratio(operator_n, domain_n)

    return 0.55 * s_domain + 0.35 * s_title + (10 - search_rank)


def search(operator: OperatorInfo) -> SearchResult:
    headers = {
        "Authorization": f"Bearer {os.environ['BRIGHTDATA_SERP_API_KEY']}",
        "Content-Type": "application/json",
    }
    data = {
        "zone": "arival_crawler",
        "url": f"https://www.google.com/search?q={quote_plus(operator.name)}",
        "format": "raw",
    }

    try:
        response = httpx.post(
            "https://api.brightdata.com/request", json=data, headers=headers
        )
    except httpx.RequestError:
        return SearchResult(ok=False, message="SERP Request Error")

    try:
        results = json.loads(response.text)
    except json.JSONDecodeError:
        return SearchResult(ok=False, message="SERP provided invalid JSON")

    best: str = ""
    best_score: float = -99999

    try:
        candidates = results["organic"]

        for i, c in enumerate(candidates):
            link = c.get("link", "")
            title = c.get("title", "")
            if not link or not title:
                continue

            score = _score(operator=operator.name, title=title, link=link, search_rank=i)
            if score > best_score:
                best_score = score
                best = link

    except Exception:
        return SearchResult(ok=False, message="SERP provided invalid JSON schema")

    return SearchResult(ok=True, url=best)