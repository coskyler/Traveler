from crawler.pipeline.fetcher import fetch
from crawler.pipeline.parser import parse
from crawler.pipeline.classifier import classify
from crawler.pipeline.types import OperatorInfo, FetchResult, ParseResult, ClassifyResult
import sys

operator = OperatorInfo(
    name="MPC Yacht Charter",
    country="Spain",
    city="Los Gigantes",
    url="https://www.mpcyachtcharter.com/en/"
)

fetched: FetchResult = fetch(operator.url)

print(
    f"[FETCH]\nok={fetched.ok}\n"
    f"url={fetched.url or '-'}\n"
    f"message={fetched.message or '-'}\n"
)

if not fetched.ok:
    sys.exit(1)
operator.url = fetched.url

parsed: ParseResult = parse(fetched)

print(
    f"[PARSE]\nok={parsed.ok}\n"
    f"url={parsed.url}\n"
    f"hyperlink_key_text={parsed.hyperlink_key_text or '-'}\n"
    f"parsed_text={parsed.parsed_text or '-'}\n"
    f"emails={parsed.emails or '-'}\n"
    f"phones={parsed.phones or '-'}\n"
    f"socials={parsed.socials or '-'}\n"
    f"message={parsed.message or '-'}\n"
)

classified: ClassifyResult = classify(parsed, operator)

print(
    f"[CLASSIFY]\nok={classified.ok}\n"
    f"category={classified.category or '-'}\n"
    f"sub_category={classified.sub_category or '-'}\n"
    f"booking_method={classified.booking_method or '-'}\n"
    f"message={classified.message or '-'}\n"
)
