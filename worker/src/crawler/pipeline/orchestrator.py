from pathlib import Path

from crawler.pipeline.fetcher import fetch
from crawler.pipeline.parser import parse
from crawler.pipeline.classifier import classify
from crawler.pipeline.searcher import search
from crawler.pipeline.prompts.expected_shapes import (
    ExpectedBooking,
    ExpectedLanding,
    ExpectedProfiles,
)
from crawler.pipeline.types import (
    OperatorInfo,
    FetchResult,
    ParseResult,
    ClassifyResult,
    SearchResult,
)
from schema import Schema


_PROMPT_DIR = Path(__file__).with_name("prompts")
_LANDING_PROMPT = (_PROMPT_DIR / "landing.txt").read_text(encoding="utf-8")
_BOOKING_PROMPT = (_PROMPT_DIR / "booking.txt").read_text(encoding="utf-8")
_PROFILES_PROMPT = (_PROMPT_DIR / "profiles.txt").read_text(encoding="utf-8")


def _classify_pipeline(url: str, operator: OperatorInfo, prompt: str, model_output_shape: Schema) -> tuple[ClassifyResult, str | None]:
    fetched: FetchResult = fetch(url)
    if not fetched.ok:
        return ClassifyResult(ok=False, message=fetched.message, final_url=fetched.url), "fetch"

    parsed: ParseResult = parse(fetched)
    if not parsed.ok:
        return ClassifyResult(ok=False, message=parsed.message, final_url=fetched.url), "parse"
    
    classification: ClassifyResult = classify(parsed, operator, prompt, model_output_shape)
    if not classification.ok:
        return ClassifyResult(ok=False, message=classification.message, final_url=fetched.url), "classification"
    
    classification.final_url = fetched.url
    return classification, None


def run(operator: OperatorInfo) -> ClassifyResult:
    classification, landing_err = _classify_pipeline(operator.url, operator, _LANDING_PROMPT, ExpectedLanding)

    # if the pipeline fails, retry once with a serped URL
    if landing_err and not (landing_err == "classification" and classification.message != "Webpage is not about the operator"):
        searched: SearchResult = search(operator)
        classification.searched = True

        if searched.ok:
            searched_classification, searched_err = _classify_pipeline(searched.url, operator, _LANDING_PROMPT, ExpectedLanding)
            classification.merge(searched_classification)

            if searched_err:
                return classification
        else:
            classification.merge(ClassifyResult(ok=False, message=searched.message))
            return classification
            
    if classification.follow_booking:
        booking_classification, booking_err = _classify_pipeline(classification.follow_booking, operator, _BOOKING_PROMPT, ExpectedBooking)
        classification.merge(booking_classification)

    if classification.follow_contact:
        profiles_classification, profiles_err = _classify_pipeline(classification.follow_contact, operator, _PROFILES_PROMPT, ExpectedProfiles)
        classification.merge(profiles_classification)

    return classification