from crawler.pipeline.fetcher import fetch
from crawler.pipeline.parser import parse
from crawler.pipeline.classifier import classify, classify_booking, classify_contacts
from crawler.pipeline.types import (
    OperatorInfo,
    FetchResult,
    ParseResult,
    ClassifyResult,
)
from pydantic import BaseModel


class GetResult(BaseModel):
    ok: bool
    parsed: ParseResult | None = None
    followed_url: str | None = None
    message: str | None = None


def _get_content(url: str) -> GetResult:
    fetched: FetchResult = fetch(url)
    if not fetched.ok:
        return GetResult(ok=False, message=fetched.message)

    parsed: ParseResult = parse(fetched)
    if not parsed.ok:
        return GetResult(ok=False, message=parsed.message)

    return GetResult(ok=True, parsed=parsed, followed_url=fetched.url)


def run(operator: OperatorInfo) -> ClassifyResult:
    # fetch and parse the URL
    landing_content: GetResult = _get_content(operator.url)
    if not landing_content.ok:
        return ClassifyResult(ok=False, message=landing_content.message)

    # update operator URL to followed URL
    operator.url = landing_content.followed_url

    # classify content with LLM
    classification = classify(landing_content.parsed, operator)
    if not classification.ok:
        return ClassifyResult(ok=False, message=classification.message)

    # navigate website
    if classification.follow_booking:
        booking_content: GetResult = _get_content(classification.follow_booking)
        if booking_content.ok:
            booking_classification = classify_booking(booking_content.parsed, operator)
            if booking_classification.ok:
                classification.booking_method = booking_classification.booking_method
            else:
                print(booking_classification.message)
        else:
            print(booking_content.message)

    if classification.follow_contact:
        if classification.follow_contact == operator.url:
            contact_content = landing_content
        else:
            contact_content: GetResult = _get_content(classification.follow_contact)
        if contact_content.ok:
            contacts_classification = classify_contacts(
                contact_content.parsed, operator
            )
            if contacts_classification.ok:
                classification.profiles = contacts_classification.profiles
            else:
                print(contacts_classification.message)
        else:
            print(contact_content.message)

    return classification
