from crawler.pipeline.fetcher import fetch
from crawler.pipeline.parser import parse
from crawler.pipeline.classifier import classify, classify_booking, classify_contacts
from crawler.pipeline.searcher import search
from crawler.pipeline.types import (
    OperatorInfo,
    FetchResult,
    ParseResult,
    ClassifyResult,
    SearchResult,
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
    searched = False

    # fetch and parse the URL
    landing_content: GetResult = _get_content(operator.url)
    if not landing_content.ok:
        # if provided URL fails, attempt to find the operator's website with google SERP
        url_search: SearchResult = search(operator)
        searched = True
        if url_search.ok:
            landing_content: GetResult = _get_content(url_search.url)
            if not landing_content.ok:
                return ClassifyResult(
                    ok=False, message=landing_content.message, searched=searched
                )

        else:
            return ClassifyResult(
                ok=False,
                message=f"{landing_content.message}, {url_search.message}",
                searched=searched,
            )

    # update operator URL to followed URL
    operator.url = landing_content.followed_url

    # classify content with LLM
    classification = classify(landing_content.parsed, operator)
    classification.searched = searched
    if not classification.ok:
        if (
            classification.message == "Website does not belong to specified operator"
            and not searched
        ):
            # if provided URL fails, attempt to find the operator's website with google SERP
            url_search: SearchResult = search(operator)
            searched = True
            if url_search.ok:
                landing_content: GetResult = _get_content(url_search.url)
                if landing_content.ok:
                    operator.url = landing_content.followed_url
                    new_classification = classify(landing_content.parsed, operator)
                    new_classification.searched = searched
                    new_classification.input_tokens += classification.input_tokens
                    new_classification.cached_input_tokens += classification.cached_input_tokens
                    new_classification.output_tokens += classification.output_tokens
                    if not new_classification.ok:
                        new_classification.message=f"{classification.message}, {new_classification.message}"
                        return new_classification
                    classification = new_classification
                    classification.searched = searched
                else:
                    return ClassifyResult(
                        ok=False,
                        message=f"{classification.message}, {landing_content.message}",
                        searched=searched,
                    )
            else:
                return ClassifyResult(
                    ok=False,
                    message=f"{classification.message}, {url_search.message}",
                    searched=searched,
                )
        else:
            return classification

    # follow the booking page
    if classification.follow_booking:
        booking_content: GetResult = _get_content(classification.follow_booking)
        if booking_content.ok:
            booking_classification = classify_booking(booking_content.parsed, operator)
            if booking_classification.ok:
                classification.booking_method = booking_classification.booking_method
            else:
                print(booking_classification.message)

            # update total token usage
            classification.input_tokens += booking_classification.input_tokens
            classification.cached_input_tokens += (
                booking_classification.cached_input_tokens
            )
            classification.output_tokens += booking_classification.output_tokens
        else:
            print(booking_content.message)

    # follow the contacts page
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

            # update total token usage
            classification.input_tokens += contacts_classification.input_tokens
            classification.cached_input_tokens += (
                contacts_classification.cached_input_tokens
            )
            classification.output_tokens += contacts_classification.output_tokens
        else:
            print(contact_content.message)

    return classification
