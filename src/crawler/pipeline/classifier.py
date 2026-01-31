from crawler.pipeline.types import OperatorInfo, Profile, ParseResult, ClassifyResult
from openai import OpenAI
from dotenv import load_dotenv
import json


load_dotenv()
client = OpenAI()

_PROMPT_CONTEXT = """
You are crawling a webpage that is presumably about an experience. You will respond with JSON where each field indicates the cooresponding characteristic about the experience operator. Complete each field in order, following the instructions in the cooresponding comment.

Respond with a parsable JSON only (no markdowns, no comments) with this exact type:
{
  ok: boolean; // is the webpage valid and contains at least some information (not an error/shell page)?
  is_experience: boolean; // is the webpage broadly about some experience?
  belongs_to_specified_operator: boolean; // is the webpage about or related to the specified operator? It most likely is. Enter true unless you are very confident it is false.
  // If any of the first three fields are false, fail fast and return null for the rest.
  
  classification:
    | null
    | { operator_type: "Activity"; business_type: "Air-based adventure, activity, or rentals" | "Cultural activity, experience or classes" | "Land-based adventure, activity, or rentals" | "Water-based adventure, activity, or rentals" | "Wellness" }
    | { operator_type: "Attraction"; business_type: "Amusement & Theme Parks" | "Cultural Sites & Landmarks" | "Museums & Galleries" | "Natural Attraction" | "Observation Decks & Towers" | "Zoos & Aquariums" }
    | { operator_type: "Event"; business_type: "Festivals" | "Performing arts" | "Sporting event" }
    | { operator_type: "Tour"; business_type: "Active / adventure" | "Boat Tours" | "Cultural & Specialty Tours" | "Food & Drink" | "Multi-day Tours" | "Sightseeing" | "Tour of a specific attraction" }
    | { operator_type: "Transportation"; business_type: "Transportation" };
    // it is imperative to select the most accurate and specific operator_type and business_type based on the dominating feature of the experience. For example, the dominating feature of a bike sightseeing tour is likely biking (Active / Adventure), but the dominating feature of a walking sightseeing tour is sightseeing.

  is_commercial_operator: boolean | null; // does the operator provide a commercial experience (do they likely offer some experience-related product such as tickets, tours, rentals, etc)?
  booking_method: "Online Booking" | "Form Submission" | "Contact Info" | "Cannot Infer" | null; // If you are not confident, put "Cannot Infer" and put one hyperlink (in the follow_booking field) to goto to a page that will contain the booking method.
  operating_scope: "local" | "multi_regional" | "international" | null; // Does the operator offer experiences in one destination (local), more than one destination within a country (multi_regional), or more than one country (international)?
  follow_contact: string | null; // If the website is owned by the specified operator, choose one hyperlink to follow to the website's contact page. If none, enter the current URL if it contains contact info.
  follow_booking: string | null;
}

"""

_BOOKING_PROMPT_CONTEXT = """
You are crawling a tour operator's website. Your task is to determine what booking method they use, whether they offer online booking, or just a form, or just contact information. Make your best educated guess.

Respond with a parsable JSON only (no markdowns, no comments) with this exact type:
{
    booking_method: "Online Booking" | "Form Submission" | "Contact Info" | "Cannot Infer"; // if multiple options are applicable, choose the firstmost option
}

"""

_CONTACTS_PROMPT_CONTEXT = """
You are crawling a tour operator’s contacts page. Return a list of contact profiles.

If no contact information exists, return an empty list.
Only create a profile if it contains at least one contact method.
No contact method may appear in more than one profile.
Each email, phone, or WhatsApp field must contain exactly one value. If multiple emails, phones, or WhatsApp numbers are present, create separate profiles.
If more individuals are listed than contact methods, associate each contact with the most relevant individual.
Do not assume a phone number is WhatsApp unless the website states it.

Respond with a parsable JSON only (no markdowns, no comments) with this exact type:
{
  profiles: {
    profile_type: "Company" | "Individual"; // assume "Company" unless a person's name is specified
    role: "Owner" | "Manager" | "Guide" | "Booking Agent" | "Support" | "Unknown" | null; // null if profile_type is "Company"
    profile_name: string | null; // null if profile_type is "Company." must be a person's name, otherwise null
    email: string | null;
    phone: string | null;
    whatsapp: string | null;
  }[];
}

"""


def _validate_llm_output(res: dict) -> bool:
    if not isinstance(res, dict):
        return False

    REQUIRED_KEYS = {
        "ok",
        "is_experience",
        "belongs_to_specified_operator",
        "classification",
        "is_commercial_operator",
        "booking_method",
        "operating_scope",
        "follow_contact",
        "follow_booking",
    }
    if set(res.keys()) != REQUIRED_KEYS:
        return False

    def _is_bool(v) -> bool:
        return isinstance(v, bool)

    def _nonempty_str(v) -> bool:
        return isinstance(v, str) and bool(v.strip())

    def _nonempty_str_or_none(v) -> bool:
        return v is None or _nonempty_str(v)

    # --- base booleans ---
    if not _is_bool(res["ok"]):
        return False
    if not _is_bool(res["is_experience"]):
        return False
    if not _is_bool(res["belongs_to_specified_operator"]):
        return False
    ico = res["is_commercial_operator"]
    if ico is not None and not isinstance(ico, bool):
        return False


    # --- classification ---
    CLASSIFICATION_BY_OPERATOR: dict[str, set[str]] = {
        "Activity": {
            "Air-based adventure, activity, or rentals",
            "Cultural activity, experience or classes",
            "Land-based adventure, activity, or rentals",
            "Water-based adventure, activity, or rentals",
            "Wellness",
        },
        "Attraction": {
            "Amusement & Theme Parks",
            "Cultural Sites & Landmarks",
            "Museums & Galleries",
            "Natural Attraction",
            "Observation Decks & Towers",
            "Zoos & Aquariums",
        },
        "Event": {
            "Festivals",
            "Performing arts",
            "Sporting event",
        },
        "Tour": {
            "Active / adventure",
            "Boat Tours",
            "Cultural & Specialty Tours",
            "Food & Drink",
            "Multi-day Tours",
            "Sightseeing",
            "Tour of a specific attraction",
        },
        "Transportation": {"Transportation"},
    }

    classification = res["classification"]
    if classification is not None:
        if not isinstance(classification, dict):
            return False
        if set(classification.keys()) != {"operator_type", "business_type"}:
            return False

        ot = classification.get("operator_type")
        bt = classification.get("business_type")
        if not isinstance(ot, str) or not isinstance(bt, str):
            return False

        allowed_bts = CLASSIFICATION_BY_OPERATOR.get(ot)
        if allowed_bts is None or bt not in allowed_bts:
            return False

    # --- fail-fast rule: if any of the first three are false, rest must be null ---
    if (
        (res["ok"] is False)
        or (res["is_experience"] is False)
        or (res["belongs_to_specified_operator"] is False)
    ):
        if classification is not None:
            return False
        if res["booking_method"] is not None:
            return False
        if res["operating_scope"] is not None:
            return False
        if res["follow_contact"] is not None:
            return False
        if res["follow_booking"] is not None:
            return False
        # is_commercial_operator is still required (and already validated as bool)
        return True

    # --- booking_method ---
    BOOKING = {"Online Booking", "Form Submission", "Contact Info", "Cannot Infer"}
    bm = res["booking_method"]
    if bm is not None and bm not in BOOKING:
        return False

    # If bm == "Cannot Infer", follow_booking must be a non-empty string
    if bm == "Cannot Infer" and not _nonempty_str(res["follow_booking"]):
        return False

    # --- operating_scope ---
    OPERATING_SCOPE = {"local", "multi_regional", "international"}
    scope = res["operating_scope"]
    if scope is not None and scope not in OPERATING_SCOPE:
        return False

    # --- follow links ---
    if not _nonempty_str_or_none(res["follow_contact"]):
        return False
    if not _nonempty_str_or_none(res["follow_booking"]):
        return False

    return True


def _validate_llm_booking_output(res: dict) -> bool:
    return (
        isinstance(res, dict)
        and set(res) == {"booking_method"}
        and res["booking_method"]
        in {
            "Online Booking",
            "Form Submission",
            "Contact Info",
            "Cannot Infer",
        }
    )


def _validate_llm_contacts_output(res: dict) -> bool:
    if not isinstance(res, dict) or set(res) != {"profiles"}:
        return False

    profiles = res["profiles"]
    if not isinstance(profiles, list):
        return False

    allowed_profile_types = {"Company", "Individual"}
    allowed_roles = {
        "Owner",
        "Manager",
        "Guide",
        "Booking Agent",
        "Support",
        "Unknown",
    }

    for p in profiles:
        if not isinstance(p, dict):
            return False

        if set(p) != {
            "profile_type",
            "role",
            "profile_name",
            "email",
            "phone",
            "whatsapp",
        }:
            return False

        if p["profile_type"] not in allowed_profile_types:
            return False

        if p["profile_type"] == "Company":
            if p["role"] is not None:
                return False
        else:
            if p["role"] not in allowed_roles and p["role"] is not None:
                return False

        for k in ("profile_name", "email", "phone", "whatsapp"):
            if p[k] is not None and not isinstance(p[k], str):
                return False

    return True


def classify(parsed: ParseResult, operator: OperatorInfo) -> ClassifyResult:
    prompt = (
        _PROMPT_CONTEXT
        + "Specified operator: "
        + operator.name
        + "\nSpecified operator location: "
        + (operator.city + (", " if operator.city else "") + operator.country)
        + "\n\nYou are crawling "
        + operator.url
        + "\n\nHyperlink key:\n"
        + parsed.hyperlink_key_text
        + "\n\nParsed webpage HTML:\n"
        + parsed.parsed_text
    )

    # openai request
    try:
        res = client.responses.create(
            model="gpt-5-mini",
            service_tier="flex",
            input=prompt,
            # text={"verbosity": "low"},
            # reasoning={"effort": "low"},
            prompt_cache_key="AOIOUSJD98231u89hKAJSHf1982u3JKAHSDAKSHJD1982zxkhfkl",
        )
    except Exception:
        return ClassifyResult(ok=False, message="ChatGPT API error")

    # validate response
    classified = ClassifyResult(
        ok=False,
        input_tokens=res.usage.input_tokens
        - res.usage.input_tokens_details.cached_tokens,
        cached_input_tokens=res.usage.input_tokens_details.cached_tokens,
        output_tokens=res.usage.output_tokens,
    )

    try:
        parsed_output = json.loads(res.output_text)
    except json.JSONDecodeError:
        classified.ok = False
        classified.message = "ChatGPT provided invalid JSON"
        return classified
    
    print(json.dumps(parsed_output, indent=2))

    if not _validate_llm_output(parsed_output):
        classified.ok = False
        classified.message = "ChatGPT provided invalid JSON schema"
        return classified

    if not parsed_output["ok"]:
        classified.ok = False
        classified.message = "LLM identified webpage error"
        return classified
    elif not parsed_output["belongs_to_specified_operator"]:
        classified.ok = False
        classified.message = "Webpage is not about the operator"
        return classified
    elif not parsed_output["is_experience"]:
        classified.ok = False
        classified.message = "Webpage is not an experience"
        return classified

    classified.ok = True
    classified.operator_type = parsed_output["classification"]["operator_type"]
    classified.business_type = parsed_output["classification"]["business_type"]
    classified.is_commercial = parsed_output["is_commercial_operator"]
    classified.booking_method = parsed_output["booking_method"]
    classified.operating_scope = parsed_output["operating_scope"]
    classified.follow_booking = parsed_output["follow_booking"]
    classified.follow_contact = parsed_output["follow_contact"]

    return classified


def classify_booking(parsed: ParseResult, operator: OperatorInfo) -> ClassifyResult:
    prompt = (
        _BOOKING_PROMPT_CONTEXT
        + "Specified operator: "
        + operator.name
        + "\nSpecified operator location: "
        + (operator.city + (", " if operator.city else "") + operator.country)
        + "\n\nYou are crawling "
        + operator.url
        + "\n\nHyperlink key:\n"
        + parsed.hyperlink_key_text
        + "\n\nWebsite HTML:\n"
        + parsed.parsed_text
    )

    # openai request
    try:
        res = client.responses.create(
            model="gpt-5-mini",
            service_tier="flex",
            input=prompt,
            # text={"verbosity": "low"},
            # reasoning={"effort": "low"},
            # prompt_cache_key="AOIOUSJD98231u89hKAJSHf1982u3JKAHSDAKSHJD1982zxkhfkl",
        )
    except Exception:
        return ClassifyResult(ok=False, message="ChatGPT API error")

    # validate response
    classified = ClassifyResult(
        ok=False,
        input_tokens=res.usage.input_tokens
        - res.usage.input_tokens_details.cached_tokens,
        cached_input_tokens=res.usage.input_tokens_details.cached_tokens,
        output_tokens=res.usage.output_tokens,
    )

    try:
        parsed_output = json.loads(res.output_text)
    except json.JSONDecodeError:
        classified.ok = False
        classified.message = "ChatGPT provided invalid JSON"
        return classified

    if not _validate_llm_booking_output(parsed_output):
        classified.ok = False
        classified.message = "ChatGPT provided invalid JSON schema"
        return classified

    classified.ok = True
    classified.booking_method = parsed_output["booking_method"]

    return classified


def classify_contacts(parsed: ParseResult, operator: OperatorInfo) -> ClassifyResult:
    prompt = (
        _CONTACTS_PROMPT_CONTEXT
        + "Specified operator: "
        + operator.name
        + "\nSpecified operator location: "
        + (operator.city + (", " if operator.city else "") + operator.country)
        + "\n\nYou are crawling "
        + operator.url
        + "\n\nHyperlink key:\n"
        + parsed.hyperlink_key_text
        + "\n\nWebsite HTML:\n"
        + parsed.parsed_text
    )

    # openai request
    try:
        res = client.responses.create(
            model="gpt-5-mini",
            service_tier="flex",
            input=prompt,
            # text={"verbosity": "low"},
            # reasoning={"effort": "low"},
            # prompt_cache_key="AOIOUSJD98231u89hKAJSHf1982u3JKAHSDAKSHJD1982zxkhfkl",
        )
    except Exception:
        return ClassifyResult(ok=False, message="ChatGPT API error")

    # validate response
    classified = ClassifyResult(
        ok=False,
        input_tokens=res.usage.input_tokens
        - res.usage.input_tokens_details.cached_tokens,
        cached_input_tokens=res.usage.input_tokens_details.cached_tokens,
        output_tokens=res.usage.output_tokens,
    )

    try:
        parsed_output = json.loads(res.output_text)
    except json.JSONDecodeError:
        classified.ok = False
        classified.message = "ChatGPT provided invalid JSON"
        return classified

    if not _validate_llm_contacts_output(parsed_output):
        classified.ok = False
        classified.message = "ChatGPT provided invalid JSON schema"
        return classified

    profiles: list[Profile] = []

    # convert parsed output to list of profiles
    for p in parsed_output["profiles"]:
        new_profile = Profile(
            operator_name=operator.name,
            operator_country=operator.country,
            operator_city=operator.city,
            profile_type=p["profile_type"],
            role=p["role"],
            profile_name=p["profile_name"],
            email=p["email"],
            phone=p["phone"],
            whatsapp=p["whatsapp"],
        )

        profiles.append(new_profile)

    classified.ok = True
    classified.profiles = profiles

    return classified
