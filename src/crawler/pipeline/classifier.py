from crawler.pipeline.types import OperatorInfo, Profile, ParseResult, ClassifyResult
from openai import OpenAI
from dotenv import load_dotenv
import json


load_dotenv()
client = OpenAI()

_PROMPT_CONTEXT = """
You are crawling a website that is presumed to be a specific private tour/activity/attraction operator's website. Your task is to confirm that it is a real operator's website and that it belongs to the specified operator. If it is not a private operator (e.g., government, public park, non-profit, misc., etc.), or the website does not belong to the specified operator, immediately respond with the appropriate status.
Otherwise, the status is OK and you will classify the operator. It is imperative that the most accurate business type is selected, followed by the experience type, based on the operator's brand and products. You will then determine the website's booking method (online booking, form submission, or simply contact info), and the operator's scope (local, multiple regions, or multiple countries). If you cannot determine the booking method with high confidence, you will choose 1 hyperlink to follow which likely contains the needed information. You will also choose 1 hyperlink to follow to website's contact page if available; if there is no contact page and contact information is available in the current page, enter the URL of the page you are currently crawling in "follow_contact".

Respond with a parsable JSON only (no markdowns, no comments) with this exact type:
{
    status: "OK" | "Website does not belong to specified operator" | "Not an operator website" | "Insufficient information: website likely requires JavaScript rendering" | `Other error: ${string}`; // if multiple options are applicable, choose the firstmost option
    classification: null | (
    | { operator_type: "Activity"; business_type: "Air-based adventure, activity, or rentals"; experience_type: "Parasailing & Paragliding" | "Skydiving"; }
    | { operator_type: "Activity"; business_type: "Cultural activity, experience or classes"; experience_type: "Arts & Crafts" | "Education / Cultural"; }
    | { operator_type: "Activity"; business_type: "Land-based adventure, activity, or rentals"; experience_type: "Ax Throwing" | "Bike and E-Bike Rentals" | "Bungee Jumping" | "Camel Rides" | "Caving & Climbing" | "Escape Room Games" | "Extreme Sports" | "Fitness Classes" | "Flight Simulator" | "Gear Rentals" | "Hiking" | "Horseback Riding" | "Martial Arts Classes" | "Off-Road & ATV" | "Games & Entertainment Centers" | "Other Outdoor Activities" | "Race Track Car Racing (self-drive)" | "Shooting Ranges" | "Shore Excursions" | "Sports Lessons" | "Swordsmanship Classes" | "Tennis" | "Trams" | "Winter Sports" | "Zipline & Aerial Adventure Parks" | "Zorbing"; }
    | { operator_type: "Activity"; business_type: "Water-based adventure, activity, or rentals"; experience_type: "Boat Rentals" | "Fishing Charters" | "Marinas" | "River Rafting, Kayaking, Canoeing" | "Scuba & Snorkeling" | "Swim with Dolphins" | "Water Sports"; }
    | { operator_type: "Activity"; business_type: "Wellness"; experience_type: "Spas" | "Thermal & Mineral Springs" | "Yoga & Pilates"; }

    | { operator_type: "Attraction"; business_type: "Amusement & Theme Parks"; experience_type: "Adventure Parks" | "Amusement & Theme Parks" | "Amusement Parks" | "Ghost Towns" | "Water Parks"; }
    | { operator_type: "Attraction"; business_type: "Cultural Sites & Landmarks"; experience_type: "Architectural Landmark" | "Battlefields" | "Lighthouses" | "Monuments & Statues" | "Other sites & landmarks"; }
    | { operator_type: "Attraction"; business_type: "Museums & Galleries"; experience_type: "Art Galleries" | "Art Museums" | "Children's Museums" | "History & Culture Museums" | "Natural History Museums" | "Science Museums" | "Specialty Museums"; }
    | { operator_type: "Attraction"; business_type: "Natural Attraction"; experience_type: "Gardens" | "Caverns & Caves" | "Hot Springs & Geysers" | "National & State Parks" | "Other Natural Attractions" | "Waterfalls"; }
    | { operator_type: "Attraction"; business_type: "Observation Decks & Towers"; experience_type: "Observation Decks & Towers"; }
    | { operator_type: "Attraction"; business_type: "Zoos & Aquariums"; experience_type: "Aquariums" | "Zoo & Aquariums" | "Zoos"; }

    | { operator_type: "Event"; business_type: "Festivals"; experience_type: "Cultural Events" | "Food & Drink Festivals" | "Music Festivals"; }
    | { operator_type: "Event"; business_type: "Performing arts"; experience_type: "Concerts & Shows" | "Cultural Events" | "Dinner Theaters" | "Experience nights" | "Theater, play or musical"; }
    | { operator_type: "Event"; business_type: "Sporting event"; experience_type: "Sporting Events"; }

    | { operator_type: "Tour"; business_type: "Active / adventure"; experience_type: "Adrenaline & Extreme Tours" | "Adventure Tours" | "ATV & Off-Road Tours" | "Bike Tours" | "Canyoning & Rappelling Tours" | "Climbing Tours" | "Eco Tours" | "Hiking & Camping Tours" | "Horseback Riding Tours" | "Motorcycle, Scooter & Moped Tours" | "Nature & Wildlife Tours" | "Running Tours" | "Safaris" | "Self-guided Tours" | "Ski & Snow Tours" | "Wildlife Tours"; }
    | { operator_type: "Tour"; business_type: "Boat Tours"; experience_type: "Boat Tours" | "Dolphin & Whale Watching"; }
    | { operator_type: "Tour"; business_type: "Cultural & Specialty Tours"; experience_type: "Art & Music Tours" | "Cultural Tours" | "Ghost & Vampire Tours" | "Historical & Heritage Tours" | "Movie & TV Tours" | "Night Tours" | "Shopping Tours" | "Private Tours" | "Self-guided Tours" | "Tours"; }
    | { operator_type: "Tour"; business_type: "Food & Drink"; experience_type: "Beer Tastings & Tours" | "Coffee & Tea Tours" | "Cooking Classes" | "Distillery or Spirit Tours" | "Food Tours" | "Wine Tours & Tastings"; }
    | { operator_type: "Tour"; business_type: "Multi-day Tours"; experience_type: "Multi-day Tours"; }
    | { operator_type: "Tour"; business_type: "Sightseeing"; experience_type: "Air Tours" | "Balloon Rides" | "Bus Tours" | "Cable Car Tours" | "Car Tours" | "City Tours" | "Classic Car Tours" | "Day tours & Excursions" | "Hop-On Hop-Off Tours" | "Horse-Drawn Carriage Tours" | "Luxury Car Tours" | "Private Tours" | "Rail Tours" | "Sidecar Tours" | "Sightseeing Tours" | "Sightseeing Passes" | "Sports Complexes" | "Vespa, Scooter & Moped Tours" | "Walking Tours" | "Other Tours"; }
    | { operator_type: "Tour"; business_type: "Tour of a specific attraction"; experience_type: "Site Tours"; }

    | { operator_type: "Transportation"; business_type: "Transportation"; experience_type: "Bus or Shuttle Transportation" | "Helicopter Transfers" | "Other Ground Transportation" | "Water Transfers"; }
    );
    booking_method: "Online Booking" | "Form Submission" | "Contact Info" | "Cannot Infer" | null; // if multiple options are applicable, choose the firstmost option
    operating_scope: "local" | "multi_regional" | "international" | null; // Most operators are local. You should have a moderately high confidence if assigning multi_regional or international.
    follow_contact: string | null;
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
You are crawling the contacts page of a tour operator's website. You will return a list of profiles as complete as possible given the information. If no information is available, profiles should be an empty list.
A profile should only be added if it contains at least one method of contact. Do not add multiple profiles with the same contact information. Do not assume a phone number is a whatsapp number unless stated by the website.

Respond with a parsable JSON only (no markdowns, no comments) with this exact type:
{
  profiles: {
    profile_type: "Company" | "Individual"; // assume "Company" unless specified
    role: "Owner" | "Manager" | "Guide" | "Booking Agent" | "Support" | "Unknown" | null; // null if profile_type is "Company"
    profile_name: string | null; // Name of the individual or null
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
        "status",
        "classification",
        "booking_method",
        "operating_scope",
        "follow_contact",
        "follow_booking",
    }
    if set(res.keys()) != REQUIRED_KEYS:
        return False

    STATUS_FIXED = {
        "OK",
        "Website does not belong to specified operator",
        "Not an operator website",
        "Insufficient information: website likely requires JavaScript rendering",
    }
    BOOKING = {"Online Booking", "Form Submission", "Contact Info", "Cannot Infer"}
    OPERATING_SCOPE = {"local", "multi_regional", "international"}

    CLASSIFICATION_RULES: dict[tuple[str, str], set[str]] = {
        ("Activity", "Air-based adventure, activity, or rentals"): {
            "Parasailing & Paragliding",
            "Skydiving",
        },
        ("Activity", "Cultural activity, experience or classes"): {
            "Arts & Crafts",
            "Education / Cultural",
        },
        ("Activity", "Land-based adventure, activity, or rentals"): {
            "Ax Throwing",
            "Bike and E-Bike Rentals",
            "Bungee Jumping",
            "Camel Rides",
            "Caving & Climbing",
            "Escape Room Games",
            "Extreme Sports",
            "Fitness Classes",
            "Flight Simulator",
            "Gear Rentals",
            "Hiking",
            "Horseback Riding",
            "Martial Arts Classes",
            "Off-Road & ATV",
            "Games & Entertainment Centers",
            "Other Outdoor Activities",
            "Race Track Car Racing (self-drive)",
            "Shooting Ranges",
            "Shore Excursions",
            "Sports Lessons",
            "Swordsmanship Classes",
            "Tennis",
            "Trams",
            "Winter Sports",
            "Zipline & Aerial Adventure Parks",
            "Zorbing",
        },
        ("Activity", "Water-based adventure, activity, or rentals"): {
            "Boat Rentals",
            "Fishing Charters",
            "Marinas",
            "River Rafting, Kayaking, Canoeing",
            "Scuba & Snorkeling",
            "Swim with Dolphins",
            "Water Sports",
        },
        ("Activity", "Wellness"): {
            "Spas",
            "Thermal & Mineral Springs",
            "Yoga & Pilates",
        },
        ("Attraction", "Amusement & Theme Parks"): {
            "Adventure Parks",
            "Amusement & Theme Parks",
            "Amusement Parks",
            "Ghost Towns",
            "Water Parks",
        },
        ("Attraction", "Cultural Sites & Landmarks"): {
            "Architectural Landmark",
            "Battlefields",
            "Lighthouses",
            "Monuments & Statues",
            "Other sites & landmarks",
        },
        ("Attraction", "Museums & Galleries"): {
            "Art Galleries",
            "Art Museums",
            "Children's Museums",
            "History & Culture Museums",
            "Natural History Museums",
            "Science Museums",
            "Specialty Museums",
        },
        ("Attraction", "Natural Attraction"): {
            "Gardens",
            "Caverns & Caves",
            "Hot Springs & Geysers",
            "National & State Parks",
            "Other Natural Attractions",
            "Waterfalls",
        },
        ("Attraction", "Observation Decks & Towers"): {"Observation Decks & Towers"},
        ("Attraction", "Zoos & Aquariums"): {"Aquariums", "Zoo & Aquariums", "Zoos"},
        ("Event", "Festivals"): {
            "Cultural Events",
            "Food & Drink Festivals",
            "Music Festivals",
        },
        ("Event", "Performing arts"): {
            "Concerts & Shows",
            "Cultural Events",
            "Dinner Theaters",
            "Experience nights",
            "Theater, play or musical",
        },
        ("Event", "Sporting event"): {"Sporting Events"},
        ("Tour", "Active / adventure"): {
            "Adrenaline & Extreme Tours",
            "Adventure Tours",
            "ATV & Off-Road Tours",
            "Bike Tours",
            "Canyoning & Rappelling Tours",
            "Climbing Tours",
            "Eco Tours",
            "Hiking & Camping Tours",
            "Horseback Riding Tours",
            "Motorcycle, Scooter & Moped Tours",
            "Nature & Wildlife Tours",
            "Running Tours",
            "Safaris",
            "Self-guided Tours",
            "Ski & Snow Tours",
            "Wildlife Tours",
        },
        ("Tour", "Boat Tours"): {"Boat Tours", "Dolphin & Whale Watching"},
        ("Tour", "Cultural & Specialty Tours"): {
            "Art & Music Tours",
            "Cultural Tours",
            "Ghost & Vampire Tours",
            "Historical & Heritage Tours",
            "Movie & TV Tours",
            "Night Tours",
            "Shopping Tours",
            "Private Tours",
            "Self-guided Tours",
            "Tours",
        },
        ("Tour", "Food & Drink"): {
            "Beer Tastings & Tours",
            "Coffee & Tea Tours",
            "Cooking Classes",
            "Distillery or Spirit Tours",
            "Food Tours",
            "Wine Tours & Tastings",
        },
        ("Tour", "Multi-day Tours"): {"Multi-day Tours"},
        ("Tour", "Sightseeing"): {
            "Air Tours",
            "Balloon Rides",
            "Bus Tours",
            "Cable Car Tours",
            "Car Tours",
            "City Tours",
            "Classic Car Tours",
            "Day tours & Excursions",
            "Hop-On Hop-Off Tours",
            "Horse-Drawn Carriage Tours",
            "Luxury Car Tours",
            "Private Tours",
            "Rail Tours",
            "Sidecar Tours",
            "Sightseeing Tours",
            "Sightseeing Passes",
            "Sports Complexes",
            "Vespa, Scooter & Moped Tours",
            "Walking Tours",
            "Other Tours",
        },
        ("Tour", "Tour of a specific attraction"): {"Site Tours"},
        ("Transportation", "Transportation"): {
            "Bus or Shuttle Transportation",
            "Helicopter Transfers",
            "Other Ground Transportation",
            "Water Transfers",
        },
    }

    def _nonempty_str_or_none(v) -> bool:
        return v is None or (isinstance(v, str) and v.strip())

    # status
    status = res["status"]
    if not isinstance(status, str):
        return False
    if status not in STATUS_FIXED:
        if not status.startswith("Other error: ") or len(status) <= len(
            "Other error: "
        ):
            return False

    # classification
    classification = res["classification"]
    if classification is not None:
        if not isinstance(classification, dict) or set(classification) != {
            "operator_type",
            "business_type",
            "experience_type",
        }:
            return False

        key = (classification["operator_type"], classification["business_type"])
        allowed = CLASSIFICATION_RULES.get(key)
        if allowed is None or classification["experience_type"] not in allowed:
            return False

    # booking_method
    bm = res["booking_method"]
    if bm is not None and bm not in BOOKING:
        return False

    # operating_scope
    scope = res["operating_scope"]
    if scope is not None and scope not in OPERATING_SCOPE:
        return False

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
            prompt_cache_key="AOIOUSJD98231u89hKAJSHf1982u3JKAHSDAKSHJD1982zxkhfkl",
        )
    except Exception:
        return ClassifyResult(ok=False, message="ChatGPT API error")

    # validate response
    try:
        parsed_output = json.loads(res.output_text)
    except json.JSONDecodeError:
        return ClassifyResult(ok=False, message="ChatGPT provided invalid JSON")

    if not _validate_llm_output(parsed_output):
        return ClassifyResult(ok=False, message="ChatGPT provided invalid JSON schema")

    if parsed_output["status"] != "OK":
        return ClassifyResult(ok=False, message=parsed_output["status"])

    return ClassifyResult(
        ok=True,
        operator_type=parsed_output["classification"]["operator_type"],
        business_type=parsed_output["classification"]["business_type"],
        experience_type=parsed_output["classification"]["experience_type"],
        booking_method=parsed_output["booking_method"],
        operating_scope=parsed_output["operating_scope"],
        follow_booking=parsed_output["follow_booking"],
        follow_contact=parsed_output["follow_contact"],
        input_tokens=res.usage.input_tokens
        - res.usage.input_tokens_details.cached_tokens,
        cached_input_tokens=res.usage.input_tokens_details.cached_tokens,
        output_tokens=res.usage.output_tokens,
    )


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
    try:
        parsed_output = json.loads(res.output_text)
    except json.JSONDecodeError:
        return ClassifyResult(ok=False, message="ChatGPT provided invalid JSON")

    if not _validate_llm_booking_output(parsed_output):
        return ClassifyResult(ok=False, message="ChatGPT provided invalid JSON schema")

    return ClassifyResult(
        ok=True,
        booking_method=parsed_output["booking_method"],
        input_tokens=res.usage.input_tokens
        - res.usage.input_tokens_details.cached_tokens,
        cached_input_tokens=res.usage.input_tokens_details.cached_tokens,
        output_tokens=res.usage.output_tokens,
    )


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
    try:
        parsed_output = json.loads(res.output_text)
    except json.JSONDecodeError:
        return ClassifyResult(ok=False, message="ChatGPT provided invalid JSON")

    if not _validate_llm_contacts_output(parsed_output):
        return ClassifyResult(ok=False, message="ChatGPT provided invalid JSON schema")

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

    return ClassifyResult(
        ok=True,
        profiles=profiles,
        input_tokens=res.usage.input_tokens
        - res.usage.input_tokens_details.cached_tokens,
        cached_input_tokens=res.usage.input_tokens_details.cached_tokens,
        output_tokens=res.usage.output_tokens,
    )
