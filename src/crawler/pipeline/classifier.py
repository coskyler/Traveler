from crawler.pipeline.types import OperatorInfo, Profile, ParseResult, ClassifyResult
from openai import OpenAI
from dotenv import load_dotenv
import json


load_dotenv()
client = OpenAI()


def _top_level_category(sub_category: str) -> str | None:
    mapping = {
        # Air-based
        "Parasailing & Paragliding": "Air-based adventure, activity, or rentals",
        "Skydiving": "Air-based adventure, activity, or rentals",
        # Cultural activity
        "Arts & Crafts": "Cultural activity, experience or classes",
        "Education / Cultural": "Cultural activity, experience or classes",
        # Land-based
        "Ax Throwing": "Land-based adventure, activity, or rentals",
        "Bike and E-Bike Rentals": "Land-based adventure, activity, or rentals",
        "Bungee Jumping": "Land-based adventure, activity, or rentals",
        "Camel Rides": "Land-based adventure, activity, or rentals",
        "Caving & Climbing": "Land-based adventure, activity, or rentals",
        "Escape Room Games": "Land-based adventure, activity, or rentals",
        "Extreme Sports": "Land-based adventure, activity, or rentals",
        "Fitness Classes": "Land-based adventure, activity, or rentals",
        "Flight Simulator": "Land-based adventure, activity, or rentals",
        "Gear Rentals": "Land-based adventure, activity, or rentals",
        "Hiking": "Land-based adventure, activity, or rentals",
        "Horseback Riding": "Land-based adventure, activity, or rentals",
        "Martial Arts Classes": "Land-based adventure, activity, or rentals",
        "Off-Road & ATV": "Land-based adventure, activity, or rentals",
        "Games & Entertainment Centers": "Land-based adventure, activity, or rentals",
        "Other Outdoor Activities": "Land-based adventure, activity, or rentals",
        "Race Track Car Racing (self-drive)": "Land-based adventure, activity, or rentals",
        "Shooting Ranges": "Land-based adventure, activity, or rentals",
        "Shore Excursions": "Land-based adventure, activity, or rentals",
        "Sports Lessons": "Land-based adventure, activity, or rentals",
        "Swordsmanship Classes": "Land-based adventure, activity, or rentals",
        "Tennis": "Land-based adventure, activity, or rentals",
        "Trams": "Land-based adventure, activity, or rentals",
        "Winter Sports": "Land-based adventure, activity, or rentals",
        "Zipline & Aerial Adventure Parks": "Land-based adventure, activity, or rentals",
        "Zorbing": "Land-based adventure, activity, or rentals",
        # Water-based
        "Boat Rentals": "Water-based adventure, activity, or rentals",
        "Fishing Charters": "Water-based adventure, activity, or rentals",
        "Marinas": "Water-based adventure, activity, or rentals",
        "River Rafting, Kayaking, Canoeing": "Water-based adventure, activity, or rentals",
        "Scuba & Snorkeling": "Water-based adventure, activity, or rentals",
        "Swim with Dolphins": "Water-based adventure, activity, or rentals",
        "Water Sports": "Water-based adventure, activity, or rentals",
        # Wellness
        "Spas": "Wellness",
        "Thermal & Mineral Springs": "Wellness",
        "Yoga & Pilates": "Wellness",
        # Amusement & Theme Parks
        "Adventure Parks": "Amusement & Theme Parks",
        "Amusement Parks": "Amusement & Theme Parks",
        "Ghost Towns": "Amusement & Theme Parks",
        "Water Parks": "Amusement & Theme Parks",
        # Cultural Sites & Landmarks
        "Architectural Landmark": "Cultural Sites & Landmarks",
        "Battlefields": "Cultural Sites & Landmarks",
        "Lighthouses": "Cultural Sites & Landmarks",
        "Monuments & Statues": "Cultural Sites & Landmarks",
        "Other Sites & Landmarks": "Cultural Sites & Landmarks",
        # Museums & Galleries
        "Art Galleries": "Museums & Galleries",
        "Art Museums": "Museums & Galleries",
        "Children's Museums": "Museums & Galleries",
        "History & Culture Museums": "Museums & Galleries",
        "Natural History Museums": "Museums & Galleries",
        "Science Museums": "Museums & Galleries",
        "Specialty Museums": "Museums & Galleries",
        # Natural Attraction
        "Gardens": "Natural Attraction",
        "Caverns & Caves": "Natural Attraction",
        "Hot Springs & Geysers": "Natural Attraction",
        "National & State Parks": "Natural Attraction",
        "Other Natural Attractions": "Natural Attraction",
        "Waterfalls": "Natural Attraction",
        # Observation
        "Observation Decks & Towers": "Observation Decks & Towers",
        # Zoos & Aquariums
        "Aquariums": "Zoos & Aquariums",
        "Zoos": "Zoos & Aquariums",
        # Festivals
        "Cultural Events": "Festivals",
        "Food & Drink Festivals": "Festivals",
        "Music Festivals": "Festivals",
        # Performing arts
        "Concerts & Shows": "Performing arts",
        "Dinner Theaters": "Performing arts",
        "Experience Nights": "Performing arts",
        "Theater, Play or Musical": "Performing arts",
        # Sporting event
        "Sporting Events": "Sporting event",
        # Active / adventure
        "Adrenaline & Extreme Tours": "Active / adventure",
        "Adventure Tours": "Active / adventure",
        "ATV & Off-Road Tours": "Active / adventure",
        "Bike Tours": "Active / adventure",
        "Canyoning & Rappelling Tours": "Active / adventure",
        "Climbing Tours": "Active / adventure",
        "Eco Tours": "Active / adventure",
        "Hiking & Camping Tours": "Active / adventure",
        "Horseback Riding Tours": "Active / adventure",
        "Motorcycle, Scooter & Moped Tours": "Active / adventure",
        "Nature & Wildlife Tours": "Active / adventure",
        "Running Tours": "Active / adventure",
        "Safaris": "Active / adventure",
        "Self-guided Tours": "Active / adventure",
        "Ski & Snow Tours": "Active / adventure",
        "Wildlife Tours": "Active / adventure",
        # Boat Tours
        "Boat Tours": "Boat Tours",
        "Dolphin & Whale Watching": "Boat Tours",
        # Cultural & Specialty Tours
        "Art & Music Tours": "Cultural & Specialty Tours",
        "Cultural Tours": "Cultural & Specialty Tours",
        "Ghost & Vampire Tours": "Cultural & Specialty Tours",
        "Historical & Heritage Tours": "Cultural & Specialty Tours",
        "Movie & TV Tours": "Cultural & Specialty Tours",
        "Night Tours": "Cultural & Specialty Tours",
        "Private Tours": "Cultural & Specialty Tours",
        "Shopping Tours": "Cultural & Specialty Tours",
        "Tours": "Cultural & Specialty Tours",
        # Food & Drink
        "Beer Tastings & Tours": "Food & Drink",
        "Coffee & Tea Tours": "Food & Drink",
        "Cooking Classes": "Food & Drink",
        "Distillery or Spirit Tours": "Food & Drink",
        "Food Tours": "Food & Drink",
        "Wine Tours & Tastings": "Food & Drink",
        # Multi-day Tours
        "Multi-day Tours": "Multi-day Tours",
        # Sightseeing
        "Air Tours": "Sightseeing",
        "Balloon Rides": "Sightseeing",
        "Bus Tours": "Sightseeing",
        "Cable Car Tours": "Sightseeing",
        "Car Tours": "Sightseeing",
        "City Tours": "Sightseeing",
        "Classic Car Tours": "Sightseeing",
        "Day Tours & Excursions": "Sightseeing",
        "Hop-On Hop-Off Tours": "Sightseeing",
        "Horse-Drawn Carriage Tours": "Sightseeing",
        "Luxury Car Tours": "Sightseeing",
        "Other Tours": "Sightseeing",
        "Rail Tours": "Sightseeing",
        "Sidecar Tours": "Sightseeing",
        "Sightseeing Tours": "Sightseeing",
        "Sightseeing Passes": "Sightseeing",
        "Sports Complexes": "Sightseeing",
        "Vespa, Scooter & Moped Tours": "Sightseeing",
        "Walking Tours": "Sightseeing",
        # Specific attraction
        "Site Tours": "Tour of a specific attraction",
        # Transportation
        "Bus or Shuttle Transportation": "Transportation",
        "Helicopter Transfers": "Transportation",
        "Other Ground Transportation": "Transportation",
        "Water Transfers": "Transportation",
    }

    return mapping.get(sub_category)


_PROMPT_CONTEXT = """
You are crawling a website that is presumed to be a specific private tour/activity/attraction operator's website. Your task is to confirm that it is a real operator's website and that it belongs to the specified operator.
If it is, the status is OK and you will categorize it and determine the website's booking method (if there is a book button, form submission, or simply contact info). If you cannot infer the booking method, you will, only if necessary, choose 1 hyperlink to follow which likely contains the needed information if available. You will also choose 1 hyperlink to follow to website's contact page if available; if there is no contact page and contact information is available in the current page, enter the URL of the page you are currently crawling in "follow_contact".
If not (e.g., government, public park, non-profit, misc., etc.), immediately respond with the appropriate status.

Respond with a parsable JSON only (no markdowns, no comments) with this exact type:
{
    status: "OK" | "Website does not belong to specified operator" | "Not an operator website" | "Insufficient information: website likely requires JavaScript rendering" | `Other error: ${string}`; // if multiple options are applicable, choose the firstmost option
    category: "Parasailing & Paragliding" | "Skydiving" | "Arts & Crafts" | "Education / Cultural" | "Ax Throwing" | "Bike and E-Bike Rentals" | "Bungee Jumping" | "Camel Rides" | "Caving & Climbing" | "Escape Room Games" | "Extreme Sports" | "Fitness Classes" | "Flight Simulator" | "Gear Rentals" | "Hiking" | "Horseback Riding" | "Martial Arts Classes" | "Off-Road & ATV" | "Games & Entertainment Centers" | "Other Outdoor Activities" | "Race Track Car Racing (self-drive)" | "Shooting Ranges" | "Shore Excursions" | "Sports Lessons" | "Swordsmanship Classes" | "Tennis" | "Trams" | "Winter Sports" | "Zipline & Aerial Adventure Parks" | "Zorbing" | "Boat Rentals" | "Fishing Charters" | "Marinas" | "River Rafting, Kayaking, Canoeing" | "Scuba & Snorkeling" | "Swim with Dolphins" | "Water Sports" | "Spas" | "Thermal & Mineral Springs" | "Yoga & Pilates" | "Adventure Parks" | "Amusement Parks" | "Ghost Towns" | "Water Parks" | "Architectural Landmark" | "Battlefields" | "Lighthouses" | "Monuments & Statues" | "Other Sites & Landmarks" | "Art Galleries" | "Art Museums" | "Children's Museums" | "History & Culture Museums" | "Natural History Museums" | "Science Museums" | "Specialty Museums" | "Gardens" | "Caverns & Caves" | "Hot Springs & Geysers" | "National & State Parks" | "Other Natural Attractions" | "Waterfalls" | "Observation Decks & Towers" | "Aquariums" | "Zoos" | "Cultural Events" | "Food & Drink Festivals" | "Music Festivals" | "Concerts & Shows" | "Cultural Events" | "Dinner Theaters" | "Experience Nights" | "Theater, Play or Musical" | "Sporting Events" | "Adrenaline & Extreme Tours" | "Adventure Tours" | "ATV & Off-Road Tours" | "Bike Tours" | "Canyoning & Rappelling Tours" | "Climbing Tours" | "Eco Tours" | "Hiking & Camping Tours" | "Horseback Riding Tours" | "Motorcycle, Scooter & Moped Tours" | "Nature & Wildlife Tours" | "Running Tours" | "Safaris" | "Self-guided Tours" | "Ski & Snow Tours" | "Wildlife Tours" | "Boat Tours" | "Dolphin & Whale Watching" | "Art & Music Tours" | "Cultural Tours" | "Ghost & Vampire Tours" | "Historical & Heritage Tours" | "Movie & TV Tours" | "Night Tours" | "Private Tours" | "Self-guided Tours" | "Shopping Tours" | "Tours" | "Beer Tastings & Tours" | "Coffee & Tea Tours" | "Cooking Classes" | "Distillery or Spirit Tours" | "Food Tours" | "Wine Tours & Tastings" | "Multi-day Tours" | "Air Tours" | "Balloon Rides" | "Bus Tours" | "Cable Car Tours" | "Car Tours" | "City Tours" | "Classic Car Tours" | "Day Tours & Excursions" | "Hop-On Hop-Off Tours" | "Horse-Drawn Carriage Tours" | "Luxury Car Tours" | "Other Tours" | "Private Tours" | "Rail Tours" | "Sidecar Tours" | "Sightseeing Tours" | "Sightseeing Passes" | "Sports Complexes" | "Vespa, Scooter & Moped Tours" | "Walking Tours" | "Site Tours" | "Bus or Shuttle Transportation" | "Helicopter Transfers" | "Other Ground Transportation" | "Water Transfers" | null;
    booking_method: "Online Booking" | "Form Submission" | "Contact Info" | "Cannot Infer" | null; // if multiple options are applicable, choose the firstmost option
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
You are crawling the contacts page of a tour operator's website. You will return a list of profiles as complete as possible given the information. If no information is available, profiles should be an empty list. A profile should only be added if it contains at least one method of contact. Do not add multiple profiles with the same contact information.

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

    # exact shape
    REQUIRED_KEYS = {
        "status",
        "category",
        "booking_method",
        "follow_contact",
        "follow_booking",
    }
    if set(res.keys()) != REQUIRED_KEYS:
        return False

    CATEGORIES = {
        "Parasailing & Paragliding",
        "Skydiving",
        "Arts & Crafts",
        "Education / Cultural",
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
        "Boat Rentals",
        "Fishing Charters",
        "Marinas",
        "River Rafting, Kayaking, Canoeing",
        "Scuba & Snorkeling",
        "Swim with Dolphins",
        "Water Sports",
        "Spas",
        "Thermal & Mineral Springs",
        "Yoga & Pilates",
        "Adventure Parks",
        "Amusement Parks",
        "Ghost Towns",
        "Water Parks",
        "Architectural Landmark",
        "Battlefields",
        "Lighthouses",
        "Monuments & Statues",
        "Other Sites & Landmarks",
        "Art Galleries",
        "Art Museums",
        "Children's Museums",
        "History & Culture Museums",
        "Natural History Museums",
        "Science Museums",
        "Specialty Museums",
        "Gardens",
        "Caverns & Caves",
        "Hot Springs & Geysers",
        "National & State Parks",
        "Other Natural Attractions",
        "Waterfalls",
        "Observation Decks & Towers",
        "Aquariums",
        "Zoos",
        "Cultural Events",
        "Food & Drink Festivals",
        "Music Festivals",
        "Concerts & Shows",
        "Dinner Theaters",
        "Experience Nights",
        "Theater, Play or Musical",
        "Sporting Events",
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
        "Boat Tours",
        "Dolphin & Whale Watching",
        "Art & Music Tours",
        "Cultural Tours",
        "Ghost & Vampire Tours",
        "Historical & Heritage Tours",
        "Movie & TV Tours",
        "Night Tours",
        "Private Tours",
        "Shopping Tours",
        "Tours",
        "Beer Tastings & Tours",
        "Coffee & Tea Tours",
        "Cooking Classes",
        "Distillery or Spirit Tours",
        "Food Tours",
        "Wine Tours & Tastings",
        "Multi-day Tours",
        "Air Tours",
        "Balloon Rides",
        "Bus Tours",
        "Cable Car Tours",
        "Car Tours",
        "City Tours",
        "Classic Car Tours",
        "Day Tours & Excursions",
        "Hop-On Hop-Off Tours",
        "Horse-Drawn Carriage Tours",
        "Luxury Car Tours",
        "Other Tours",
        "Rail Tours",
        "Sidecar Tours",
        "Sightseeing Tours",
        "Sightseeing Passes",
        "Sports Complexes",
        "Vespa, Scooter & Moped Tours",
        "Walking Tours",
        "Site Tours",
        "Bus or Shuttle Transportation",
        "Helicopter Transfers",
        "Other Ground Transportation",
        "Water Transfers",
    }

    BOOKING = {
        "Online Booking",
        "Form Submission",
        "Contact Info",
        "Cannot Infer",
    }

    STATUS_FIXED = {
        "OK",
        "Website does not belong to specified operator",
        "Not an operator website",
        "Insufficient information: website likely requires JavaScript rendering",
    }

    # status
    status = res.get("status")
    if not isinstance(status, str):
        return False
    if status in STATUS_FIXED:
        pass
    elif status.startswith("Other error: "):
        if len(status) <= len("Other error: "):
            return False
    else:
        return False

    # category
    cat = res.get("category")
    if cat is not None and cat not in CATEGORIES:
        return False

    # booking_method
    bm = res.get("booking_method")
    if bm is not None and bm not in BOOKING:
        return False

    # follow_contact
    fc = res.get("follow_contact")
    if fc is not None:
        if not isinstance(fc, str) or not fc.strip():
            return False

    # follow_booking
    fb = res.get("follow_booking")
    if fb is not None:
        if not isinstance(fb, str) or not fb.strip():
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
        category=_top_level_category(parsed_output["category"]),
        sub_category=parsed_output["category"],
        booking_method=parsed_output["booking_method"],
        follow_booking=parsed_output["follow_booking"],
        follow_contact=parsed_output["follow_contact"],
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

    return ClassifyResult(ok=True, booking_method=parsed_output["booking_method"])


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

    return ClassifyResult(ok=True, profiles=profiles)
