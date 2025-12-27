import httpx
import re
import json
from lxml import html
from urllib.parse import urljoin, urlparse
from dotenv import load_dotenv
from openai import OpenAI
import random

load_dotenv()
client = OpenAI()

USER_AGENTS = [
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
headers = {
    "User-Agent": random.choice(USER_AGENTS),
    "Accept": (
        "text/html,application/xhtml+xml,application/xml;"
        "q=0.9,image/avif,image/webp,*/*;q=0.8"
    ),
    "Accept-Language": "en-US,en;q=0.9",
    "Accept-Encoding": "gzip, deflate, br",
    "Connection": "keep-alive",
    "Upgrade-Insecure-Requests": "1",
}
INVISIBLE_TAGS = {
    "script",
    "style",
    "noscript",
    "template",
    "meta",
    "link",
    "head",
    "base",
    "iframe",
    "frame",
    "frameset",
    "object",
    "embed",
    "param",
    "source",
    "track",
    "audio",
    "video",
    "canvas",
    "svg",
    "path",
    "circle",
    "rect",
    "polygon",
    "g",
    "defs",
    "mask",
    "pattern",
    "picture",
    "portal",
    "slot",
    "-text",
    "#comment",
}
INLINE_TAGS = {"span", "b", "i", "em", "strong", "a"}
NAV_LINK_ATTRS = {
    # primary navigation
    "href",
    "action",
    "formaction",
    # SVG links
    "xlink:href",
    # SPA / JS routing
    "onclick",
    "data-href",
    "data-url",
    "data-route",
    "data-link",
    # meta redirects
    "content",  # meta refresh only (needs parsing)
}
SOCIAL_ORIGINS = {
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
URL_RE = re.compile(r'https?://[^\s"\'<>]+|/[^\s"\'<>]+')

def _is_social_url(url):
    for u in SOCIAL_ORIGINS:
        if u in url:
            return True, u
        
    return False, None

def _is_valid_url(s):
    try:
        urlparse(s)
        return True
    except Exception:
        return False
    
def _parse_llm_output(s):
    m = re.search(r'\{.*\}', s, re.S)
    if not m:
        raise ValueError("No JSON found")

    data = json.loads(m.group())

    # validate
    assert set(data) == {"category", "sub_category", "status"}
    
    # assert data["status"] in {
    #     "Success",
    #     "Err: Not an operator website",
    #     "Err: Insufficient information",
    # }
    # if data["status"] != "Success":
    #     assert data["category"] is None
    #     assert data["sub_category"] is None
    
    return data

def classify_operator(origin):
    if not origin:
        return "", "", "Err: No website link", 0, 0, 0
    
    is_social, social_url = _is_social_url(origin)

    if is_social:
        return "", "", "Err: social platform (" + social_url + ")", 0, 0, 0
    
    if not _is_valid_url(origin):
        return "", "", "Err: Invalid or insecure URL", 0, 0, 0
    
    print("Crawling " + origin)

    tree = None

    try:
        r = httpx.get(
            origin,
            headers=headers,
            follow_redirects=True,
            timeout=15,
        )

        res = r.text

        if not res.strip():
            return "", "", "Err: Invalid response text", 0, 0, 0

        tree = html.fromstring(res)
    except httpx.RequestError:
        return "", "", "Err: Invalid response", 0, 0, 0


    lines = []

    links = {}


    def add_link(link):
        if link in links:
            return links[link]

        links[link] = len(links)
        return len(links) - 1


    def extract_attrib_urls(node):
        found = set()

        # attributes
        for attr, val in node.attrib.items():
            if not val:
                continue

            if not attr in NAV_LINK_ATTRS:
                continue

            # srcset-style attributes
            if "," in val:
                for part in val.split(","):
                    tokens = part.strip().split()
                    if not tokens:
                        continue
                    url = tokens[0]
                    if url:
                        found.add(url)
            else:
                found.update(URL_RE.findall(val))

        normalized = [
            urljoin(origin, u) if origin else u
            for u in found
            if not u.startswith(("javascript:", "mailto:", "tel:", "#"))
        ]

        return [f"links[{add_link(link)}]" for link in normalized]


    # parse while preserving inline elements
    def parse_dom(node, indent=0, buf=None, bufIndent=0, top=True):
        if buf is None:
            buf = []

        if not isinstance(node.tag, str) or node.tag in INVISIBLE_TAGS:
            return

        if node.text and node.text.strip():
            buf.append(node.text)

        for child in node:
            is_inline = child.tag in INLINE_TAGS

            if is_inline:
                parse_dom(child, indent, buf, bufIndent, False)
            else:
                if buf:
                    lines.append("  " * bufIndent + "".join(buf).strip())
                    buf.clear()
                parse_dom(child, indent + 1, buf, indent + 1, True)

            if child.tail and child.tail.strip():
                buf.append(child.tail)

            attrib_urls = extract_attrib_urls(child)
            if attrib_urls:
                buf.append("(" + ", ".join(attrib_urls) + ")")

        if top and buf:
            lines.append("  " * bufIndent + "".join(buf).strip())
            buf.clear()


    def remove_whitespace(lines):
        leadingSpaces = float("inf")
        for s in lines:
            spaces = len(s) - len(s.lstrip(" "))

            leadingSpaces = min(leadingSpaces, spaces)

        for i in range(len(lines)):
            lines[i] = lines[i][leadingSpaces:]


    parse_dom(tree)
    remove_whitespace(lines)
    dom_string = "\n".join(lines)

    inverted_links = {v: k for k, v in links.items()}
    prompt_env = (
        """
You are crawling a (likely) tour/activity/attraction company's website. Your task is to categorize it into my taxonomy.

Respond with a parsable JSON ONLY (no markdowns, no comments) with this exact type:
{
category: string;
sub_category: string;
status: "Success" | "Err: Not an operator website" | "Err: Insufficient information" | "Err: Website error";
}

IF THE WEBSITE IS NOT A PRIVATE TOUR/ACTIVITY/ATTRACTION OPERATOR (E.G. GOVERNMENT, INFORMATIONAL, NON-PROFIT), IMMEDIATELY RESPOND WITH THE APPROPRIATE STATUS
IF STATUS IS NOT Success, category AND sub_category MUST BE AN EMPTY STRING

TAXONOMY:
Air-based adventure, activity, or rentals
  - Parasailing & Paragliding
  - Skydiving

Cultural activity, experience or classes
  - Arts & Crafts
  - Education / Cultural

Land-based adventure, activity, or rentals
  - Ax Throwing
  - Bike and E-Bike Rentals
  - Bungee Jumping
  - Camel Rides
  - Caving & Climbing
  - Escape Room Games
  - Extreme Sports
  - Fitness Classes
  - Flight Simulator
  - Gear Rentals
  - Hiking
  - Horseback Riding
  - Martial Arts Classes
  - Off-Road & ATV
  - Games & Entertainment Centers
  - Other Outdoor Activities
  - Race Track Car Racing (self-drive)
  - Shooting Ranges
  - Shore Excursions
  - Sports Lessons
  - Swordsmanship Classes
  - Tennis
  - Trams
  - Winter Sports
  - Zipline & Aerial Adventure Parks
  - Zorbing

Water-based adventure, activity, or rentals
  - Boat Rentals
  - Fishing Charters
  - Marinas
  - River Rafting, Kayaking, Canoeing
  - Scuba & Snorkeling
  - Swim with Dolphins
  - Water Sports

Wellness
  - Spas
  - Thermal & Mineral Springs
  - Yoga & Pilates

Amusement & Theme Parks
  - Adventure Parks
  - Amusement Parks
  - Ghost Towns
  - Water Parks

Cultural Sites & Landmarks
  - Architectural Landmark
  - Battlefields
  - Lighthouses
  - Monuments & Statues
  - Other Sites & Landmarks

Museums & Galleries
  - Art Galleries
  - Art Museums
  - Children's Museums
  - History & Culture Museums
  - Natural History Museums
  - Science Museums
  - Specialty Museums

Natural Attraction
  - Gardens
  - Caverns & Caves
  - Hot Springs & Geysers
  - National & State Parks
  - Other Natural Attractions
  - Waterfalls

Observation Decks & Towers
  - Observation Decks & Towers

Zoos & Aquariums
  - Aquariums
  - Zoos

Festivals
  - Cultural Events
  - Food & Drink Festivals
  - Music Festivals

Performing arts
  - Concerts & Shows
  - Cultural Events
  - Dinner Theaters
  - Experience Nights
  - Theater, Play or Musical

Sporting event
  - Sporting Events

Active / adventure
  - Adrenaline & Extreme Tours
  - Adventure Tours
  - ATV & Off-Road Tours
  - Bike Tours
  - Canyoning & Rappelling Tours
  - Climbing Tours
  - Eco Tours
  - Hiking & Camping Tours
  - Horseback Riding Tours
  - Motorcycle, Scooter & Moped Tours
  - Nature & Wildlife Tours
  - Running Tours
  - Safaris
  - Self-guided Tours
  - Ski & Snow Tours
  - Wildlife Tours

Boat Tours
  - Boat Tours
  - Dolphin & Whale Watching

Cultural & Specialty Tours
  - Art & Music Tours
  - Cultural Tours
  - Ghost & Vampire Tours
  - Historical & Heritage Tours
  - Movie & TV Tours
  - Night Tours
  - Private Tours
  - Self-guided Tours
  - Shopping Tours
  - Tours

Food & Drink
  - Beer Tastings & Tours
  - Coffee & Tea Tours
  - Cooking Classes
  - Distillery or Spirit Tours
  - Food Tours
  - Wine Tours & Tastings

Multi-day Tours
  - Multi-day Tours

Sightseeing
  - Air Tours
  - Balloon Rides
  - Bus Tours
  - Cable Car Tours
  - Car Tours
  - City Tours
  - Classic Car Tours
  - Day Tours & Excursions
  - Hop-On Hop-Off Tours
  - Horse-Drawn Carriage Tours
  - Luxury Car Tours
  - Other Tours
  - Private Tours
  - Rail Tours
  - Sidecar Tours
  - Sightseeing Tours
  - Sightseeing Passes
  - Sports Complexes
  - Vespa, Scooter & Moped Tours
  - Walking Tours

Tour of a specific attraction
  - Site Tours

Transportation
  - Bus or Shuttle Transportation
  - Helicopter Transfers
  - Other Ground Transportation
  - Water Transfers

YOU ARE CRAWLING:
""" + origin + """

HYPERLINK KEY:
"""
        + "".join(f"[L{k}] {v}\n" for k, v in inverted_links.items())
        + "\nWEBSITE HTML:\n"
    )

    prompt = prompt_env + "\n" + dom_string

    response = client.responses.create(
        model="gpt-5-mini",
        service_tier="flex",
        input=prompt,
        # text={"verbosity": "low"},
        # reasoning={"effort": "low"},
        prompt_cache_key="save me money pls",
    )

    # print(prompt)
    # print(response.model_dump_json(indent=2))
    # print(response.output_text)

    parsed = _parse_llm_output(response.output_text)

    return parsed["category"], parsed["sub_category"], parsed["status"], response.usage.input_tokens, response.usage.input_tokens_details.cached_tokens, response.usage.output_tokens

    
