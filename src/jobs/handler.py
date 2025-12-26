import httpx
import re
from lxml import html
from urllib.parse import urljoin
from dotenv import load_dotenv
from openai import OpenAI
from pprint import pprint

load_dotenv()
client = OpenAI()


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
URL_RE = re.compile(r'https?://[^\s"\'<>]+|/[^\s"\'<>]+')

def classify_operator(origin):

    return "TEST", "TEST"

    if not origin:
        return ""

    res = httpx.get(origin).text
    tree = html.fromstring(res)


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
                    url = part.strip().split()[0]
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
    You are an extraction agent.

    Input: text extracted from a tour operator’s webpage HTML.
    Hyperlinks referenced in the text are labeled inline as [L1], [L2], etc.

    Task:
    Identify tour products on the page. A product is a purchasable tour, activity, or package.
    Identify which links may contain additional product or pricing information.
    For each product extract:
    category (list of categories below)
    destination (city or region)
    price (number to 2 decimals if present converted to USD, otherwise null)

    Rules:
    Use only information present in the HTML
    Do not infer or guess missing data
    If no products or links are found, return empty arrays.
    Use null for unknown fields
    Be concise

    Output:
    Return ONLY valid JSON in this exact shape:

    {
    "relevant_links": ["L1", "L3"],
    "products": [
    {
    "category": null,
    "destination": null,
    "price": null
    }
    ]
    }

    Category must match exactly one of the following:
    parasailing & paragliding
    skydiving
    arts & crafts
    education / cultural
    ax throwing
    bike and e-bike rentals
    bungee jumping
    camel rides
    caving & climbing
    escape room games
    extreme sports
    fitness classes
    flight simulator
    gear rentals
    hiking
    horseback riding
    martial arts classes
    off-road& atv
    games & entertainment centers
    other outdoor activities
    race track car racing (self-drive)
    shooting ranges
    shore excursions
    sports lessons
    swordsmanship classes
    tennis
    trams
    winter sports
    zipline & aerial adventure parks
    zorbing
    boat rentals
    fishing charters
    marinas
    river rafting, kayaking, canoeing
    scuba & snorkeling
    swim with dolphins
    water sports
    spas
    thermal & mineral springs
    yoga & pilates
    adventure parks
    amusement & theme parks
    amusement parks
    ghost towns
    water parks
    architectural landmark
    battlefields
    lighthouses
    monuments & statues
    other sites & landmarks
    art galleries
    art museums
    children's museums
    history & culture museums
    natural history museums
    science museums
    specialty museums
    gardens
    caverns & caves
    hot springs & geysers
    national & state parks
    other natural attractions
    waterfalls
    observation decks & towers
    aquariums
    zoo & aquariums
    zoos
    cultural events
    food & drink festivals
    music festivals
    concerts & shows
    dinner theaters
    experience nights
    theater, play or musical
    sporting events
    adrenaline & extreme tours
    adventure tours
    atv & off-road tours
    bike tours
    canyoning & rappelling tours
    climbing tours
    eco tours
    hiking & camping tours
    horseback riding tours
    motorcycle, scooter & moped tours
    nature & wildlife tours
    running tours
    safaris
    self-guided tours
    ski & snow tours
    wildlife tours
    boat tours
    dolphin & whale watching
    historical & heritage tours
    art & music tours
    cultural tours
    ghost & vampire tours
    movie & tv tours
    night tours
    shopping tours
    private tours
    tours
    beer tastings & tours
    coffee & tea tours
    cooking classes
    distillery or spirit tours
    food tours
    wine tours & tastings
    multi-day tours
    air tours
    balloon rides
    bus tours
    cable car tours
    car tours
    city tours
    classic car tours
    hop-on hop-off tours
    horse-drawn carriage tours
    luxury car tours
    rail tours
    sidecar tours
    sightseeing tours
    sports complexes
    day tours & excursions
    other tours
    vespa, scooter & moped tours
    walking tours
    sightseeing passes
    site tours
    bus or shuttle transportation
    helicopter transfers
    other ground transportation
    water transfers

    Hyperlink key:
    """
        + "".join(f"[L{k}] {v}\n" for k, v in inverted_links.items())
        + "\nInput:\n"
    )

    prompt = prompt_env + "\n" + dom_string

    print(prompt)


    response = client.responses.create(
        model="gpt-5-mini",
        service_tier="flex",
        input=prompt,
        # text={"verbosity": "low"},
        # reasoning={"effort": "low"},
        prompt_cache_key="save me money pls",
    )

    print(response.model_dump_json(indent=2))
    print(response.output_text)
