import httpx
import re
from lxml import html, etree
from urllib.parse import urljoin

origin = "https://www.coskyler.com/"

res = httpx.get(origin).text
tree = html.fromstring(res)

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
prompt_env = "Prompt\n" + "".join(f"links[{k}]: {v}\n" for k, v in inverted_links.items())

prompt = prompt_env + "\n" + dom_string

print(prompt)
