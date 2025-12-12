import httpx
from selectolax.lexbor import LexborHTMLParser

html = httpx.get("https://www.coskyler.com").text
tree = LexborHTMLParser(html)

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


def print_dom(node, indent=0):
    if node.tag in INVISIBLE_TAGS:
        return

    if node.text(False):
        print("  " * indent + f"<{node.text(False)}>")
    child = node.child
    while child:
        print_dom(child, indent + 1)
        child = child.next


print_dom(tree.root)
