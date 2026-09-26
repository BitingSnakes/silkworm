"""HTML character references reach spiders decoded, using the real scraper_rs (>=0.11)."""

from __future__ import annotations

from silkworm import Engine, HTMLResponse, Request, Response, Spider
from silkworm.declarative import Attr, Item, Text

PAGE = (
    b"<html><body>"
    b'<div class="card"><a href="/p?a=1&amp;b=2&not=3" title="&quot;Q&quot;">'
    b"Fish &amp; Chips &mdash; &#169;</a>"
    b"<script>if (a &amp;&amp; b) {}</script></div>"
    b"</body></html>"
)


def _html_response(url: str = "https://example.com/list") -> HTMLResponse:
    request = Request(url=url)
    return HTMLResponse(
        url=url,
        status=200,
        headers={"content-type": "text/html"},
        body=PAGE,
        request=request,
    )


async def test_selectors_return_decoded_text_and_attributes() -> None:
    response = _html_response()

    link = await response.select_first("a")
    assert link is not None
    # Only complete references are decoded, so `&not=3` stays literal.
    assert link.attr("href") == "/p?a=1&b=2&not=3"
    assert link.attr("title") == '"Q"'
    assert link.text == "Fish & Chips — ©"

    xpath_link = await response.xpath_first("//a")
    assert xpath_link is not None
    assert xpath_link.attr("href") == "/p?a=1&b=2&not=3"

    script = await response.select_first("script")
    assert script is not None
    assert script.text == "if (a &amp;&amp; b) {}"


async def test_declarative_fields_are_decoded() -> None:
    class Card(Item):
        __selector__ = ".card"

        title: str = Text("a", strip=True)
        url: str = Attr("a", "href", absolute=True)

    cards = [card async for card in Card.extract(_html_response())]

    assert [card.to_dict() for card in cards] == [
        {
            "title": "Fish & Chips — ©",
            "url": "https://example.com/p?a=1&b=2&not=3",
        }
    ]


async def test_followed_links_use_decoded_urls() -> None:
    fetched: list[str] = []

    class LinkSpider(Spider):
        start_urls = ("https://example.com/list",)

        async def parse(self, response: Response) -> None:
            if not isinstance(response, HTMLResponse):
                return
            link = await response.select_first("a")
            if link is not None and (href := link.attr("href")):
                await response.follow(href, callback=self.parse_detail)

        async def parse_detail(self, response: Response) -> None:
            return None

    engine = Engine(LinkSpider(), concurrency=1)

    async def fake_fetch(request: Request) -> Response:
        fetched.append(request.url)
        return _html_response(request.url)

    engine.http.fetch = fake_fetch  # type: ignore[method-assign]
    await engine.run()

    assert fetched == [
        "https://example.com/list",
        "https://example.com/p?a=1&b=2&not=3",
    ]
