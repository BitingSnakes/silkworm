from __future__ import annotations

from silkworm.declarative import Attr, Item, Text
from silkworm.request import Request
from silkworm.response import HTMLResponse


async def test_declarative_item_extracts_with_real_scraper_rs():
    class Quote(Item):
        __selector__ = ".quote"

        text: str = Text(".text", strip=True)
        author: str = Text(".author")
        author_url: str = Attr(".author-link", "href", absolute=True)
        tags: list[str] = Text(".tag")
        note: str | None = Text(".note")

    html = """
    <html>
      <body>
        <article class="quote">
          <span class="text"> First quote </span>
          <span class="author">Ada</span>
          <a class="author-link" href="/authors/ada">Profile</a>
          <a class="tag">python</a>
          <a class="tag">rust</a>
        </article>
        <article class="quote">
          <span class="text">Second quote</span>
          <span class="author">Lin</span>
          <a class="author-link" href="authors/lin">Profile</a>
          <span class="note">featured</span>
        </article>
      </body>
    </html>
    """
    request = Request(url="https://example.com/quotes/")
    response = HTMLResponse(
        url=request.url,
        status=200,
        headers={"content-type": "text/html; charset=utf-8"},
        body=html.encode(),
        request=request,
    )

    quotes = [quote async for quote in Quote.extract(response)]

    assert [quote.to_dict() for quote in quotes] == [
        {
            "text": "First quote",
            "author": "Ada",
            "author_url": "https://example.com/authors/ada",
            "tags": ["python", "rust"],
            "note": None,
        },
        {
            "text": "Second quote",
            "author": "Lin",
            "author_url": "https://example.com/quotes/authors/lin",
            "tags": [],
            "note": "featured",
        },
    ]
