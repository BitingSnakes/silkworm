"""Adaptable offline test scaffold for a Silkworm spider callback."""

from __future__ import annotations

from spider import ExampleSpider

from silkworm.testing import html_response, run_callback


async def test_parse_emits_items_and_follows_pagination() -> None:
    response = html_response(
        """
        <article class="item">
          <h2> First item </h2>
          <a href="/items/1">Details</a>
        </article>
        <a class="next" href="/items/?page=2">Next</a>
        """,
        url="https://example.com/items/",
    )

    result = await run_callback(ExampleSpider().parse, response)

    assert result.items == [
        {"title": "First item", "url": "https://example.com/items/1"}
    ]
    assert result.urls == ["https://example.com/items/?page=2"]


async def test_parse_tolerates_missing_optional_content_and_stops() -> None:
    response = html_response(
        '<article class="item"><h2>Missing link</h2></article>',
        url="https://example.com/items/?page=2",
    )

    result = await run_callback(ExampleSpider().parse, response)

    assert result.items == []
    assert result.requests == []
