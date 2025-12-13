"""
Integration tests for XPath functionality using real scraper_rs library.
"""

import pytest

from silkworm.request import Request
from silkworm.response import HTMLResponse


async def test_xpath_selects_multiple_elements():
    """Test that xpath() returns multiple matching elements."""
    html = """
    <html>
        <body>
            <div class="quote">
                <span class="text">Quote 1</span>
                <span class="author">Author 1</span>
            </div>
            <div class="quote">
                <span class="text">Quote 2</span>
                <span class="author">Author 2</span>
            </div>
        </body>
    </html>
    """
    req = Request(url="http://example.com")
    resp = HTMLResponse(
        url=req.url,
        status=200,
        headers={},
        body=html.encode("utf-8"),
        request=req,
    )

    quotes = await resp.xpath("//div[@class='quote']")

    assert len(quotes) == 2
    # Verify elements have expected methods
    assert hasattr(quotes[0], "xpath_first")
    assert hasattr(quotes[0], "text")


async def test_xpath_extracts_text_content():
    """Test that xpath can extract text from elements."""
    html = """
    <html>
        <body>
            <div class="container">
                <h1>Title</h1>
                <p class="description">Description text</p>
            </div>
        </body>
    </html>
    """
    req = Request(url="http://example.com")
    resp = HTMLResponse(
        url=req.url,
        status=200,
        headers={},
        body=html.encode("utf-8"),
        request=req,
    )

    # Test xpath
    paragraphs = await resp.xpath("//p[@class='description']")
    assert len(paragraphs) == 1
    assert paragraphs[0].text.strip() == "Description text"

    # Test nested xpath on element
    containers = await resp.xpath("//div[@class='container']")
    assert len(containers) == 1
    h1 = containers[0].xpath_first(".//h1")
    assert h1 is not None
    assert h1.text.strip() == "Title"


async def test_xpath_first_returns_single_element():
    """Test that xpath_first() returns only the first matching element."""
    html = """
    <html>
        <body>
            <a href="/page1">Link 1</a>
            <a href="/page2">Link 2</a>
        </body>
    </html>
    """
    req = Request(url="http://example.com")
    resp = HTMLResponse(
        url=req.url,
        status=200,
        headers={},
        body=html.encode("utf-8"),
        request=req,
    )

    first_link = await resp.xpath_first("//a")

    assert first_link is not None
    assert first_link.attr("href") == "/page1"
    assert first_link.text == "Link 1"


async def test_xpath_first_returns_none_when_not_found():
    """Test that xpath_first() returns None when no match is found."""
    html = """
    <html>
        <body>
            <p>No links here</p>
        </body>
    </html>
    """
    req = Request(url="http://example.com")
    resp = HTMLResponse(
        url=req.url,
        status=200,
        headers={},
        body=html.encode("utf-8"),
        request=req,
    )

    link = await resp.xpath_first("//a")

    assert link is None


async def test_xpath_handles_attributes():
    """Test that xpath can query and extract attributes."""
    html = """
    <html>
        <body>
            <div id="main" data-value="123">
                <span class="text">Content</span>
            </div>
        </body>
    </html>
    """
    req = Request(url="http://example.com")
    resp = HTMLResponse(
        url=req.url,
        status=200,
        headers={},
        body=html.encode("utf-8"),
        request=req,
    )

    # Select by ID
    div = await resp.xpath_first("//div[@id='main']")
    assert div is not None
    assert div.attr("data-value") == "123"

    # Select by attribute
    divs = await resp.xpath("//div[@data-value='123']")
    assert len(divs) == 1


async def test_xpath_with_complex_expressions():
    """Test that xpath can handle complex expressions."""
    html = """
    <html>
        <body>
            <ul>
                <li class="item">Item 1</li>
                <li class="item">Item 2</li>
                <li class="item special">Item 3</li>
                <li class="item">Item 4</li>
            </ul>
        </body>
    </html>
    """
    req = Request(url="http://example.com")
    resp = HTMLResponse(
        url=req.url,
        status=200,
        headers={},
        body=html.encode("utf-8"),
        request=req,
    )

    # Select all items
    all_items = await resp.xpath("//li[contains(@class, 'item')]")
    assert len(all_items) == 4

    # Select specific item
    special = await resp.xpath_first("//li[contains(@class, 'special')]")
    assert special is not None
    assert "Item 3" in special.text


async def test_xpath_empty_result():
    """Test that xpath returns empty list when no matches found."""
    html = """
    <html>
        <body>
            <div>No tables here</div>
        </body>
    </html>
    """
    req = Request(url="http://example.com")
    resp = HTMLResponse(
        url=req.url,
        status=200,
        headers={},
        body=html.encode("utf-8"),
        request=req,
    )

    tables = await resp.xpath("//table")

    assert isinstance(tables, list)
    assert len(tables) == 0


async def test_xpath_respects_max_size_bytes():
    """Test that xpath methods respect doc_max_size_bytes setting."""
    html = "<html><body><p>Test</p></body></html>"
    req = Request(url="http://example.com")
    resp = HTMLResponse(
        url=req.url,
        status=200,
        headers={},
        body=html.encode("utf-8"),
        request=req,
        doc_max_size_bytes=100,
    )

    # Should work fine with small HTML
    paragraphs = await resp.xpath("//p")
    assert len(paragraphs) == 1
    assert paragraphs[0].text == "Test"


async def test_xpath_works_alongside_css():
    """Test that xpath and css methods can be used together."""
    html = """
    <html>
        <body>
            <div class="container">
                <p class="text">Paragraph 1</p>
                <p class="text">Paragraph 2</p>
            </div>
        </body>
    </html>
    """
    req = Request(url="http://example.com")
    resp = HTMLResponse(
        url=req.url,
        status=200,
        headers={},
        body=html.encode("utf-8"),
        request=req,
    )

    # Use CSS selector
    css_results = await resp.select(".text")
    assert len(css_results) == 2

    # Use XPath selector
    xpath_results = await resp.xpath("//p[@class='text']")
    assert len(xpath_results) == 2

    # Both should find the same elements (in terms of content)
    assert css_results[0].text == xpath_results[0].text
    assert css_results[1].text == xpath_results[1].text
