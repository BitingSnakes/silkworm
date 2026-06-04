from __future__ import annotations

from silkworm import (
    HTMLResponse,
    Request,
    convert_html_to_markdown,
    html_to_markdown,
    stream_html_to_markdown,
    stream_html_to_markdown_async,
)


HTML = """
<html>
  <head><title>Example Page</title></head>
  <body>
    <article>
      <h1>Hello</h1>
      <p>This is <strong>fast</strong> conversion.</p>
      <ul>
        <li>Rust backed</li>
        <li>Markdown output</li>
      </ul>
    </article>
  </body>
</html>
"""


def test_html_to_markdown_uses_full_mode_by_default() -> None:
    markdown = html_to_markdown(HTML)

    assert "title: Example Page" in markdown
    assert "# Hello" in markdown
    assert "**fast**" in markdown
    assert "Rust backed" in markdown


def test_html_to_markdown_supports_minimal_mode() -> None:
    markdown = html_to_markdown(HTML, mode="minimal")

    assert "# Hello" in markdown
    assert "**fast**" in markdown
    assert "title: Example Page" not in markdown


def test_convert_html_to_markdown_returns_structured_result() -> None:
    result = convert_html_to_markdown(HTML)

    assert "# Hello" in str(result["content"])
    metadata = result["metadata"]
    assert isinstance(metadata, dict)
    document = metadata["document"]
    assert isinstance(document, dict)
    assert document["title"] == "Example Page"


def test_stream_html_to_markdown_converts_chunks() -> None:
    markdown = stream_html_to_markdown(
        [
            "<article><h1>Hello</h1>",
            "<p>World</p></article>",
        ],
    )

    assert "# Hello" in markdown
    assert "World" in markdown


async def test_stream_html_to_markdown_async_converts_chunks() -> None:
    async def chunks():
        for chunk in ("<h1>Hello</h1>", "<p>World</p>"):
            yield chunk

    markdown = await stream_html_to_markdown_async(chunks())

    assert "# Hello" in markdown
    assert "World" in markdown


async def test_html_response_to_markdown_helpers() -> None:
    request = Request(url="https://example.com")
    response = HTMLResponse(
        url=request.url,
        status=200,
        headers={"content-type": "text/html; charset=utf-8"},
        body=HTML.encode(),
        request=request,
    )

    markdown = await response.to_markdown(mode="minimal")
    result = await response.to_markdown_result()

    assert "# Hello" in markdown
    assert "**fast**" in markdown
    assert "# Hello" in str(result["content"])
