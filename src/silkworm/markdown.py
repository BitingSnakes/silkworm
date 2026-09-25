"""Convert complete or streaming HTML into Markdown with ``fast-h2m``."""

from __future__ import annotations

from collections.abc import AsyncIterable, Iterable, Mapping
from typing import Literal

from .exceptions import MarkdownConversionError

type MarkdownMode = Literal["full", "minimal", "mdream"]
type MarkdownOptions = Mapping[str, object]
type MarkdownResult = dict[str, object]

_MODE_TIER_STRATEGY: dict[MarkdownMode, str | None] = {
    "full": None,
    "minimal": "fast_dom",
    "mdream": "mdream",
}


def _merged_options(
    mode: MarkdownMode,
    options: MarkdownOptions | None = None,
) -> dict[str, object] | None:
    merged = dict(options or {})
    tier_strategy = _MODE_TIER_STRATEGY[mode]
    if tier_strategy is not None:
        merged.setdefault("tier_strategy", tier_strategy)
    return merged or None


def html_to_markdown(
    html: str,
    *,
    mode: MarkdownMode = "full",
    options: MarkdownOptions | None = None,
) -> str:
    """Convert HTML to a Markdown string.

    ``full`` uses fast-h2m's default rich converter, ``minimal`` uses the lean
    Fast DOM path, and ``mdream`` exposes the mdream-backed lean path.

    Args:
        html: Complete HTML input.
        mode: Conversion strategy.
        options: Additional ``fast-h2m`` conversion options. Explicit values
            override defaults supplied by the selected mode.

    Returns:
        The converted Markdown text.

    Raises:
        ValueError: If ``mode`` is unsupported.
        MarkdownConversionError: If conversion fails.
    """
    try:
        import fast_h2m

        return fast_h2m.convert_to_markdown(
            html,
            _merged_options(mode, options),
        )
    except KeyError as exc:
        raise ValueError(f"unsupported markdown mode: {mode}") from exc
    except Exception as exc:
        raise MarkdownConversionError("HTML to Markdown conversion failed") from exc


def convert_html_to_markdown(
    html: str,
    *,
    mode: MarkdownMode = "full",
    options: MarkdownOptions | None = None,
) -> MarkdownResult:
    """Convert HTML and return ``fast-h2m``'s structured result.

    Args:
        html: Complete HTML input.
        mode: Conversion strategy.
        options: Additional ``fast-h2m`` conversion options.

    Returns:
        A mapping containing Markdown and converter metadata.

    Raises:
        ValueError: If ``mode`` is unsupported.
        MarkdownConversionError: If conversion fails or returns an invalid
            result.
    """
    try:
        import fast_h2m

        result = fast_h2m.convert(
            html,
            _merged_options(mode, options),
        )
    except KeyError as exc:
        raise ValueError(f"unsupported markdown mode: {mode}") from exc
    except Exception as exc:
        raise MarkdownConversionError("HTML to Markdown conversion failed") from exc

    if not isinstance(result, dict):
        raise MarkdownConversionError("fast-h2m returned an unsupported result")
    return result


class MarkdownStream:
    """Incrementally convert HTML chunks with ``fast-h2m``.

    Args:
        mode: Streaming conversion strategy. ``minimal`` is the default to
            favor low-overhead incremental conversion.
        options: Additional ``fast-h2m`` stream options.

    Raises:
        ValueError: If ``mode`` is unsupported.
        MarkdownConversionError: If the stream processor cannot be created.
    """

    def __init__(
        self,
        *,
        mode: MarkdownMode = "minimal",
        options: MarkdownOptions | None = None,
    ) -> None:
        try:
            import fast_h2m

            self._processor = fast_h2m.MarkdownStreamProcessor(
                _merged_options(mode, options),
            )
        except KeyError as exc:
            raise ValueError(f"unsupported markdown mode: {mode}") from exc
        except Exception as exc:
            raise MarkdownConversionError(
                "HTML to Markdown stream initialization failed",
            ) from exc

    def process_chunk(self, html: str) -> str:
        """Feed one HTML chunk to the converter and return available Markdown.

        Raises:
            MarkdownConversionError: If conversion fails or returns a
                non-string value.
        """
        try:
            markdown = self._processor.process_chunk(html)
        except Exception as exc:
            raise MarkdownConversionError(
                "HTML to Markdown stream chunk conversion failed",
            ) from exc
        if not isinstance(markdown, str):
            raise MarkdownConversionError("fast-h2m returned an unsupported chunk")
        return markdown

    def finish(self) -> str:
        """Finalize the stream and return any buffered Markdown.

        Raises:
            MarkdownConversionError: If finalization fails or returns a
                non-string value.
        """
        try:
            markdown = self._processor.finish()
        except Exception as exc:
            raise MarkdownConversionError(
                "HTML to Markdown stream finalization failed",
            ) from exc
        if not isinstance(markdown, str):
            raise MarkdownConversionError("fast-h2m returned an unsupported chunk")
        return markdown


def stream_html_to_markdown(
    chunks: Iterable[str],
    *,
    mode: MarkdownMode = "minimal",
    options: MarkdownOptions | None = None,
) -> str:
    """Convert an iterable of HTML chunks and concatenate all Markdown output.

    Args:
        chunks: Ordered HTML fragments.
        mode: Streaming conversion strategy.
        options: Additional ``fast-h2m`` stream options.

    Returns:
        Markdown emitted while consuming and finalizing the stream.
    """
    stream = MarkdownStream(mode=mode, options=options)
    markdown: list[str] = []
    for chunk in chunks:
        markdown.append(stream.process_chunk(chunk))
    markdown.append(stream.finish())
    return "".join(markdown)


async def stream_html_to_markdown_async(
    chunks: AsyncIterable[str],
    *,
    mode: MarkdownMode = "minimal",
    options: MarkdownOptions | None = None,
) -> str:
    """Convert an async iterable of HTML chunks into one Markdown string.

    Args:
        chunks: Asynchronously produced, ordered HTML fragments.
        mode: Streaming conversion strategy.
        options: Additional ``fast-h2m`` stream options.

    Returns:
        Markdown emitted while consuming and finalizing the stream.
    """
    stream = MarkdownStream(mode=mode, options=options)
    markdown: list[str] = []
    async for chunk in chunks:
        markdown.append(stream.process_chunk(chunk))
    markdown.append(stream.finish())
    return "".join(markdown)


__all__ = [
    "MarkdownMode",
    "MarkdownOptions",
    "MarkdownResult",
    "MarkdownStream",
    "convert_html_to_markdown",
    "html_to_markdown",
    "stream_html_to_markdown",
    "stream_html_to_markdown_async",
]
