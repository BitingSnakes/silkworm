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
    """
    Convert HTML to Markdown.

    ``full`` uses fast-h2m's default rich converter, ``minimal`` uses the lean
    Fast DOM path, and ``mdream`` exposes the mdream-backed lean path.
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
    """
    Convert HTML to Markdown and return fast-h2m's structured result.
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
    """
    Incrementally convert HTML chunks to Markdown with fast-h2m's stream processor.
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
    stream = MarkdownStream(mode=mode, options=options)
    markdown = []
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
    stream = MarkdownStream(mode=mode, options=options)
    markdown = []
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
