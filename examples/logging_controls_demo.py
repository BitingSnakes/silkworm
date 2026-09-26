"""
Make Silkworm's logs quieter and keep secrets (like API tokens) out of them.

Two common logging problems:

1. Too many logs. With `SILKWORM_LOG_LEVEL=DEBUG`, every single item writes a
   log line. With thousands of items, the useful lines get lost.
2. Secrets in logs. If a URL contains a token (`?access_token=...`), the
   default "Fetched response" log line prints it.

This demo runs the same spider in two modes so you can compare:

    --mode noisy   default logging: a line per item, full URL (with the token!)
    --mode quiet   per-item logs turned off, and the URL hidden

It doesn't use the internet: the "API response" is faked with
MOCK_RESPONSE_META_KEY.

How to run:
    SILKWORM_LOG_LEVEL=DEBUG python examples/logging_controls_demo.py --mode noisy
    SILKWORM_LOG_LEVEL=DEBUG python examples/logging_controls_demo.py --mode quiet
"""

from __future__ import annotations

import argparse
import json
from typing import override

from silkworm import EngineLogger, Request, Response, Spider, run_spider
from silkworm.http import MOCK_RESPONSE_META_KEY
from silkworm.pipelines import ItemPipeline, JsonLinesPipeline


class RedactedEngineLogger(EngineLogger):
    """
    Customize ONE of the engine's log messages.

    `EngineLogger` has one method per kind of engine log message. Here we
    override `fetched_response` so it logs a fixed endpoint name instead of
    the full URL (which contains the secret token).
    """

    @override
    def fetched_response(
        self,
        logger,
        request: Request,
        response: Response,
        spider: Spider,
    ) -> None:
        logger.debug(
            "Fetched response",
            status=response.status,
            spider=spider.name,
            endpoint="api.example.test/items",  # No token here.
        )


class TokenizedApiSpider(Spider):
    """Pretends to call a JSON API and yields every item it returns."""

    name = "tokenized_api_logging"

    def __init__(self, *, item_count: int = 100, **kwargs) -> None:
        super().__init__(**kwargs)
        self.item_count = item_count

    async def start_requests(self):
        # Build a fake API response with `item_count` items.
        items = []
        for index in range(self.item_count):
            items.append(
                {
                    "id": index,
                    "title": f"Item {index}",
                    "description": "large-ish payload field" * 5,
                }
            )
        fake_body = json.dumps({"items": items})

        yield Request(
            # Note the secret token in the URL.
            url="https://api.example.test/items?access_token=secret-token",
            callback=self.parse_api,
            # Return this fake response instead of really downloading the URL.
            meta={
                MOCK_RESPONSE_META_KEY: {
                    "status": 200,
                    "headers": {"content-type": "application/json"},
                    "body": fake_body,
                }
            },
        )

    async def parse_api(self, response: Response):
        data = json.loads(response.text)
        for item in data.get("items", []):
            yield item


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Compare noisy logging with the quieter logging controls."
    )
    parser.add_argument(
        "--mode",
        choices=("noisy", "quiet"),
        default="quiet",
        help="Which logging setup to use.",
    )
    parser.add_argument(
        "--items",
        type=int,
        default=100,
        help="Number of fake API items to emit.",
    )
    args = parser.parse_args()
    output_path = f"data/logging_controls_{args.mode}.jl"

    engine_logger: EngineLogger | None
    pipelines: list[ItemPipeline]

    if args.mode == "noisy":
        print(
            "Noisy mode: every item writes debug logs, and the response log "
            "shows the full URL including the token."
        )
        engine_logger = None  # None = use the default EngineLogger.
        pipelines = [JsonLinesPipeline(output_path, use_opendal=False)]
    else:
        print(
            "Quiet mode: per-item logs are turned off and the response log "
            "hides the URL."
        )
        # item_pipeline_level=None turns off the engine's per-item log line.
        engine_logger = RedactedEngineLogger(item_pipeline_level=None)
        # log_level=None turns off the pipeline's own per-item log line.
        pipelines = [JsonLinesPipeline(output_path, use_opendal=False, log_level=None)]

    run_spider(
        TokenizedApiSpider(item_count=args.items),
        item_pipelines=pipelines,
        engine_logger=engine_logger,
        concurrency=1,
    )
    print(f"Wrote {args.items} items to {output_path}")


if __name__ == "__main__":
    main()
