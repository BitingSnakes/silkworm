from __future__ import annotations

import argparse
import json
from typing import override

from silkworm import EngineLogger, Response, Spider, run_spider
from silkworm.http import MOCK_RESPONSE_META_KEY
from silkworm.pipelines import JsonLinesPipeline
from silkworm.request import CallbackOutput, Request


class RedactedEngineLogger(EngineLogger):
    """
    Keep response status logging, but avoid writing tokenized URLs to logs.
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
            endpoint="api.example.test/items",
        )


class TokenizedApiSpider(Spider):
    name = "tokenized_api_logging"

    def __init__(self, *, item_count: int = 100, **kwargs) -> None:
        super().__init__(**kwargs)
        self.item_count = item_count

    async def start_requests(self):
        payload = {
            "items": [
                {
                    "id": index,
                    "title": f"Item {index}",
                    "description": "large-ish payload field" * 5,
                }
                for index in range(self.item_count)
            ]
        }
        yield Request(
            url="https://api.example.test/items?access_token=secret-token",
            callback=self.parse_api,
            meta={
                MOCK_RESPONSE_META_KEY: {
                    "status": 200,
                    "headers": {"content-type": "application/json"},
                    "body": json.dumps(payload),
                }
            },
        )

    async def parse_api(self, response: Response) -> CallbackOutput:
        payload = json.loads(response.text)
        items = payload.get("items", [])
        if not isinstance(items, list):
            return

        for item in items:
            yield item


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Show noisy item pipeline logs and the quieter logging controls."
    )
    parser.add_argument(
        "--mode",
        choices=("noisy", "quiet"),
        default="quiet",
        help="Run the original noisy configuration or the resolved quieter setup.",
    )
    parser.add_argument(
        "--items",
        type=int,
        default=100,
        help="Number of synthetic API items to emit.",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    output_path = f"data/logging_controls_{args.mode}.jl"

    if args.mode == "noisy":
        print(
            "Noisy mode: with SILKWORM_LOG_LEVEL=DEBUG, every item emits engine "
            "and pipeline debug logs; the INFO response log includes the tokenized URL."
        )
        engine_logger = None
        pipelines = [JsonLinesPipeline(output_path, use_opendal=False)]
    else:
        print(
            "Quiet mode: per-item pipeline logs are suppressed and response logging "
            "is redacted/demoted through EngineLogger."
        )
        engine_logger = RedactedEngineLogger(item_pipeline_level=None)
        pipelines = [
            JsonLinesPipeline(output_path, use_opendal=False, log_level=None),
        ]

    run_spider(
        TokenizedApiSpider,
        item_pipelines=pipelines,
        engine_logger=engine_logger,
        concurrency=1,
        item_count=args.items,
    )
    print(f"Wrote {args.items} items to {output_path}")


if __name__ == "__main__":
    main()
