# `silkworm`

```{eval-rst}
.. automodule:: silkworm
   :no-members:
```

## Spiders

```{eval-rst}
.. autoclass:: silkworm.Spider
```

## Requests and Responses

```{eval-rst}
.. autoclass:: silkworm.Request

.. autoclass:: silkworm.Response

.. autoclass:: silkworm.HTMLResponse
```

## Engine

```{eval-rst}
.. autoclass:: silkworm.Engine

.. autoclass:: silkworm.EngineOptions

.. autoclass:: silkworm.EngineLogger

.. autofunction:: silkworm.default_dedup_key

.. autotype:: silkworm.DedupKey
```

## Runners

```{eval-rst}
.. autofunction:: silkworm.crawl

.. autofunction:: silkworm.run_spider

.. autofunction:: silkworm.run_spider_rsloop

.. autofunction:: silkworm.run_spider_uvloop

.. autofunction:: silkworm.run_spider_winloop

.. autofunction:: silkworm.run_spider_trio
```

## Convenience Helpers

```{eval-rst}
.. autofunction:: silkworm.fetch_html

.. autofunction:: silkworm.fetch_html_cdp

.. autofunction:: silkworm.fetch_html_servo
```

## Client Adapters

```{eval-rst}
.. autoclass:: silkworm.http.HttpClient

.. autodata:: silkworm.http.MOCK_RESPONSE_META_KEY
   :no-value:

   Request ``meta`` key holding a ``{"status", "headers", "body", "url"}`` mapping
   that :class:`~silkworm.http.HttpClient` returns instead of making a network call.

.. autoclass:: silkworm.CDPClient

.. autoclass:: silkworm.ServoFetchClient

.. autodata:: silkworm.servo.SERVO_JAVASCRIPT_META_KEY
.. autodata:: silkworm.servo.SERVO_SETTLE_MS_META_KEY
.. autodata:: silkworm.servo.SERVO_USER_AGENT_META_KEY
.. autodata:: silkworm.servo.SERVO_SCREENSHOT_META_KEY
.. autodata:: silkworm.servo.SERVO_FULL_PAGE_META_KEY

.. autoclass:: silkworm.OnionLinkClient
```
