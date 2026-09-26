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

.. autoclass:: silkworm.CDPClient

.. autoclass:: silkworm.ServoFetchClient

.. autoclass:: silkworm.OnionLinkClient
```
