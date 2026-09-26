# Declarative Extraction

`silkworm.declarative` lets you *describe* the data on a page instead of writing
selector code for every field. Use it when the extraction is regular, and keep
ordinary callbacks for pagination or site-specific behavior. See the
[declarative API reference](api/declarative.md) for the full API.

## Defining Items
Subclass `Item`, set `__selector__` to the CSS selector of each record's root
element, and assign `Text` or `Attr` fields:

```python
from silkworm import HTMLResponse, Response, Spider
from silkworm.declarative import Attr, Item, Text


def parse_price(value: str) -> float:
    return float(value.removeprefix("$").strip())


class Product(Item):
    __selector__ = ".product"

    title: str = Text("h2", strip=True)
    price: float = Text(".price", transform=parse_price)
    url: str = Attr("a", "href", absolute=True)
    image: str | None = Attr("img", "src", absolute=True)
    tags: list[str] = Text(".tag")


class ProductsSpider(Spider):
    start_urls = ("https://shop.example.com/products/",)

    async def parse(self, response: Response):
        if not isinstance(response, HTMLResponse):
            return

        async for product in Product.extract(response):
            # Existing pipelines consume JSON-compatible values.
            yield product.to_dict()
```

- **`Text(selector)`** reads the text of the matching element.
- **`Attr(selector, attribute)`** reads an HTML attribute of the matching element.
- **`Item.extract(response)`** yields one item per element matching `__selector__`.
- **`item.to_dict()`** returns a JSON-compatible dict for pipelines.

## Cardinality From Annotations
The field annotation controls how the selector is evaluated:

| Annotation | Selector operation | Missing result |
| --- | --- | --- |
| `T` | `select_first()` | raises `MissingFieldError` |
| `T \| None` | `select_first()` | `None` |
| `list[T]` | `select()` | `[]` |

## Field Options
- **`default=...`** supplies the final scalar value when an element or attribute is missing.
- **`transform=...`** applies a synchronous conversion to each extracted string. Annotations are not coerced implicitly.
- **`Text(..., strip=True)`** strips surrounding whitespace before the transform.
- **`Attr(..., absolute=True)`** resolves the attribute through `response.url_join()`.

## Post-processing
Plans are compiled from annotations once per `Item` class and then cached.
Override `after_extract()` for the irregular part of an otherwise declarative
extraction:

```python
class Article(Item):
    title: str = Text("h1")

    async def after_extract(self, response: HTMLResponse) -> None:
        self.title = self.title.strip()
```

## Inspecting Plans
`Item.extraction_plan()` returns the compiled, cached `ExtractionPlan` for a class:
its `item_type`, `root_selector`, and a tuple of `FieldPlan` objects (`name`, `field`,
`cardinality`, `value_type`, `annotation`) in declaration order. Invalid declarations
raise `DeclarativeConfigurationError` when the plan is first compiled.

```python
plan = Product.extraction_plan()
for field_plan in plan.fields:
    print(field_plan.name, field_plan.cardinality, field_plan.field.selector)
```

`Field` is the shared base of `Text` and `Attr` (arguments: `selector`, `transform`, `default`) and is the extension point for custom field kinds.

## Errors
All declarative errors derive from `DeclarativeError` (a `SilkwormError`):

- **`DeclarativeConfigurationError`**: the `Item` class declaration is invalid.
- **`FieldExtractionError`**: base class for per-field extraction failures:
  - **`MissingFieldError`**: a required field matched nothing.
  - **`FieldTransformError`**: a `transform` raised.
- **`DeclarativeSerializationError`**: a value cannot be converted by `to_dict()`.

Extraction errors include the item, field, selector, response URL, and root index.

`Item.extract()` intentionally does not replace `Spider`, callbacks, requests,
middlewares, or pipelines. See
[examples/declarative_quotes_spider.py](https://github.com/BitingSnakes/silkworm/blob/main/examples/declarative_quotes_spider.py)
for a complete spider with pagination.
