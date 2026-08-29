from __future__ import annotations

from typing import TYPE_CHECKING, Any, cast
from urllib.parse import urljoin

import pytest

from silkworm.declarative import (
    Attr,
    DeclarativeConfigurationError,
    DeclarativeSerializationError,
    FieldTransformError,
    Item,
    MissingFieldError,
    Text,
)

if TYPE_CHECKING:
    from silkworm import HTMLResponse


class FakeElement:
    def __init__(
        self,
        *,
        text: str = "",
        attrs: dict[str, str] | None = None,
        matches: dict[str, list[FakeElement]] | None = None,
    ) -> None:
        self.text = text
        self._attrs = attrs or {}
        self._matches = matches or {}

    def attr(self, name: str) -> str | None:
        return self._attrs.get(name)

    async def select(self, selector: str) -> list[FakeElement]:
        return list(self._matches.get(selector, []))

    async def select_first(self, selector: str) -> FakeElement | None:
        matches = self._matches.get(selector, [])
        return matches[0] if matches else None


class FakeResponse(FakeElement):
    def __init__(
        self,
        *,
        url: str,
        matches: dict[str, list[FakeElement]],
    ) -> None:
        super().__init__(matches=matches)
        self.url = url

    def url_join(self, href: str) -> str:
        return urljoin(self.url, href)


def as_html_response(response: FakeResponse) -> HTMLResponse:
    return cast("HTMLResponse", response)


async def test_extracts_scalar_optional_list_absolute_and_transformed_fields():
    def parse_price(value: str) -> float:
        return float(value.removeprefix("$").strip())

    class Product(Item):
        __selector__ = ".product"

        title: str = Text("h2", strip=True)
        price: float = Text(".price", transform=parse_price)
        url: str = Attr("a", "href", absolute=True)
        image: str | None = Attr("img", "src", absolute=True)
        tags: list[str] = Text(".tag")

        async def after_extract(self, response: HTMLResponse) -> None:
            self.title = self.title.upper()

    first = FakeElement(
        matches={
            "h2": [FakeElement(text="  Keyboard  ")],
            ".price": [FakeElement(text="$99.50")],
            "a": [FakeElement(attrs={"href": "../products/keyboard"})],
            "img": [FakeElement(attrs={"src": "/images/keyboard.png"})],
            ".tag": [FakeElement(text="hardware"), FakeElement(text="usb")],
        }
    )
    second = FakeElement(
        matches={
            "h2": [FakeElement(text="Mouse")],
            ".price": [FakeElement(text="$20")],
            "a": [FakeElement(attrs={"href": "/products/mouse"})],
        }
    )
    response = FakeResponse(
        url="https://shop.example.com/category/electronics/",
        matches={".product": [first, second]},
    )

    products = [
        product async for product in Product.extract(as_html_response(response))
    ]

    assert products == [
        Product(
            title="KEYBOARD",
            price=99.5,
            url="https://shop.example.com/category/products/keyboard",
            image="https://shop.example.com/images/keyboard.png",
            tags=["hardware", "usb"],
        ),
        Product(
            title="MOUSE",
            price=20.0,
            url="https://shop.example.com/products/mouse",
            image=None,
            tags=[],
        ),
    ]


async def test_item_without_root_selector_extracts_from_response():
    class PageMetadata(Item):
        title: str = Text("title")

    response = FakeResponse(
        url="https://example.com/",
        matches={"title": [FakeElement(text="Example")]},
    )

    results = [item async for item in PageMetadata.extract(as_html_response(response))]

    assert results == [PageMetadata(title="Example")]


async def test_scalar_default_is_not_transformed():
    calls: list[str] = []

    def transform(value: str) -> str:
        calls.append(value)
        return value.upper()

    class Article(Item):
        author: str = Text(".author", default="anonymous", transform=transform)

    response = FakeResponse(url="https://example.com/", matches={})

    articles = [item async for item in Article.extract(as_html_response(response))]

    assert articles[0].author == "anonymous"
    assert calls == []


async def test_list_attribute_can_declare_optional_members():
    class Links(Item):
        urls: list[str | None] = Attr("a", "href")

    response = FakeResponse(
        url="https://example.com/",
        matches={"a": [FakeElement(attrs={"href": "/one"}), FakeElement()]},
    )

    results = [item async for item in Links.extract(as_html_response(response))]

    assert results[0].urls == ["/one", None]


async def test_required_missing_field_reports_item_field_selector_and_url():
    class Article(Item):
        title: str = Text("h1")

    response = FakeResponse(url="https://example.com/news/1", matches={})

    with pytest.raises(MissingFieldError) as excinfo:
        _ = [item async for item in Article.extract(as_html_response(response))]

    message = str(excinfo.value)
    assert "Article.title" in message
    assert "selector 'h1'" in message
    assert "https://example.com/news/1" in message
    assert "root 0" in message


async def test_required_list_attribute_reports_match_index():
    class Links(Item):
        urls: list[str] = Attr("a", "href")

    response = FakeResponse(
        url="https://example.com/",
        matches={"a": [FakeElement(attrs={"href": "/one"}), FakeElement()]},
    )

    with pytest.raises(MissingFieldError, match=r"Links\.urls.*match 1"):
        _ = [item async for item in Links.extract(as_html_response(response))]


@pytest.mark.parametrize(
    ("transform", "expected"),
    [
        (lambda value: int(value), "invalid literal"),
        (lambda value: value, "extracted str, expected int"),
    ],
)
async def test_transform_failures_have_field_context(transform, expected: str):
    class Score(Item):
        value: int = cast("int", Text(".score", transform=transform))

    response = FakeResponse(
        url="https://example.com/scores",
        matches={".score": [FakeElement(text="not-a-number")]},
    )

    with pytest.raises(FieldTransformError) as excinfo:
        _ = [item async for item in Score.extract(as_html_response(response))]

    assert "Score.value" in str(excinfo.value)
    assert expected in str(excinfo.value)


async def test_async_transform_is_rejected():
    async def transform(value: str) -> str:
        return value

    class Article(Item):
        title: str = Text("h1", transform=transform)

    response = FakeResponse(
        url="https://example.com/",
        matches={"h1": [FakeElement(text="Title")]},
    )

    with pytest.raises(FieldTransformError, match="async transforms are not supported"):
        _ = [item async for item in Article.extract(as_html_response(response))]


def test_extraction_plan_is_compiled_once_and_preserves_field_order():
    class BaseArticle(Item):
        title: str = Text("h1")

    class Article(BaseArticle):
        author: str | None = Text(".author")

    first = Article.extraction_plan()
    second = Article.extraction_plan()

    assert first is second
    assert first.root_selector is None
    assert [field.name for field in first.fields] == ["title", "author"]


@pytest.mark.parametrize(
    "item_factory",
    [
        pytest.param(
            lambda: type("NoFields", (Item,), {"__annotations__": {}}),
            id="no-fields",
        ),
        pytest.param(
            lambda: type(
                "Untyped",
                (Item,),
                {"value": Text(".value"), "__annotations__": {}},
            ),
            id="untyped-field",
        ),
        pytest.param(
            lambda: type(
                "OptionalList",
                (Item,),
                {
                    "value": Text(".value"),
                    "__annotations__": {"value": list[str] | None},
                },
            ),
            id="optional-list",
        ),
        pytest.param(
            lambda: type(
                "NeedsTransform",
                (Item,),
                {
                    "value": Text(".value"),
                    "__annotations__": {"value": int},
                },
            ),
            id="needs-transform",
        ),
        pytest.param(
            lambda: type(
                "BadDefault",
                (Item,),
                {
                    "value": Text(".value", default=cast("Any", 1)),
                    "__annotations__": {"value": str},
                },
            ),
            id="bad-default",
        ),
    ],
)
def test_invalid_declarations_fail_when_plan_is_first_requested(item_factory):
    item_type = item_factory()

    with pytest.raises(DeclarativeConfigurationError):
        item_type.extraction_plan()


def test_to_dict_recursively_serializes_items_mappings_and_sequences():
    class Child(Item):
        name: str = Text(".name")

    class Container(Item):
        payload: object = Text(".payload")

    item = Container(
        payload={
            "children": [Child(name="Ada"), Child(name="Lin")],
            "coordinates": (1, 2),
        }
    )

    assert item.to_dict() == {
        "payload": {
            "children": [{"name": "Ada"}, {"name": "Lin"}],
            "coordinates": [1, 2],
        }
    }


def test_to_dict_rejects_non_json_values_with_their_path():
    class Container(Item):
        payload: object = Text(".payload")

    item = Container(payload={"bad": {1, 2}})

    with pytest.raises(DeclarativeSerializationError, match=r"Container\.payload\.bad"):
        item.to_dict()
