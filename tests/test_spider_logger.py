"""Tests for spider logger configuration."""

import pytest

from silkworm.spiders import Spider
import silkworm.logging as logging_mod


class _RecordingLogger:
    """Mock logger for testing."""

    def __init__(self) -> None:
        self.configured_kwargs: dict[str, object] | None = None
        self.bound_context: dict[str, object] | None = None

    def configure(self, **kwargs: object) -> None:
        self.configured_kwargs = kwargs

    def bind(self, **context: object) -> "_RecordingLogger":
        self.bound_context = context
        return self

    def info(self, *args: object, **kwargs: object) -> None: ...
    def debug(self, *args: object, **kwargs: object) -> None: ...
    def warning(self, *args: object, **kwargs: object) -> None: ...
    def error(self, *args: object, **kwargs: object) -> None: ...
    def complete(self) -> None: ...


@pytest.fixture
def recording_logger(monkeypatch: pytest.MonkeyPatch) -> _RecordingLogger:
    """
    Provide a mock logger for testing.

    Resets module-level globals (_configured and _typed_logger) to ensure
    each test gets a fresh logger configuration state and prevent test
    interference.
    """
    logger = _RecordingLogger()
    # Reset module globals so we reconfigure for each test
    monkeypatch.setattr(logging_mod, "_configured", False)
    monkeypatch.setattr(logging_mod, "_typed_logger", logger)
    return logger


def test_spider_logger_defaults_to_none():
    """Test that logger is None by default."""
    spider = Spider()
    assert spider.logger is None


def test_spider_logger_with_dict_context(recording_logger: _RecordingLogger):
    """Test that logger can be configured with a dict context."""
    spider = Spider(logger={"component": "test_spider", "custom_key": "value"})

    assert spider.logger is not None
    assert spider.logger is recording_logger
    assert recording_logger.bound_context == {
        "component": "test_spider",
        "custom_key": "value",
    }


def test_spider_logger_with_logger_instance():
    """Test that logger can be passed as an instance."""
    custom_logger = _RecordingLogger()
    spider = Spider(logger=custom_logger)

    assert spider.logger is custom_logger


def test_spider_logger_preserved_with_other_params(recording_logger: _RecordingLogger):
    """Test that logger works alongside other Spider parameters."""
    spider = Spider(
        name="custom_spider",
        start_urls=["https://example.com"],
        custom_settings={"key": "value"},
        logger={"component": "my_component"},
    )

    assert spider.name == "custom_spider"
    assert spider.start_urls == ("https://example.com",)
    assert spider.custom_settings == {"key": "value"}
    assert spider.logger is not None
    assert recording_logger.bound_context == {"component": "my_component"}


def test_spider_subclass_can_override_logger():
    """Test that spider subclasses can still manually set logger."""

    class CustomSpider(Spider):
        def __init__(self, **kwargs) -> None:
            super().__init__(**kwargs)
            # Subclass can still override logger if desired
            if self.logger is None:
                self.logger = _RecordingLogger()

    spider = CustomSpider()
    assert spider.logger is not None
    assert isinstance(spider.logger, _RecordingLogger)


def test_spider_subclass_respects_passed_logger():
    """Test that passed logger is respected even in subclasses."""

    class CustomSpider(Spider):
        def __init__(self, **kwargs) -> None:
            super().__init__(**kwargs)
            # Subclass can add additional setup after super().__init__
            pass

    custom_logger = _RecordingLogger()
    spider = CustomSpider(logger=custom_logger)
    assert spider.logger is custom_logger
