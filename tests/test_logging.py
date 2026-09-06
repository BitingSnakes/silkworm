import io
import json
import logging

import pytest

import silkworm.logging as logging_mod


class _RecordingLogger:
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
    logger = _RecordingLogger()
    # Reset module globals so we reconfigure for each test
    monkeypatch.setattr(logging_mod, "_configured", False)
    monkeypatch.setattr(logging_mod, "_typed_logger", logger)
    return logger


def test_env_log_level_configures_stderr_handler(
    monkeypatch: pytest.MonkeyPatch, recording_logger: _RecordingLogger
) -> None:
    monkeypatch.setenv("SILKWORM_LOG_LEVEL", "INFO")

    logging_mod.get_logger()

    configured = recording_logger.configured_kwargs
    assert configured is not None
    assert configured["handlers"] == [{"sink": "stderr", "level": "INFO"}]


def test_invalid_env_level_defaults_to_info(
    monkeypatch: pytest.MonkeyPatch, recording_logger: _RecordingLogger
) -> None:
    monkeypatch.setenv("SILKWORM_LOG_LEVEL", "nope")

    logging_mod.get_logger()

    configured = recording_logger.configured_kwargs
    assert configured is not None
    assert configured["handlers"] == [{"sink": "stderr", "level": "INFO"}]


def test_adapter_preserves_bound_and_call_context() -> None:
    stream = io.StringIO()
    adapter = logging_mod._LoggerAdapter(logging.getLogger("silkworm.test.context"))
    adapter.configure(handlers=[{"sink": stream, "level": "INFO"}])

    adapter.bind(component="engine").info("Request completed", status=200)

    output = stream.getvalue()
    assert "INFO | Request completed" in output
    assert "component='engine'" in output
    assert "status=200" in output


def test_adapter_writes_structured_json() -> None:
    stream = io.StringIO()
    adapter = logging_mod._LoggerAdapter(logging.getLogger("silkworm.test.json"))
    adapter.configure(handlers=[{"sink": stream, "level": "DEBUG", "serialize": True}])

    adapter.bind(component="http").debug("Fetching", url="https://example.com")

    record = json.loads(stream.getvalue())
    assert record["level"] == "DEBUG"
    assert record["message"] == "Fetching"
    assert record["component"] == "http"
    assert record["url"] == "https://example.com"
    assert "timestamp" in record


def test_adapter_filters_messages_below_handler_level() -> None:
    stream = io.StringIO()
    adapter = logging_mod._LoggerAdapter(logging.getLogger("silkworm.test.level"))
    adapter.configure(handlers=[{"sink": stream, "level": "WARNING"}])

    adapter.info("Hidden")
    adapter.warning("Visible")

    output = stream.getvalue()
    assert "Hidden" not in output
    assert "WARNING | Visible" in output
