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


def test_env_log_level_filters_debug(
    monkeypatch: pytest.MonkeyPatch, recording_logger: _RecordingLogger
) -> None:
    monkeypatch.setenv("SILKWORM_LOG_LEVEL", "INFO")

    logging_mod.get_logger()

    configured = recording_logger.configured_kwargs
    assert configured is not None
    levels = configured["console_levels"]
    assert isinstance(levels, dict)
    assert levels["TRACE"] is False
    assert levels["DEBUG"] is False
    assert levels["INFO"] is True
    assert levels["ERROR"] is True
    assert configured["level"] == "INFO"


def test_invalid_env_level_defaults_to_info(
    monkeypatch: pytest.MonkeyPatch, recording_logger: _RecordingLogger
) -> None:
    monkeypatch.setenv("SILKWORM_LOG_LEVEL", "nope")

    logging_mod.get_logger()

    configured = recording_logger.configured_kwargs
    assert configured is not None
    assert configured["level"] == "INFO"
