from __future__ import annotations

from typing import Any, TYPE_CHECKING

try:
    from taskiq import AsyncBroker  # type: ignore[import-not-found]

    TASKIQ_AVAILABLE = True
except ImportError:
    AsyncBroker = None  # type: ignore
    TASKIQ_AVAILABLE = False

from ..logging import get_logger
from .base import _log_pipeline_item

if TYPE_CHECKING:
    from taskiq import AsyncBroker as _AsyncBroker  # type: ignore[import-not-found]
    from taskiq.decor import AsyncTaskiqDecoratedTask  # type: ignore[import-not-found]

    from .._types import JSONValue
    from ..spiders import Spider

    type _TaskiqTask = AsyncTaskiqDecoratedTask[Any, Any]


class TaskiqPipeline:
    """
    Pipeline that sends scraped items to a Taskiq broker/queue instead of writing to a file.

    This allows you to process items asynchronously with Taskiq workers, enabling
    distributed processing, retries, and other Taskiq features.

    Example:
        from taskiq import InMemoryBroker
        from silkworm.pipelines import TaskiqPipeline

        broker = InMemoryBroker()

        @broker.task
        async def process_item(item):
            # Your item processing logic here
            print(f"Processing: {item}")

        pipeline = TaskiqPipeline(broker, task=process_item)
        # Or: pipeline = TaskiqPipeline(broker, task_name=".:process_item")
    """

    def __init__(
        self,
        broker: _AsyncBroker,
        task: _TaskiqTask | None = None,
        task_name: str | None = None,
    ) -> None:
        """
        Initialize TaskiqPipeline.

        Args:
            broker: A Taskiq AsyncBroker instance (e.g., InMemoryBroker, RedisBroker)
            task: A decorated task function (created with @broker.task). If provided, task_name is ignored.
            task_name: Full name of the task registered on the broker (e.g., ".:process_item").
                      Either task or task_name must be provided.
        """
        if not TASKIQ_AVAILABLE:
            raise ImportError(
                "taskiq is required for TaskiqPipeline. Install it with: pip install taskiq",
            )
        if task is None and task_name is None:
            raise ValueError("Either 'task' or 'task_name' must be provided")

        self.broker: _AsyncBroker = broker
        self._provided_task: _TaskiqTask | None = task
        self._task: _TaskiqTask | None = None
        self.task_name = task_name
        self.logger = get_logger(component="TaskiqPipeline")

    async def open(self, spider: Spider) -> None:
        """Open the pipeline and start the broker if needed."""
        await self.broker.startup()

        # If task was provided directly, use it
        if self._provided_task is not None:
            self._task = self._provided_task
            actual_task_name = self._provided_task.task_name
        else:
            # Find the registered task by name
            if self.task_name is None:
                raise ValueError("task_name cannot be None when task is not provided")
            self._task = self.broker.find_task(self.task_name)
            if self._task is None:
                raise ValueError(
                    f"Task '{self.task_name}' not found in broker. "
                    f"Make sure you've registered it with @broker.task and use the full task name (e.g., '.:task_name')",
                )
            actual_task_name = self.task_name

        self.logger.info(
            "Opened Taskiq pipeline",
            task_name=actual_task_name,
            broker=self.broker.__class__.__name__,
        )

    async def close(self, spider: Spider) -> None:
        """Close the pipeline and shutdown the broker."""
        await self.broker.shutdown()
        task_name = (
            self._task.task_name
            if self._task is not None
            else self.task_name
            if self.task_name is not None
            else "unknown"
        )
        self.logger.info("Closed Taskiq pipeline", task_name=task_name)

    async def process_item(self, item: JSONValue, spider: Spider) -> JSONValue:
        """Send the item to the Taskiq broker for processing."""
        if self._task is None:
            raise RuntimeError("TaskiqPipeline not opened")

        # Send item to the task queue
        task_result = await self._task.kiq(item)
        task_name = self._task.task_name
        task_id = task_result.task_id
        _log_pipeline_item(
            self,
            "Sent item to Taskiq queue",
            task_name=task_name,
            task_id=task_id or "unknown",
            spider=spider.name,
        )
        return item
