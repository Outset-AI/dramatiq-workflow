import logging
import time
from uuid import uuid4

import dramatiq
import dramatiq.rate_limits

from ._constants import CALLBACK_BARRIER_TTL, OPTION_KEY_CALLBACKS
from ._helpers import workflow_with_completion_callbacks
from ._middleware import WorkflowMiddleware, workflow_noop
from ._models import Chain, Group, Message, SerializedCompletionCallbacks, WithDelay, WorkflowType
from ._serialize import serialize_workflow
from ._storage import CallbackStorage

logger = logging.getLogger(__name__)


class Workflow:
    """
    A workflow allows running tasks in parallel and in sequence. It is a way to
    define a workflow of tasks, a combination of chains and groups in any
    order and nested as needed.

    Example:

    Let's assume we want a workflow that looks like this:

                 ╭────────╮  ╭────────╮
                 │ Task 2 │  │ Task 5 │
              ╭──┼●      ●┼──┼●      ●┼╮
    ╭────────╮│  ╰────────╯  ╰────────╯│  ╭────────╮
    │ Task 1 ││  ╭────────╮            │  │ Task 8 │
    │       ●┼╯  │ Task 3 │            ╰──┼●       │
    │       ●┼───┼●      ●┼───────────────┼●       │
    │       ●┼╮  ╰────────╯             ╭─┼●       │
    ╰────────╯│  ╭────────╮   ╭────────╮│╭┼●       │
              │  │ Task 4 │   │ Task 6 │││╰────────╯
              ╰──┼●      ●┼───┼●      ●┼╯│
                 │       ●┼╮  ╰────────╯ │
                 ╰────────╯│             │
                           │  ╭────────╮ │
                           │  │ Task 7 │ │
                           ╰──┼●      ●┼─╯
                              ╰────────╯

    We can define this workflow as follows:

    ```python
    from dramatiq_workflow import Workflow, Chain, Group

    workflow = Workflow(
        Chain(
            task1.message(),
            Group(
                Chain(
                    task2.message(),
                    task5.message(),
                ),
                task3.message(),
                Chain(
                    task4.message(),
                    Group(
                        task6.message(),
                        task7.message(),
                    ),
                ),
            ),
            task8.message(),
        ),
    )
    workflow.run() # Schedules the workflow to run in the background
    ```

    In this example, the execution would look like this*:
    1. Task 1 runs
    2. Task 2, 3, and 4 run in parallel once Task 1 finishes
    3. Task 5 runs once Task 2 finishes
    4. Task 6 and 7 run in parallel once Task 4 finishes
    5. Task 8 runs once Task 5, 6, and 7 finish

    * This is a simplified example. The actual execution order may vary because
    tasks that can run in parallel (i.e. in a Group) are not guaranteed to run
    in the order they are defined in the workflow.
    """

    def __init__(
        self,
        workflow: WorkflowType,
        broker: dramatiq.Broker | None = None,
    ):
        self.workflow = workflow
        self.broker = broker or dramatiq.get_broker()

        self._delay = None
        self._completion_callbacks: SerializedCompletionCallbacks | None = None

        while isinstance(self.workflow, WithDelay):
            self._delay = (self._delay or 0) + self.workflow.delay
            self.workflow = self.workflow.task

    def run(self):
        current = self.workflow
        completion_callbacks = self._completion_callbacks or []

        if isinstance(current, Message):
            current = self.__augment_message(current, completion_callbacks)
            self.broker.enqueue(current, delay=self._delay)
            return

        if isinstance(current, Chain):
            tasks = current.tasks[:]
            if not tasks:
                self.__schedule_noop(completion_callbacks)
                return

            task = tasks.pop(0)
            if tasks:
                completion_id = self.__create_barrier(1)
                completion_callbacks = [
                    *completion_callbacks,
                    (completion_id, serialize_workflow(Chain(*tasks)), False),
                ]
            self.__workflow_with_completion_callbacks(task, completion_callbacks).run()
            return

        if isinstance(current, Group):
            tasks = current.tasks[:]
            if not tasks:
                self.__schedule_noop(completion_callbacks)
                return

            completion_id = self.__create_barrier(len(tasks))
            completion_callbacks = [*completion_callbacks, (completion_id, None, True)]
            for task in tasks:
                self.__workflow_with_completion_callbacks(task, completion_callbacks).run()
            return

        raise TypeError(f"Unsupported workflow type: {type(current)}")

    def __workflow_with_completion_callbacks(self, task, completion_callbacks) -> "Workflow":
        return workflow_with_completion_callbacks(
            task,
            self.broker,
            completion_callbacks,
            delay=self._delay,
        )

    def __schedule_noop(self, completion_callbacks: SerializedCompletionCallbacks):
        """
        Schedules a no-op task to trigger the workflow middleware.

        This is necessary when a Chain or a Group is empty, to ensure that
        the completion callbacks are still processed and the workflow can
        continue.
        """

        if not self._delay:
            # If there is no delay, we can process the completion callbacks
            # immediately instead of scheduling a noop task. This saves us a
            # round trip to the broker and having to encode the workflow.
            self.__middleware._process_completion_callbacks(self.broker, completion_callbacks)
            return

        noop_message = workflow_noop.message()
        noop_message = self.__augment_message(noop_message, completion_callbacks)
        self.broker.enqueue(noop_message, delay=self._delay)

    def __augment_message(self, message: Message, completion_callbacks: SerializedCompletionCallbacks) -> Message:
        options = {}
        if completion_callbacks:
            callbacks_ref = self.__callback_storage.store(completion_callbacks)
            options = {OPTION_KEY_CALLBACKS: callbacks_ref}

        return message.copy(
            # We reset the message timestamp to better represent the time the
            # message was actually enqueued.  This is to avoid tripping the max_age
            # check in the broker.
            message_timestamp=time.time() * 1000,
            options=options,
        )

    @property
    def __middleware(self) -> WorkflowMiddleware:
        if not hasattr(self, "_cached_middleware"):
            for middleware in self.broker.middleware:
                if isinstance(middleware, WorkflowMiddleware):
                    self._cached_middleware = middleware
                    break
            else:
                raise RuntimeError(
                    "WorkflowMiddleware middleware not found! Did you forget "
                    "to set it up? It is required if you want to use "
                    "workflows."
                )
        return self._cached_middleware

    @property
    def __rate_limiter_backend(self) -> dramatiq.rate_limits.RateLimiterBackend:
        return self.__middleware.rate_limiter_backend

    @property
    def __barrier_type(self) -> type[dramatiq.rate_limits.Barrier]:
        return self.__middleware.barrier_type

    @property
    def __callback_storage(self) -> CallbackStorage:
        return self.__middleware.callback_storage

    def __create_barrier(self, count: int) -> str:
        completion_uuid = str(uuid4())
        completion_barrier = self.__barrier_type(self.__rate_limiter_backend, completion_uuid, ttl=CALLBACK_BARRIER_TTL)
        completion_barrier.create(count)
        logger.debug("Barrier created: %s (%d tasks)", completion_uuid, count)
        return completion_uuid

    def __str__(self):
        return f"Workflow({serialize_workflow(self.workflow)})"
