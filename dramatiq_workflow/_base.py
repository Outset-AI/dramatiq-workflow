import logging
import time
from uuid import uuid4

import dramatiq
import dramatiq.rate_limits

from ._constants import CALLBACK_BARRIER_TTL, OPTION_KEY_CALLBACKS
from ._helpers import workflow_with_completion_callbacks
from ._middleware import WorkflowMiddleware, workflow_noop
from ._models import Barrier, Chain, CompletionCallbacks, Group, Message, WithDelay, WorkflowType
from ._serialize import serialize_callbacks, serialize_workflow

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
        self._completion_callbacks = []

        while isinstance(self.workflow, WithDelay):
            self._delay = (self._delay or 0) + self.workflow.delay
            self.workflow = self.workflow.task

    def run(self):
        current = self.workflow
        completion_callbacks = self._completion_callbacks.copy()

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
                completion_callbacks.append((completion_id, Chain(*tasks), False))
            self.__workflow_with_completion_callbacks(task, completion_callbacks).run()
            return

        if isinstance(current, Group):
            tasks = current.tasks[:]
            if not tasks:
                self.__schedule_noop(completion_callbacks)
                return

            completion_id = self.__create_barrier(len(tasks))
            completion_callbacks.append((completion_id, None, True))
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

    def __schedule_noop(self, completion_callbacks: CompletionCallbacks):
        noop_message = workflow_noop.message()
        noop_message = self.__augment_message(noop_message, completion_callbacks)
        self.broker.enqueue(noop_message, delay=self._delay)

    def __augment_message(self, message: Message, completion_callbacks: CompletionCallbacks) -> Message:
        return message.copy(
            # We reset the message timestamp to better represent the time the
            # message was actually enqueued.  This is to avoid tripping the max_age
            # check in the broker.
            message_timestamp=time.time() * 1000,
            options={OPTION_KEY_CALLBACKS: serialize_callbacks(completion_callbacks)},
        )

    @property
    def __rate_limiter_backend(self):
        if not hasattr(self, "__cached_rate_limiter_backend"):
            for middleware in self.broker.middleware:
                if isinstance(middleware, WorkflowMiddleware):
                    self.__cached_rate_limiter_backend = middleware.rate_limiter_backend
                    break
            else:
                raise RuntimeError(
                    "WorkflowMiddleware middleware not found! Did you forget "
                    "to set it up? It is required if you want to use "
                    "workflows."
                )
        return self.__cached_rate_limiter_backend

    def __create_barrier(self, count: int):
        if count == 1:
            # No need to create a distributed barrier if there is only one task
            return None

        completion_uuid = str(uuid4())
        completion_barrier = Barrier(self.__rate_limiter_backend, completion_uuid, ttl=CALLBACK_BARRIER_TTL)
        completion_barrier.create(count)
        logger.debug("Barrier created: %s (%d tasks)", completion_uuid, count)
        return completion_uuid

    def __str__(self):
        return f"Workflow({serialize_workflow(self.workflow)})"
