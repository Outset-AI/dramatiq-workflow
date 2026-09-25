from typing import TYPE_CHECKING

import dramatiq

from ._models import SerializedCompletionCallbacks, WorkflowType

if TYPE_CHECKING:
    from ._base import Workflow


def workflow_with_completion_callbacks(
    workflow: WorkflowType,
    broker: dramatiq.Broker,
    completion_callbacks: SerializedCompletionCallbacks,
    delay: int | None = None,
) -> "Workflow":
    from ._base import Workflow

    w = Workflow(workflow, broker)
    w._completion_callbacks = completion_callbacks
    if delay is not None:
        w._delay = (w._delay or 0) + delay
    return w
