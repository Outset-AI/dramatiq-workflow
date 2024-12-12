import dramatiq

from ._models import CompletionCallbacks, WorkflowType


def workflow_with_completion_callbacks(
    workflow: WorkflowType,
    broker: dramatiq.Broker,
    completion_callbacks: CompletionCallbacks,
    delay: int | None = None,
):
    from ._base import Workflow

    w = Workflow(workflow, broker)
    w._completion_callbacks = completion_callbacks
    if delay is not None:
        w._delay = (w._delay or 0) + delay
    return w
