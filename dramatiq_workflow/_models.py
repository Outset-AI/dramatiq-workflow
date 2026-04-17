import typing

import dramatiq
import dramatiq.rate_limits


class Chain:
    def __init__(self, *tasks: "WorkflowType"):
        self.tasks = list(tasks)

    def __str__(self):
        return f"Chain({self.tasks})"

    def __eq__(self, other):
        return isinstance(other, Chain) and self.tasks == other.tasks


class Group:
    def __init__(self, *tasks: "WorkflowType"):
        self.tasks = list(tasks)

    def __str__(self):
        return f"Group({self.tasks})"

    def __eq__(self, other):
        return isinstance(other, Group) and self.tasks == other.tasks


class WithDelay:
    def __init__(self, task: "WorkflowType", delay: int):
        self.task = task
        self.delay = delay

    def __str__(self):
        return f"WithDelay({self.task}, {self.delay})"

    def __eq__(self, other):
        return isinstance(other, WithDelay) and self.task == other.task and self.delay == other.delay


Message = dramatiq.Message
WorkflowType = Message | Chain | Group | WithDelay

LazyWorkflow = typing.Callable[[], dict]
SerializedCompletionCallback = tuple[str, dict | LazyWorkflow | None, bool]
SerializedCompletionCallbacks = list[SerializedCompletionCallback]


def walk_messages(workflow: WorkflowType) -> typing.Iterator[Message]:
    """Yield every Message in a workflow tree, in definition order.

    Useful for stamping custom options on every message before calling
    ``Workflow(...).run()`` — see the "Handling Permanent Failures"
    section of the README for the failure-callback pattern.
    """
    if isinstance(workflow, Message):
        yield workflow
    elif isinstance(workflow, (Chain, Group)):
        for task in workflow.tasks:
            yield from walk_messages(task)
    elif isinstance(workflow, WithDelay):
        yield from walk_messages(workflow.task)
    else:
        raise TypeError(f"Unsupported workflow type: {type(workflow)}")
