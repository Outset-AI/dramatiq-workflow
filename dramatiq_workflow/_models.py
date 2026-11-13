import typing

import dramatiq
import dramatiq.rate_limits


class Chain:
    def __init__(self, *tasks: "WorkflowType") -> None:
        self.tasks = list(tasks)

    def __str__(self) -> str:
        return f"Chain({self.tasks})"

    def __eq__(self, other: object) -> bool:
        return isinstance(other, Chain) and self.tasks == other.tasks


class Group:
    def __init__(self, *tasks: "WorkflowType") -> None:
        self.tasks = list(tasks)

    def __str__(self) -> str:
        return f"Group({self.tasks})"

    def __eq__(self, other: object) -> bool:
        return isinstance(other, Group) and self.tasks == other.tasks


class WithDelay:
    def __init__(self, task: "WorkflowType", delay: int) -> None:
        self.task = task
        self.delay = delay

    def __str__(self) -> str:
        return f"WithDelay({self.task}, {self.delay})"

    def __eq__(self, other: object) -> bool:
        return isinstance(other, WithDelay) and self.task == other.task and self.delay == other.delay


Message = dramatiq.Message
# Not Message[object]: Message's type parameter is invariant before dramatiq
# 1.18, so Message[None] etc. would be rejected there.
WorkflowType = Message[typing.Any] | Chain | Group | WithDelay

LazyWorkflow = typing.Callable[[], dict[str, typing.Any]]
SerializedCompletionCallback = tuple[str, dict[str, typing.Any] | LazyWorkflow | None, bool]
SerializedCompletionCallbacks = list[SerializedCompletionCallback]


def walk_messages(workflow: WorkflowType) -> typing.Iterator[Message[typing.Any]]:
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
