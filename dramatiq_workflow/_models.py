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

SerializedCompletionCallback = tuple[str | None, dict | None, bool]
SerializedCompletionCallbacks = list[SerializedCompletionCallback]
