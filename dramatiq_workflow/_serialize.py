import typing

from ._models import (
    Chain,
    Group,
    Message,
    WithDelay,
    WorkflowType,
)


def serialize_workflow(workflow: WorkflowType | None) -> dict | None:
    """
    Return a serialized version of the workflow that can be JSON-encoded.
    """
    if workflow is None:
        return None

    if isinstance(workflow, Message):
        return {
            "__type__": "message",
            **workflow.asdict(),
        }
    if isinstance(workflow, Chain):
        return {
            "__type__": "chain",
            "children": [serialize_workflow(task) for task in workflow.tasks],
        }
    if isinstance(workflow, Group):
        return {
            "__type__": "group",
            "children": [serialize_workflow(task) for task in workflow.tasks],
        }
    if isinstance(workflow, WithDelay):
        return {
            "__type__": "with_delay",
            "delay": workflow.delay,
            "task": serialize_workflow(workflow.task),
        }

    # NOTE: This line is unreachable by design but I don't know how to tell
    # Pyright that.
    raise TypeError(f"Unsupported workflow type: {type(workflow)}")


def unserialize_workflow(workflow: typing.Any) -> WorkflowType:
    """
    Return an unserialized version of the workflow that can be used to create
    a Workflow instance.
    """
    if workflow is None:
        raise ValueError("Cannot unserialize None")

    if not isinstance(workflow, dict):
        raise TypeError(f"Unsupported data type: {type(workflow)}")

    workflow_type = workflow.pop("__type__")
    match workflow_type:
        case "message":
            return Message(**workflow)
        case "chain":
            return Chain(*[unserialize_workflow(task) for task in workflow["children"]])
        case "group":
            return Group(*[unserialize_workflow(task) for task in workflow["children"]])
        case "with_delay":
            return WithDelay(
                task=unserialize_workflow(workflow["task"]),
                delay=workflow["delay"],
            )

    raise TypeError(f"Unsupported workflow type: {workflow_type}")


def unserialize_workflow_or_none(workflow: typing.Any) -> WorkflowType | None:
    if workflow is None:
        return None
    return unserialize_workflow(workflow)
