import abc
from functools import partial
from typing import Any, Generic, TypeVar, cast

from ._models import LazyWorkflow, SerializedCompletionCallbacks

CallbacksRefT = TypeVar("CallbacksRefT")


class CallbackStorage(abc.ABC, Generic[CallbacksRefT]):
    """
    Abstract base class for callback storage backends.
    """

    @abc.abstractmethod
    def store(self, callbacks: SerializedCompletionCallbacks) -> CallbacksRefT:
        """
        Stores callbacks and returns a reference to them.

        This reference will be stored in the dramatiq message options. It must
        be serializable by the broker's encoder (e.g. JSON).

        Args:
            callbacks: The callbacks to store.

        Returns:
            A serializable reference to the stored callbacks.
        """
        raise NotImplementedError

    @abc.abstractmethod
    def retrieve(self, ref: CallbacksRefT) -> SerializedCompletionCallbacks:
        """
        Retrieves callbacks using a reference.

        Args:
            ref: The reference to the callbacks, as returned by `store`.

        Returns:
            The retrieved callbacks.
        """
        raise NotImplementedError


class InlineCallbackStorage(CallbackStorage[SerializedCompletionCallbacks]):
    """
    A storage backend that stores callbacks inline with the message.
    This is the default storage backend.
    """

    def store(self, callbacks: SerializedCompletionCallbacks) -> SerializedCompletionCallbacks:
        return callbacks

    def retrieve(self, ref: SerializedCompletionCallbacks) -> SerializedCompletionCallbacks:
        return ref


WorkflowRefT = TypeVar("WorkflowRefT")
CompletionCallbacksWithWorkflowRef = list[tuple[str, WorkflowRefT | None, bool]]


class _LazyLoadedWorkflow(Generic[WorkflowRefT]):
    def __init__(self, ref: Any, load_func: LazyWorkflow):
        self.ref = ref
        self.load_func = load_func

    def __call__(self) -> dict:
        return self.load_func()

    def __str__(self):
        return f"_LazyLoadedWorkflow({self.ref})"


class DedupWorkflowCallbackStorage(CallbackStorage[CompletionCallbacksWithWorkflowRef], abc.ABC, Generic[WorkflowRefT]):
    """
    An abstract storage backend that separates storage of workflows from
    callbacks, allowing for deduplication of workflows.
    """

    @abc.abstractmethod
    def _store_workflow(self, id: str, workflow: dict) -> WorkflowRefT:
        """
        Stores a workflow and returns a reference to it. The `id` can be used
        to deduplicate workflows, and the `workflow` is the actual workflow to
        store. The reference returned must be serializable by the broker's
        encoder (e.g. JSON).
        """
        raise NotImplementedError

    @abc.abstractmethod
    def _load_workflow(self, id: str, ref: WorkflowRefT) -> dict:
        """
        Loads a workflow using the deduplication ID and reference previously
        returned by `store_workflow`.
        """
        raise NotImplementedError

    def store(self, callbacks: SerializedCompletionCallbacks) -> CompletionCallbacksWithWorkflowRef[WorkflowRefT]:
        """
        Stores callbacks, offloading workflow storage to `store_workflow`.
        """
        new_callbacks: CompletionCallbacksWithWorkflowRef[WorkflowRefT] = []
        for completion_id, remaining_workflow, is_group in callbacks:
            remaining_workflow_ref = None
            if isinstance(remaining_workflow, _LazyLoadedWorkflow):
                remaining_workflow_ref = cast(WorkflowRefT, remaining_workflow.ref)
            elif isinstance(remaining_workflow, dict):
                remaining_workflow_ref = self._store_workflow(completion_id, remaining_workflow)
            elif remaining_workflow is not None:
                raise TypeError(
                    "Unsupported workflow type: "
                    f"{type(remaining_workflow)}. Expected None, dict, or _LazyLoadedWorkflow."
                )
            new_callbacks.append((completion_id, remaining_workflow_ref, is_group))

        return new_callbacks

    def retrieve(self, ref: CompletionCallbacksWithWorkflowRef[WorkflowRefT]) -> SerializedCompletionCallbacks:
        """
        Retrieves callbacks and prepares lazy loaders for workflows.
        """
        new_callbacks: SerializedCompletionCallbacks = []
        for completion_id, workflow_ref, is_group in ref:
            if workflow_ref is not None and not callable(workflow_ref):
                workflow_ref = _LazyLoadedWorkflow(
                    ref=workflow_ref,
                    load_func=partial(self._load_workflow, completion_id, workflow_ref),
                )
            new_callbacks.append((completion_id, workflow_ref, is_group))

        return new_callbacks
