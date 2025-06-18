import abc
from functools import partial
from typing import Any

from ._models import LazyWorkflow, SerializedCompletionCallbacks


class CallbackStorage(abc.ABC):
    """
    Abstract base class for callback storage backends.
    """

    @abc.abstractmethod
    def store(self, callbacks: SerializedCompletionCallbacks) -> Any:
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
    def retrieve(self, ref: Any) -> SerializedCompletionCallbacks:
        """
        Retrieves callbacks using a reference.

        Args:
            ref: The reference to the callbacks, as returned by `store`.

        Returns:
            The retrieved callbacks.
        """
        raise NotImplementedError

    def _determine_dedup_key(self, callbacks: SerializedCompletionCallbacks) -> tuple[str, bool]:
        """
        Determines a deduplication key for the given callbacks.

        This is used by deduplication storage backends to identify unique
        callback sets.

        Returns:
            A tuple containing the completion ID and a boolean indicating if
            the callbacks are part of a group (i.e. if deduplication is
            strictly needed).
        """

        # NOTE: `Workflow.__augment_message` only calls the `CallbackStorage`
        # when the `callbacks` list is not empty. This `assert` should always
        # hold true.
        assert isinstance(callbacks, list) and len(callbacks) > 0, "Callbacks must be a non-empty list"

        last_callback = callbacks[-1]
        completion_id, _, is_group = last_callback
        return completion_id, is_group


class InlineCallbackStorage(CallbackStorage):
    """
    A storage backend that stores callbacks inline with the message.
    This is the default storage backend.
    """

    def store(self, callbacks: SerializedCompletionCallbacks) -> SerializedCompletionCallbacks:
        return callbacks

    def retrieve(self, ref: SerializedCompletionCallbacks) -> SerializedCompletionCallbacks:
        return ref


class _LazyLoadedWorkflow:
    def __init__(self, ref: Any, load_func: LazyWorkflow):
        self.ref = ref
        self.load_func = load_func

    def __call__(self) -> dict:
        return self.load_func()


class DedupWorkflowCallbackStorage(CallbackStorage, abc.ABC):
    """
    An abstract storage backend that separates storage of workflows from
    callbacks, allowing for deduplication of workflows.
    """

    @abc.abstractmethod
    def _store_workflow(self, id: str, workflow: dict) -> Any:
        """
        Stores a workflow and returns a reference to it. The `id` can be used
        to deduplicate workflows, and the `workflow` is the actual workflow to
        store. The reference returned must be serializable by the broker's
        encoder (e.g. JSON).
        """
        raise NotImplementedError

    @abc.abstractmethod
    def _load_workflow(self, id: str, ref: Any) -> dict:
        """
        Loads a workflow using the deduplication ID and reference previously
        returned by `store_workflow`.
        """
        raise NotImplementedError

    def _store_callbacks(self, callbacks: list[tuple[str, Any | None, bool]]) -> Any:
        """
        Stores the callbacks, which may include references to workflows. By
        default, this implementation simply returns the callbacks as-is to be stored inline.
        """
        return callbacks

    def _retrieve_callbacks(self, ref: Any) -> list[tuple[str, Any | None, bool]]:
        """
        Retrieves callbacks from a reference. By default, this implementation
        simply returns the reference as-is since the default `_store_callbacks`
        implementation returns the callbacks inline.
        """
        return ref

    def store(self, callbacks: SerializedCompletionCallbacks) -> Any:
        """
        Stores callbacks, offloading workflow storage to `store_workflow`.
        """
        new_callbacks = []
        for completion_id, remaining_workflow, is_group in callbacks:
            if isinstance(remaining_workflow, _LazyLoadedWorkflow):
                remaining_workflow = remaining_workflow.ref
            elif isinstance(remaining_workflow, dict):
                remaining_workflow = self._store_workflow(completion_id, remaining_workflow)
            new_callbacks.append((completion_id, remaining_workflow, is_group))

        return self._store_callbacks(new_callbacks)

    def retrieve(self, ref: Any) -> SerializedCompletionCallbacks:
        """
        Retrieves callbacks and prepares lazy loaders for workflows.
        """
        callbacks = self._retrieve_callbacks(ref)
        new_callbacks = []
        for completion_id, workflow_ref, is_group in callbacks:
            if workflow_ref is not None and not callable(workflow_ref):
                workflow_ref = _LazyLoadedWorkflow(
                    ref=workflow_ref,
                    load_func=partial(self._load_workflow, completion_id, workflow_ref),
                )
            new_callbacks.append((completion_id, workflow_ref, is_group))

        return new_callbacks
