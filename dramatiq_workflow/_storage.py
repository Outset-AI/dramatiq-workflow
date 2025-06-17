import abc
from typing import Any

from ._models import SerializedCompletionCallbacks


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
