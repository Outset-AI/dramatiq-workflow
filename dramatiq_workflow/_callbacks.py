from ._models import SerializedCompletionCallbacks, UnserializedCompletionCallback, UnserializedCompletionCallbacks
from ._serialize import serialize_callbacks, unserialize_callbacks


class CompletionCallbacks:
    def __init__(
        self,
        *,
        serialized: SerializedCompletionCallbacks | None = None,
        unserialized: UnserializedCompletionCallbacks | None = None,
        exclusive: bool = True,
    ):
        assert serialized is not None or unserialized is not None, "Either serialized or unserialized must be provided"
        self._serialized = serialized
        self._unserialized = unserialized
        self._exclusive = exclusive

    def serialize(self) -> SerializedCompletionCallbacks:
        if self._serialized is None:
            assert self._unserialized is not None
            self._serialized = serialize_callbacks(self._unserialized)
        return self._serialized

    def unserialize(self) -> UnserializedCompletionCallbacks:
        if self._unserialized is None:
            assert self._serialized is not None
            self._unserialized = unserialize_callbacks(self._serialized)
        return self._unserialized

    def append(self, callback: UnserializedCompletionCallback):
        if not self._exclusive:
            if self._serialized is not None:
                self._serialized = self._serialized.copy()
            if self._unserialized is not None:
                self.unserialized = self._unserialized.copy()

        if self._serialized is not None:
            self._serialized.extend(serialize_callbacks([callback]))
        if self._unserialized is not None:
            self._unserialized.append(callback)

    def copy(self) -> "CompletionCallbacks":
        self._exclusive = False
        return CompletionCallbacks(serialized=self._serialized, unserialized=self._unserialized, exclusive=False)
