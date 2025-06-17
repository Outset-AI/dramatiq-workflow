import unittest
from typing import Any
from unittest import mock

import dramatiq
from dramatiq.broker import Broker
from dramatiq.rate_limits.backends import StubBackend

from dramatiq_workflow import Chain, WorkflowMiddleware
from dramatiq_workflow._barrier import AtMostOnceBarrier
from dramatiq_workflow._constants import OPTION_KEY_CALLBACKS
from dramatiq_workflow._models import SerializedCompletionCallbacks
from dramatiq_workflow._serialize import serialize_workflow
from dramatiq_workflow._storage import CallbackStorage


class MyDedupStorage(CallbackStorage):
    def __init__(self):
        self.storage = {}
        self.retrieve_calls = []

    def store(self, callbacks: SerializedCompletionCallbacks) -> Any:
        dedup_key, _ = self._determine_dedup_key(callbacks)
        if dedup_key not in self.storage:
            self.storage[dedup_key] = callbacks
        return dedup_key

    def retrieve(self, ref: Any) -> SerializedCompletionCallbacks:
        self.retrieve_calls.append(ref)
        return self.storage[ref]


class WorkflowMiddlewareTests(unittest.TestCase):
    def setUp(self):
        # Initialize common mocks and the middleware instance for each test
        self.rate_limiter_backend = StubBackend()
        self.middleware = WorkflowMiddleware(self.rate_limiter_backend)

        self.broker = mock.MagicMock(spec=Broker)

    def _make_message(
        self, message_options: dict | None = None, message_timestamp: int = 1717526084640
    ) -> dramatiq.broker.MessageProxy:
        """
        Creates a dramatiq MessageProxy object with given options.
        """
        message_id = 1  # Simplistic message ID for testing
        message = dramatiq.Message(
            message_id=str(message_id),
            message_timestamp=message_timestamp,
            queue_name="default",
            actor_name="test_task",
            args=(),
            kwargs={},
            options=message_options or {},
        )
        return dramatiq.broker.MessageProxy(message)

    def _create_serialized_workflow(self) -> dict | None:
        """
        Creates and serializes a simple workflow for testing.
        """
        # Define a simple workflow (Chain with a single task)
        workflow = Chain(self._make_message()._message)
        serialized = serialize_workflow(workflow)
        return serialized

    def test_after_process_message_without_callbacks(self):
        message = self._make_message()

        self.middleware.after_process_message(self.broker, message)

        self.broker.enqueue.assert_not_called()

    def test_after_process_message_with_exception(self):
        message = self._make_message({OPTION_KEY_CALLBACKS: [(None, self._create_serialized_workflow(), True)]})

        self.middleware.after_process_message(self.broker, message, exception=Exception("Test exception"))

        self.broker.enqueue.assert_not_called()

    def test_after_process_message_with_failed_message(self):
        message = self._make_message({OPTION_KEY_CALLBACKS: [(None, self._create_serialized_workflow(), True)]})
        message.failed = True

        self.middleware.after_process_message(self.broker, message)

        self.broker.enqueue.assert_not_called()

    @mock.patch("dramatiq_workflow._base.time.time")
    def test_after_process_message_with_workflow(self, mock_time):
        mock_time.return_value = 1337
        barrier_key = "barrier_1"
        barrier = AtMostOnceBarrier(self.rate_limiter_backend, barrier_key)
        barrier.create(1)
        message = self._make_message({OPTION_KEY_CALLBACKS: [(barrier_key, self._create_serialized_workflow(), True)]})

        self.middleware.after_process_message(self.broker, message)

        self.broker.enqueue.assert_called_once_with(self._make_message(message_timestamp=1337_000)._message, delay=None)

    @mock.patch("dramatiq_workflow._base.time.time")
    def test_after_process_message_with_barriered_workflow(self, mock_time):
        mock_time.return_value = 1337
        barrier = AtMostOnceBarrier(self.rate_limiter_backend, "barrier_1")
        barrier.create(2)
        message = self._make_message({OPTION_KEY_CALLBACKS: [(barrier.key, self._create_serialized_workflow(), True)]})

        self.middleware.after_process_message(self.broker, message)
        self.broker.enqueue.assert_not_called()

        # Calling again, barrier should be completed now
        self.middleware.after_process_message(self.broker, message)
        self.broker.enqueue.assert_called_once_with(self._make_message(message_timestamp=1337_000)._message, delay=None)

    @mock.patch("dramatiq_workflow._base.time.time")
    def test_after_process_message_with_custom_storage(self, mock_time):
        mock_time.return_value = 1337
        storage = MyDedupStorage()
        self.middleware = WorkflowMiddleware(self.rate_limiter_backend, callback_storage=storage)

        serialized_workflow = self._create_serialized_workflow()
        callbacks = [("barrier_1", serialized_workflow, True)]

        barrier = AtMostOnceBarrier(self.rate_limiter_backend, "barrier_1")
        barrier.create(1)

        dedup_key = storage.store(callbacks)

        message = self._make_message({OPTION_KEY_CALLBACKS: dedup_key})

        self.middleware.after_process_message(self.broker, message)

        self.assertEqual(len(storage.retrieve_calls), 1)
        self.assertEqual(storage.retrieve_calls[0], dedup_key)

        self.broker.enqueue.assert_called_once_with(self._make_message(message_timestamp=1337_000)._message, delay=None)
