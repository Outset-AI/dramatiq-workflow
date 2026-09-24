import unittest
from typing import Any
from unittest import mock

import dramatiq
from dramatiq.broker import Broker
from dramatiq.rate_limits.backends import StubBackend

from dramatiq_workflow import Chain, WorkflowMiddleware
from dramatiq_workflow._barrier import AtMostOnceBarrier
from dramatiq_workflow._constants import OPTION_KEY_CALLBACKS
from dramatiq_workflow._models import LazyWorkflow, SerializedCompletionCallbacks
from dramatiq_workflow._serialize import serialize_workflow
from dramatiq_workflow._storage import CallbackStorage


class MyLazyStorage(CallbackStorage[int]):
    def __init__(self) -> None:
        self.workflows: dict[int, dict[str, Any]] = {}
        self.callbacks: dict[int, SerializedCompletionCallbacks] = {}
        self.workflow_ref_counter = 0
        self.callback_ref_counter = 0
        self.retrieve_calls: list[int] = []
        self.loaded_workflows: set[int] = set()

    def _create_loader(self, ref: int) -> LazyWorkflow:
        def loader() -> dict[str, Any]:
            self.loaded_workflows.add(ref)
            return self.retrieve_workflow(ref)

        return loader

    def store(self, callbacks: SerializedCompletionCallbacks) -> int:
        new_callbacks: SerializedCompletionCallbacks = []
        for completion_id, remaining_workflow, is_group in callbacks:
            if isinstance(remaining_workflow, dict):
                ref = self.workflow_ref_counter
                self.workflows[ref] = remaining_workflow
                self.workflow_ref_counter += 1
                lazy_workflow = self._create_loader(ref)
                new_callbacks.append((completion_id, lazy_workflow, is_group))
            else:
                new_callbacks.append((completion_id, remaining_workflow, is_group))

        ref = self.callback_ref_counter
        self.callbacks[ref] = new_callbacks
        self.callback_ref_counter += 1
        return ref

    def retrieve(self, ref: int) -> SerializedCompletionCallbacks:
        self.retrieve_calls.append(ref)
        return self.callbacks[ref]

    def retrieve_workflow(self, ref: int) -> dict[str, Any]:
        return self.workflows[ref]


class WorkflowMiddlewareTests(unittest.TestCase):
    def setUp(self) -> None:
        # Initialize common mocks and the middleware instance for each test
        self.rate_limiter_backend = StubBackend()
        self.middleware = WorkflowMiddleware(self.rate_limiter_backend)

        self.broker = mock.MagicMock(spec=Broker)

    def _make_message(
        self, message_options: dict[str, Any] | None = None, message_timestamp: int = 1717526084640
    ) -> dramatiq.broker.MessageProxy:
        """
        Creates a dramatiq MessageProxy object with given options.
        """
        message_id = 1  # Simplistic message ID for testing
        message: dramatiq.Message[Any] = dramatiq.Message(
            message_id=str(message_id),
            message_timestamp=message_timestamp,
            queue_name="default",
            actor_name="test_task",
            args=(),
            kwargs={},
            options=message_options or {},
        )
        return dramatiq.broker.MessageProxy(message)

    def _create_serialized_workflow(self) -> dict[str, Any] | None:
        """
        Creates and serializes a simple workflow for testing.
        """
        # Define a simple workflow (Chain with a single task)
        workflow = Chain(self._make_message()._message)
        serialized = serialize_workflow(workflow)
        return serialized

    def test_after_process_message_without_callbacks(self) -> None:
        message = self._make_message()

        self.middleware.after_process_message(self.broker, message)

        self.broker.enqueue.assert_not_called()

    def test_after_process_message_with_exception(self) -> None:
        message = self._make_message({OPTION_KEY_CALLBACKS: [(None, self._create_serialized_workflow(), True)]})

        self.middleware.after_process_message(self.broker, message, exception=Exception("Test exception"))

        self.broker.enqueue.assert_not_called()

    def test_after_process_message_with_failed_message(self) -> None:
        message = self._make_message({OPTION_KEY_CALLBACKS: [(None, self._create_serialized_workflow(), True)]})
        message.failed = True

        self.middleware.after_process_message(self.broker, message)

        self.broker.enqueue.assert_not_called()

    @mock.patch("dramatiq_workflow._base.time.time")
    def test_after_process_message_with_workflow(self, mock_time: mock.MagicMock) -> None:
        mock_time.return_value = 1337
        barrier_key = "barrier_1"
        barrier = AtMostOnceBarrier(self.rate_limiter_backend, barrier_key)
        barrier.create(1)
        message = self._make_message({OPTION_KEY_CALLBACKS: [(barrier_key, self._create_serialized_workflow(), True)]})

        self.middleware.after_process_message(self.broker, message)

        self.broker.enqueue.assert_called_once_with(self._make_message(message_timestamp=1337_000)._message, delay=None)

    @mock.patch("dramatiq_workflow._base.time.time")
    def test_after_process_message_with_barriered_workflow(self, mock_time: mock.MagicMock) -> None:
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
    def test_after_process_message_with_lazy_loaded_workflow(self, mock_time: mock.MagicMock) -> None:
        mock_time.return_value = 1337
        storage = MyLazyStorage()
        self.middleware = WorkflowMiddleware(self.rate_limiter_backend, callback_storage=storage)

        # Create a workflow that will be lazy loaded
        serialized_workflow = self._create_serialized_workflow()
        callbacks: SerializedCompletionCallbacks = [("barrier_1", serialized_workflow, True)]

        # Store it, which will convert it to a lazy workflow
        callbacks_ref = storage.store(callbacks)

        # The lazy workflow object is now inside storage.callbacks[callbacks_ref]
        lazy_workflow_obj = storage.callbacks[callbacks_ref][0][1]
        self.assertTrue(callable(lazy_workflow_obj))

        workflow_ref = 0
        self.assertNotIn(workflow_ref, storage.loaded_workflows)

        # Set up barrier
        barrier = AtMostOnceBarrier(self.rate_limiter_backend, "barrier_1")
        barrier.create(1)

        # Create message and process it
        message = self._make_message({OPTION_KEY_CALLBACKS: callbacks_ref})
        self.middleware.after_process_message(self.broker, message)

        # Assertions
        self.assertEqual(len(storage.retrieve_calls), 1)
        self.assertEqual(storage.retrieve_calls[0], callbacks_ref)
        self.assertIn(workflow_ref, storage.loaded_workflows)

        self.broker.enqueue.assert_called_once_with(self._make_message(message_timestamp=1337_000)._message, delay=None)
