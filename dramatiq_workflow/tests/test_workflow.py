import unittest
from unittest import mock

import dramatiq
import dramatiq.rate_limits

from .. import Chain, Group, WithDelay, Workflow, WorkflowMiddleware
from .._serialize import serialize_workflow, unserialize_workflow


class WorkflowTests(unittest.TestCase):
    def setUp(self):
        self.rate_limiter_backend = mock.create_autospec(dramatiq.rate_limits.RateLimiterBackend, instance=True)
        self.barrier = mock.create_autospec(dramatiq.rate_limits.Barrier)
        self.broker = mock.MagicMock(
            middleware=[
                WorkflowMiddleware(
                    rate_limiter_backend=self.rate_limiter_backend,
                    barrier_type=self.barrier,
                )
            ]
        )
        self.task = mock.MagicMock()
        self.task.message.side_effect = lambda *args, **kwargs: self.__make_message(
            self.__generate_id(), *args, **kwargs
        )
        self.message_ids = []
        self.message_timestamp = 1717526084640

    def __generate_id(self):
        current_id = len(self.message_ids) + 1
        self.message_ids.append(current_id)
        return current_id

    def __make_message(self, message_id, *args, message_options={}, message_timestamp=None, **kwargs):
        return dramatiq.Message(
            message_id=message_id,
            message_timestamp=message_timestamp or self.message_timestamp,
            queue_name="default",
            actor_name="task",
            args=args,
            kwargs=kwargs,
            options=message_options,
        )

    @mock.patch("dramatiq_workflow._base.time.time")
    def test_simple_workflow(self, time_mock):
        time_mock.return_value = 1717526000.12
        updated_timestamp = time_mock.return_value * 1000
        workflow = Workflow(
            Chain(
                self.task.message(),
                Group(
                    Chain(
                        self.task.message(),
                        self.task.message(),
                    ),
                    self.task.message(),
                    Chain(
                        self.task.message(),
                        Group(
                            self.task.message(),
                            self.task.message(),
                        ),
                    ),
                ),
                Chain(),
                self.task.message(),
            ),
            broker=self.broker,
        )
        workflow.run()

        time_mock.assert_called_once()

        self.broker.enqueue.assert_called_once_with(
            self.__make_message(
                1,
                message_timestamp=updated_timestamp,
                message_options={
                    "workflow_completion_callbacks": [
                        (
                            mock.ANY,  # Accept any callback ID
                            {
                                "__type__": "chain",
                                "children": [
                                    {
                                        "__type__": "group",
                                        "children": [
                                            {
                                                "__type__": "chain",
                                                "children": [
                                                    {
                                                        "__type__": "message",
                                                        "queue_name": "default",
                                                        "actor_name": "task",
                                                        "args": (),
                                                        "kwargs": {},
                                                        "options": {},
                                                        "message_id": 2,
                                                        "message_timestamp": self.message_timestamp,
                                                    },
                                                    {
                                                        "__type__": "message",
                                                        "queue_name": "default",
                                                        "actor_name": "task",
                                                        "args": (),
                                                        "kwargs": {},
                                                        "options": {},
                                                        "message_id": 3,
                                                        "message_timestamp": self.message_timestamp,
                                                    },
                                                ],
                                            },
                                            {
                                                "__type__": "message",
                                                "queue_name": "default",
                                                "actor_name": "task",
                                                "args": (),
                                                "kwargs": {},
                                                "options": {},
                                                "message_id": 4,
                                                "message_timestamp": self.message_timestamp,
                                            },
                                            {
                                                "__type__": "chain",
                                                "children": [
                                                    {
                                                        "__type__": "message",
                                                        "queue_name": "default",
                                                        "actor_name": "task",
                                                        "args": (),
                                                        "kwargs": {},
                                                        "options": {},
                                                        "message_id": 5,
                                                        "message_timestamp": self.message_timestamp,
                                                    },
                                                    {
                                                        "__type__": "group",
                                                        "children": [
                                                            {
                                                                "__type__": "message",
                                                                "queue_name": "default",
                                                                "actor_name": "task",
                                                                "args": (),
                                                                "kwargs": {},
                                                                "options": {},
                                                                "message_id": 6,
                                                                "message_timestamp": self.message_timestamp,
                                                            },
                                                            {
                                                                "__type__": "message",
                                                                "queue_name": "default",
                                                                "actor_name": "task",
                                                                "args": (),
                                                                "kwargs": {},
                                                                "options": {},
                                                                "message_id": 7,
                                                                "message_timestamp": self.message_timestamp,
                                                            },
                                                        ],
                                                    },
                                                ],
                                            },
                                        ],
                                    },
                                    {"__type__": "chain", "children": []},
                                    {
                                        "__type__": "message",
                                        "queue_name": "default",
                                        "actor_name": "task",
                                        "args": (),
                                        "kwargs": {},
                                        "options": {},
                                        "message_id": 8,
                                        "message_timestamp": self.message_timestamp,
                                    },
                                ],
                            },
                            False,
                        )
                    ]
                },
            ),
            delay=None,
        )

    @mock.patch("dramatiq_workflow._base.time.time")
    def test_noop_workflow(self, time_mock):
        time_mock.return_value = 1717526000.12
        workflow = Workflow(Chain(), broker=self.broker)
        workflow.run()

        self.broker.enqueue.assert_called_once()

    def test_unsupported_workflow(self):
        with self.assertRaises(TypeError):
            Workflow(1).run()  # type: ignore

    @mock.patch("dramatiq_workflow._base.time.time")
    def test_chain_with_delay(self, time_mock):
        time_mock.return_value = 1717526000.12
        updated_timestamp = time_mock.return_value * 1000
        workflow = Workflow(
            WithDelay(
                Chain(self.task.message(), self.task.message()),
                delay=10,
            ),
            broker=self.broker,
        )
        workflow.run()

        self.broker.enqueue.assert_called_once_with(
            self.__make_message(
                1,
                message_timestamp=updated_timestamp,
                message_options={
                    "workflow_completion_callbacks": [
                        (
                            mock.ANY,  # Accept any callback ID
                            {
                                "__type__": "chain",
                                "children": [
                                    {
                                        "__type__": "message",
                                        "queue_name": "default",
                                        "actor_name": "task",
                                        "args": (),
                                        "kwargs": {},
                                        "options": {},
                                        "message_id": 2,
                                        "message_timestamp": self.message_timestamp,
                                    },
                                ],
                            },
                            False,
                        )
                    ]
                },
            ),
            delay=10,
        )
        self.barrier.assert_called_once_with(self.rate_limiter_backend, mock.ANY, ttl=mock.ANY)

    @mock.patch("dramatiq_workflow._base.time.time")
    def test_group_with_delay(self, time_mock):
        time_mock.return_value = 1717526000.12
        updated_timestamp = time_mock.return_value * 1000
        workflow = Workflow(
            WithDelay(
                Group(self.task.message(), self.task.message()),
                delay=10,
            ),
            broker=self.broker,
        )
        workflow.run()
        self.broker.enqueue.assert_has_calls(
            [
                mock.call(
                    self.__make_message(
                        1,
                        message_timestamp=updated_timestamp,
                        message_options={
                            "workflow_completion_callbacks": [
                                (
                                    mock.ANY,  # Accept any callback ID
                                    None,  # No more workflow to run after this
                                    True,
                                )
                            ]
                        },
                    ),
                    delay=10,
                ),
                mock.call(
                    self.__make_message(
                        2,
                        message_timestamp=updated_timestamp,
                        message_options={
                            "workflow_completion_callbacks": [
                                (
                                    mock.ANY,
                                    None,
                                    True,
                                )
                            ]
                        },
                    ),
                    delay=10,
                ),
            ],
            any_order=True,
        )

    def test_serialize_unserialize(self):
        workflow = Workflow(
            Chain(
                self.task.message(),
                WithDelay(
                    Group(
                        self.task.message(),
                        self.task.message(),
                    ),
                    delay=10,
                ),
            ),
        )

        serialized = serialize_workflow(workflow.workflow)
        unserialized = unserialize_workflow(serialized)
        self.assertEqual(workflow.workflow, unserialized)

    @mock.patch("dramatiq_workflow._base.time.time")
    def test_additive_delays(self, time_mock):
        time_mock.return_value = 1717526000.12
        updated_timestamp = time_mock.return_value * 1000
        workflow = Workflow(
            WithDelay(
                Group(
                    self.task.message(),
                    WithDelay(
                        self.task.message(),
                        delay=10,
                    ),
                ),
                delay=10,
            ),
            broker=self.broker,
        )
        workflow.run()
        self.broker.enqueue.assert_has_calls(
            [
                mock.call(
                    self.__make_message(
                        1,
                        message_timestamp=updated_timestamp,
                        message_options={
                            "workflow_completion_callbacks": [
                                (
                                    mock.ANY,  # Accept any callback ID
                                    None,  # No more workflow to run after this
                                    True,
                                )
                            ]
                        },
                    ),
                    delay=10,
                ),
                mock.call(
                    self.__make_message(
                        2,
                        message_timestamp=updated_timestamp,
                        message_options={
                            "workflow_completion_callbacks": [
                                (
                                    mock.ANY,
                                    None,
                                    True,
                                )
                            ]
                        },
                    ),
                    delay=20,
                ),
            ],
            any_order=True,
        )

    @mock.patch("dramatiq_workflow._base.time.time")
    def test_nested_delays(self, time_mock):
        time_mock.return_value = 1717526000.12
        updated_timestamp = time_mock.return_value * 1000
        workflow = Workflow(
            WithDelay(
                WithDelay(
                    self.task.message(),
                    delay=10,
                ),
                delay=10,
            ),
            broker=self.broker,
        )

        workflow.run()
        self.broker.enqueue.assert_called_once_with(
            self.__make_message(
                1,
                message_timestamp=updated_timestamp,
            ),
            delay=20,
        )
