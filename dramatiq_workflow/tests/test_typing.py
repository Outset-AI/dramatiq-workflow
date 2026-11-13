"""
Builds workflows from messages of real actors, not mocks, so the type checkers
run in CI verify the public API accepts what users actually pass. The runtime
assertions are secondary; the point is that this file type-checks.
"""

import unittest

import dramatiq
from dramatiq.brokers.stub import StubBroker

from .. import Chain, Group, WithDelay, Workflow, walk_messages

broker = StubBroker()


@dramatiq.actor(broker=broker)
def returns_none() -> None:
    pass


@dramatiq.actor(broker=broker)
def returns_int(x: int) -> int:
    return x


@dramatiq.actor(broker=broker)
def returns_str(s: str) -> str:
    return s


class RealActorTypingTests(unittest.TestCase):
    def test_workflow_accepts_messages_with_any_return_type(self) -> None:
        a = returns_none.message()
        b = returns_int.message(1)
        c = returns_str.message("x")
        workflow = Chain(a, Group(b, WithDelay(c, delay=10)))

        Workflow(workflow, broker=broker)
        self.assertEqual(list(walk_messages(workflow)), [a, b, c])
