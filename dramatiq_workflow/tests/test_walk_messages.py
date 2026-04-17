import unittest

import dramatiq

from .. import Chain, Group, WithDelay, walk_messages


def _make_message(message_id: int) -> dramatiq.Message:
    return dramatiq.Message(
        message_id=str(message_id),
        message_timestamp=0,
        queue_name="default",
        actor_name="task",
        args=(),
        kwargs={},
        options={},
    )


class WalkMessagesTests(unittest.TestCase):
    def test_bare_message_yields_itself(self):
        msg = _make_message(1)
        self.assertEqual(list(walk_messages(msg)), [msg])

    def test_bare_message_yields_same_identity(self):
        msg = _make_message(1)
        [yielded] = list(walk_messages(msg))
        self.assertIs(yielded, msg)

    def test_chain_yields_in_order(self):
        a, b, c = _make_message(1), _make_message(2), _make_message(3)
        self.assertEqual(list(walk_messages(Chain(a, b, c))), [a, b, c])

    def test_group_yields_in_order(self):
        a, b = _make_message(1), _make_message(2)
        self.assertEqual(list(walk_messages(Group(a, b))), [a, b])

    def test_nested_chain_group_flattens_in_definition_order(self):
        a, b, c, d = (_make_message(i) for i in range(1, 5))
        workflow = Chain(a, Group(b, c), d)
        self.assertEqual(list(walk_messages(workflow)), [a, b, c, d])

    def test_deeply_nested(self):
        a, b, c, d = (_make_message(i) for i in range(1, 5))
        workflow = Chain(a, Group(Chain(b, c)), d)
        self.assertEqual(list(walk_messages(workflow)), [a, b, c, d])

    def test_with_delay_at_root_is_transparent(self):
        a, b = _make_message(1), _make_message(2)
        self.assertEqual(list(walk_messages(WithDelay(Chain(a, b), delay=1000))), [a, b])

    def test_with_delay_inside_chain_is_transparent(self):
        a, b = _make_message(1), _make_message(2)
        self.assertEqual(list(walk_messages(Chain(WithDelay(a, delay=100), b))), [a, b])

    def test_with_delay_wrapping_single_message(self):
        a = _make_message(1)
        self.assertEqual(list(walk_messages(WithDelay(a, delay=100))), [a])

    def test_empty_chain_yields_nothing(self):
        self.assertEqual(list(walk_messages(Chain())), [])

    def test_empty_group_yields_nothing(self):
        self.assertEqual(list(walk_messages(Group())), [])

    def test_unsupported_type_raises_type_error(self):
        with self.assertRaises(TypeError):
            list(walk_messages("not a workflow"))  # type: ignore[arg-type]

    def test_is_a_generator(self):
        # Make sure it's lazy — mutations during iteration should affect yields
        a, b = _make_message(1), _make_message(2)
        gen = walk_messages(Chain(a, b))
        self.assertIs(next(gen), a)
        a.options["stamped"] = True
        self.assertIs(next(gen), b)
        # Confirm the mutation survived — this is what the README pattern relies on
        self.assertEqual(a.options.get("stamped"), True)
