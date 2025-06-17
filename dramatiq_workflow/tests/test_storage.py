import unittest

from .._models import SerializedCompletionCallbacks
from .._storage import InlineCallbackStorage


class CallbackStorageTests(unittest.TestCase):
    def setUp(self):
        self.storage = InlineCallbackStorage()

    def test_determine_dedup_key_for_group(self):
        callbacks: SerializedCompletionCallbacks = [("group-id-123", None, True)]
        dedup_key, is_group = self.storage._determine_dedup_key(callbacks)
        self.assertEqual(dedup_key, "group-id-123")
        self.assertTrue(is_group)

    def test_determine_dedup_key_for_chain(self):
        callbacks: SerializedCompletionCallbacks = [("chain-id-456", {"__type__": "chain"}, False)]
        dedup_key, is_group = self.storage._determine_dedup_key(callbacks)
        self.assertEqual(dedup_key, "chain-id-456")
        self.assertFalse(is_group)

    def test_determine_dedup_key_for_nested_callbacks(self):
        callbacks: SerializedCompletionCallbacks = [
            ("chain-id-456", {"__type__": "chain"}, False),
            ("group-id-123", None, True),
        ]
        dedup_key, is_group = self.storage._determine_dedup_key(callbacks)
        self.assertEqual(dedup_key, "group-id-123")
        self.assertTrue(is_group)

    def test_determine_dedup_key_with_empty_list_raises_assertion_error(self):
        with self.assertRaises(AssertionError):
            self.storage._determine_dedup_key([])
