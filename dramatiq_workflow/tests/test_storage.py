import unittest
from typing import Any

from .._models import SerializedCompletionCallbacks
from .._storage import DedupWorkflowCallbackStorage


class MyDedupStorage(DedupWorkflowCallbackStorage):
    def __init__(self):
        self.workflows = {}
        self.callbacks = {}
        self.workflow_store_calls = []
        self.workflow_load_calls = []

    def _store_workflow(self, id: str, workflow: dict) -> Any:
        self.workflow_store_calls.append((id, workflow))
        ref = f"workflow-ref-{id}"
        self.workflows[ref] = workflow
        return ref

    def _load_workflow(self, id: str, ref: Any) -> dict:
        self.workflow_load_calls.append((id, ref))
        return self.workflows[ref]


class DedupWorkflowCallbackStorageTests(unittest.TestCase):
    def setUp(self):
        self.storage = MyDedupStorage()

    def test_store_and_retrieve(self):
        workflow_dict = {"__type__": "chain", "children": []}
        callbacks: SerializedCompletionCallbacks = [
            ("id1", workflow_dict, False),
            ("id2", None, True),
        ]

        # Store callbacks
        callbacks_ref = self.storage.store(callbacks)
        self.assertEqual(callbacks_ref, [("id1", "workflow-ref-id1", False), ("id2", None, True)])

        # Check what was stored
        self.assertEqual(len(self.storage.workflow_store_calls), 1)
        self.assertEqual(self.storage.workflow_store_calls[0], ("id1", workflow_dict))

        # Retrieve callbacks
        retrieved_callbacks = self.storage.retrieve(callbacks_ref)
        self.assertEqual(len(retrieved_callbacks), 2)

        # Check first callback (with workflow)
        id1, loader1, is_group1 = retrieved_callbacks[0]
        self.assertEqual(id1, "id1")
        self.assertFalse(is_group1)
        self.assertTrue(callable(loader1))

        # Check second callback (without workflow)
        id2, loader2, is_group2 = retrieved_callbacks[1]
        self.assertEqual(id2, "id2")
        self.assertTrue(is_group2)
        self.assertIsNone(loader2)

        # Load the workflow
        self.assertEqual(len(self.storage.workflow_load_calls), 0)
        assert callable(loader1)
        loaded_workflow = loader1()
        self.assertEqual(len(self.storage.workflow_load_calls), 1)
        self.assertEqual(self.storage.workflow_load_calls[0], ("id1", "workflow-ref-id1"))
        self.assertEqual(loaded_workflow, workflow_dict)

    def test_store_with_unsupported_workflow_type(self):
        def unsupported_lazy_workflow():
            return {"__type__": "chain", "children": []}

        callbacks: SerializedCompletionCallbacks = [
            ("id1", unsupported_lazy_workflow, False),
        ]

        with self.assertRaises(TypeError):
            self.storage.store(callbacks)

    def test_store_with_already_lazy_loaded_workflow(self):
        # This test ensures that when we store a workflow that has already been
        # loaded and wrapped by the storage, we don't try to store it again,
        # but instead use its reference.

        # 1. Store a workflow and retrieve it.
        workflow_dict = {"__type__": "chain", "children": []}
        callbacks1: SerializedCompletionCallbacks = [("id1", workflow_dict, False)]
        callbacks_ref1 = self.storage.store(callbacks1)
        retrieved_callbacks1 = self.storage.retrieve(callbacks_ref1)
        _, lazy_loader, _ = retrieved_callbacks1[0]

        # We should have one call to store the workflow
        self.assertEqual(len(self.storage.workflow_store_calls), 1)
        self.assertTrue(callable(lazy_loader))

        # 2. Now, create new callbacks using the lazy loader from the previous step
        #    and store them.
        callbacks2: SerializedCompletionCallbacks = [("id2", lazy_loader, False)]
        callbacks_ref2 = self.storage.store(callbacks2)

        # 3. _store_workflow should NOT have been called again.
        self.assertEqual(len(self.storage.workflow_store_calls), 1)

        # 4. The new stored callbacks should contain the original workflow reference.
        self.assertEqual(callbacks_ref2[0][1], callbacks_ref1[0][1])
