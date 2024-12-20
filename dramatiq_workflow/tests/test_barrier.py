import unittest

from dramatiq.rate_limits.backends import StubBackend

from .._barrier import AtMostOnceBarrier


class AtMostOnceBarrierTests(unittest.TestCase):
    def setUp(self):
        self.backend = StubBackend()
        self.key = "test_barrier"
        self.parties = 3
        self.ttl = 900000
        self.barrier = AtMostOnceBarrier(self.backend, self.key, ttl=self.ttl)

    def test_wait_block_true_raises(self):
        with self.assertRaises(ValueError) as context:
            self.barrier.wait(block=True)
        self.assertEqual(str(context.exception), "Blocking is not supported by AtMostOnceBarrier")

    def test_wait_releases_once(self):
        self.barrier.create(self.parties)
        for _ in range(self.parties - 1):
            result = self.barrier.wait(block=False)
            self.assertFalse(result)
        result = self.barrier.wait(block=False)
        self.assertTrue(result)
        result = self.barrier.wait(block=False)
        self.assertFalse(result)

    def test_wait_does_not_release_when_db_emptied(self):
        """
        If the store is emptied, the barrier should not be released.
        """
        self.barrier.create(self.parties)
        self.backend.db = {}
        for _ in range(self.parties):
            result = self.barrier.wait(block=False)
            self.assertFalse(result)
