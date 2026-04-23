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

    def test_confirm_release_only_succeeds_once(self):
        self.barrier.create(self.parties)
        for _ in range(self.parties - 1):
            self.assertFalse(self.barrier.wait(block=False))

        self.assertTrue(self.barrier.wait(block=False))
        self.assertTrue(self.barrier.confirm_release())
        self.assertFalse(self.barrier.confirm_release())
