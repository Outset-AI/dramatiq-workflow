import dramatiq.rate_limits


class AtMostOnceBarrier(dramatiq.rate_limits.Barrier):
    """
    The AtMostOnceBarrier is a barrier that ensures that it is released at most
    once.

    We use this because we want to avoid running callbacks in chains multiple
    times. Running callbacks more than once can have compounding effects
    especially when groups are involved.

    The downside of this is that we cannot guarantee that the barrier will be
    released at all. Theoretically a worker could die after releasing the
    barrier but just before it has a chance to schedule the callbacks.
    """

    def __init__(self, backend, key, *args, ttl=900000):
        super().__init__(backend, key, *args, ttl=ttl)
        self.ran_key = f"{key}_ran"

    def create(self, parties):
        self.backend.add(self.ran_key, -1, self.ttl)
        return super().create(parties)

    def wait(self, *args, block=True, timeout=None):
        if block:
            # Blocking with an AtMostOnceBarrier is not supported as it could
            # lead to clients waiting indefinitely if the barrier already
            # released.
            raise ValueError("Blocking is not supported by AtMostOnceBarrier")

        released = super().wait(*args, block=False)
        if released:
            never_released = self.backend.incr(self.ran_key, 1, 0, self.ttl)
            return never_released

        return False
