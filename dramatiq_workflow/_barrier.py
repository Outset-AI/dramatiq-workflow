import logging
from typing import Any

import dramatiq.rate_limits

logger = logging.getLogger(__name__)


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

    def __init__(
        self, backend: dramatiq.rate_limits.RateLimiterBackend, key: str, *args: Any, ttl: int = 900000
    ) -> None:
        super().__init__(backend, key, *args, ttl=ttl)
        self.ran_key = f"{key}_ran"

    def create(self, parties: int) -> bool:
        self.backend.add(self.ran_key, -1, self.ttl)
        created: bool = super().create(parties)
        return created

    def wait(self, *args: Any, block: bool = True, timeout: int | None = None) -> bool:
        if block:
            # Blocking with an AtMostOnceBarrier is not supported as it could
            # lead to clients waiting indefinitely if the barrier already
            # released.
            raise ValueError("Blocking is not supported by AtMostOnceBarrier")

        released: bool = super().wait(*args, block=False)
        if released:
            never_released: bool = self.backend.incr(self.ran_key, 1, 0, self.ttl)
            if not never_released:
                logger.warning("Barrier %s release already recorded; ignoring subsequent release attempt", self.key)
            return never_released

        return False
