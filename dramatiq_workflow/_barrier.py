import logging

import dramatiq.rate_limits

logger = logging.getLogger(__name__)


class AtMostOnceBarrier(dramatiq.rate_limits.Barrier):
    """
    A barrier that lets the workflow middleware record completion in two steps.

    The regular barrier `wait` semantics are untouched. Once all parties arrive,
    callers may optionally invoke :meth:`confirm_release` to permanently record
    that the completion callbacks have been scheduled. By deferring this final
    confirmation until after expensive work (for example, unserializing a large
    workflow) has succeeded, we lower the chance that a worker fails between
    releasing the barrier and scheduling follow-up tasks.
    """

    def __init__(self, backend, key, *args, ttl=900000):
        super().__init__(backend, key, *args, ttl=ttl)
        self.ran_key = f"{key}_ran"

    def create(self, parties):
        self.backend.add(self.ran_key, -1, self.ttl)
        return super().create(parties)

    def confirm_release(self):
        """
        Check and set the flag that ensures callbacks only run once.
        """
        never_released = self.backend.incr(self.ran_key, 1, 0, self.ttl)
        if not never_released:
            logger.warning("Barrier %s release already recorded; ignoring subsequent release attempt", self.key)
        return never_released
