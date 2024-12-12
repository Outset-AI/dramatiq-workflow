import logging

import dramatiq
import dramatiq.rate_limits

from ._constants import OPTION_KEY_CALLBACKS
from ._helpers import workflow_with_completion_callbacks
from ._models import Barrier
from ._serialize import unserialize_callbacks, unserialize_workflow

logger = logging.getLogger(__name__)


class WorkflowMiddleware(dramatiq.Middleware):
    def __init__(self, rate_limiter_backend: dramatiq.rate_limits.RateLimiterBackend):
        self.rate_limiter_backend = rate_limiter_backend

    def after_process_boot(self, broker: dramatiq.Broker):
        broker.declare_actor(workflow_noop)

    def after_process_message(
        self, broker: dramatiq.Broker, message: dramatiq.broker.MessageProxy, *, result=None, exception=None
    ):
        if exception is not None:
            # TODO: Add a way to handle exceptions in the workflow?
            return

        if message.failed:
            return

        completion_callbacks: list[dict] | None = message.options.get(OPTION_KEY_CALLBACKS)
        if completion_callbacks is None:
            return

        # Go through the completion callbacks backwards until we hit the first non-completed barrier
        while len(completion_callbacks) > 0:
            completion_id, remaining_workflow, propagate = completion_callbacks[-1]
            if completion_id is not None:
                barrier = Barrier(self.rate_limiter_backend, completion_id)
                if not barrier.wait(block=False):
                    logger.debug("Barrier not completed: %s", completion_id)
                    break

            logger.debug("Barrier completed: %s", completion_id)
            completion_callbacks.pop()
            if remaining_workflow is None:
                continue
            workflow_with_completion_callbacks(
                unserialize_workflow(remaining_workflow),
                broker,
                # TODO: This is somewhat inefficient because we're unserializing all callbacks
                # even though we are just going to serialize them again.
                unserialize_callbacks(completion_callbacks),
            ).run()

            if not propagate:
                break


@dramatiq.actor
def workflow_noop():
    """
    This task does nothing. It is used as a placeholder when a Group or a Chain
    has no tasks. This allows us to schedule the execution of the completion
    callbacks via the WorkflowMiddleware.
    """
    pass
