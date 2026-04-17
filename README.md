# dramatiq-workflow

[![Run Tests](https://github.com/Outset-AI/dramatiq-workflow/actions/workflows/test.yml/badge.svg)](https://github.com/Outset-AI/dramatiq-workflow/actions/workflows/test.yml)
[![PyPI version](https://badge.fury.io/py/dramatiq-workflow.svg)](https://badge.fury.io/py/dramatiq-workflow)

`dramatiq-workflow` allows running workflows (chains and groups of tasks) using
the Python background task processing library [dramatiq](https://dramatiq.io/).

## Sponsors

[![Outset](docs/outset-logo.svg)](https://outset.ai)

## Motivation

While dramatiq allows running tasks in parallel via groups, and in sequence via
pipelines, it does not provide a way to combine these two concepts.
`dramatiq-workflow` aims to fill this gap and allows creating complex
workflows, similar to the canvas feature in Celery.

## Features

- Define workflows with tasks running in parallel and in sequence using chains
  and groups.
- Nest chains and groups of tasks to create complex workflows.
- Schedules workflows to run in the background using dramatiq.

**Note:** `dramatiq-workflow` does not support passing the results from one task
to the next one in a chain. We recommend using a database to store intermediate
results if needed.

## Installation

You can install `dramatiq-workflow` from PyPI:

```sh
pip install dramatiq-workflow
```

Then, add the `dramatiq-workflow` middleware to your dramatiq broker:

```python
from dramatiq.rate_limits.backends import RedisBackend  # or MemcachedBackend
from dramatiq_workflow import WorkflowMiddleware

backend = RedisBackend()  # or MemcachedBackend()
broker.add_middleware(WorkflowMiddleware(backend))
```

Please refer to the [dramatiq documentation](https://dramatiq.io/guide.html)
for details on how to set up a broker.

## Example

Let's assume we want a workflow that looks like this:

```text
             ╭────────╮  ╭────────╮
             │ Task 2 │  │ Task 5 │
          ╭──┼●      ●┼──┼●      ●┼╮
╭────────╮│  ╰────────╯  ╰────────╯│  ╭────────╮
│ Task 1 ││  ╭────────╮            │  │ Task 8 │
│       ●┼╯  │ Task 3 │            ╰──┼●       │
│       ●┼───┼●      ●┼───────────────┼●       │
│       ●┼╮  ╰────────╯             ╭─┼●       │
╰────────╯│  ╭────────╮   ╭────────╮│╭┼●       │
          │  │ Task 4 │   │ Task 6 │││╰────────╯
          ╰──┼●      ●┼───┼●      ●┼╯│
             │       ●┼╮  ╰────────╯ │
             ╰────────╯│             │
                       │  ╭────────╮ │
                       │  │ Task 7 │ │
                       ╰──┼●      ●┼─╯
                          ╰────────╯
```

We can define this workflow as follows:

```python
import dramatiq
from dramatiq_workflow import Workflow, Chain, Group

@dramatiq.actor
def task1(arg1, arg2, arg3):
    print("Task 1")

@dramatiq.actor
def task2():
    print("Task 2")

# ...

workflow = Workflow(
    Chain(
        task1.message("arguments", "go", "here"),
        Group(
            Chain(
                task2.message(),
                task5.message(),
            ),
            task3.message(),
            Chain(
                task4.message(),
                Group(
                    task6.message(),
                    task7.message(),
                ),
            ),
        ),
        task8.message(),
    ),
)
workflow.run()  # Schedules the workflow to run in the background
```

### Execution Order

In this example, the execution would look like this:

1. Task 1 runs (with arguments `"arguments"`, `"go"`, and `"here"`)
2. Task 2, 3, and 4 run in parallel once Task 1 finishes
3. Task 5 runs once Task 2 finishes
4. Task 6 and 7 run in parallel once Task 4 finishes
5. Task 8 runs once Task 5, 6, and 7 finish

*This is a simplified example. The actual execution order may vary because
tasks that can run in parallel (i.e. in a `Group`) are not guaranteed to run in
the order they are defined in the workflow.*

## Advanced Usage

### `WithDelay`

The `WithDelay` class allows delaying the execution of a task or a group of tasks:

```python
from dramatiq_workflow import Chain, Group, WithDelay, Workflow

workflow = Workflow(
    Chain(
        task1.message("arguments", "go", "here"),
        WithDelay(task2.message(), delay=1_000),
        WithDelay(
            Group(
                task3.message(),
                task4.message(),
            ),
            delay=2_000,
        ),
    )
)
```

In this example, Task 2 will run roughly 1 second after Task 1 finishes, and
Tasks 3 and 4 will run 2 seconds after Task 2 finishes.

### Handling Permanent Failures

`dramatiq-workflow` does not provide built-in workflow-level failure handling.
"What should happen when a workflow fails" is highly application-specific —
fire-once vs. per-message, cancel pending steps or not, what to pass to the
handler, how to interact with retries — and no single default is right for
everyone.

Instead, `dramatiq-workflow` exposes a helper, `walk_messages`, that makes it
easy to build the semantics you want in a handful of lines of your own code.
The pattern: stamp every message in the workflow with a failure-callback
payload, then register a middleware that enqueues the payload when a message
permanently fails.

```python
import dramatiq
from dramatiq_workflow import Chain, Group, Workflow, walk_messages

@dramatiq.actor
def handle_failure(failed_message_id, failed_actor_name):
    # Your alerting / cleanup / compensation logic.
    # Should be idempotent — see caveats below.
    ...

# 1. Build the workflow tree and stamp every message with a failure payload.
workflow_tree = Chain(
    task1.message(),
    Group(task2.message(), task3.message()),
    task4.message(),
)
for msg in walk_messages(workflow_tree):
    msg.options["on_failure"] = handle_failure.message(
        msg.message_id, msg.actor_name
    ).asdict()

Workflow(workflow_tree).run()
```

```python
# 2. Register a middleware that fires the stored callback on permanent failure.
class WorkflowFailureMiddleware(dramatiq.Middleware):
    def after_process_message(self, broker, message, *, result=None, exception=None):
        if message.failed and "on_failure" in message.options:
            broker.enqueue(dramatiq.Message(**message.options["on_failure"]))

broker.add_middleware(WorkflowFailureMiddleware())
```

#### Caveats

- **Fires per failing message.** A `Group` with three failing members enqueues
  three failure callbacks. Make your handler idempotent.
- **Requires dramatiq's `Retries` middleware** (active by default) to set
  `message.failed=True` once retries are exhausted.
- **The stamped payload lives in `message.options`** and must be
  JSON-serializable by your broker's encoder.
- **Nested workflows** dispatched from inside an actor are not stamped
  automatically — call `walk_messages` on those trees too.
- **`workflow_noop`** (auto-inserted by the library for empty `Chain` or
  `Group`) is never stamped, but it does nothing and cannot meaningfully fail.
- **The failure-handler message itself** should not carry a completion-callback
  option — enqueue it fresh as shown, don't clone a workflow message.

### Large Workflows

Because of how `dramatiq-workflow` is implemented, each task in a workflow has
to know about the remaining tasks in the workflow that could potentially run
after it. By default, this is stored alongside your messages in the message
queue. When a workflow has a large number of tasks, it can lead to an
increase of memory usage in the broker and increased network traffic between
the broker and the workers, especially when using `Group` tasks: Each task in a
`Group` can potentially be the last one to finish, so each task has to retain a
copy of the remaining tasks that run after the `Group`.

There are a few things you can do to alleviate this issue:

- Minimize the usage of parameters in the `message` method. Instead, consider
  using a database to store data that is required by your tasks.
- Limit the size of groups to a reasonable number of tasks. Instead of
  scheduling one task with 1000 tasks in a group, consider scheduling 10 groups
  with 100 tasks each and chaining them together.
- Consider breaking down large workflows into smaller partial workflows that
  then schedule a subsequent workflow at the very end of the outermost `Chain`.

#### Compression

You can use compression to reduce the size of the messages in your queue. While
dramatiq does not provide a compression implementation by default, one can be
added with just a few lines of code. For example:

```python
import dramatiq
from dramatiq.encoder import JSONEncoder, MessageData
import lz4.frame

class DramatiqLz4JSONEncoder(JSONEncoder):
    def encode(self, data: MessageData) -> bytes:
        return lz4.frame.compress(super().encode(data))

    def decode(self, data: bytes) -> MessageData:
        try:
            decompressed = lz4.frame.decompress(data)
        except RuntimeError:
            # Uncompressed data from before the switch to lz4
            decompressed = data
        return super().decode(decompressed)

dramatiq.set_encoder(DramatiqLz4JSONEncoder())
```

#### Callback Storage

To completely eliminate the issue of large workflows being stored in your
message queue, you can provide a custom callback storage backend to the
`WorkflowMiddleware`. A callback storage backend is responsible for storing and
retrieving the list of callbacks. For example, you could implement a storage
backend that stores the callbacks in S3 and only stores a reference to the S3
object in the message options.

A storage backend must implement the `CallbackStorage` interface:

```python
from typing import Any
from dramatiq_workflow import CallbackStorage, SerializedCompletionCallbacks

class MyS3Storage(CallbackStorage):
    def store(self, callbacks: SerializedCompletionCallbacks) -> Any:
        # ... store in S3 and return a key
        pass

    def retrieve(self, ref: Any) -> SerializedCompletionCallbacks:
        # ... retrieve from S3 using the key
        pass
```

Then, you can pass an instance of your custom storage backend to the
`WorkflowMiddleware`:

```python
from dramatiq.rate_limits.backends import RedisBackend
from dramatiq_workflow import WorkflowMiddleware

backend = RedisBackend()
storage = MyS3Storage()  # Your custom storage backend
broker.add_middleware(WorkflowMiddleware(backend, callback_storage=storage))
```

##### Deduplicating Workflows

For convenience, `dramatiq-workflow` provides an abstract
`DedupWorkflowCallbackStorage` class that you can use to separate the storage
of workflows from the storage of callbacks. This is useful for deduplicating
large workflow definitions that may be part of multiple callbacks, especially
when chaining large groups of tasks.

To use it, you need to subclass `DedupWorkflowCallbackStorage` and implement
the `_store_workflow` and `_load_workflow` methods.

```python
from typing import Any
from dramatiq_workflow import DedupWorkflowCallbackStorage

class MyDedupStorage(DedupWorkflowCallbackStorage):
    def __init__(self):
        # In a real application, this would be a persistent storage like a
        # database or a distributed cache so that workers and producers can
        # both access it.
        self.__workflow_storage = {}

    def _store_workflow(self, id: str, workflow: dict) -> Any:
        # Using the `id` (which is the completion ID) to deduplicate.
        workflow_key = id
        if workflow_key not in self.__workflow_storage:
            self.__workflow_storage[workflow_key] = workflow
        return workflow_key  # Return a reference to the workflow.

    def _load_workflow(self, id: str, ref: Any) -> dict:
        # `ref` is what `_store_workflow` returned.
        return self.__workflow_storage[ref]
```

### Barrier

`dramatiq-workflow` uses a barrier mechanism to keep track of the current state
of a workflow. For example, every time a task in a `Group` is completed, the
barrier is decreased by one. When the barrier reaches zero, the next task in
the outer `Chain` is scheduled to run.

By default, `dramatiq-workflow` uses a custom `AtMostOnceBarrier` that ensures
the barrier is never released more than once. When the barrier reaches zero, an
additional key is set in the backend to prevent releasing the barrier again. In
almost all cases, this is the desired behavior since releasing a barrier more
than once could lead to duplicate tasks being scheduled - which would have
severe compounding effects in a workflow with many `Group` tasks.

However, there is a small chance that the barrier is never released. This can
happen when the configured `rate_limiter_backend` loses its state or when the
worker unexpectedly crashes before scheduling the next task in the workflow.

To configure a different barrier implementation such as dramatiq's default
`Barrier`, you can pass it to the `WorkflowMiddleware`:

```python
from dramatiq.rate_limits import Barrier
from dramatiq.rate_limits.backends import RedisBackend
from dramatiq_workflow import WorkflowMiddleware

backend = RedisBackend()
broker.add_middleware(WorkflowMiddleware(backend, barrier_type=Barrier))
```

## License

This project is licensed under the MIT License. See the [LICENSE](LICENSE) file
for details.
