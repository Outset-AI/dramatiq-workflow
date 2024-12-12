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
from dramatiq.rate_limits.backends import RedisBackend
from dramatiq_workflow import WorkflowMiddleware

backend = RedisBackend()
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
Task 3 and will run 2 seconds after Task 2 finishes.

## License

This project is licensed under the MIT License. See the [LICENSE](LICENSE) file
for details.
