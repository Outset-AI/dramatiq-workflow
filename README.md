# dramatiq-workflow

`dramatiq-workflow` allows running workflows (chains and groups of tasks) using
the Python background task processing library [dramatiq](https://dramatiq.io/).

A workflow allows running tasks in parallel and in sequence. It is a way to
define a workflow of tasks, a combination of chains and groups in any order and
nested as needed.

## Features

- Define workflows with tasks running in parallel and in sequence.
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
from dramatiq_workflow import WorkflowMiddleware

broker.add_middleware(WorkflowMiddleware())
```

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
from dramatiq_workflow import Workflow, Chain, Group

workflow = Workflow(
    Chain(
        task1.message(),
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

1. Task 1 runs
2. Task 2, 3, and 4 run in parallel once Task 1 finishes
3. Task 5 runs once Task 2 finishes
4. Task 6 and 7 run in parallel once Task 4 finishes
5. Task 8 runs once Task 5, 6, and 7 finish

*This is a simplified example. The actual execution order may vary because
tasks that can run in parallel (i.e., in a Group) are not guaranteed to run in
the order they are defined in the workflow.*

## License

This project is licensed under the MIT License. See the [LICENSE](LICENSE) file for details.
