# Night Batch - Async Workflow Orchestration

A declarative, type-safe workflow engine for building and executing directed acyclic graphs (DAGs) of tasks.

## Features

✅ **Async execution** with concurrent task support
✅ **Declarative API** with decorators and builders
✅ **Type-safe** with full type hints
✅ **Dependency management** for DAG workflows
✅ **Subflows** (nested workflows)
✅ **Retry policies** (automatic and manual)
✅ **State persistence** (SQLite, JSON)
✅ **Event-driven** execution model
✅ **DRY and SOLID** architecture

## Requirements

- Python >= 3.10
- No external dependencies (uses only Python standard library)

## Installation

### Basic Installation

```bash
# Clone repository
git clone <repository-url>
cd night-batch

# Install runtime dependencies (none required, uses stdlib)
# Library is ready to use!
```

### Development Installation

```bash
# Install development dependencies
pip install -r requirements-dev.txt

# Or just testing dependencies
pip install -r requirements.txt
```

## Quick Start

### Example 1: Simple Workflow with Decorators

```python
from app.infra.flow import AsyncFlow, Context, SQLiteStore

# Create workflow
store = SQLiteStore("workflows.db")
store.open()
flow = AsyncFlow("etl_pipeline", store)

# Define tasks with decorators
@flow.task("extract")
def extract(ctx: Context):
    return {"data": [1, 2, 3, 4, 5]}

@flow.task("transform", depends_on=["extract"])
def transform(ctx: Context):
    data = ctx.pull_output("extract")
    return {"doubled": [x * 2 for x in data["data"]]}

@flow.task("load", depends_on=["transform"])
def load(ctx: Context):
    result = ctx.pull_output("transform")
    print(f"Loading: {result}")
    return {"status": "success"}

# Execute workflow
import asyncio

async def main():
    run_id = flow.init_run(params={"user": "alice"})
    await flow.run_until_complete(run_id)
    print(f"Workflow completed: {run_id}")

asyncio.run(main())
```

### Example 2: Workflow with Builder Pattern

```python
from app.infra.flow import FlowBuilder, SQLiteStore

store = SQLiteStore("workflows.db")
store.open()

# Build workflow declaratively
builder = FlowBuilder("data_pipeline", store)
flow = (builder
    .add_task("extract", extract_func)
    .add_task("transform", transform_func, depends_on=["extract"])
    .add_task("load", load_func, depends_on=["transform"])
    .with_retry("extract", max_retries=3)
    .build())

# Execute
import asyncio
run_id = flow.init_run()
asyncio.run(flow.run_until_complete(run_id))
```

### Example 3: Parallel Execution

```python
@flow.task("task_a")
def task_a(ctx):
    return "A"

@flow.task("task_b")
def task_b(ctx):
    return "B"

@flow.task("task_c")
def task_c(ctx):
    return "C"

@flow.task("aggregate", depends_on=["task_a", "task_b", "task_c"])
def aggregate(ctx):
    a = ctx.pull_output("task_a")
    b = ctx.pull_output("task_b")
    c = ctx.pull_output("task_c")
    return f"{a}{b}{c}"
```

### Example 4: Subflows

```python
# Create a validation subflow
validation_flow = AsyncFlow("validation", store)

@validation_flow.task("validate_schema")
def validate_schema(ctx):
    return {"valid": True}

@validation_flow.task("validate_quality", depends_on=["validate_schema"])
def validate_quality(ctx):
    return {"score": 95}

# Use subflow in main workflow
@flow.task("extract")
def extract(ctx):
    return {"data": [...]}

flow.add_subflow_definition(
    subflow_id="validation",
    child_flow=validation_flow,
    dependencies=["extract"]
)

@flow.task("load", depends_on=["validation"])
def load(ctx):
    return {"loaded": True}
```

## Running Tests

```bash
# Run all tests (from project root)
python -m pytest

# Or simply
pytest

# Run specific test file
python -m pytest app/infra/flow/__tests__/test_single_task.py

# Run with coverage
python -m pytest --cov=app/infra/flow --cov-report=html

# Run verbose
python -m pytest -v

# Run specific test
python -m pytest app/infra/flow/__tests__/test_single_task.py::test_single_task_success

# Quick summary
python -m pytest -q
```

## Project Structure

```
night-batch/
├── app/
│   └── infra/
│       └── flow/
│           ├── __init__.py           # Public API
│           ├── flow.py               # AsyncFlow main class
│           ├── models.py             # Data models and types
│           ├── executors.py          # Task execution logic
│           ├── event_manager.py      # Event handling
│           ├── retry_manager.py      # Retry logic
│           ├── sqlite_store.py       # SQLite storage
│           ├── json_store.py         # JSON storage
│           ├── workflow_builders.py  # Builder pattern
│           └── __tests__/            # Test suite
│               ├── conftest.py       # Test fixtures
│               ├── test_single_task.py
│               └── test_sequential.py
├── requirements.txt                   # Runtime dependencies
├── requirements-dev.txt               # Dev dependencies
├── pytest.ini                         # Pytest configuration
└── README.md                          # This file
```

## API Documentation

### Core Classes

- **`AsyncFlow`**: Main workflow orchestration engine
- **`Context`**: Execution context passed to task functions
- **`SQLiteStore`**: SQLite-based state persistence
- **`JsonStore`**: JSON file-based state persistence
- **`FlowBuilder`**: Fluent API for programmatic workflow construction

### Key Methods

#### AsyncFlow

```python
# Task registration
@flow.task(task_id, depends_on=[], max_retries=0)
def my_task(ctx: Context):
    ...

# Subflow registration
flow.add_subflow_definition(subflow_id, child_flow, dependencies=[])

# Execution
run_id = flow.init_run(params={})
await flow.run_until_complete(run_id, max_concurrency=0)

# Retry
await flow.retry(run_id, task_id, reset_downstream=True)
```

#### Context

```python
# Access workflow params
ctx.workflow_run.params

# Pull data from upstream task
data = ctx.pull_output("upstream_task_id")

# Push output value
ctx.push({"result": "value"})
```

## Architecture

### SOLID Principles

- **Single Responsibility**: Separate modules for execution, events, retry
- **Open/Closed**: Extensible via protocols and abstract classes
- **Liskov Substitution**: Storage backends are interchangeable
- **Interface Segregation**: Minimal, focused protocols
- **Dependency Inversion**: Depends on abstractions (Store protocol)

### Design Patterns

- **Decorator Pattern**: Task registration via `@task()`
- **Builder Pattern**: Fluent API via `FlowBuilder`
- **Strategy Pattern**: Pluggable storage backends
- **Observer Pattern**: Event-driven execution
- **Template Method**: Task executors

## Testing

The project includes comprehensive test coverage:

- ✅ **11 tests** for single-task workflows
- ✅ **13 tests** for sequential workflows
- ✅ **DRY test helpers** for reusability
- ✅ **Declarative test specifications**
- ✅ **Parameterized tests** for multiple scenarios

Test statistics:
```
24 passed in 0.15s
0 warnings
100% success rate
```

## Contributing

1. Fork the repository
2. Create a feature branch
3. Add tests for new functionality
4. Ensure all tests pass: `pytest`
5. Submit a pull request

## License

[Add your license here]

## Support

For issues and questions, please open an issue on GitHub.
