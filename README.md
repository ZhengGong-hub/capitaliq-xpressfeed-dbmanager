# CapitalIQ XpressFeed DB Manager

A Python package for managing CapitalIQ data from XpressFeed channel.

## Installation

### Local Development Installation

This project uses `uv` for dependency management. To install:

```bash
# Install uv if you haven't already
curl -LsSf https://astral.sh/uv/install.sh | sh

# Install the package
uv pip install .
```

For development installation:

```bash
uv pip install -e .
```

### Installing in Other Projects

#### Using pyproject.toml

Add this to your project's `pyproject.toml`:

```toml
[project]
dependencies = [
    "capitaliq-xpressfeed-dbmanager @ git+https://github.com/ZhengGong-hub/capitaliq-xpressfeed-dbmanager.git"
]
```

Then install dependencies:
```bash
uv pip install .
```

#### Using requirements.txt

Add this line to your `requirements.txt`:
```
git+https://github.com/ZhengGong-hub/capitaliq-xpressfeed-dbmanager.git
```

Then install:
```bash
uv pip install -r requirements.txt
```

#### Direct Installation

You can also install directly using:
```bash
uv pip install git+https://github.com/ZhengGong-hub/capitaliq-xpressfeed-dbmanager.git
```

## Quick Start

```python
from capitaliq_xpressfeed_dbmanager import PostgresDatabase, TaskManagerRepository

# Initialize dependencies
database = PostgresDatabase(**config.database.db_config)
task_manager = TaskManagerRepository(database)
```

## Requirements

- Python 3.10 or higher
- PostgreSQL database
- uv for dependency management

## Development

### Running Tests

```bash
# Install test dependencies
uv pip install -e ".[test]"

# Run tests
uv run pytest

# Run tests with coverage
uv run pytest --cov=capitaliq_xpressfeed_dbmanager
```

how to use?
    # Initialize dependencies
    database = PostgresDatabase(**config.database.db_config)
    task_manager = TaskManagerRepository(database)