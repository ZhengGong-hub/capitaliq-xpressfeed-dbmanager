# CapitalIQ XpressFeed DB Manager

A Python package for managing CapitalIQ data from XpressFeed channel.

## Installation

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

## Quick Start

```python
from capitaliq_xpressfeed_dbmanager import PostgresDatabase, TaskManagerRepository

# Initialize dependencies
database = PostgresDatabase(**config.database.db_config)
task_manager = TaskManagerRepository(database)
```

## Requirements

- Python 3.7 or higher
- PostgreSQL database
- uv for dependency management

how to use?
    # Initialize dependencies
    database = PostgresDatabase(**config.database.db_config)
    task_manager = TaskManagerRepository(database)