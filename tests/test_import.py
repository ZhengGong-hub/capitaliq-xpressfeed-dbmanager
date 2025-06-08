import pytest
from capitaliq_xpressfeed_dbmanager import PostgresDatabase, TaskManagerRepository
import os
from dotenv import load_dotenv

@pytest.fixture
def db_config():
    load_dotenv()
    return {
        "dbname": os.getenv("POSTGRES_DB"),
        "user": os.getenv("POSTGRES_USER"),
        "password": os.getenv("POSTGRES_PASSWORD"),
        "host": os.getenv("POSTGRES_HOST"),
        "port": os.getenv("POSTGRES_PORT"),
    }

@pytest.fixture
def database(db_config):
    return PostgresDatabase(**db_config)

@pytest.fixture
def task_manager(database):
    return TaskManagerRepository(database)

def test_imports():
    """Test that required classes can be imported"""
    assert PostgresDatabase is not None
    assert TaskManagerRepository is not None

def test_database_connection(database):
    """Test database connection"""
    assert database is not None

def test_task_manager_connection(task_manager):
    """Test task manager connection query"""
    result = task_manager.test_connection_query()
    assert result is not None
    assert not result.empty

def test_get_past_price(task_manager):
    """Test getting past price data"""
    result = task_manager.get_past_price(companyid=24937, traling_x_years=1)
    assert result is not None
    assert not result.empty
    assert 'pricedate' in result.columns
    assert 'priceclose' in result.columns