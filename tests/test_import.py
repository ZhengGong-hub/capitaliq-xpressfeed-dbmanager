from capitaliq_xpressfeed_dbmanager import PostgresDatabase, TaskManagerRepository
import os
from dotenv import load_dotenv

load_dotenv()

print("Successfully imported PostgresDatabase and TaskManagerRepository")
print(f"PostgresDatabase: {PostgresDatabase}")
print(f"TaskManagerRepository: {TaskManagerRepository}") 

if __name__ == "__main__":

    db_config = {
        "dbname": os.getenv("POSTGRES_DB"),
        "user": os.getenv("POSTGRES_USER"),
        "password": os.getenv("POSTGRES_PASSWORD"),
        "host": os.getenv("POSTGRES_HOST"),
        "port": os.getenv("POSTGRES_PORT"),
    }
    print(db_config)

    database = PostgresDatabase(**db_config)
    task_manager = TaskManagerRepository(database)

    print(task_manager.test_connection_query())