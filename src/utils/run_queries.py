from capitaliq_xpressfeed_dbmanager import PostgresDatabase, TaskManagerRepository
import os
from dotenv import load_dotenv
import json
import pandas as pd

def db_config():
    load_dotenv()
    return {
        "dbname": os.getenv("POSTGRES_DB"),
        "user": os.getenv("POSTGRES_USER"),
        "password": os.getenv("POSTGRES_PASSWORD"),
        "host": os.getenv("POSTGRES_HOST"),
        "port": os.getenv("POSTGRES_PORT"),
    }

def test_dataitem_info(task_manager):
    dataitem_info = task_manager.get_dataitem_info(all=True)
    dataitem_info.to_csv("data/ref/dataitem_info.csv", index=False)
    print(dataitem_info)

def test_fundamental_data(task_manager):
    with open('data/ref/fundamental.json', 'r') as f:
        fundamental_data = json.load(f)
    
    fundamental_dataitem = [item['dataitemid'] for item in fundamental_data['is']]
    fundamental_margins = [item['dataitemid'] for item in fundamental_data['margins']]
    fundamental_bs = [item['dataitemid'] for item in fundamental_data['bs']]
    fundamental_other = [item['dataitemid'] for item in fundamental_data['other']]
    fundamental_eps = [item['dataitemid'] for item in fundamental_data['eps']]

    df = task_manager.get_historical_fundamental(
        ls_ids=[24937],
        ls_dataitemid=fundamental_dataitem + fundamental_margins + fundamental_bs + fundamental_eps,
        startyear=2014,
        periodtypeid=[1, 2],
    ).sort_values(by=['calendaryear', 'calendarquarter', 'instancedate'])

    df_yearly = df[df['periodtypeid'] == 1]
    df_quarterly = df[df['periodtypeid'] == 2]

    df_yearly.drop_duplicates(subset=['companyid', 'dataitemid', 'calendaryear', 'calendarquarter'], inplace=True)

    df_yearly_pivot = df_yearly.pivot(index=['companyid', 'calendaryear', 'calendarquarter'], columns='dataitemname', values='dataitemvalue')
    print(df_yearly_pivot)

def test_metadata_info(task_manager):
    task_manager.get_metadata_info(ticker="BEPC", country="all")

if __name__ == "__main__":
    db = PostgresDatabase(**db_config())
    task_manager = TaskManagerRepository(db)
    
    # Choose which test to run
    test_metadata_info(task_manager)
    # test_dataitem_info(task_manager)
    # test_fundamental_data(task_manager)