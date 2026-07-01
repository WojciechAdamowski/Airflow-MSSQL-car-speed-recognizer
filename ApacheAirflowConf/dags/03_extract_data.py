from typing import List

from airflow import DAG, Asset
from airflow.providers.standard.operators.python import PythonOperator
from airflow.providers.standard.sensors.filesystem import FileSensor
from airflow.providers.microsoft.mssql.hooks.mssql import MsSqlHook
from airflow.sdk.bases.hook import BaseHook
from airflow.sdk import chain

from datetime import datetime

import pandas as pd
import pathlib
import glob
import os
import shutil

FILE_STORAGE_PATH = BaseHook.get_connection("source_fs").extra_dejson["path"]
DB_CONNECTION_HOOK = MsSqlHook(mssql_conn_id="target_ms_db")
SOURCE_DIRECTORY_ASSET = Asset(uri=FILE_STORAGE_PATH)


def check_column_count(source_df: pd.DataFrame, column_count: int) -> bool:
    return len(source_df.columns) == column_count


def check_column_exists(source_df: pd.DataFrame, expected_columns: List[str]) -> bool:
    return len(
        [expected_column for expected_column in expected_columns if expected_column not in source_df.columns]) == 0


def check_files_correctness():
    quarantine_dir_path = SOURCE_DIRECTORY_ASSET.uri + "/quarantine"
    if not os.path.exists(quarantine_dir_path):
        os.mkdir(quarantine_dir_path)

    files_to_check = glob.glob(SOURCE_DIRECTORY_ASSET.uri + "/*.csv")
    df_metadata = pd.DataFrame()

    print(f"CHECKING CORRECTNESS FOR {len(files_to_check)} FILES")

    expected_cols = [
        "entry_timestamp",
        "exit_timestamp",
        "segment_id",
        "segment_name",
        "segment_length_m",
        "speed_limit_kmh",
        "plate_number",
        "vehicle_type"
    ]

    wrong_files_count = 0
    for file in files_to_check:
        print(f"CHECKING CORRECTNESS FOR {file} FILE ({files_to_check.index(file)+1}/{len(files_to_check)})")
        df = pd.read_csv(file)

        if not check_column_count(df, len(expected_cols)):
            print("WRONG COLUMN COUNT")
            shutil.move(file, quarantine_dir_path)
            wrong_files_count += 1
        elif not check_column_exists(df, expected_cols):
            print("MISSING COLUMNS")
            shutil.move(file, quarantine_dir_path)
            wrong_files_count += 1
        else:
            print("CORRECT")
        print(f"END OF CHECKING CORRECTNESS THERE WAS A {wrong_files_count} WRONG FILES FOR {len(files_to_check)} FILES")


def extract_data_from_file_to_staging_table(**context):
    run_id = context["run_id"]

    csv_files = glob.glob(SOURCE_DIRECTORY_ASSET.uri + "/*.csv")
    for file in csv_files:
        try:

            df = pd.read_csv(file)
            df['md_batch_id'] = run_id
            df['md_file_name'] = pathlib.Path(file).name

            conn = DB_CONNECTION_HOOK.get_sqlalchemy_engine()

            with conn.begin() as connection:
                df.to_sql(
                    name="fs_car_speed_catches"
                    , con=connection
                    , schema="bronze"
                    , if_exists="append"
                    , index=False
                    , chunksize=1000
                    , method='multi'
                )

            os.remove(file)
        except Exception as e:
            print('ERROR WHILE EXTRACTING DATA:', e)


with DAG(dag_id='03_extract_data', schedule=None, start_date=None, tags={'extract'}, catchup=False):
    t_check_file_exists = FileSensor(
        task_id="check_file_exists"
        , fs_conn_id="source_fs"
        , filepath="*.csv"
        , poke_interval=10
        , timeout=60
        , mode="reschedule"
    )

    t_check_files_correctness = PythonOperator(
        task_id='check_files_correctness'
        , python_callable=check_files_correctness
    )

    t_extract_data_to_staging_table = PythonOperator(
        task_id='extract_data_to_staging_table'
        , python_callable=extract_data_from_file_to_staging_table
    )

chain(t_check_file_exists, t_check_files_correctness, t_extract_data_to_staging_table)
