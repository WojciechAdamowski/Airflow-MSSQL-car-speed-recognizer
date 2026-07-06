from modules import tools

from airflow import DAG, Asset
from airflow.providers.standard.operators.python import PythonOperator
from airflow.providers.standard.sensors.filesystem import FileSensor
from airflow.providers.microsoft.mssql.hooks.mssql import MsSqlHook
from airflow.sdk.bases.hook import BaseHook
from airflow.sdk import chain

import glob

FILE_STORAGE_PATH = BaseHook.get_connection("source_fs").extra_dejson["path"]
DB_CONNECTION_HOOK = MsSqlHook(mssql_conn_id="target_ms_db")
SOURCE_DIRECTORY_ASSET = Asset(uri=FILE_STORAGE_PATH)

TARGET_TABLE_NAME = "fs_car_speed_catches"
TARGET_SCHEMA_NAME = "bronze"


def check_files_correctness(**context):
    tools.check_files_correctness_csv(
        file_source_directory=SOURCE_DIRECTORY_ASSET.uri
        , target_table_name=TARGET_TABLE_NAME
        , target_schema_name=TARGET_SCHEMA_NAME
        , db_conn_hook=DB_CONNECTION_HOOK
        , file_name='*'
        , run_id=context['run_id']
    )


def chunk_big_files():
    tools.split_csv_files(
        file_source_directory=SOURCE_DIRECTORY_ASSET.uri
        , file_name='*'
    )


def get_files_to_extract():
    csv_files = glob.glob(SOURCE_DIRECTORY_ASSET.uri + f"/ready/*.csv")
    csv_files.sort()
    return [
        {
            "file_name": file.split("/")[-1].split(".")[0]
        }
        for file in csv_files
    ]


def extract_data_from_files_to_staging_table(**context):
    tools.extract_csv_files(
        file_source_directory=SOURCE_DIRECTORY_ASSET.uri
        , target_table_name=TARGET_TABLE_NAME
        , target_schema_name=TARGET_SCHEMA_NAME
        , db_conn_hook=DB_CONNECTION_HOOK
        , file_name='*'
        , run_id=context['run_id']
    )


def extract_data_from_file_to_staging_table(
        file_name: str
        , **context
):
    tools.extract_csv_file(
        file_source_directory=SOURCE_DIRECTORY_ASSET.uri
        , target_table_name=TARGET_TABLE_NAME
        , target_schema_name=TARGET_SCHEMA_NAME
        , db_conn_hook=DB_CONNECTION_HOOK
        , file_name=file_name
        , run_id=context['run_id']
    )


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

    t_chunk_big_files = PythonOperator(
        task_id='chunk_big_files'
        , python_callable=chunk_big_files
    )

    t_get_files_to_extract = PythonOperator(
        task_id='get_files_to_extract'
        , python_callable=get_files_to_extract
    )

    t_extract_data_to_staging_table = PythonOperator.partial(
        task_id='extract_data_to_staging_table'
        , python_callable=extract_data_from_file_to_staging_table
        , max_active_tis_per_dag=4
    ).expand(op_kwargs=t_get_files_to_extract.output)

chain(
    t_check_file_exists
    , t_check_files_correctness
    , t_chunk_big_files
    , t_get_files_to_extract
    , t_extract_data_to_staging_table
)
