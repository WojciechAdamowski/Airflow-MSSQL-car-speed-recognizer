from modules import tools
from modules import logging

from airflow import DAG, Asset
from airflow.providers.standard.operators.python import PythonOperator
from airflow.providers.standard.sensors.filesystem import FileSensor
from airflow.providers.microsoft.mssql.hooks.mssql import MsSqlHook
from airflow.sdk.bases.hook import BaseHook
from airflow.sdk import chain, cross_downstream

FILE_STORAGE_PATH = BaseHook.get_connection("source_fs").extra_dejson["path"]
DB_CONNECTION_HOOK = MsSqlHook(mssql_conn_id="target_ms_db")
SOURCE_DIRECTORY_ASSET = Asset(uri=FILE_STORAGE_PATH)

TARGET_TABLE_NAME = "fs_car_speed_catches"
TARGET_SCHEMA_NAME = "bronze"


def get_all_files_properties(**context):
    return tools.get_correct_files(
        file_source_directory=SOURCE_DIRECTORY_ASSET.uri
        , run_id=context['run_id']
        , dag_id=context['task_instance'].dag_id
        , target_schema_name=TARGET_SCHEMA_NAME
        , target_table_name=TARGET_TABLE_NAME
        , db_conn_hook=DB_CONNECTION_HOOK
        , file_schema="*"
        , file_extension="csv"
    )


def get_chunks_by_files(**context):
    ti = context["ti"]
    file_properties = ti.xcom_pull(task_ids="get_all_files_properties")
    return tools.get_chunks_by_files(
        file_properties=file_properties
        , chunk_size=50_000
    )


def log_loading_files(**context):
    ti = context["ti"]
    file_properties = ti.xcom_pull(task_ids="get_all_files_properties")
    chunk_properties = ti.xcom_pull(task_ids="extract_data_to_staging_table")
    tools.log_loading_files(
        file_properties=file_properties
        , chunk_properties=chunk_properties
        , file_source_directory=SOURCE_DIRECTORY_ASSET.uri
        , db_conn_hook=DB_CONNECTION_HOOK
    )


def extract_data_from_file_to_staging_table(
        chunk_properties
):
    return tools.extract_csv_file_by_chunks(
        db_conn_hook=DB_CONNECTION_HOOK
        , chunk_properties=chunk_properties
    )


with DAG(
        dag_id='03_extract_data'
        , schedule=None
        , start_date=None
        , tags={'extract'}
        , catchup=False
        , default_args={"on_failure_callback": tools.push_error_to_xcom}
):
    t_start = PythonOperator(
        task_id="start_process"
        , python_callable=logging.start_process
        , op_kwargs={
            "run_id": "{{ run_id }}"
            , "dag_id": "{{ dag.dag_id }}"
            , "db_conn_hook": DB_CONNECTION_HOOK
        }
    )

    t_init_default_directories = PythonOperator(
        task_id='init_default_directories'
        , python_callable=tools.init_default_directories
        , op_kwargs={"base_dir_path": SOURCE_DIRECTORY_ASSET.uri}
    )

    t_check_file_exists = FileSensor(
        task_id="check_file_exists"
        , fs_conn_id="source_fs"
        , filepath="*.csv"
        , poke_interval=10
        , timeout=60
        , mode="reschedule"
    )

    t_get_all_files_properties = PythonOperator(
        task_id="get_all_files_properties"
        , python_callable=get_all_files_properties
    )

    t_get_chunks_by_files = PythonOperator(
        task_id="get_chunks_by_files"
        , python_callable=get_chunks_by_files
    )

    t_extract_data_to_staging_table = PythonOperator.partial(
        task_id='extract_data_to_staging_table'
        , python_callable=extract_data_from_file_to_staging_table
        , max_active_tis_per_dag=4
    ).expand(op_kwargs=t_get_chunks_by_files.output)

    t_log_loading_files = PythonOperator(
        task_id='log_loading_files'
        , python_callable=log_loading_files
        , trigger_rule="all_done"
    )

    t_success = PythonOperator(
        task_id="success"
        , python_callable=logging.end_process_success
        , trigger_rule="all_success"
        , op_kwargs={
            "process_details": t_start.output
            , "db_conn_hook": DB_CONNECTION_HOOK
        }
    )

    t_failure = PythonOperator(
        task_id="failure"
        , python_callable=logging.end_process_failure
        , trigger_rule="one_failed"
        , op_kwargs={
            "process_details": t_start.output
            , "db_conn_hook": DB_CONNECTION_HOOK
        }
    )


chain(
    t_start
    ,t_init_default_directories
    , t_check_file_exists
    , t_get_all_files_properties
    , t_get_chunks_by_files
    , t_extract_data_to_staging_table
    , t_log_loading_files
    , t_success
)

cross_downstream([
    t_init_default_directories
    , t_check_file_exists
    , t_get_all_files_properties
    , t_get_chunks_by_files
    , t_extract_data_to_staging_table
    , t_log_loading_files
], [t_failure])
