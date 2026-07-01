from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.sdk.bases.hook import BaseHook

from pathlib import Path
import shutil

T_TRUNCATE_BRONZE_TABLES_SCRIPT = """
TRUNCATE TABLE [bronze].[fs_car_speed_catches]
"""


def clear_file_storage():
    file_storage_path = Path(BaseHook.get_connection("source_fs").extra_dejson["path"])
    for item in file_storage_path.iterdir():
        if item.is_file() or item.is_symlink():
            item.unlink()
        elif item.is_dir():
            shutil.rmtree(item)


with DAG(dag_id="02_clear_all_data", schedule=None, start_date=None, tags={"deleting_data"}):
    t_clear_file_storage = PythonOperator(
        task_id="clear_file_storage"
        , python_callable=clear_file_storage
    )

    t_truncate_bronze_tables = SQLExecuteQueryOperator(
        task_id="truncate_bronze_tables"
        , conn_id="target_ms_db"
        , sql=T_TRUNCATE_BRONZE_TABLES_SCRIPT
    )