from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.sdk.bases.hook import BaseHook
from modules import tools

from pathlib import Path
import shutil

T_TRUNCATE_BRONZE_TABLES_SCRIPT = """
TRUNCATE TABLE [bronze].[fs_car_speed_catches]
"""

T_TRUNCATE_SILVER_TABLES_SCRIPT = """
TRUNCATE TABLE [silver].[d_seg_segment]
TRUNCATE TABLE [silver].[d_vet_vehicle_type]
TRUNCATE TABLE [silver].[d_veh_vehicle]
TRUNCATE TABLE [silver].[f_spc_speed_catch]
TRUNCATE TABLE [gold].[a_swr_seg_weekly_ranking]
"""

T_TRUNCATE_META_TABLES_SCRIPT = """
TRUNCATE TABLE [meta].[aul_audit_load]
TRUNCATE TABLE [meta].[alf_audit_loaded_files]
TRUNCATE TABLE [meta].[aup_audit_process]
TRUNCATE TABLE [meta].[aua_audit_aggregate]
"""

T_DROP_MATERIALIZED_VIEWS = """
DROP VIEW IF EXISTS [gold].[MV_Segment_Daily_Stats]
"""


def clear_file_storage():
    file_storage_path = Path(BaseHook.get_connection("source_fs").extra_dejson["path"])
    for item in file_storage_path.iterdir():
        if item.is_file() or item.is_symlink():
            item.unlink()
        elif item.is_dir():
            shutil.rmtree(item)


with DAG(
        dag_id="02_clear_all_data"
        , schedule=None
        , start_date=None
        , tags={"deleting_data"}
        , template_searchpath=["/opt/airflow/InitScripts/"]
        , doc_md=tools.get_doc_by_dag_name("02_clear_all_data")
):
    t_clear_file_storage = PythonOperator(
        task_id="clear_file_storage"
        , python_callable=clear_file_storage
    )

    t_drop_materialized_views = SQLExecuteQueryOperator(
        task_id="drop_materialized_views"
        , conn_id="target_ms_db"
        , sql=T_DROP_MATERIALIZED_VIEWS
    )

    t_truncate_bronze_tables = SQLExecuteQueryOperator(
        task_id="truncate_bronze_tables"
        , conn_id="target_ms_db"
        , sql=T_TRUNCATE_BRONZE_TABLES_SCRIPT
    )

    t_truncate_silver_tables = SQLExecuteQueryOperator(
        task_id="truncate_silver_tables"
        , conn_id="target_ms_db"
        , sql=T_TRUNCATE_SILVER_TABLES_SCRIPT
    )

    t_truncate_meta_tables = SQLExecuteQueryOperator(
        task_id="truncate_meta_tables"
        , conn_id="target_ms_db"
        , sql=T_TRUNCATE_META_TABLES_SCRIPT
    )

    t_recreate_materialized_views = SQLExecuteQueryOperator(
        task_id="recreate_materialized_views"
        , conn_id="target_ms_db"
        , sql="init_views.sql"
    )

t_drop_materialized_views >> [t_truncate_bronze_tables, t_truncate_silver_tables, t_truncate_meta_tables] >> t_recreate_materialized_views
