from airflow import DAG
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator

from datetime import datetime


with DAG(
    dag_id='04_load_data'
    , schedule="0 * * * *"
    , start_date=datetime(2026, 7, 4)
    , tags={'extract'}
    , template_searchpath=["/opt/airflow/sqlScripts/"]
):
    t_load_silver_table_vet_vehicle_type = SQLExecuteQueryOperator(
        task_id='load_silver_table_vet_vehicle_type'
        , conn_id="target_ms_db"
        , sql="select * from vehicle_type"
    )
