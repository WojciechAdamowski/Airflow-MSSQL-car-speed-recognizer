from airflow import DAG
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator

from datetime import datetime

with DAG(
    dag_id='04_load_data'
    , schedule="0 * * * *"
    , start_date=datetime(2026, 7, 4)
    , tags={'load'}
    , template_searchpath=["/opt/airflow/sqlScripts/"]
):
    t_load_silver_table_d_vet_vehicle_type = SQLExecuteQueryOperator(
        task_id='load_silver_table_d_vet_vehicle_type'
        , conn_id="target_ms_db"
        , sql=f"""
            EXECUTE [silver].[load_d_vet_vehicle_type] 
                @aul_window_from_time=%(aul_window_from_time)s
                , @aul_window_to_time=%(aul_window_to_time)s
                , @aul_pipeline_name=%(aul_pipeline_name)s
                , @aul_logical_date=%(aul_logical_date)s
                , @aul_batch_id=%(aul_batch_id)s
        """
        , parameters={
            "aul_window_from_time": "{{ (logical_date - macros.timedelta(hours=1)).strftime('%Y-%m-%d %H:%M:%S') }}",
            "aul_window_to_time": "{{ logical_date.strftime('%Y-%m-%d %H:%M:%S') }}",
            "aul_pipeline_name": "{{ dag.dag_id }}",
            "aul_logical_date": "{{ (logical_date - macros.timedelta(hours=1)).strftime('%Y-%m-%d %H:%M:%S') }}",
            "aul_batch_id": "{{ run_id }}"
        }
    )

    t_load_silver_table_d_seg_segment = SQLExecuteQueryOperator(
        task_id='load_silver_table_d_seg_segment'
        , conn_id="target_ms_db"
        , sql=f"""
                EXECUTE [silver].[load_d_seg_segment] 
                    @aul_window_from_time=%(aul_window_from_time)s
                    , @aul_window_to_time=%(aul_window_to_time)s
                    , @aul_pipeline_name=%(aul_pipeline_name)s
                    , @aul_logical_date=%(aul_logical_date)s
                    , @aul_batch_id=%(aul_batch_id)s
            """
        , parameters={
            "aul_window_from_time": "{{ (logical_date - macros.timedelta(hours=1)).strftime('%Y-%m-%d %H:%M:%S') }}",
            "aul_window_to_time": "{{ logical_date.strftime('%Y-%m-%d %H:%M:%S') }}",
            "aul_pipeline_name": "{{ dag.dag_id }}",
            "aul_logical_date": "{{ (logical_date - macros.timedelta(hours=1)).strftime('%Y-%m-%d %H:%M:%S') }}",
            "aul_batch_id": "{{ run_id }}"
        }
    )
