from airflow import DAG
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.providers.standard.operators.python import PythonOperator
from airflow.providers.odbc.hooks.odbc import OdbcHook

from airflow.sdk import chain, cross_downstream
from modules import logging
from datetime import datetime
from modules import tools

DB_CONNECTION_HOOK = OdbcHook(odbc_conn_id="target_ms_db")

with DAG(
    dag_id='04_load_data'
    # , schedule="0 * * * *"
    , start_date=datetime(2026, 7, 4)
    , tags={'load'}
    , template_searchpath=["/opt/airflow/sqlScripts/"]
    , doc_md=tools.get_doc_by_dag_name("04_load_data")
    , catchup=False
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

    t_load_silver_table_d_vet_vehicle_type = SQLExecuteQueryOperator(
        task_id='load_silver_table_d_vet_vehicle_type'
        , conn_id="target_ms_db"
        , sql=f"""
            EXECUTE [silver].[load_d_vet_vehicle_type] 
                @aul_window_from_time=?
                , @aul_window_to_time=?
                , @aul_pipeline_name=?
                , @aul_logical_date=?
                , @aul_batch_id=?
        """
        , parameters=[
            "{{ (logical_date - macros.timedelta(hours=1)).strftime('%Y-%m-%d %H:%M:%S') }}",
            "{{ logical_date.strftime('%Y-%m-%d %H:%M:%S') }}",
            "{{ dag.dag_id }}",
            "{{ (logical_date - macros.timedelta(hours=1)).strftime('%Y-%m-%d %H:%M:%S') }}",
            "{{ run_id }}"
        ]
        , autocommit=True
    )

    t_load_silver_table_d_seg_segment = SQLExecuteQueryOperator(
        task_id='load_silver_table_d_seg_segment'
        , conn_id="target_ms_db"
        , sql=f"""
                EXECUTE [silver].[load_d_seg_segment] 
                    @aul_window_from_time=?
                    , @aul_window_to_time=?
                    , @aul_pipeline_name=?
                    , @aul_logical_date=?
                    , @aul_batch_id=?
            """
        , parameters=[
            "{{ (logical_date - macros.timedelta(hours=1)).strftime('%Y-%m-%d %H:%M:%S') }}",
            "{{ logical_date.strftime('%Y-%m-%d %H:%M:%S') }}",
            "{{ dag.dag_id }}",
            "{{ (logical_date - macros.timedelta(hours=1)).strftime('%Y-%m-%d %H:%M:%S') }}",
            "{{ run_id }}"
        ]
        , autocommit=True
    )

    t_load_silver_table_d_veh_vehicle = SQLExecuteQueryOperator(
        task_id='load_silver_table_d_veh_vehicle'
        , conn_id="target_ms_db"
        , sql=f"""            
                EXECUTE [silver].[load_d_veh_vehicle] 
                    @aul_window_from_time=?
                    , @aul_window_to_time=?
                    , @aul_pipeline_name=?
                    , @aul_logical_date=?
                    , @aul_batch_id=?
            """
        , parameters=[
            "{{ (logical_date - macros.timedelta(hours=1)).strftime('%Y-%m-%d %H:%M:%S') }}",
            "{{ logical_date.strftime('%Y-%m-%d %H:%M:%S') }}",
            "{{ dag.dag_id }}",
            "{{ (logical_date - macros.timedelta(hours=1)).strftime('%Y-%m-%d %H:%M:%S') }}",
            "{{ run_id }}"
        ]
        , autocommit=True
    )

    t_load_silver_table_f_spc_speed_catch = SQLExecuteQueryOperator(
        task_id='load_silver_table_f_spc_speed_catch'
        , conn_id="target_ms_db"
        , sql=f"""            
                EXECUTE [silver].[load_f_spc_speed_catch] 
                    @aul_window_from_time=?
                    , @aul_window_to_time=?
                    , @aul_pipeline_name=?
                    , @aul_logical_date=?
                    , @aul_batch_id=?
            """
        , parameters=[
            "{{ (logical_date - macros.timedelta(hours=1)).strftime('%Y-%m-%d %H:%M:%S') }}",
            "{{ logical_date.strftime('%Y-%m-%d %H:%M:%S') }}",
            "{{ dag.dag_id }}",
            "{{ (logical_date - macros.timedelta(hours=1)).strftime('%Y-%m-%d %H:%M:%S') }}",
            "{{ run_id }}"
        ]
        , autocommit=True
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
    , t_load_silver_table_d_seg_segment
    , t_load_silver_table_d_veh_vehicle
    , t_load_silver_table_f_spc_speed_catch
    , t_success
)

chain(
    t_start
    , t_load_silver_table_d_vet_vehicle_type
    , t_load_silver_table_f_spc_speed_catch
    , t_success
)

cross_downstream([
    t_load_silver_table_d_seg_segment
    , t_load_silver_table_d_veh_vehicle
    , t_load_silver_table_d_vet_vehicle_type
    , t_load_silver_table_f_spc_speed_catch
], [t_failure])
