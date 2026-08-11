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
    dag_id='05_aggregate_data'
    # , schedule="0 1 1 * *"
    , start_date=datetime(2026, 7, 4)
    , tags={'aggregate'}
    , doc_md=tools.get_doc_by_dag_name("05_aggregate_data")
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

    t_aggregate_a_swr_seg_weekly_ranking = SQLExecuteQueryOperator(
        task_id='aggregate_a_swr_seg_weekly_ranking'
        , conn_id="target_ms_db"
        , sql=f"""
            EXECUTE [gold].[aggregate_a_swr_seg_weekly_ranking] 
                @aua_logical_date=?
                , @aua_from_datetime=?
                , @aua_to_datetime=?
                , @aua_pipeline_name=?
                , @aua_batch_id=?
        """
        , parameters=[
            "{{ (logical_date - macros.timedelta(hours=1)).strftime('%Y-%m-%d %H:%M:%S') }}",
            "{{ (logical_date - macros.timedelta(weeks=1)).strftime('%Y-%m-%d %H:%M:%S') }}",
            "{{ logical_date.strftime('%Y-%m-%d %H:%M:%S') }}",
            "{{ dag.dag_id }}",
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

chain(t_start, t_aggregate_a_swr_seg_weekly_ranking, t_success)
cross_downstream([t_aggregate_a_swr_seg_weekly_ranking], [t_failure])
