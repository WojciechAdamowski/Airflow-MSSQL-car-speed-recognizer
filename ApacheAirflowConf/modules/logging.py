from datetime import datetime

from airflow.providers.microsoft.mssql.hooks.mssql import MsSqlHook


#################
#  FILE LOGGING #
#################

def _start_file_loading_database(
        loading_file_details
        , db_conn_hook: MsSqlHook
):
    query_for_metadata = """
        EXECUTE [meta].[load_alf_audit_loaded_files]
            @alf_batch_id = %s
            , @alf_pipeline_name = %s
            , @alf_file_path = %s
            , @alf_load_start_datetime = %s
            , @alf_source_system = %s
            , @alf_target_schema = %s
            , @alf_target_table = %s
            , @action = 'started'
    """

    conn = db_conn_hook.get_conn()
    cursor = conn.cursor()

    cursor.execute(
        query_for_metadata
        , (
            loading_file_details['alf_batch_id']
            , loading_file_details['alf_pipeline_name']
            , loading_file_details['alf_file_path']
            , loading_file_details['alf_load_start_datetime']
            , loading_file_details['alf_source_system']
            , loading_file_details['alf_target_schema']
            , loading_file_details['alf_target_table']
        )
    )

    alf_id = cursor.fetchone()[0]

    conn.commit()
    cursor.close()
    conn.close()

    loading_file_details['alf_id'] = alf_id

    return loading_file_details


def start_file_loading(
        run_id: str
        , dag_id: str
        , file_path: str
        , target_schema_name: str
        , target_table_name: str
        , source_system: str
        , db_conn_hook: MsSqlHook
):
    file_properties = {
        "alf_batch_id": run_id
        , "alf_pipeline_name": dag_id
        , "alf_file_path": file_path
        , "alf_load_start_datetime": datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        , "alf_source_system": source_system
        , "alf_target_schema": target_schema_name
        , "alf_target_table": target_table_name
    }

    file_properties = _start_file_loading_database(
        file_properties
        , db_conn_hook
    )

    return file_properties


def _end_file_loading_database(
        loading_file_details
        , db_conn_hook: MsSqlHook
):
    query_for_metadata = """
        EXECUTE [meta].[load_alf_audit_loaded_files]
            @alf_source_rows = %s
            , @alf_file_size_bytes = %s
            , @alf_rejected_rows = %s
            , @alf_status = %s
            , @alf_error_message = %s
            , @alf_load_end_datetime = %s
            , @alf_id = %s
            , @action = 'finished'
    """

    conn = db_conn_hook.get_conn()
    cursor = conn.cursor()

    cursor.execute(
        query_for_metadata
        , (
            loading_file_details['alf_source_rows']
            , loading_file_details['alf_file_size_bytes']
            , loading_file_details.get('alf_rejected_rows')
            , loading_file_details['alf_status']
            , loading_file_details.get('alf_error_message')
            , loading_file_details['alf_load_end_datetime']
            , loading_file_details['alf_id']
        )
    )

    conn.commit()
    cursor.close()
    conn.close()


def end_file_loading(
        loading_file_details
        , db_conn_hook: MsSqlHook
):
    loading_file_details["alf_load_end_datetime"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    _end_file_loading_database(loading_file_details, db_conn_hook)


####################
#  PROCESS LOGGING #
####################

def _start_process_database(
        process_details
        , db_conn_hook: MsSqlHook
):
    query_for_metadata = """
        EXECUTE [meta].[load_aup_audit_process] 
            @aup_batch_id = %s
            , @aup_pipeline_name = %s
            , @aup_start_datetime = %s
            , @aup_status = %s
            , @action = 'started'
    """

    conn = db_conn_hook.get_conn()
    cursor = conn.cursor()

    cursor.execute(
        query_for_metadata
        , (
            process_details['aup_batch_id']
            , process_details['aup_pipeline_name']
            , process_details['aup_start_datetime']
            , process_details['aup_status']
        )
    )

    aup_id = cursor.fetchone()[0]

    conn.commit()
    cursor.close()
    conn.close()

    process_details['aup_id'] = aup_id

    return process_details


def start_process(
        run_id: str
        , dag_id: str
        , db_conn_hook: MsSqlHook
):
    process_details = {
        "aup_batch_id": run_id
        , "aup_pipeline_name": dag_id
        , "aup_start_datetime": datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        , "aup_end_datetime": None
        , "aup_status": "running"
        , "aup_error_message": None
    }

    return _start_process_database(process_details, db_conn_hook)


def _end_process_database(
        process_details
        , db_conn_hook: MsSqlHook
):
    query_for_metadata = """
        EXECUTE [meta].[load_aup_audit_process] 
            @aup_id = %s
            , @aup_end_datetime = %s
            , @aup_error_message = %s
            , @aup_status = %s
            , @action = 'finished'
    """

    conn = db_conn_hook.get_conn()
    cursor = conn.cursor()

    cursor.execute(
        query_for_metadata
        , (
            process_details['aup_id']
            , process_details['aup_end_datetime']
            , process_details['aup_error_message']
            , process_details['aup_status']
        )
    )

    conn.commit()
    cursor.close()
    conn.close()


def end_process_success(
        process_details
        , db_conn_hook: MsSqlHook
):
    process_details["aup_end_datetime"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    process_details["aup_status"] = "success"

    _end_process_database(process_details, db_conn_hook)


def end_process_failure(
        process_details
        , db_conn_hook: MsSqlHook
        , **context
):
    dag = context["dag"]
    ti = context["ti"]
    task_ids = [t.task_id for t in dag.tasks]

    error_message = None
    failed_task_id = None
    for task_id in task_ids:
        msg = ti.xcom_pull(task_ids=task_id, key="error_message")
        if msg:
            error_message, failed_task_id = msg, task_id
            break

    process_details["aup_error_message"] = f"TASK_ID: '{failed_task_id}' \t ERROR_MESSAGE: '{error_message}'"
    process_details["aup_end_datetime"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    process_details["aup_status"] = "failure"

    _end_process_database(process_details, db_conn_hook)
