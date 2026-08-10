from airflow.providers.microsoft.mssql.hooks.mssql import MsSqlHook
from datetime import datetime

import pandera.pandas as pa
import pandas as pd
import os
import glob
import shutil
from typing import List

from modules import logging


def get_doc_by_dag_name(dag_name: str) -> str:
    try:
        with open("/opt/airflow/dagDocs/" + dag_name + ".md") as file:
            file_content = file.read()
        return file_content
    except (FileNotFoundError, OSError) as e:
        return "Default Docs"


def init_default_directories(base_dir_path: str) -> None:
    quarantine_dir_path = base_dir_path + "/quarantine"
    if not os.path.exists(quarantine_dir_path):
        os.mkdir(quarantine_dir_path)

    error_dir_path = base_dir_path + "/error"
    if not os.path.exists(error_dir_path):
        os.mkdir(error_dir_path)

    archive_dir_path = base_dir_path + "/archive"
    if not os.path.exists(archive_dir_path):
        os.mkdir(archive_dir_path)


def check_column_count(source_df: pd.DataFrame, schema_df: pa.DataFrameSchema) -> dict[str, bool | str]:
    source_columns_count = len(source_df.columns)
    target_columns_count = len(schema_df.columns)
    return {
        "correctness": source_columns_count == target_columns_count,
        "message": f"\n### WRONG COLUMNS COUNT EXPECTED: {target_columns_count} GIVEN: {len(source_df.columns)}"
    }


def check_column_exists(source_df: pd.DataFrame, schema_df: pa.DataFrameSchema) -> dict[str, bool | str]:
    source_columns = source_df.columns
    target_columns = schema_df.columns

    missing_columns = [col for col in target_columns if col not in source_columns]

    message = "\n### MISSING COLUMNS:"
    for col in missing_columns:
        message += f"\n\t - {col}"

    return {
        "correctness": len(missing_columns) == 0,
        "message": message
    }


def check_column_types(source_df: pd.DataFrame, schema_df: pa.DataFrameSchema) -> dict[str, bool | str]:
    try:
        validated_df = schema_df.validate(source_df, lazy=True)
        return {
            "correctness": True,
            "message": "CORRECT",
            "rejected_rows_count": 0
        }
    except pa.errors.SchemaErrors as exc:
        failure_df = exc.failure_cases

        failure_df["error_text"] = (
                " - row=" + (failure_df["index"] + 2).astype(str) +
                ", column=" + failure_df["column"].astype(str) +
                ", reason=" + failure_df["check"].astype(str) +
                ", bad_value=" + failure_df["failure_case"].astype(str)
        )

        errors_as_string = "\n### WRONG DATA TYPES:\n\t"
        errors_as_string += "\n\t".join(failure_df["error_text"].tolist())
        return {
            "correctness": False,
            "message": errors_as_string,
            "rejected_rows_count": failure_df.shape[0]
        }


def get_table_schema(table_name: str, schema_name: str, conn: MsSqlHook) -> pa.DataFrameSchema:
    query_for_metadata = f"""
        SELECT COLUMN_NAME, DATA_TYPE, IS_NULLABLE, CHARACTER_MAXIMUM_LENGTH
        FROM INFORMATION_SCHEMA.COLUMNS
        WHERE 1=1
            AND TABLE_NAME = '{table_name}'
            AND TABLE_SCHEMA = '{schema_name}'
            AND COLUMN_NAME NOT LIKE 'md%'
    """

    df_metadata = conn.get_pandas_df(query_for_metadata)
    columns_dict = {}

    for _, row in df_metadata.iterrows():
        col_name = row['COLUMN_NAME']
        data_type = row['DATA_TYPE'].lower()
        is_nullable = (row['IS_NULLABLE'] == 'YES')
        max_len = row['CHARACTER_MAXIMUM_LENGTH']

        pa_type = str
        checks = []

        if data_type in ['int', 'bigint', 'smallint', 'tinyint']:
            pa_type = "Int64"
        elif data_type in ['decimal', 'numeric', 'float', 'real']:
            pa_type = "Float64"
        elif data_type in ['varchar', 'nvarchar', 'char', 'nchar']:
            pa_type = "string"
            if pd.notna(max_len) and max_len > 0:
                checks.append(pa.Check.str_length(max_value=int(max_len)))
        elif data_type == 'bit':
            pa_type = "boolean"
        elif data_type in ['datetime', 'datetime2', 'smalldatetime', 'date']:
            pa_type = pd.Timestamp
        elif data_type == 'time':
            pa_type = datetime.time
        elif data_type == 'datetimeoffset':
            pa_type = pd.Timestamp
        elif data_type == 'uniqueidentifier':
            pa_type = str
            checks.append(
                pa.Check.str_matches(r"^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}$")
            )

        columns_dict[col_name] = pa.Column(
            pa_type,
            nullable=is_nullable,
            checks=checks if checks else None,
            coerce=True
        )

    return pa.DataFrameSchema(columns_dict)


def check_correctness(df, table_schema_df) -> dict[str, bool | str]:
    result = check_column_count(df, table_schema_df)
    if not result['correctness']:
        print(result['message'])
        return result

    result = check_column_exists(df, table_schema_df)
    if not result['correctness']:
        print(result['message'])
        return result

    result = check_column_types(df, table_schema_df)
    if not result['correctness']:
        print(result['message'])
        return result

    return {
        "correctness": True,
        "message": "CORRECT"
    }


def remove_error_file_rows_from_db(
        db_conn_hook: MsSqlHook
        , file_properties
):
    conn = db_conn_hook.get_conn()
    cursor = conn.cursor()

    table_name = file_properties["alf_target_table"]
    table_schema = file_properties["alf_target_schema"]
    file_path = file_properties["alf_file_path"]

    sql = f"DELETE FROM [{table_schema}].[{table_name}] WHERE md_file_path = '{file_path}'"

    cursor.execute(sql)

    conn.commit()
    cursor.close()
    conn.close()


def check_files_correctness_csv(
        files_properties: List[dict]
        , db_conn_hook: MsSqlHook
):
    print(f"CHECKING CORRECTNESS FOR {len(files_properties)} FILES")

    correct_files = []
    target_table = files_properties[0]['alf_target_table']
    target_schema = files_properties[0]['alf_target_schema']
    base_dir_path = os.path.dirname(files_properties[0]['alf_file_path'])
    quarantine_dir_path = base_dir_path + "/quarantine"

    table_schema_df = get_table_schema(
        target_table
        , target_schema
        , db_conn_hook
    )

    information_for_wrong_files = ""

    for file in files_properties:
        file_path = file['alf_file_path']
        print(f"CHECKING CORRECTNESS FOR {file_path} FILE ({files_properties.index(file) + 1}/{len(files_properties)})")

        source_df = pd.read_csv(file['alf_file_path'])
        file['alf_source_rows'] = source_df.shape[0]
        file['alf_file_size_bytes'] = os.path.getsize(file_path)

        result = check_correctness(source_df, table_schema_df)
        if result['correctness']:
            correct_files.append(file)
        else:
            information_for_wrong_files += f"\n\n## QUARANTINE FOR {file_path}\n"

            shutil.move(file_path, quarantine_dir_path)
            information_for_wrong_files += result['message']

            file['alf_rejected_rows'] = result.get('rejected_rows_count', 0)
            file['alf_status'] = 'quarantine'
            file['alf_error_message'] = result['message']

            logging.end_file_loading(file, db_conn_hook)

    print(
        f"END OF CHECKING CORRECTNESS THERE WAS A {len(files_properties) - len(correct_files)} WRONG FILES FOR {len(files_properties)} FILES")

    if len(files_properties) - len(correct_files) > 0:
        with open(quarantine_dir_path + "/info.md", "a", encoding="utf-8") as file:
            file.write(information_for_wrong_files)

    return correct_files


def cut_datetime(df: pd.DataFrame) -> pd.DataFrame:
    object_cols = df.select_dtypes(include=["object"]).columns
    print(object_cols)

    for col in object_cols:
        try:
            converted = pd.to_datetime(df[col], errors="raise")
            df[col] = converted.dt.round("s")
        except (ValueError, TypeError) as e:
            print(e)
            continue

    datetime_cols = df.select_dtypes(include=["datetime64"]).columns
    print(datetime_cols)

    for col in datetime_cols:
        df[col] = df[col].dt.round('s')

    return df


def extract_csv_file_by_chunks(
        db_conn_hook: MsSqlHook
        , chunk_properties
):
    try:
        df = pd.read_csv(
            chunk_properties["file_path"]
            , parse_dates=True
            , skiprows=range(1, chunk_properties["chunk_from"] + 1)
            , nrows=chunk_properties["chunk_to"] - chunk_properties["chunk_from"] + 1
        )

        df['md_batch_id'] = chunk_properties["chunk_batch_id"]
        df['md_file_path'] = chunk_properties["file_path"]
        df = cut_datetime(df)

        conn = db_conn_hook.get_sqlalchemy_engine()

        with conn.begin() as connection:
            df.to_sql(
                name=chunk_properties["chunk_target_table_name"]
                , con=connection
                , schema=chunk_properties["chunk_target_schema_name"]
                , if_exists="append"
                , index=False
                , chunksize=1000
                , method='multi'
            )

        chunk_properties["chunk_status"] = "success"
    except Exception as e:
        chunk_properties["chunk_status"] = "error"
        chunk_properties["chunk_error_message"] = str(e)
        print('ERROR WHILE EXTRACTING DATA:', str(e))

    return chunk_properties


def get_correct_files(
        file_source_directory: str
        , target_schema_name: str
        , target_table_name: str
        , db_conn_hook: MsSqlHook
        , file_schema: str = '*'
        , file_extension: str = 'csv'
        , source_system: str = "source_fs"
        , **context
):
    all_files = glob.glob(file_source_directory + f"/{file_schema}.{file_extension}")
    run_id = context['run_id']
    dag_id = context['task_instance'].dag_id

    files_properties = []
    for file_path in all_files:
        files_properties.append(logging.start_file_loading(
            run_id
            , dag_id
            , file_path
            , target_schema_name
            , target_table_name
            , source_system
            , db_conn_hook
        ))

    correct_files = check_files_correctness_csv(
        files_properties=files_properties
        , db_conn_hook=db_conn_hook
    )

    return correct_files


def get_chunks_by_files(
        file_properties
        , chunk_size: int = 50_000
):
    chunks = []

    for file in file_properties:
        n_rows = file["alf_source_rows"]

        start = 0
        while start < n_rows:
            end = min(start + chunk_size, n_rows) - 1

            chunks.append({"chunk_properties": {
                "chunk_from": start
                , "chunk_to": end
                , "chunk_status": "started"
                , "file_path": file["alf_file_path"]
                , "chunk_target_table_name": file["alf_target_table"]
                , "chunk_target_schema_name": file["alf_target_schema"]
                , "chunk_batch_id": file["alf_batch_id"]
                , "chunk_error_message": None
            }})

            start = end + 1

    return chunks


def log_loading_files(
        file_properties
        , chunk_properties
        , file_source_directory: str
        , db_conn_hook: MsSqlHook
        , **context
):
    chunk_properties = list(chunk_properties)

    if not isinstance(chunk_properties, list):
        chunk_properties = [chunk_properties]

    for file in file_properties:
        failed_chunks = [chunk for chunk in chunk_properties if
                         chunk["file_path"] == file["alf_file_path"] and chunk["chunk_status"] == "error"]

        if len(failed_chunks) > 0:
            first_failed_chunk = failed_chunks[0]

            file["alf_status"] = "error"
            file["alf_error_message"] = first_failed_chunk["chunk_error_message"]

            remove_error_file_rows_from_db(db_conn_hook, file)
            shutil.move(file["alf_file_path"], file_source_directory + "/error")
        else:
            file["alf_status"] = "success"
            shutil.move(file["alf_file_path"], file_source_directory + "/archive")

        logging.end_file_loading(file, db_conn_hook)


def push_error_to_xcom(context):
    ti = context["ti"]
    exception = context.get("exception")
    ti.xcom_push(key="error_message", value=str(exception))
