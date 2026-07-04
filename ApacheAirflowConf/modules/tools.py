from airflow.providers.microsoft.mssql.hooks.mssql import MsSqlHook
from datetime import datetime

import pandera.pandas as pa
import pandas as pd
import os
import glob
import shutil
import typing
import pathlib



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
            "message": "CORRECT"
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
            "message": errors_as_string
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


def check_files_correctness_csv(
        file_source_directory: str
        , target_table_name: str
        , target_schema_name: str
        , run_id: str
        , db_conn_hook: MsSqlHook
        , file_name: str = '*'
):
    quarantine_dir_path = file_source_directory + "/quarantine"
    if not os.path.exists(quarantine_dir_path):
        os.mkdir(quarantine_dir_path)

    ready_dir_path = file_source_directory + "/ready"
    if not os.path.exists(ready_dir_path):
        os.mkdir(ready_dir_path)

    error_dir_path = file_source_directory + "/error"
    if not os.path.exists(error_dir_path):
        os.mkdir(error_dir_path)

    information_for_wrong_files = f"# INFORMATION ABOUT WRONG FILES FOR RUN ID: {run_id}"
    files_to_check = glob.glob(file_source_directory + f"/{file_name}.csv")

    print(f"CHECKING CORRECTNESS FOR {len(files_to_check)} FILES")

    table_schema_df = get_table_schema(
        target_table_name
        , target_schema_name
        , db_conn_hook
    )

    wrong_files_count = 0
    for file in files_to_check:
        print(f"CHECKING CORRECTNESS FOR {file} FILE ({files_to_check.index(file) + 1}/{len(files_to_check)})")
        source_df = pd.read_csv(file)
        information_for_wrong_files += f"\n\n## QUARANTINE FOR {file}\n"

        result = check_correctness(source_df, table_schema_df)
        if result['correctness']:
            shutil.move(file, ready_dir_path)
        else:
            shutil.move(file, quarantine_dir_path)
            information_for_wrong_files += result['message']
            wrong_files_count += 1

    print(
            f"END OF CHECKING CORRECTNESS THERE WAS A {wrong_files_count} WRONG FILES FOR {len(files_to_check)} FILES")

    if wrong_files_count > 0:
        with open(quarantine_dir_path + "/info.md", "a", encoding="utf-8") as file:
            file.write(information_for_wrong_files)

def extract_csv_files(
        file_source_directory: str
        , target_table_name: str
        , target_schema_name: str
        , run_id: str
        , db_conn_hook: MsSqlHook
        , file_name: str = '*'
):
    error_dir_path = file_source_directory + "/error"
    csv_files = glob.glob(file_source_directory + f"/ready/{file_name}.csv")

    error_files_paths = []
    for file in csv_files:
        try:
            print(f"LOADING: {file}")
            df = pd.read_csv(file)
            df['md_batch_id'] = run_id
            df['md_file_name'] = pathlib.Path(file).name

            conn = db_conn_hook.get_sqlalchemy_engine()

            with conn.begin() as connection:
                df.to_sql(
                    name=target_table_name
                    , con=connection
                    , schema=target_schema_name
                    , if_exists="append"
                    , index=False
                    , chunksize=1000
                    , method='multi'
                )

            os.remove(file)
        except Exception as e:
            error_files_paths.append(file)
            shutil.move(file, error_dir_path)
            print('ERROR WHILE EXTRACTING DATA:', e)

    if len(error_files_paths) > 0:
        error_message = "ERROR OCCUR FOR THIS FILES:\n"

        for file in error_files_paths:
            error_message += f" - {file}\n"

        raise Exception(error_message)

def extract_csv_file(
        file_source_directory: str
        , target_table_name: str
        , target_schema_name: str
        , run_id: str
        , db_conn_hook: MsSqlHook
        , file_name: str
):
    error_dir_path = file_source_directory + "/error"
    file_path = file_source_directory + f"/ready/{file_name}.csv"
    try:
        df = pd.read_csv(file_path)
        df['md_batch_id'] = run_id
        df['md_file_name'] = pathlib.Path(file_path).name

        conn = db_conn_hook.get_sqlalchemy_engine()

        with conn.begin() as connection:
            df.to_sql(
                name=target_table_name
                , con=connection
                , schema=target_schema_name
                , if_exists="append"
                , index=False
                , chunksize=1000
                , method='multi'
            )

        os.remove(file_path)
    except Exception as e:
        shutil.move(file_path, error_dir_path)
        print('ERROR WHILE EXTRACTING DATA:', e)

def split_csv_files(
    file_source_directory: str
    , file_name: str = '*'
    , rows_per_file=50_000
):
    for file in glob.glob(file_source_directory + f"/ready/{file_name}.csv"):

        input_file = pathlib.Path(file)
        output_dir = pathlib.Path(file_source_directory + "/ready")

        chunk_iter = pd.read_csv(input_file, chunksize=rows_per_file)

        for i, chunk in enumerate(chunk_iter, start=1):
            output_file = output_dir / f"{input_file.stem}_{i:04d}.csv"
            chunk.to_csv(output_file, index=False)
            print(f"Saved: {output_file} ({len(chunk)} rows)")

        os.remove(file)