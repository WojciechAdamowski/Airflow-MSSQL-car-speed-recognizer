# DAG: 03_extract_data

## Overview

This DAG extracts car speed catch data from CSV files and loads it into the bronze layer (bronze.fs_car_speed_catches) in SQL Server. It waits for a source file, validates and chunks it, loads the data in parallel, and logs the outcome to the audit tables.

| Property | Value |
|---|---|
| DAG ID | 03_extract_data |
| Schedule | None (manual trigger only) |
| Start date | None |
| Tags | extract |
| Catchup | False |
| Tasks | 9 |

## What it does

1. start_process - logs a "started" entry in the audit table and returns the process details used later for logging success or failure.
2. init_default_directories - ensures the required source_fs directories exist.
3. check_file_exists - a FileSensor that waits (poking every 10s, timeout 60s, reschedule mode) for a CSV file to appear in source_fs.
4. get_all_files_properties - scans source_fs for valid, not-yet-loaded CSV files (checked against the audit table) and collects their properties.
5. get_chunks_by_files - splits each file into chunks of 50,000 rows for parallel loading.
6. extract_data_to_staging_table - a dynamically mapped task (expand) that loads each chunk into the staging table, with up to 4 chunks processed in parallel.
7. log_loading_files - after all chunks succeed, logs which files and chunks were loaded to the audit table.
8. success - logs a "success" status for the whole process.
9. failure - runs if any task fails, logging a "failure" status.

## Task order

The main path runs sequentially: start_process, init_default_directories, check_file_exists, get_all_files_properties, get_chunks_by_files, extract_data_to_staging_table, log_loading_files, success.

The failure task is connected to every task in that chain (except start_process) via cross_downstream, so it triggers if any of them fails.

## Error handling

Every task has an on_failure_callback (tools.push_error_to_xcom) that pushes the exception message to XCom, so the failure task can retrieve and log the actual error.

## Dependencies

Connections: source_fs (filesystem), target_ms_db (SQL Server).
Custom modules: modules.tools, modules.logging.

## Pipeline context

03_extract_data reads the CSV produced by 01_generate_data_file and loads it into the bronze layer, which is then processed by the silver-layer procedures.