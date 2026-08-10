# DAG: 02_clear_all_data

## Overview

This DAG resets the environment by clearing all generated files and truncating all pipeline tables across the bronze, silver, gold, and meta schemas. It is intended for development and testing, allowing a clean re-run of the full pipeline from scratch.

| Property | Value |
|---|---|
| DAG ID | 02_clear_all_data |
| Schedule | None (manual trigger only) |
| Start date | None |
| Tags | deleting_data |
| Tasks | 6 |

## What it does

1. Clear file storage - deletes all files and folders from the source_fs connection path (the directory where 01_generate_data_file writes CSVs).
2. Drop materialized views - drops gold.MV_Segment_Daily_Stats before truncating underlying tables, since indexed views are schema-bound to them.
3. Truncate bronze tables - clears bronze.fs_car_speed_catches.
4. Truncate silver tables - clears d_seg_segment, d_vet_vehicle_type, d_veh_vehicle, f_spc_speed_catch, and gold.a_swr_seg_weekly_ranking.
5. Truncate meta tables - clears aul_audit_load, alf_audit_loaded_files, aup_audit_process, and aua_audit_aggregate.
6. Recreate materialized views - re-runs init_views.sql (loaded from template_searchpath /opt/airflow/InitScripts/) to rebuild the dropped view.

## Task order

clear_file_storage runs independently. The SQL tasks run as: drop_materialized_views first, then truncate_bronze_tables, truncate_silver_tables, and truncate_meta_tables in parallel, then recreate_materialized_views last.

## Dependencies

Connections: source_fs (filesystem path), target_ms_db (SQL Server).
Requires init_views.sql to exist under /opt/airflow/InitScripts/.