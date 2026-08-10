# DAG: 04_load_data

## Overview

This DAG loads data from the bronze layer into the silver layer by executing the SCD dimension and fact loading stored procedures for segments, vehicles, and speed catches.

| Property | Value |
|---|---|
| DAG ID | 04_load_data |
| Schedule | 0 * * * * (hourly) |
| Start date | 2026-07-04 |
| Tags | load |
| Tasks | 6 |

## What it does

1. start_process - logs a "started" entry in the audit table and returns the process details used later for logging success or failure.
2. load_silver_table_d_vet_vehicle_type - executes silver.load_d_vet_vehicle_type, loading the vehicle type dimension.
3. load_silver_table_d_seg_segment - executes silver.load_d_seg_segment, loading the segment dimension.
4. load_silver_table_d_veh_vehicle - executes silver.load_d_veh_vehicle, loading the vehicle dimension.
5. load_silver_table_f_spc_speed_catch - executes silver.load_f_spc_speed_catch, loading the fact table, after all three dimensions are ready.
6. success / failure - log the final process status to the audit table.

Each stored procedure receives the same set of parameters: a one-hour time window (aul_window_from_time to aul_window_to_time, based on logical_date), the pipeline name, the logical date, and a batch id (the Airflow run_id).

## Task order

start_process runs first. From there, two dimension chains run before the fact table load:

- d_seg_segment, then d_veh_vehicle
- d_vet_vehicle_type

Once both d_veh_vehicle and d_vet_vehicle_type have completed, f_spc_speed_catch runs, followed by success.

The failure task is connected to all four load tasks via cross_downstream, so it triggers if any of them fails.

## Dependencies

Connection: target_ms_db (SQL Server).
Custom modules: modules.tools, modules.logging.
Requires the silver schema stored procedures: load_d_vet_vehicle_type, load_d_seg_segment, load_d_veh_vehicle, load_f_spc_speed_catch.

## Pipeline context

04_load_data reads from the bronze layer (loaded by 03_extract_data) and populates the silver-layer dimension and fact tables, which are then used by the gold-layer aggregates.