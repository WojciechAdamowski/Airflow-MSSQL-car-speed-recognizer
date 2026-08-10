# DAG: 05_aggregate_data

## Overview

This DAG builds the gold-layer aggregate that ranks road segments weekly by speeding activity, based on the silver fact table.

| Property | Value |
|---|---|
| DAG ID | 05_aggregate_data |
| Schedule | 0 1 1 * * (monthly, 1st day at 01:00) |
| Start date | 2026-07-04 |
| Tags | aggregate |
| Tasks | 3 |

## What it does

1. start_process - logs a "started" entry in the audit table and returns the process details used later for logging success or failure.
2. aggregate_a_swr_seg_weekly_ranking - executes gold.aggregate_a_swr_seg_weekly_ranking, refreshing the weekly segment ranking table over a one-week window ending at logical_date.
3. success / failure - log the final process status to the audit table.

The procedure receives the logical date, a one-week time window (aua_from_datetime to aua_to_datetime), the pipeline name, and a batch id (the Airflow run_id).

## Task order

start_process, then aggregate_a_swr_seg_weekly_ranking, then success. The failure task is connected to the aggregate task and triggers if it fails.

## Dependencies

Connection: target_ms_db (SQL Server).
Custom modules: modules.tools, modules.logging.
Requires the gold schema stored procedure: aggregate_a_swr_seg_weekly_ranking.

## Pipeline context

05_aggregate_data reads from the silver fact table (populated by 04_load_data) and refreshes the gold-layer weekly ranking aggregate used for reporting.