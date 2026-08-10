# DAG: 01_generate_data_file

## Overview

This DAG generates a synthetic CSV dataset simulating speed camera readings on Polish road segments. The file serves as source data for the downstream extraction pipeline (03_extract_data).

| Property | Value |
|---|---|
| DAG ID | 01_generate_data_file |
| Schedule | None (manual trigger only) |
| Start date | None |
| Tags | generate_data |
| Tasks | 1 (generate_data_file) |
| Params | row_counts (default: 10000) |

## What it does

The single task generate_data_file builds the dataset in memory and writes it to CSV:

1. Defines a catalog of 12 fixed road segments (id, name, length, speed limit).
2. Generates a pool of unique Polish-style license plates, reused across multiple crossings to simulate repeat vehicles.
3. Assigns a vehicle type per record using weighted random sampling (mostly passenger cars).
4. Simulates entry/exit timestamps based on segment length, speed limit, vehicle type, and a random traffic factor.
5. Injects realistic data quality issues into a share of records: malformed or missing plates, invalid timestamps (exit before entry, missing exit, very long trips), invalid segment data (negative/zero length, missing speed limit, unknown segment id), and text errors (typos, wrong casing, unknown vehicle type).
6. Writes the resulting records to a timestamped CSV file at the path defined by the source_fs connection.

A fixed random seed (42) makes plate generation and error injection reproducible across runs. Data covers a rolling 30-day window ending at DAG execution time.

## Dependencies

Connection: source_fs (must define an extra path pointing to the output directory).
Python package: pandas.

## Pipeline context

01_generate_data_file writes the CSV that 03_extract_data later reads and loads into the bronze layer.