# Airflow-MSSQL-car-speed-recognizer
This is a project aimed at demonstrating skills in processing data from a file containing traffic statistics on road sections.
A data pipeline built with Apache Airflow and SQL Server that simulates, ingests, and processes speed camera data for Polish road segments, following a bronze / silver / gold (medallion) architecture.

## TODO
1. [X] Create meta data tables for logging processes 
2. [X] Partitioning bronze tables
3. [X] Add indexes bronze
4. [X] Make all dimensions
5. [X] Add BLANK rows to every Dimension and change it in fact
6. [X] Simple reports (views / materialized views)
7. [X] Add indexes silver
8. [X] Add indexes gold
9. [X] Go through all database objects and check them 
10. [X] Add documentation for all DAGs
11. [ ] Better README: How project works, main features, main focuses, why dags are without schedules etc.

## Additional TODO
1. [ ] Delta lake source
2. [ ] Different source file extensions e.g. Excel, json, xml, parquet
3. [ ] ML DAG

## First fast project run 

1. Install and run following apps:
- Docker
- Git

2. Run project 
```powershell
git clone https://github.com/WojciechAdamowski/Airflow-MSSQL-car-speed-recognizer.git
cd ./Airflow-MSSQL-car-speed-recognizer/ApacheAirflowConf
docker compose build
docker compose up airflow-init -d
docker compose up -d

docker-compose cp ./Metadata/connections.json airflow-apiserver:/tmp/connections.json
docker-compose exec -it airflow-apiserver airflow connections import /tmp/connections.json

Start-Process "http://localhost:8080/dags" 
```

## Database

### Extra information
- Database login properties
  - LOGIN: SA
  - PASSWORD: Th1sS3cret!
  - ADDRESS: localhost
  - PORT: 1433

## Airflow

### Export connections 
```powershell
docker-compose exec -it airflow-apiserver airflow connections export /tmp/connections.json
docker-compose cp airflow-apiserver:/tmp/connections.json ./Metadata/connections.json
```

### Import connections
```powershell
docker-compose cp ./Metadata/connections.json airflow-apiserver:/tmp/connections.json
docker-compose exec -it airflow-apiserver airflow connections import /tmp/connections.json
```

## How the project works

### What kinds of errors are generated in source file

License plate errors:
- missing last character,  
- extra space in the middle,  
- lowercase letters,  
- confusion of similar OCR characters, e.g. 0/O, 1/I, 2/Z, 5/S, 8/B,  
- empty string,  
- None.

Timestamp errors:
- missing entry_timestamp,  
- missing exit_timestamp,  
- exit_timestamp < entry_timestamp,  
- exit_timestamp = entry_timestamp,  
- unrealistically long trip time, e.g. several hours for a short segment.

Segment data errors:
- negative segment length,  
- segment length equal to 0,  
- missing speed limit,  
- unknown segment_id.

Text and dictionary errors:
- typo or simplified segment name,  
- vehicle_type written in lowercase,  
- vehicle_type = UNKNOWN.

### File checking 
- number of columns
- column names
- basic types

### Pipeline Overview
 
```
01_generate_data_file  ->  02_clear_all_data (optional reset)
        |
        v
03_extract_data  ->  bronze layer
        |
        v
04_load_data  ->  silver layer (dimensions + fact table)
        |
        v
05_aggregate_data  ->  gold layer (aggregates / reports)
```

## DAGs
 
### [01_generate_data_file](ApacheAirflowConf/dags/01_generate_data_file.py)
Generates a synthetic CSV dataset simulating speed camera readings on Polish road segments, including realistic data quality issues (malformed plates, invalid timestamps, missing values). Used as the source file for the pipeline. Manual trigger only.
 
### [02_clear_all_data](ApacheAirflowConf/dags/02_clear_all_data.py)
Resets the environment for development and testing: clears generated CSV files and truncates all bronze, silver, gold, and meta tables. Drops and recreates the gold-layer materialized view. Manual trigger only.
 
### [03_extract_data](ApacheAirflowConf/dags/03_extract_data.py)
Waits for a source CSV file, validates and splits it into chunks, and loads the data in parallel into the bronze layer (bronze.fs_car_speed_catches). Logs the run outcome to the audit tables. Manual trigger only.
 
### [04_load_data](ApacheAirflowConf/dags/04_load_data.py)
Loads data from the bronze layer into the silver layer by running the dimension (segment, vehicle, vehicle type) and fact (speed catch) stored procedures over an hourly time window. Runs hourly.
 
### [05_aggregate_data](ApacheAirflowConf/dags/05_aggregate_data.py)
Builds the gold-layer weekly segment ranking aggregate from the silver fact table. Runs monthly.

## Database objects

### [TABLES](ApacheAirflowConf/InitScripts/init_tables.sql)

### [PROCEDURES](ApacheAirflowConf/InitScripts/init_procedures.sql)

### [VIEWS](ApacheAirflowConf/InitScripts/init_views.sql)

## Gold Layer Views
 
Defined in [init_views.sql](InitScripts/init_views.sql), created on demand by 02_clear_all_data after truncation.
 
### gold.V_Segment_Speeding_Live
A plain view listing individual speeding events. Joins the fact table with the segment and vehicle dimensions, keeping only crossings where the recorded speed exceeds the segment's speed limit, and excluding rows with unknown ("EMPTY") plate numbers or segment names. Reflects live data, since it is recalculated on every query.
 
### gold.MV_Segment_Daily_Stats
An indexed (materialized) view aggregating crossings per segment per day: total crossing count and summed speed. Created WITH SCHEMABINDING and backed by a unique clustered index (IX_seg_daily_mat), so SQL Server maintains it automatically and incrementally as rows are inserted into the underlying fact table, rather than recalculating on read.
 
### gold.AV_Segment_Weekly_Ranking
A plain view exposing the gold.a_swr_seg_weekly_ranking aggregate table (populated by the 05_aggregate_data DAG) under business-friendly column names: total crossings, over-speeding count and percentage, and each segment's rank position within its year-week.

## Architecture
 
- Bronze - raw data as loaded from source CSV files.
- Silver - cleaned, validated data modeled as SCD1/SCD2 dimensions and an insert-only fact table.
- Gold - business-ready aggregates and reports built on top of silver.
- Meta - audit tables tracking load status, timing, row counts, and errors for each pipeline stage.