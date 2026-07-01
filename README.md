# Airflow-MSSQL-car-speed-recognizer
This is a project aimed at demonstrating skills in processing data from a file containing traffic statistics on road sections.

## TODO
1. [X] Add removing data from bronze table in 02 DAG
2. [ ] Checking data types correctness in file
3. [ ] Move expected file schema to meta data table
4. [X] Something is wrong with mapping csv files in data folder
5. [X] Add argument for 01 DAG for generating given number of rows
6. [ ] Add documentation for all DAGs
7. [ ] Create meta data tables for logging processes 
8. [ ] Create meta data table for loading files (batch_id, file_name, rows count etc)

## First fast project run 

1. Install and run following apps:
- Docker
- Git

2. Run project 
```powershell
git clone https://github.com/WojciechAdamowski/Airflow-MSSQL-car-speed-recognizer.git
cd ./Airflow-MSSQL-car-speed-recognizer/ApacheAirflowConf
docker compose build
docker compose up airflow-init
docker compose up -d

docker-compose cp ./Metadata/connections.json airflow-apiserver:/tmp/connections.json
docker-compose exec -it airflow-apiserver airflow connections import /tmp/connections.json

Start-Process "http://localhost:8080/assets"
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

## Data quality

### File checking 
- number of columns
- column names
- basic types