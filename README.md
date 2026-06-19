# Airflow-MSSQL-car-speed-recognizer
This is a project aimed at demonstrating skills in processing data from a file containing traffic statistics on road sections.

## Run project 

1. Install and run following apps:
- Docker
- Git

2. Run project 
```powershell
git clone https://github.com/WojciechAdamowski/Airflow-MSSQL-car-speed-recognizer.git
cd ./Airflow-MSSQL-car-speed-recognizer/ApacheAirflowConf
docker-compose up airflow-init
docker-compose up -d
Start-Process "http://localhost:8080/assets"

```

