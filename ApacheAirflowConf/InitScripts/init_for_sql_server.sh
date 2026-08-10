#!/bin/bash
/opt/mssql/bin/sqlservr &

echo "Waiting for SQL Server"
sleep 20s

echo "Run init script"
/opt/mssql-tools18/bin/sqlcmd -S localhost -U SA -P "$MSSQL_SA_PASSWORD" -No -i /usr/config/InitScripts/init_database.sql
/opt/mssql-tools18/bin/sqlcmd -S localhost -U SA -P "$MSSQL_SA_PASSWORD" -No -i /usr/config/InitScripts/init_tables.sql
/opt/mssql-tools18/bin/sqlcmd -S localhost -U SA -P "$MSSQL_SA_PASSWORD" -No -i /usr/config/InitScripts/init_functions.sql
/opt/mssql-tools18/bin/sqlcmd -S localhost -U SA -P "$MSSQL_SA_PASSWORD" -No -i /usr/config/InitScripts/init_procedures.sql
/opt/mssql-tools18/bin/sqlcmd -S localhost -U SA -P "$MSSQL_SA_PASSWORD" -No -i /usr/config/InitScripts/init_jobs.sql
/opt/mssql-tools18/bin/sqlcmd -S localhost -U SA -P "$MSSQL_SA_PASSWORD" -No -i /usr/config/InitScripts/init_views.sql

echo "Completed"
wait