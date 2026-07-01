FROM apache/airflow:3.2.2
USER root

RUN apt-get update && apt-get install -y gnupg curl unixodbc-dev apt-transport-https

RUN curl -fsSL https://packages.microsoft.com/keys/microsoft.asc | gpg --dearmor > microsoft.asc.gpg \
    && mv microsoft.asc.gpg /etc/apt/trusted.gpg.d/microsoft.asc.gpg \
    && chown root:root /etc/apt/trusted.gpg.d/microsoft.asc.gpg

RUN curl -fsSL https://packages.microsoft.com/config/debian/12/prod.list > /etc/apt/sources.list.d/mssql-release.list

RUN apt-get update && ACCEPT_EULA=Y apt-get install -y msodbcsql18 mssql-tools18 unixodbc-dev

USER airflow
RUN pip install apache-airflow-providers-microsoft-mssql
RUN pip install apache-airflow-providers-common-sql
RUN pip install pyodbc

RUN pip install pandas