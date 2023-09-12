#!/bin/sh

env=/opt/sdwan_airflow_env/
source /opt/sdwan_airflow_env/bin/activate
cd $env
echo $(pwd)
PORT=5000
bin/airflow webserver -D -p $PORT
bin/airflow scheduler -D
echo $(ps -ef | grep airflow)