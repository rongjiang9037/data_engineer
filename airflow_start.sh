#!/usr/bin/env bash

echo "===Starting Airflow Service"
## kill airflow webserver if it's running
if ps xu | grep airflow | grep -v grep > /dev/null
then
    kill -9 $(ps xu | grep airflow | grep -v grep | awk '{print $2}') > /dev/null
    rm $HOME/airflow/airflow-webserver.pid > /dev/null
    sleep 5
fi

nohup airflow scheduler $1 &> nohup_scheduler.log &
pid_sch=$!
echo "Airflow scheduler started in PID $pid_sch"

nohup airflow webserver $1 &> nohup_webserver.log &
pid_web=$!
echo "Airflow webserver started in PID $pid_web"

## wait till webserver is ready.
pid_dir=$HOME/airflow/airflow-webserver.pid
pid=$(cat $pid_dir)
until [ -f $pid_dir ]
do
    sleep 1
done

AIRFLOW_UI=$(dig +short myip.opendns.com @resolver1.opendns.com)
echo "===========Airflow UI interface is available at $AIRFLOW_UI:8080"

exit 0
