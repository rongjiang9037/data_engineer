#!/usr/bin/env bash

## set your AWS credential here
export AWS_ACCESS_KEY_ID=
export AWS_SECRET_ACCESS_KEY=

## set up AWS credentials
cd ~/.aws
sed -i "/^aws_access_key_id */s/=.*$/=$AWS_ACCESS_KEY_ID/" credentials
sed -i "/^aws_secret_access_key */s/=.*$/=$AWS_SECRET_ACCESS_KEY/" credentials

## install airflow
echo "===Installing Apache Airflow..."
pip install -r requirments.txt

echo "===Installing PostgresSQL"
sudo yum install postgresql-server

## get endpoint of RDS
echo "Connecting Airflow to Postgres DB"
cd ~/airflow
export USERNAME=immigrate_demo
export PWD=p<%%Gs7TS_pd%%-
export ENDPOINT=$(aws rds --region us-east-2 describe-db-instances --db-instance-identifier immigrate-demo-db --query "DBInstances[*].Endpoint.Address" \
                    | sed -n '2 p' | sed "s/[[:blank:]]*\"[[:blank:]]*//g")
export DB_NAME=IMMIGRATE_DEMOGRAPHICS_DB
echo "Pointing Airflow metadata database to PostgresSQL DB."
export PGSQL="postgresql://$USERNAME:$PWD@$ENDPOINT:5432/$DB_NAME"
sed -i "s+sqlite:////home/ec2-user/airflow/airflow.db+$PGSQL+" airflow.cfg


echo "===Initiating Airflow database"
airflow initdb

echo "===Airflow is ready to use!"
