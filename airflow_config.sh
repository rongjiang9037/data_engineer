#!/usr/bin/env bash

## get AWS credential from aws_credentials.cfg
export AWS_ACCESS_KEY_ID=$(sed -n "/KEY=/p" config/aws_credentials.cfg | sed "s/KEY=//g")
export AWS_SECRET_ACCESS_KEY=$(sed -n "/SECRET=/p" config/aws_credentials.cfg | sed "s/SECRET=//g")

## set up AWS credentials
sed -i "/aws_access_key_id */s/=.*$/=$AWS_ACCESS_KEY_ID/" ~/.aws/credentials
### escaping '/' if secret_access_key contains it
export AWS_SECRET_ACCESS_KEY=$(echo $AWS_SECRET_ACCESS_KEY | sed "s+\/+\\\\/+g")
sed -i "/aws_secret_access_key */s/=.*$/=$AWS_SECRET_ACCESS_KEY/" ~/.aws/credentials

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
