#!/usr/bin/env bash

## get AWS credential from aws_credentials.cfg
## setting them as environment variables
export AWS_ACCESS_KEY_ID=$(sed -n "/KEY=/p" config/aws_credentials.cfg | sed "s/KEY=//g")
export AWS_SECRET_ACCESS_KEY=$(sed -n "/SECRET=/p" config/aws_credentials.cfg | sed "s/SECRET=//g")
export AWS_DEFAULT_REGION=us-east-2
export AWS_DEFAULT_OUTPUT=json

## save AWS credentials into ~/.aws/credentials and ~/.aws/config
sed -i "/^aws_access_key_id */s/=.*$/= $AWS_ACCESS_KEY_ID/" $HOME/.aws/credentials
sed -i "/^aws_secret_access_key */s/=.*$/= $AWS_SECRET_ACCESS_KEY/" $HOME/.aws/credentials
sed -i "/^output */s/=.*$/= $AWS_DEFAULT_OUTPUT/" $HOME/.aws/config
sed -i "/^region */s/=.*$/= $AWS_DEFAULT_REGION/" $HOME/.aws/config


## get RDS parameters from aws_setup config
export USERNAME=$(sed -n "/USER_NAME=/p" config/aws_setup.cfg | sed "s/USER_NAME=//g")
export PASSWRD=$(sed -n "/PASSWRD=/p" config/aws_setup.cfg | sed "s/PASSWRD=//g")
export DB_NAME=$(sed -n "/DB_NAME=/p" config/aws_setup.cfg | sed "s/DB_NAME=//g")


## install airflow & PostgreSQL
echo "===Installing Apache Airflow..."
pip install -r requirments.txt

echo "===Installing PostgresSQL"
sudo yum install postgresql-server

## get endpoint of RDS
echo "Connecting Airflow to Postgres DB"
export ENDPOINT=$(aws rds --region us-east-2 describe-db-instances --db-instance-identifier immigrate-demo-db --query "DBInstances[*].Endpoint.Address" \
                    | sed -n '2 p' | sed "s/[[:blank:]]*\"[[:blank:]]*//g")

## get PostgreSQL connection string
export PGSQL=$(echo "postgresql://$USERNAME:$PASSWRD@$ENDPOINT:5432/$DB_NAME" | sed 's/\//\\\//g')


## config airflow.cfg
echo "Pointing Airflow metadata database to PostgresSQL DB."
cd ~/airflow
sed -i "/^sql_alchemy_conn */s/=.*$/= $PGSQL/" airflow.cfg

echo "Disable sample dag"
sed -i "/^load_examples */s/=.*$/= False/" airflow.cfg

## Careful, use LocalExecutor require more RAM
echo "Use LocalExecutor."
sed -i "/^executor */s/=.*$/= LocalExecutor/" airflow.cfg

echo "Change dag folder"
export HOME_PATH=$(echo $HOME | sed 's/\//\\\//g')
sed -i "/^dags_folder */s/=.*$/= $HOME_PATH\/data_engineer\/airflow\/dags/" airflow.cfg

## reinit database
echo "===Initiating Airflow database"
#source ~/.bashrc
airflow initdb

echo "===Airflow is ready to use!"

exit 0
