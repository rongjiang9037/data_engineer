# How immigration affects population growth

> The U.S. Census Bureau projects that **net international migration to the United States will become the primary driver of the nation’s population growth** between 2027 and 2038.
> ------U.S. Census Bureau, “International Migration is Projected to Become Primary Driver of U.S. Population Growth for First Time in Nearly Two Centuries,”

**We are instrested to know if we can verify the above statement with current data.**
# Table of contents
1. [Description](README.md#description)
2. [AWS infrastructure](README.md#aws-infrastructure)
3. [General prerequisites](README.md#general-prerequisites)
4. [Data Schema](README.md#data-schema)
5. [Apache Airflow Chart](README.md#apache-airflow-chart)
6. [How to Start](README.md#how-to-start)

# Description
This project contains a process to extract US I94 immigration and demographic data stored in AWS S3, transform with Spark and load back to S3 as parquet files.
The ETL process is orchestrated with Apache Airflow.
# AWS Infrasctructure
# General Prerequisites
# Data Schema
# Apache Airflow Chart
# How to start
## Create AWS Account
You need to have an AWS account in order to use this application. If you don't have it yet, follow the instructions via the [Amazon Web Service Help Center](https://aws.amazon.com/premiumsupport/knowledge-center/create-and-activate-aws-account/).
Most of the AWS service used in this application is covered by the free-tier program, except the EMR service for Spark. All the instances will be deleted after the process. It shouldn't cost you more than 2 dollars a month.
## Create an IAM User with programmatic access
Follow the [instructions](https://docs.aws.amazon.com/IAM/latest/UserGuide/id_users_create.html#id_users_create_console) to create an IAM user. 
In the 4th step, choose "programmatic access". And attach **"Administrator Access"** and **"AmazonEC2RoleforSSM"** to the user when setting permissions. A more detailed instruction can be found [here](https://blog.ipswitch.com/how-to-create-an-ec2-instance-with-python).
Upon completion, download **Access Key ID and Secret Access Key** and copy them to aws_credentials.cfg config file.
**To prevent others from connecting your AWS resources. Don't expose aws_credential.cfg online.**
## Run the following command in your local terminal
```
git clone https://github.com/rongjiang9037/data_engineer.git
cd data_enginner/aws
python aws.py
```
The console will have output:
```
===EC instance is ready!
ssh -i ec2-key.pem ec2-user@**.**.**.**
```
Use `ssh -i ec2-key.pem ec2-user@**.**.**.**` command to connect the new EC2 instance from your local computer.

## Set up apache airflow at the EC2 instance
After log in into the EC2 instance, follow [here](https://medium.com/@christo.lagali/getting-airflow-up-and-running-on-an-ec2-instance-ae4f3a69441) to set up Apache Airflow installation.

