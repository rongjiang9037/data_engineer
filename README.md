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
The ETL process is orchestrated with Apache Airflow and the whole process is running on Amazon Web Service cloud.
# AWS Infrasctructure
![Image of aws architecture](https://www.dropbox.com/s/4c0zv3fjkteyzgx/aws_architecture.jpg?raw=1)
# General Prerequisites
Python 3.6 or higher version is required for this application.
- s3fs
- fsspec
# Data Schema
![Image of ER Diagram](https://www.dropbox.com/s/399y0g2vwishgxa/capstone_project.jpeg?raw=1)
# Apache Airflow DAGs
- Dag to process i94, time and demographics data with EMR cluster
![Image of dag](https://www.dropbox.com/s/r1nurixzmrruka6/process_label_desciprion_dag.png?raw=1)
- Dag to process state, visa type, port, country tables from label description file
![Image of dag](https://www.dropbox.com/s/r1nurixzmrruka6/process_label_desciprion_dag.png?raw=1)

# How to start
## Create AWS Account
You need to have an AWS account in order to use this application. If you don't have it yet, follow the instructions via the [Amazon Web Service Help Center](https://aws.amazon.com/premiumsupport/knowledge-center/create-and-activate-aws-account/).\
Most of the AWS service used in this application is covered by the free-tier program, except the EMR service for Spark. All the instances will be deleted after the process. It shouldn't cost you more than 2 dollars a month.
## Create an IAM User with programmatic access
Follow the [instructions](https://docs.aws.amazon.com/IAM/latest/UserGuide/id_users_create.html#id_users_create_console) to create an IAM user. 
In the 4th step, choose "programmatic access". And attach **"Administrator Access"** and **"AmazonEC2RoleforSSM"** to the user when setting permissions. A more detailed instruction can be found [here](https://blog.ipswitch.com/how-to-create-an-ec2-instance-with-python).\
Upon completion, download **Access Key ID and Secret Access Key**. 
**To prevent others from connecting your AWS resources. Don't expose aws_credential.cfg online.**
## Run the following command in your local terminal
1. Clone git repo to local computer:
```
git clone https://github.com/rongjiang9037/data_engineer.git
```
2. Copy AWS credentials (KEY, SECRET KEY, ARN)to:   `config/aws_credential.ckg` \
A sample notice can be found in `config/aws_credentials_sample.cfg` 

3. Create VPC, subnets, EC2 instance for this application:
```
cd data_enginner/aws
bash setup_aws_architecture.sh
```
The console will have output:
```
===EC instance is ready!
ssh -i ec2-key.pem ec2-user@**.**.**.**
```
4. Use `ssh -i ec2-key.pem ec2-user@**.**.**.**` command to connect the new EC2 instance from your local computer.

## Set up EC2 instance python environment
SSH to EC2 instance with the command below:
```
ssh -i ec2-key.pem ec2-user@**.**.**.**
```
After logging in, downland and install miniconda:
```
wget https://repo.anaconda.com/miniconda/Miniconda3-latest-Linux-x86_64.sh
bash Miniconda3-latest-Linux-x86_64.sh
```
Clone codes from gitbucket repo
```
git clone https://github.com/rongjiang9037/data_engineer.git
```
Run start.sh bash script. This script create conda environment with necessary packages and.
```
bash start.sh
```
## Set up apache airflow at the EC2 instance
After log in into the EC2 instance, follow [here](https://medium.com/@christo.lagali/getting-airflow-up-and-running-on-an-ec2-instance-ae4f3a69441) to set up Apache Airflow installation.

