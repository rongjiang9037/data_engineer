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
Python 3.8 or higher version is required for this application.
- s3fs
- fsspec
- pip
- boto3 =1.14.31
- botocore =1.17.44
# Data Schema
![Image of ER Diagram](https://www.dropbox.com/s/399y0g2vwishgxa/capstone_project.jpeg?raw=1)
# Apache Airflow DAGs
- Dag to process i94, time and demographics data with EMR cluster
![Image of dag](https://www.dropbox.com/s/5ro37hcwlveyeka/process_i94_dag.png?raw=1)
- Dag to process state, visa type, port, country tables from label description file
![Image of dag](https://www.dropbox.com/s/r1nurixzmrruka6/process_label_desciprion_dag.png?raw=1)

# How to start
## Create AWS Account
You need to have an AWS account in order to use this application. If you don't have it yet, follow the instructions via the [Amazon Web Service Help Center](https://aws.amazon.com/premiumsupport/knowledge-center/create-and-activate-aws-account/).\
Most of the AWS service used in this application is covered by the free-tier program, except the EMR service for Spark. All the instances will be deleted after the process. It shouldn't cost you more than 2 dollars a month.
## Create an IAM User with programmatic access
Follow the [instructions](https://docs.aws.amazon.com/IAM/latest/UserGuide/id_users_create.html#id_users_create_console) to create an IAM user. 
In the 4th step, choose "programmatic access". And attach **"Administrator Access"** to the user when setting permissions. A more detailed instruction can be found [here](https://blog.ipswitch.com/how-to-create-an-ec2-instance-with-python).\
Upon completion, download **Access Key ID and Secret Access Key** and **copy them to `aws_credentials.cfg`**.\
**To prevent others from connecting your AWS resources, don't expose aws_credential.cfg online.**
## Setting up AWS architecture your local computer
1. Clone git repo to local computer:
```
git clone https://github.com/rongjiang9037/data_engineer.git
```
2. Copy AWS credentials (KEY, SECRET KEY)to:   `config/aws_credential.ckg` \
A sample notice can be found in `config/aws_credentials_sample.cfg` 

3. Create VPC, subnets, EC2 instance for this application:
```
cd data_engineer
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
If you recieve the following message, type in `yes` to proceed.
```=
The authenticity of host '*.**.***.** (*.**.***.**)' can't be established.
ECDSA key fingerprint is SHA256:***.
Are you sure you want to continue connecting (yes/no/[fingerprint])? 
```
After logging in, downland miniconda, and follow instructions installing it:
```
wget https://repo.anaconda.com/miniconda/Miniconda3-latest-Linux-x86_64.sh
bash Miniconda3-latest-Linux-x86_64.sh
```
The following message indicating conda has been installed on your machine:
```
Thank you for installing Miniconda3!
```
**You need to re-open current shell in order to use conda:**
```
exit
ssh -i ec2-key.pem ec2-user@**.**.**.**
```
Install git:
```
sudo yum install git
```

Next, clone codes from gitbucket repo
```
git clone https://github.com/rongjiang9037/data_engineer.git
```
Run start.sh bash script. This script create conda environment with necessary packages.
```
cd data_engineer
source ec2_env_setup.sh
```
## Set up apache airflow at the EC2 instance
run the `airflow_config.sh" file to install airflow and connect it to postgres database.
```
cd ..
source airflow_setup.sh
```
Copy your AWS credentials to `config/aws_credentials.cfg` on EC2 instance:
```
vi config/aws_credentials.cfg
```
Press ESC+`:wq` to save and exit vim editor after the copy.\
Start Apache Airflow:
```
source airflow_start.sh
``` 
## Open Apache Airflow UI
1. Open `your.ip.address:8080` in your browser and You will be able to see two airflow dags.
![Image of screenshot](https://www.dropbox.com/s/9q72fg0v81bkjxy/airflow_screenshot.png?raw=1)
In the above screenshot, my EC2 instance has public IP: 18.189.235.99. \
Replace that with public IP address of your instance!\
Now, you can go back to "DAGS" page, try to run dags and check data process status!
