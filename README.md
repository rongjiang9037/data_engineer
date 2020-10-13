# How immigration affects population growth

> The U.S. Census Bureau projects that **net international migration to the United States will become the primary driver of the nation’s population growth** between 2027 and 2038.
> ------U.S. Census Bureau, “International Migration is Projected to Become Primary Driver of U.S. Population Growth for First Time in Nearly Two Centuries,”

As U.S. Census Bureau's research shows, US immigration will be the major source of the nation's population growth. This trend will change US social, demographic, cultural landscape and will affect local policy such as economic development, social security etc.  \
To better understand this topic, we collected I94 data from US National Tourism and Trade Office and city demographics data from [OpenSoft](https://public.opendatasoft.com/explore/dataset/us-cities-demographics/export/), and build a data lake on S3 that can be easily queried by researchers, policy makers and immigrates.\
The data description and relational diagram can be found [here](README.md#data-source) and [here](README.md#data-schema).\
Data presented in the project can be used:
- for immigrates to find popular immigration destination
- for researchers to predict future popultion growth
- for policy makers to understand local demographic components and design policies to better serve people
# Table of contents
1. [Project Description](README.md#project-description)
2. [Data Source](README.md#data-source)
3. [Data Schema](README.md#data-schema)
4. [Data Dictionary](README.md#data-dictionary)
5. [Technology Chosen](README.md#technology-chosen)
6. [AWS infrastructure](README.md#aws-infrastructure)
7. [ETL Process](README.md#etl-process)
8. [Scenarios](README.md#scenarios)
9. [General prerequisites](README.md#general-prerequisites)
10. [How to Start](README.md#how-to-start)

# Project Description
This project contains a process to extract US I94 immigration and demographic data stored in **AWS S3**, transform with **Apache Spark running on EMR clusters** and load back to **AWS S3** as parquet files.\
This ETL process is orchestrated by **Apache Airflow** and the whole process is running on Amazon Web Service cloud.

# Data Source
- I94 Immigration Data: This data comes from the [US National Tourism and Trade Office](https://travel.trade.gov/research/reports/i94/historical/2016.html). This data records immigration records for each port entry partitioned by month of every year.
- I94 Immigration dictionary: This comes with I94 immigration data. It decodes the port number, country node, visa type, i94 mode, state abbreviations etc.
- U.S. City Demographic Data: This data comes from [OpenSoft](https://public.opendatasoft.com/explore/dataset/us-cities-demographics/export/). It contains city demogrpahics data such as age, male/female population for major races, number of foreign-born, number of veterans, number of household etc.


# Data Schema
We chose a galaxy-schema to model data because we have 2 fact tables and we would like to use a model similar to star-schema as it's easy to understand, query and analyze. \
The galaxy-schema can be viewed as 2 combined star-schemas and each of it has a fact table: 

![Image of ER Diagram](https://www.dropbox.com/s/399y0g2vwishgxa/capstone_project.jpeg?raw=1)

# Data Dictionary
Detailed data dictionary can be found [here](data_dictionary.md).

# Technology Chosen
- AWS S3 for data storage: We chose AWS S3 for input data storage because **it's cost effective, scalable and that it supports multiple data format**.
- AWS EMR cluster to run Apache Spark: AWS EMR is a cloud solution designed for big data processing and is consisted of EC2 instances, which perform the work that you submit to your cluster. We chose EMR because we need to run Aparche Spark to batch process the ETL process and **it's easy to configure the number, type of instances to setup EMR cluster and to install software on it, in this project, Apache Spark**.
- Apache Airflow for ETL orchestration: We chose it because it's ability to **schedule, scale and monitor** complex computational workflows. In this project, some ETL process has dependencies, for example EMR clusters creation. Airflow is better at orchestrating workflows than cron job. \
Besides, Airflow also provide the data process flow chart to view running status and log which helps developers to spot processing errors
- AWS S3 for data lake: We chose S3 for data lake for low cost, scalability, flexiblity.

# AWS Infrastructure
![Image of aws architecture](https://www.dropbox.com/s/4c0zv3fjkteyzgx/aws_architecture.jpg?raw=1)

# ETL process
- Dag to extract, process and load i94, time and demographics data with EMR cluster
    * Create EMR cluster 
    * Extract i94 entry data from S3 bucket and transform data types and save I94 data back to S3 as parquet file (partition by entry year and month).
    * Extract entry date from i94 data, extract day, day of week, month, year information and store it in S3 bucket.
    * Extract demographics data, select columns and save back to S3.
    * Check data quality for each table
    * Terminate EMR cluster.

![Image of dag](https://www.dropbox.com/s/5ro37hcwlveyeka/process_i94_dag.png?raw=1)
- Dag to process state, visa type, port, country tables from label description file
    * extract port code, country/region code, visa type raw data from I94 dictionary SAS file and save them as .csv file in S3.
    * Process port code: filter only 'US' port, clean port address and load it as parquet file in S3.
    * Process country/region, visa type: clean data format and load it as parquet file in S3.
    * Check data quality for each table.

![Image of dag](https://www.dropbox.com/s/r1nurixzmrruka6/process_label_desciprion_dag.png?raw=1)

# Scenarios
- The data was increased by 100x.
    * Add more worker nodes in Apache Spark to increase horsepower
    * For fast write: we may denormalize i94 entry data and store it in a NoSQL data base such as Apache Cassandra for fast writes
    * For fast read & analysis: add composite key to Cassandra database to improve read performance and export a portion of data to analytical database such as RedShift
- The pipelines would be run on a daily basis by 7 am every day.
    * Change "schedule_interval" parameter in dag setting to "0 7 * * *" to run the dag every morning at 7am.
- The database needed to be accessed by 100+ people.
    * Imrpove read/write performance by add indexes to database
    * Restrict write access to analytical database when more people are using to avoid conflicts.
    * Use Redshift concurrency scaling service to increase  its ability to handle heavy traffic

# General Prerequisites
Python 3.8 or higher version is required for this application.
- s3fs
- fsspec
- pip
- boto3 =1.14.31
- botocore =1.17.44

# How to start
## Create AWS Account
You need to have an AWS account in order to use this application. If you don't have it yet, follow the instructions via the [Amazon Web Service Help Center](https://aws.amazon.com/premiumsupport/knowledge-center/create-and-activate-aws-account/).\
Most of the AWS service used in this application is covered by the free-tier program, except the EMR service for Spark. All the instances will be deleted after the process. It shouldn't cost you more than 2 dollars a month.
## Create an IAM User with programmatic access
Follow the [instructions](https://docs.aws.amazon.com/IAM/latest/UserGuide/id_users_create.html#id_users_create_console) to create an IAM user. 
In the 4th step, choose "programmatic access". And attach **"Administrator Access"** to the user when setting permissions. A more detailed instruction can be found [here](https://blog.ipswitch.com/how-to-create-an-ec2-instance-with-python).\
Upon completion, download **Access Key ID and Secret Access Key** and **copy them to `aws_credentials.cfg`**.\
**To prevent others from connecting your AWS resources, don't expose aws_credential.cfg online.**
## Set up AWS architecture on your local computer
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
