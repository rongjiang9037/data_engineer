import os
import datetime
import time
import logging
import configparser

import pandas as pd
import numpy as np

from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.models import Variable
from airflow import AirflowException

from libs import emr_lib
from Immigation_ETL import desp_tables_quality as data_quality

## SET variables
## EC2
Variable.set("EC2_NAME", "IMMIGRATE_DEMOGRAPHICS")
## VPC & SUBNET
Variable.set("PUB_SUBNET_NAME", "IMMIGRATE_DEMOGRAPHICS_PUB")
## S3
Variable.set("S3_BUCKET_NAME", "immigrate-demographics-s3-1629")
## EMR
Variable.set("EMR_REGION", "us-east-2")
Variable.set("EMR_NAME", "IMMIGRATE_DEMOGRAPHICS")
Variable.set("EMR_MASTER_NAME", "IMMIGRATE_DEMOGRAPHICS_MASTER")
Variable.set("EMR_WORKER_NAME", "IMMIGRATE_DEMOGRAPHICS_WORKER")
Variable.set("EMR_TYPE", "m5.xlarge")
Variable.set("MASTER_COUNT", 1)
Variable.set("WORKER_COUNT", 2)
Variable.set("VPC_NAME", "IMMIGRATE_DEMOGRAPHICS_VPC")
## input / output file key
Variable.set("I94_INPUT_FILE_KEY", "data/immigration_data_sample.csv")
Variable.set("DEMO_INPUT_FILE_KEY", "data/us-cities-demographics.csv")
Variable.set("I94_OUTPUT_FILE_KEY", "output/i94_table.parquet")
Variable.set("TIME_OUTPUT_FILE_KEY", "output/time_table.parquet")
Variable.set("DEMO_OUTPUT_FILE_KEY", "output/demo_tabler.csv")

## AWS credentials
config_aws = configparser.ConfigParser()
config_path = os.path.join(os.environ['HOME'], "data_engineer/config/aws_credentials.cfg")
config_aws.read_file(open(config_path))

KEY                    = config_aws.get("AWS","KEY")
SECRET                 = config_aws.get("AWS","SECRET")
Variable.set("AWS_KEY", KEY)
Variable.set("AWS_SECRET", SECRET)


def create_emr():
    st = time.time()
    cluster_id = emr_lib.create_emr_cluster()
    Variable.set("cluster_id", cluster_id)
    logging.info(f"Successfully created EMR cluster {cluster_id}")
    logging.info(f"====Used{(time.time() - st)/60:5.2f}min")

def remove_emr():
    st = time.time()
    cluster_id = emr_lib.terminate_emr_cluster()
    logging.info(f"Successfully terminated EMR cluster {cluster_id}")
    logging.info(f"====Used{(time.time() - st)/60:5.2f}min")

def submit_to_emr(**kwargs):
    """
    This function submit Spark jobs to EMR and log the logs.

    :param kwargs:
    :return:
    """
    logging.info("\n Preparing to submit job to {}".format(kwargs["params"]["job_name"]))
    st = time.time()
    cluster_id = Variable.get("cluster_id")
    # get master node DNS
    cluster_dns = emr_lib.get_master_dns(cluster_id)

    # create a spark session and return the session url
    session_url = emr_lib.create_spark_session(cluster_dns)
    logging.info(f"=== Spark session created. Used {(time.time() - st)/60:5.2f}min.")

    # submit statements to EMR
    logging.info("\n Submitting statements to EMR Spark cluster.")
    st = time.time()
    statement_url = emr_lib.submit_statement(session_url, cluster_dns, kwargs["params"]["file"], kwargs["params"]["key_words"])
    # get the run log
    run_log = emr_lib.track_statement_progress(statement_url)
    logging.info(f"=== Spark job finished. Used {(time.time() - st)/60:5.2f}min.")

    if run_log["status"] == "ok":
        logging.info(run_log["data"]["text/plain"])
    if run_log["status"] == "error":
        logging.info(run_log["evalue"])
        logging.info("=== trace back ===")
        logging.info("".join(run_log["traceback"]))
        raise AirflowException("+++ Error Occurred while processing data +++")

    emr_lib.kill_spark_session(session_url)
    logging.info("=== Spark session closed.")



def demo_etl(**kwargs):
    """
    This is the ETL function for demographics data.
    :param kwargs:
    :return:
    """
    ## get parameters
    s3_bucket_name = kwargs["params"]["S3_BUCKET_NAME"]
    demo_input_key = kwargs["params"]["DEMO_INPUT_FILE_KEY"]
    demo_output_key = kwargs["params"]["DEMO_OUTPUT_FILE_KEY"]
    table_name = kwargs["params"]["TABLE_NAME"]

    ## read file
    logging.info(" Reading US demographics data.")
    st = time.time()

    demo_input_url = "s3://{}/{}".format(s3_bucket_name, demo_input_key)
    df_demo = pd.read_csv(demo_input_url, sep=";")
    df_demo.columns = ['_'.join(col.lower().split()) for col in df_demo.columns]

    ## demographic data is pretty clean so no need to process
    df_demo = df_demo[["city", "state_code", "median_age", "male_population", "female_population",
       "total_population", "number_of_veterans", "foreign-born",
       "average_household_size", "race"]]
    df_demo["demo_key"] = np.range(df_demo.shape[0])

    ## save to S3
    demo_output_url = "s3://{}/{}".format(s3_bucket_name, demo_output_key)
    df_demo.to_csv(demo_output_url)
    logging.info("=== Finished processing {} data. Used {:5.2f}min.".format(table_name, (time.time() - st)/60))


def data_check(**kwargs):
    """
    This function performs various data checks to port dataset.
    :param kwargs:
    :return:
    """
    ## get parameters
    ### ouptut file key
    s3_bucket_name = kwargs["params"]["S3_BUCKET_NAME"]
    output_file_key = kwargs["params"]["OUTPUT_DATA_FILE_KEY"]
    table_name = kwargs["params"]["CHECK_TABLE_NAME"]

    ## check if data is empty
    country_output_path = "s3a://{}/{}".format(s3_bucket_name, output_file_key)
    is_empty = data_quality.is_empty(country_output_path)
    if is_empty:
        raise AirflowException("Data check failed! {} table is empty!".format(table_name))
    else:
        logging.info("{} data check passed!".format(table_name))


dag = DAG(
    "immigration_demographics_analysis_emr_dag",
    start_date = datetime.datetime.now(),
    concurrency = 2
)


start_task = DummyOperator(
    task_id="Start_execution",
    dag=dag
)

create_emr_task = PythonOperator(
    task_id = "create_emr_job_flow",
    python_callable=create_emr,
    dag = dag
)

remove_emr_task = PythonOperator(
    task_id = "remove_emr_cluster",
    python_callable=remove_emr,
    dag=dag
)

process_i94_task = PythonOperator(
    task_id="process_i94_data",
    python_callable=submit_to_emr,
    params={
        "file":os.path.join(os.environ['HOME'], "data_engineer/airflow/dags/Immigation_ETL/i94_entry_table.py"),
        "job_name":"process i94 data",
        "key_words":{
                "S3_BUCKET_NAME": Variable.get("S3_BUCKET_NAME"),
                "I94_INPUT_FILE_KEY": Variable.get("I94_INPUT_FILE_KEY"),
                "I94_OUTPUT_FILE_KEY": Variable.get("I94_OUTPUT_FILE_KEY")
            }
    },
    provide_context=True,
    dag=dag
)


process_time_task = PythonOperator(
    task_id = "process_time_data",
    python_callable=submit_to_emr,
    params={
        "file":os.path.join(os.environ['HOME'], "data_engineer/airflow/dags/Immigation_ETL/time_table.py"),
        "job_name":"process time table",
        "key_words":{
                "S3_BUCKET_NAME": Variable.get("S3_BUCKET_NAME"),
                "I94_INPUT_FILE_KEY": Variable.get("I94_INPUT_FILE_KEY"),
                "TIME_OUTPUT_FILE_KEY": Variable.get("TIME_OUTPUT_FILE_KEY")
            }
    },
    provide_context=True,
    dag=dag
)

process_demo_task = PythonOperator(
    task_id = "process_demo_data",
    python_callable=demo_etl,
    params={
        "S3_BUCKET_NAME": Variable.get("S3_BUCKET_NAME"),
        "DEMO_INPUT_FILE_KEY": Variable.get("DEMO_INPUT_FILE_KEY"),
        "DEMO_OUTPUT_FILE_KEY": Variable.get("DEMO_OUTPUT_FILE_KEY"),
        "TABLE_NAME": "demographics"
    },
    provide_context=True,
    dag=dag
)

check_i94_data_task = PythonOperator(
    task_id="check_i94_data",
    python_callable=submit_to_emr,
    params={
        "file":os.path.join(os.environ['HOME'], "data_engineer/airflow/dags/Immigation_ETL/i94_data_quality.py"),
        "job_name":"check data quality for i94 data",
        "key_words":{
                "S3_BUCKET_NAME": Variable.get("S3_BUCKET_NAME"),
                "I94_OUTPUT_FILE_KEY": Variable.get("I94_OUTPUT_FILE_KEY")
        }
    },
    provide_context=True,
    dag=dag
)

check_time_data_task = PythonOperator(
    task_id="check_time_data",
    python_callable=submit_to_emr,
    params={
        "file":os.path.join(os.environ['HOME'], "data_engineer/airflow/dags/Immigation_ETL/time_data_quality.py"),
        "job_name":"check data quality for time data",
        "key_words":{
                "S3_BUCKET_NAME": Variable.get("S3_BUCKET_NAME"),
                "TIME_OUTPUT_FILE_KEY":Variable.get("TIME_OUTPUT_FILE_KEY")
        }
    },
    provide_context=True,
    dag=dag
)

check_demo_data_task = PythonOperator(
    task_id="check_demo_data",
    python_callable=data_check,
    params={
        "S3_BUCKET_NAME": Variable.get("S3_BUCKET_NAME"),
        "OUTPUT_DATA_FILE_KEY": Variable.get("DEMO_OUTPUT_FILE_KEY"),
        "CHECK_TABLE_NAME": "demographics"
    },
    provide_context=True,
    dag=dag
)

start_task >> create_emr_task

create_emr_task >> process_i94_task
create_emr_task >> process_time_task
create_emr_task >> process_demo_task

process_demo_task >> check_demo_data_task
process_i94_task >> check_i94_data_task
process_time_task >> check_time_data_task

check_demo_data_task >> remove_emr_task
check_i94_data_task >> remove_emr_task
check_time_data_task >> remove_emr_task

