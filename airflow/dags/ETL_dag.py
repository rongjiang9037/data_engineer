import datetime
import time
import logging

from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.models import Variable
from airflow import AirflowException

from libs import emr_lib

def set_variables():
    """
    This function set up variables.
    """
    ## AWS credentials
    # Variable.set("AWS_KEY", "")
    # Variable.set("AWS_SECRET", "")
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
    Variable.set("I94_INPUT_FILE_KEY", "data/immigration_data_sample.csv")


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
    print("\n Preparing to submit job to {}".format(kwargs["params"]["job_name"]))
    st = time.time()
    cluster_id = Variable.get("cluster_id")
    # get master node DNS
    cluster_dns = emr_lib.get_master_dns(cluster_id)
    # create a spark session
    headers = emr_lib.create_spark_session(cluster_dns, "pyspark")
    # get the session url
    session_url = emr_lib.wait_for_idle_session(cluster_dns, headers)
    print(f"=== Spark session created. Used {(time.time() - st)/60:5.2f}min.")

    statement_response = emr_lib.submit_statement(session_url, kwargs["params"]["file"], kwargs["params"]["key_words"])

    logs = emr_lib.track_statement_progress(cluster_dns, statement_response.headers)
    emr_lib.kill_spark_session(session_url)
    if kwargs["params"]["log"]:
        for line in logs:
            logging.info(line)
            if "FAIL" in str(line):
                logging.info(line)
                raise AirflowException("Normalize data Quality check Fail!")


dag = DAG(
    "immigration_demographics_analysis",
    start_date = datetime.datetime.now()
)

start_task = PythonOperator(
    task_id="setup_dag",
    python_callable=set_variables,
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
        "file":"/Users/margaret/OneDrive/Documents/Udacity/data_engineer/data_engineer/airflow/dags/Immigation_ETL/i94_entry_table.py",
        "job_name":"process i94 data"
    },
    dag=dag
)


process_time_task = PythonOperator(
    task_id = "process_time_data",
    python_callable=submit_to_emr,
    params={
        "file":"/Users/margaret/OneDrive/Documents/Udacity/data_engineer/data_engineer/airflow/dags/Immigation_ETL/time_table.py",
        "job_name":"process time table",
        "key_words":{
                "S3_BUCKET_NAME": Variable.get("S3_BUCKET_NAME"),
                "I94_INPUT_FILE_KEY": Variable.get("I94_INPUT_FILE_KEY"),
                "TIME_OUTPUT_FILE_KEY": "time_table.parquet"
            }
    },
    dag=dag
)

check_i94_data_task = PythonOperator(
    task_id = "check_i94_data",
    pythoncallable=submit_to_emr,
    params={
        "file":"/Users/margaret/OneDrive/Documents/Udacity/data_engineer/data_engineer/airflow/dags/Immigation_ETL/i94_data_quality.py",
        "job_name":"check data quality for i94 data",
        "key_words":{
                "S3_BUCKET_NAME": Variable.get("S3_BUCKET_NAME"),
                "I94_INPUT_FILE_KEY": Variable.get("I94_INPUT_FILE_KEY"),
                "I94_OUTPUT_FILE_KEY": "i94_table.parquet"
        }
    },
    dag=dag
)

check_time_data_task = PythonOperator(
    task_id = "check_time_data",
    pythoncallable=submit_to_emr,
    params={
        "file":"/Users/margaret/OneDrive/Documents/Udacity/data_engineer/data_engineer/airflow/dags/Immigation_ETL/time_data_quality.py",
        "job_name":"check data quality for time data",
        "key_words":{}
    },
    dag=dag
)


start_task >> create_emr_task

create_emr_task >> process_i94_task
create_emr_task >> process_time_task
process_i94_task >> check_i94_data_task
process_time_task >> check_time_data_task


check_i94_data_task >> remove_emr_task
check_time_data_task >> remove_emr_task
