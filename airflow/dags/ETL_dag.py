import datetime
import time
import logging

from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.models import Variable

from libs import emr_lib

def set_variables():
    """
    This function set up variables.
    """
    ## AWS credentials
    Variable.set('AWS_KEY', '')
    Variable.set('AWS_SECRET', '')
    ## VPC & SUBNET
    Variable.set('PUB_SUBNET_NAME', 'IMMIGRATE_DEMOGRAPHICS_PUB')
    ## S3
    Variable.set('S3_BUCKET_NAME', 'immigrate-demographics-s3-1629')
    ## EMR
    Variable.set('EMR_REGION', 'us-east-2')
    Variable.set('EMR_NAME', 'IMMIGRATE_DEMOGRAPHICS')
    Variable.set('EMR_MASTER_NAME', 'IMMIGRATE_DEMOGRAPHICS_MASTER')
    Variable.set('EMR_WORKER_NAME', 'IMMIGRATE_DEMOGRAPHICS_WORKER')
    Variable.set('EMR_TYPE', 'm5.xlarge')
    Variable.set('MASTER_COUNT', 1)
    Variable.set('WORKER_COUNT', 2)


def create_emr():
    st = time.time()
    cluster_id = emr_lib.create_emr_cluster()
    Variable.set('cluster_id', cluster_id)
    logging.info(f'Successfully created EMR cluster {cluster_id}')
    logging.info(f'====Used{(time.time() - st)/60:5.2f}min')

def remove_emr():
    st = time.time()
    cluster_id = emr_lib.terminate_emr_cluster()
    logging.info(f'Successfully terminated EMR cluster {cluster_id}')
    logging.info(f'====Used{(time.time() - st)/60:5.2f}min')

dag = DAG(
    'immigration_demographics_analysis',
    start_date = datetime.datetime.now()
)

start_task = PythonOperator(
    task_id='setup_dag',
    python_callable=set_variables,
    dag=dag
)
create_emr_task = PythonOperator(
    task_id = 'create_emr_job_flow',
    python_callable=create_emr,
    dag = dag
)

remove_emr_task = PythonOperator(
    task_id = 'remove_emr_cluster',
    python_callable=remove_emr,
    dag=dag
)


start_task >> create_emr_task >> remove_emr_task



