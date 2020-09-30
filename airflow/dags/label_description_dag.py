import datetime
import time
import logging

import pandas as pd

from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.models import Variable
from airflow import AirflowException

from Immigation_ETL import desp_tables
from Immigation_ETL import desp_tables_quality as data_quality

## SET variables
## S3
Variable.set("S3_BUCKET_NAME", "immigrate-demographics-s3-1629")

## input file key
Variable.set("LABEL_DESP_PATH", "data/I94_SAS_Labels_Descriptions.SAS")

## output file
Variable.set("PORT_OUTPUT_FILE_KEY", "output/port_table.csv")
Variable.set("CNTY_OUTPUT_FILE_KEY", "output/country_table.csv")
Variable.set("I94_MODE_OUTPUT_FILE_KEY", "output/i94_mode_table.csv")
Variable.set("STATE_OUTPUT_FILE_KEY", "output/state_table.csv")
Variable.set("VISA_OUTPUT_FILE_KEY", "output/visa_table.csv")
Variable.set("LABEL_DESP_STAGING_PATH", "output/label_desp_staging.csv")



def port_etl(**kwargs):
    """
    This function process port table.
    :param kwargs:
    :return:
    """
    ## get parameters
    s3_bucket_name = kwargs["params"]["S3_BUCKET_NAME"]
    label_desp_key_path = kwargs["params"]["LABEL_STAGING"]
    table_name = kwargs["params"]["TABLE_NAME"]
    port_output_key = kwargs["params"]["PORT_OUTPUT_FILE_KEY"]

    logging.info(" Reading US states and port data.")
    st = time.time()
    ## extract port and state data
    us_port_path = "s3://{}/{}".format(s3_bucket_name, label_desp_key_path)
    df = pd.read_csv(us_port_path)
    df_port = df.loc[df['type'] == 'port']
    df_states = df.loc[df['type'] == 'state']
    logging.info("port shape: {}".format(df_port.shape))
    logging.info("=== Finished reading US states and port data. Used {:5.2f}min.".format((time.time() - st)/60))

    ## data process
    logging.info(" Starting process {} table.".format(table_name))
    st = time.time()
    df_port = desp_tables.process_port(df_port, df_states)
    logging.info("=== Finished processing {} data. Used {:5.2f}min.".format(table_name, (time.time() - st)/60))

    ## save to S3
    output_path = "s3a://{}/{}".format(s3_bucket_name, port_output_key)
    df_port.to_csv(output_path)
    logging.info(" Port data has been saved to S3.")


def generic_etl(**kwargs):
    """
    This is a generic ETL function for country, i94 mode,
    :param kwargs:
    :return:
    """
    ## get parameters
    ### data tables
    # data_key_word = kwargs["params"]["DATA_KWY_WORD"]
    table_name = kwargs["params"]["TABLE_NAME"]
    s3_bucket_name = kwargs["params"]["S3_BUCKET_NAME"]
    ### etl function name
    etl_func = kwargs["params"]["ETL_FUN"]
    ### input & output key path in S3
    label_desp_key_path = kwargs["params"]["LABEL_STAGING"]
    output_key_path = kwargs["params"]["OUTPUT_FILE_KEY"]


    ## read data
    logging.info(" Reading {} data.".format(table_name))
    st = time.time()
    label_staging_path = "s3a://{}/{}".format(s3_bucket_name, label_desp_key_path)
    df = pd.read_csv(label_staging_path)
    df = df.loc[df['type'] == table_name, :]
    logging.info("=== Finished reading {} data. Used {:5.2f}min.".format(table_name, (time.time() - st)/60))

    ## data process
    logging.info(" Starting {} table ETL process.".format(table_name))
    st = time.time()
    df = etl_func(df)
    logging.info("=== Finished processing {} data. Used {:5.2f}min.".format(table_name, (time.time() - st)/60))

    ## save to S3
    output_path = "s3://{}/{}".format(s3_bucket_name, output_key_path)
    df.to_csv(output_path, index=False)
    logging.info(" {} data has been saved to S3.".format(table_name))



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
    "label_description_etl_dag",
    start_date = datetime.datetime.now(),
    concurrency = 2
)


start_task = DummyOperator(
    task_id='Start_execution',
    dag=dag
)

end_task = DummyOperator(
    task_id='End_execution',
    dag=dag
)

extract_data_from_label_desp = PythonOperator(
    task_id="extract_label_desp_data",
    python_callable=desp_tables.extract_label_desp_data,
    params={
        "S3_BUCKET_NAME": Variable.get("S3_BUCKET_NAME"),
        "LABEL_DESP_PATH": Variable.get("LABEL_DESP_PATH"),
        "LABEL_DESP_STAGING_PATH": Variable.get("LABEL_DESP_STAGING_PATH")
    },
    provide_context=True,
    dag=dag
)

process_port_data_task = PythonOperator(
    task_id="process_port_data",
    python_callable=port_etl,
    params={
        "S3_BUCKET_NAME": Variable.get("S3_BUCKET_NAME"),
        "LABEL_STAGING": Variable.get("LABEL_DESP_STAGING_PATH"),
        "PORT_OUTPUT_FILE_KEY": Variable.get("PORT_OUTPUT_FILE_KEY"),
        "TABLE_NAME":"port"
    },
    provide_context=True,
    dag=dag
)

process_country_data_task = PythonOperator(
    task_id="process_country_data",
    python_callable=generic_etl,
    params={
        "S3_BUCKET_NAME": Variable.get("S3_BUCKET_NAME"),
        "LABEL_STAGING": Variable.get("LABEL_DESP_STAGING_PATH"),
        "OUTPUT_FILE_KEY": Variable.get("CNTY_OUTPUT_FILE_KEY"),
        # "DATA_KWY_WORD": "I94CIT & I94RES",
        "TABLE_NAME":"country",
        "ETL_FUN":desp_tables.process_country
    },
    provide_context=True,
    dag=dag
)

process_i94_mode_task = PythonOperator(
    task_id="process_i94_mode_data",
    python_callable=generic_etl,
    params={
        "S3_BUCKET_NAME": Variable.get("S3_BUCKET_NAME"),
        "LABEL_STAGING": Variable.get("LABEL_DESP_STAGING_PATH"),
        "OUTPUT_FILE_KEY": Variable.get("I94_MODE_OUTPUT_FILE_KEY"),
        # "DATA_KWY_WORD": "I94MODE",
        "TABLE_NAME":"i94_mode",
        "ETL_FUN":desp_tables.process_i94_mode
    },
    provide_context=True,
    dag=dag
)

process_state_tabel_task = PythonOperator(
    task_id="process_state_table",
    python_callable=generic_etl,
    params={
        "S3_BUCKET_NAME": Variable.get("S3_BUCKET_NAME"),
        "LABEL_STAGING": Variable.get("LABEL_DESP_STAGING_PATH"),
        "OUTPUT_FILE_KEY": Variable.get("STATE_OUTPUT_FILE_KEY"),
        # "DATA_KWY_WORD": "I94ADDR",
        "TABLE_NAME":"state",
        "ETL_FUN":desp_tables.process_state
    },
    provide_context=True,
    dag=dag
)

process_visa_type_task = PythonOperator(
    task_id="process_visa_type",
    python_callable=generic_etl,
    params={
        "S3_BUCKET_NAME": Variable.get("S3_BUCKET_NAME"),
        "LABEL_STAGING": Variable.get("LABEL_DESP_STAGING_PATH"),
        "OUTPUT_FILE_KEY": Variable.get("VISA_OUTPUT_FILE_KEY"),
        # "DATA_KWY_WORD": "I94VISA",
        "TABLE_NAME":"visa",
        "ETL_FUN":desp_tables.process_i94_visa
    },
    provide_context=True,
    dag=dag
)

check_port_data_task = PythonOperator(
    task_id="check_port_data",
    python_callable=data_check,
    params={
        "S3_BUCKET_NAME": Variable.get("S3_BUCKET_NAME"),
        "OUTPUT_DATA_FILE_KEY":Variable.get("PORT_OUTPUT_FILE_KEY"),
        "CHECK_TABLE_NAME": "port"
    },
    provide_context=True,
    dag=dag
)


check_country_data_task = PythonOperator(
    task_id="check_country_data",
    python_callable=data_check,
    params={
        "S3_BUCKET_NAME": Variable.get("S3_BUCKET_NAME"),
        "OUTPUT_DATA_FILE_KEY":Variable.get("CNTY_OUTPUT_FILE_KEY"),
        "CHECK_TABLE_NAME": "country"
    },
    provide_context=True,
    dag=dag
)

check_i94_mode_task = PythonOperator(
    task_id="check_i94_mode_data",
    python_callable=data_check,
    params={
        "S3_BUCKET_NAME": Variable.get("S3_BUCKET_NAME"),
        "OUTPUT_DATA_FILE_KEY":Variable.get("STATE_OUTPUT_FILE_KEY"),
        "CHECK_TABLE_NAME": "i94_mode"
    },
    provide_context=True,
    dag=dag
)

check_state_table_task = PythonOperator(
    task_id="check_state_data",
    python_callable=data_check,
    params={
        "S3_BUCKET_NAME": Variable.get("S3_BUCKET_NAME"),
        "OUTPUT_DATA_FILE_KEY":Variable.get("STATE_OUTPUT_FILE_KEY"),
        "CHECK_TABLE_NAME": "state"
    },
    provide_context=True,
    dag=dag
)

check_visa_table_task = PythonOperator(
    task_id="check_visa_data",
    python_callable=data_check,
    params={
        "S3_BUCKET_NAME": Variable.get("S3_BUCKET_NAME"),
        "OUTPUT_DATA_FILE_KEY":Variable.get("VISA_OUTPUT_FILE_KEY"),
        "CHECK_TABLE_NAME": "visa"
    },
    provide_context=True,
    dag=dag
)


start_task >> extract_data_from_label_desp

extract_data_from_label_desp >> process_state_tabel_task
extract_data_from_label_desp >> process_port_data_task
extract_data_from_label_desp >> process_country_data_task
extract_data_from_label_desp >> process_i94_mode_task
extract_data_from_label_desp >> process_visa_type_task

process_port_data_task >> check_port_data_task
process_country_data_task >> check_country_data_task
process_i94_mode_task >> check_i94_mode_task
process_state_tabel_task >> check_state_table_task
process_visa_type_task >> check_visa_table_task

check_port_data_task >> end_task
check_country_data_task >> end_task
check_i94_mode_task >> end_task
check_state_table_task >> end_task
