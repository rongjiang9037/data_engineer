"""
This script process i94 fact & time table
"""
import time

from datetime import datetime

from pyspark.sql.types import StructType as R, StructField as Fld
from pyspark.sql.types import IntegerType, StringType, DoubleType, DateType
from pyspark.sql.functions import udf, col, to_date
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear, dayofweek, date_format, from_unixtime


def read_i94(i94_path):
    """
    This function reads i94 data from S3 as Spark DataFrame format.

    :param i94_path: S3 path for i94.
    :return:
    """
    ##i94 schema
    print("\nReading i94 table.")
    st = time.time()

    # specify schema
    i94Schema = R([
        Fld("_c0", StringType()),
        Fld("cicid", DoubleType()),
        Fld("i94yr", DoubleType()),
        Fld("i94mon", DoubleType()),
        Fld("i94cit", DoubleType()),
        Fld("i94res", DoubleType()),
        Fld("i94port", StringType()),
        Fld("arrdate", DoubleType()),
        Fld("i94mode", DoubleType()),
        Fld("i94addr", StringType()),
        Fld("depdate", DoubleType()),
        Fld("i94bir", DoubleType()),
        Fld("i94visa", DoubleType()),
        Fld("count", DoubleType()),
        Fld("dtadfile", StringType()),
        Fld("visapost", StringType()),
        Fld("occup", StringType()),
        Fld("entdepa", StringType()),
        Fld("entdepd", StringType()),
        Fld("entdepu", StringType()),
        Fld("matflag", StringType()),
        Fld("biryear", DoubleType()),
        Fld("dtaddto", StringType()),
        Fld("gender", StringType()),
        Fld("insnum", StringType()),
        Fld("airline", StringType()),
        Fld("admnum", StringType()),
        Fld("fltno", StringType()),
        Fld("visatype", StringType()),
    ])
    df_i94 = spark.read.csv(i94_path, header=True, schema=i94Schema)
    print("======== Finished reading table. Done in {{:5.2}}min.".format((time.time()- st)/60))
    return df_i94

def i94_table_etl(df_i94, i94_output_path):
    """
    This function process i94 table.

    :param df_i94: Spark DataFrame, the i94 table
    :param i94_output_path: i94 save path
    :return:
    """
    print("\n Starting to process i94 table.")
    st = time.time()
    ## rename i94 table columns
    col_rename = {{'cicid': 'i94_key', 'i94res': 'res_region_key', 'i94cit': 'cit_region_key',
                  'i94port': 'port_key', 'arrdate': 'arrival_date', 'i94mode': 'i94mode_key',
                  'i94addr': 'state_code', 'depdate': 'departure_date', 'i94bir': 'age',
                  'i94visa': 'visa_key', 'count': 'i94_count', 'dtadfile': 'i94_file_date',
                  'dtaddto': 'i94_leave_date', 'admnum': 'i94_admin_num'}}
    for old_col, new_col in col_rename.items():
        df_i94 = df_i94.withColumnRenamed(old_col, new_col)
    print("=== Column rename completed.")

    ## select columns
    select_cols = ['i94_key', 'res_region_key', 'cit_region_key', 'port_key',
                   'arrival_date', 'i94yr', 'i94mon', 'i94mode_key', 'state_code',
                   'departure_date', 'age', 'visa_key', 'i94_count', 'i94_file_date',
                   'occup', 'biryear', 'i94_leave_date', 'gender', 'insnum', 'airline',
                   'i94_admin_num', 'fltno', 'visatype']
    df_i94 = df_i94.select(select_cols)
    print("=== Column selection completed.")

    ## transform data type
    int_cols = ['i94_key', 'res_region_key', 'cit_region_key', \
                'i94yr', 'i94mon', 'i94mode_key', 'age', 'visa_key', 'i94_count', 'biryear']
    for int_col in int_cols:
        df_i94 = df_i94.withColumn(int_col, col(int_col).cast(IntegerType()))

    get_timestamp = udf(lambda x: int(x*24*60*60)-315619200, IntegerType())
    df_i94 = df_i94.withColumn('arrival_date', to_date(from_unixtime(get_timestamp(col('arrival_date')))))

    get_timestamp = udf(lambda x: None if x is None else int(x*24*60*60)-315619200, IntegerType())
    df_i94 = df_i94.withColumn('departure_date', get_timestamp(col("departure_date")))

    str_to_date = udf(lambda x: datetime.strptime(x, "%Y%m%d"), DateType())
    df_i94 = df_i94.withColumn('i94_file_date', str_to_date(col("i94_file_date")))

    str_to_date = udf(lambda x: None if x == 'D/S' else datetime.strptime(x, "%m%d%Y"), DateType())
    df_i94 = df_i94.withColumn('i94_leave_date', str_to_date(col("i94_leave_date")))

    remove_zero = udf(lambda x: x.replace('.0', ''))
    df_i94 = df_i94.withColumn('i94_admin_num', remove_zero(col("i94_admin_num")))
    print("=== Data type transformation completed.")
    print(" Finished processing i94 table. Used {{:5.2}} min.".format((time.time() - st)/60))

    print(' Writing time data to S3.')
    st = time.time()
    # write i94 table to parquet files partitioned by year and month
    df_i94.write.partitionBy("i94yr", 'i94mon').parquet(i94_output_path, "overwrite")
    print("======== i94 table load Done in {{:5.2}}min.".format((time.time()- st)/60))


## ETL statements
i94_path = "s3a://{S3_BUCKET_NAME}/{I94_INPUT_FILE_KEY}"
i94_output_path = "s3a://{S3_BUCKET_NAME}/{I94_OUTPUT_FILE_KEY}"

df_i94 = read_i94(i94_path)

i94_table_etl(df_i94, i94_output_path)

