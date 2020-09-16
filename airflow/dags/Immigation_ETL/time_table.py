"""
This script process i94 fact & time table
"""
import time

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

def time_table_etl(df_i94, time_output_path):
    """
    This function process time table.

    :param df_i94: Spark DataFrame, it"s the i94 table.
    :return:
    """
    print("\nStarting to process time table.")
    st = time.time()
    get_timestamp = udf(lambda x: int(x*24*60*60)-315619200, IntegerType())
    df_i94 = df_i94.withColumn("arrdate", to_date(from_unixtime(get_timestamp(col("arrdate")))))

    time_table = df_i94.select(col("arrdate").alias("arrival_date"),
          dayofmonth("arrdate").alias("day"),
          weekofyear("arrdate").alias("week"),
          month("arrdate").alias("month"),
          year("arrdate").alias("year"),
          dayofweek("arrdate").alias("weekday")).dropDuplicates(["arrival_date"])
    print(" Finished processing time table. Used {{:5.2}} min.".format((time.time() - st)/60))

    print(" Writing time data to S3.")
    st = time.time()
    # write time table to parquet files partitioned by year and month
    time_table.write.partitionBy("year").parquet(time_output_path, "overwrite")
    print("======== time table load Done in {{:5.2}}min.".format((time.time()- st)/60))


## ETL statements

i94_path = "s3a://{{S3_BUCKET_NAME}}/{{I94_INPUT_FILE_KEY}}"
time_output_path = "s3a://{{S3_BUCKET_NAME}}/{{TIME_OUTPUT_FILE_KEY}}"

df_i94 = read_i94(i94_path)

time_table_etl(df_i94, time_output_path)


