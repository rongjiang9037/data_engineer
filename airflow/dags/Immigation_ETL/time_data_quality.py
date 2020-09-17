import datetime
from pyspark.sql.functions import col


def not_empty(df):
    if df.count() > 0:
        return True
    else:
        return False

def date_key_notnull(df):
    count_null = df.filter(col('arrival_date').isNull()).count()
    if count_null > 0:
        return True
    else:
        return False

## read data from S3
file_path = "s3a://{S3_BUCKET_NAME}/{TIME_OUTPUT_FILE_KEY}"
df = spark.read.load(file_path)

no_empty_flag = not_empty(df)
date_key_notnull_flag =  date_key_notnull(df)

if no_empty_flag and date_key_notnull_flag:
    print("Check Pass")
elif not no_empty_flag:
    print("Check failed! \nTime data is empty!")
elif not date_key_notnull_flag:
    print("Check failed! \nTime data contains null date!")
else:
    print("Check failed! \nTime data is empty and contains null date!")
