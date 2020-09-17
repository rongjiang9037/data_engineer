import datetime

def not_empty(df):
    if df.count() > 0:
        return True
    else:
        return False

def entry_yr_not_future(df):
    curr_yr = datetime.now().year
    if df.agg({"i94yr":"max"}).collect()[0][0] <= curr_yr:
        return True
    else:
        return False

## read data from S3
file_path = "s3a://{S3_BUCKET_NAME}/{OUTPUT_I94_FILE_KEY}"
df = spark.read.load(file_path)

no_empty_flag = not_empty(df)
entry_not_future_flag =  entry_yr_not_future(df)

if no_empty_flag and entry_not_future_flag:
    print("Check Pass")
elif not no_empty_flag:
    print("Check failed! \nI94 data is empty!")
elif not entry_not_future_flag:
    print("Check failed! \nI94 data contains entry data in the future!")
else:
    print("Check failed! \nI94 data is empty and contains entry data in the future!")


