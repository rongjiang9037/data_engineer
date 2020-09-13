import boto3
from botocore.exceptions import ClientError
import os
import time
import configparser


config_aws = configparser.ConfigParser()
config_aws.read_file(open('../aws_credentials.cfg'))

KEY                    = config_aws.get('AWS','KEY')
SECRET                 = config_aws.get('AWS','SECRET')
ARN                    = config_aws.get('AWS', 'IAM_ARN')


config_s3 = configparser.ConfigParser()
config_s3.read_file(open('../aws_setup.cfg'))

S3_REGION              = config_s3.get('S3', 'REGION')
S3_BUCKET_NAME         = config_s3.get('S3', 'NAME')


def create_s3_bucket(s3, s3_client):
    """
    This function create S3 bucket.
    """
    try:
        if not s3.Bucket(S3_BUCKET_NAME) in s3.buckets.all():
            ## create s3 bucket
            response = s3_client.create_bucket(
            ACL='public-read-write',
            Bucket=S3_BUCKET_NAME,
            CreateBucketConfiguration={
                'LocationConstraint': S3_REGION
            })
    except ClientError as e:
        print(e)
#         raise ValueError(f"Check error message!")

    
def upload_folder(s3_client, folder_path, file_key):
    """
    This function upload all files in a folder to S3.
    
    input:
    s3_client - s3 client object
    folder_path - path to the folder
    file_key - file key of S3 bucket
    """
    file_path = os.path.join(folder_path, file_key)
    for root, dirs, files in os.walk(file_path):
        for file in files:
            upload_from = os.path.join(root, file)
            upload_to = os.path.join(file_key, file)
            s3_client.upload_file(upload_from, S3_BUCKET_NAME, upload_to)

            
def upload_file(s3_client, folder_path, file_key):
    """
    This function upload a single file to S3.

    input:
    s3_client - s3 client object
    folder_path - path to the folder
    file_key - file key of S3 bucket
    """
    upload_from = os.path.join(folder_path, file_key)
    upload_to = os.path.join(file_key)
    s3_client.upload_file(upload_from, S3_BUCKET_NAME, upload_to)
    

def create_folder(s3_client, folder_name):
    """
    This function creates a empty folder in S3.
    """
    s3_client.put_object(ACL='public-read-write',
                     Bucket=S3_BUCKET_NAME, 
                     Key=folder_name+'/')
    
    
if __name__ == '__main__':
    """
    This script create S3 bucket and upload file to S3.
    """
    # create a S3 client object
    s3_client = boto3.client(
        's3',
        aws_access_key_id=KEY,
        aws_secret_access_key= SECRET
    )
    
    # create a S3 resource object
    s3 = boto3.resource(
    's3',
    aws_access_key_id=KEY,
    aws_secret_access_key= SECRET)
    
    print("Creating S3 bucket...")
    st = time.time()
    create_s3_bucket(s3, s3_client)
    print(f"===S3://{S3_BUCKET_NAME} is ready. Used {(time.time() -st)/60:5.2f}min")
    
    print("Uploading immigation data to S3.")
    st = time.time()
    upload_folder(s3_client, "..", 'data/sas_data')
    print(f"===immigration data uploaded to S3. Used {(time.time() -st)/60:5.2f}min")

    print("Uploading immigation sample data to S3.")
    st = time.time()
    upload_file(s3_client, "..", 'data/immigration_data_sample.csv')
    print(f"===Immigration sample data uploaded to S3. Used {(time.time() -st)/60:5.2f}min")
              
    print("Uploading US states data to S3.")
    st = time.time()
    upload_file(s3_client, "..", 'data/us_states.csv')
    print(f"===US states data uploaded to S3. Used {(time.time() -st)/60:5.2f}min")
                        
    print("Uploading I94 label description data to S3.")
    st = time.time()
    upload_file(s3_client, "..", 'data/I94_SAS_Labels_Descriptions.SAS')
    print(f"===I94 label description data uploaded to S3. Used {(time.time() -st)/60:5.2f}min")
                                  
    print("Uploading US city demographics data to S3.")
    st = time.time()
    upload_file(s3_client, "..", 'data/us-cities-demographics.csv')
    print(f"===US city demographics data uploaded to S3. Used {(time.time() -st)/60:5.2f}min")
                                            
    print("Creating logs folder for Spark")
    st = time.time()
    create_folder(s3_client, 'logs')
    print(f"===lo. Used {(time.time() -st)/60:5.2f}min")
    
    
