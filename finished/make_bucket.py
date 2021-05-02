import logging
import boto3
from botocore.exceptions import ClientError

def create_bucket(bucket_name):
    try:
        s3_client = boto3.client('s3')
        s3_client.create_bucket(Bucket=bucket_name)
        print("Bucket successfully created!")
    except ClientError as e:
        logging.error(e)
        print('Bucket already exists or could not be created.')
    
if __name__ == '__main__':
    bucket_name = 'bucket-for-audio-12345'
    create_bucket(bucket_name)