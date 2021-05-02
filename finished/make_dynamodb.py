import logging
import boto3
from botocore.exceptions import ClientError
import argparse


def create_sentiments_table():
    dynamodb = boto3.client('dynamodb', region_name = "us-east-1")
    try:
        table = dynamodb.create_table(
            TableName='youtube-sentiments',
            KeySchema=[
                {
                    'AttributeName': 'partitionKey',
                    'KeyType': 'HASH'  # Partition key
                }
            ],
            AttributeDefinitions=[
                {
                    'AttributeName': 'partitionKey',
                    'AttributeType': 'S'
                }
    
            ],
            ProvisionedThroughput={
                'ReadCapacityUnits': 10,
                'WriteCapacityUnits': 10
            }
        )
        print("Table constructed!)
    except dynamodb.exceptions.ResourceInUseException:
        print("Table construction failed of Bucket already exists!")

def create_bucket(bucket_name):
    try:
        s3_client = boto3.client('s3')
        s3_client.create_bucket(Bucket=bucket_name)
        print("Bucket successfully created!")
    except ClientError as e:
        logging.error(e)
        print('Bucket already exists or could not be created.')


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--bucket', type = str)
    create_bucket(opt.bucket)
    create_sentiments_table()
    