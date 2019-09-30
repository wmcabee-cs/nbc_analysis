import boto3


def get_bucket(name):
    s3 = boto3.resource('s3')
    return s3.Bucket(name)


def get_client():
    return boto3.client('s3')
