from typing import Optional
import boto3

def read_s3_object(bucket: str, key: str) -> str:
    s3 = boto3.client('s3')
    obj = s3.get_object(Bucket=bucket, Key=key)
    return obj['Body'].read().decode('utf-8')

def build_jdbc_url(host: str, port: int, db: str) -> str:
    return f"postgresql+psycopg2://{host}:{port}/{db}"