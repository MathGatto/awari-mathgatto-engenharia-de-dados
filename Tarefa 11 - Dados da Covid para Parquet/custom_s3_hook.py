# MATHEUS GATTO
# Conexão com MinIO


import boto3
import os

from airflow.hooks.base import BaseHook
from airflow.models import Variable
from requests.models import Response

class CustomS3Hook(BaseHook):
    def __init__(self, bucket: str, **kwargs) -> None:
        super().__init__()
        self.bucket = bucket
        self.client = boto3.client('s3', 
            endpoint_url=Variable.get("http://localhost:9001/"),
            aws_access_key_id=Variable.get("XXzoHxKw6udGQLaV"),
            aws_secret_access_key=Variable.get("qersddYILaRxTUt1srBgHVog3WbQn1rx"),
            aws_session_token=None,
            config=boto3.session.Config(signature_version='s3v4'),
            verify=False,
            region_name=Variable.get("sa-east-1")
        ) 


    def put_object(self, key: str, buffer):
        self.client.put_object(Body=buffer, Bucket=self.bucket, Key=f"{key}")

    def get_object(self, key: str):
        response = self.client.get_object(Bucket=self.bucket, Key=key)
        return response.get("Body")





