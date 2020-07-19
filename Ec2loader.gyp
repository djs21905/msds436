from elasticsearch import Elasticsearch
import logging
import boto3
import json
import string
#Connect to S3 with boto3 --> Load data from S3 to Python. Python to ES
import requests
from elasticsearch import Elasticsearch, RequestsHttpConnection
from requests_aws4auth import AWS4Auth




aws_access_key_id=''
aws_secret_access_key=''
es = Elasticsearch(
    hosts=['vpc-finalproject-f4tchgjsqlc6jpbehndhmykrdy.us-east-1.es.amazonaws.com'],
    http_auth=AWS4Auth(aws_access_key_id, aws_secret_access_key, 'us-east-1', 'es'),
    use_ssl=True,
    verify_certs=True,
    connection_class=RequestsHttpConnection,
    port=443  # Add this for HTTPS
)


s3= boto3.client('s3',
    aws_access_key_id=aws_access_key_id,
    aws_secret_access_key=aws_secret_access_key)

index = "test7"
response= s3.list_objects(Bucket = 'finalproject436')

for content in response.get('Contents',[]):
    try:
        key = content.get('Key')
        test = s3.get_object(Bucket = 'finalproject436', Key = key )
        print(key.split('.'[0]))
        es.create(index = index, body = test['Body'].read().decode(), id = key.split('.')[0])
    except Exception as e:
        print(e)
        pass

