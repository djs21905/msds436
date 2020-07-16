from elasticsearch import Elasticsearch
import logging
import boto3
import json
import string
#Connect to S3 with boto3 --> Load data from S3 to Python. Python to ES


def connect_elasticsearch():
    _es = None
    _es = Elasticsearch([{'host': 'localhost', 'port': 9200}])
    if _es.ping():
        print('Connected')
    else:
        print('it could not connect!')
    return _es
if __name__ == '__main__':
  logging.basicConfig(level=logging.ERROR)


aws_access_key_id=''
aws_secret_access_key=''

s3= boto3.client('s3',
    aws_access_key_id=aws_access_key_id,
    aws_secret_access_key=aws_secret_access_key)

es = connect_elasticsearch()
index = "test6"
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

