import pandas as pd
import boto3
from io import StringIO
import numpy as np
import json
from craigslist import CraigslistHousing


cl = CraigslistHousing(site='chicago', area='chc', category='apa')
results = cl.get_results(sort_by='price_asc', geotagged=True, limit =50)


df = {'id': [],
'repost_of': [],
'name': [],
'url': [],
'datetime': [],
'last_updated': [],
'price': [],
'where': [],
'has_image': [],
'geotag': []}

for result in results:
    df['id'].append(result['id'])
    df['repost_of'].append(result['repost_of'])
    df['name'].append(result['name'])
    df['url'].append(result['url'])
    df['datetime'].append(result['datetime'])
    df['last_updated'].append(result['last_updated'])
    df['price'].append(result['price'])
    df['where'].append(result['where'])
    df['has_image'].append(result['has_image'])
    df['geotag'].append(result['geotag'])

# Swap latitude and longitude for geo_point in elasticsearch (long,lat)
for index, item in zip(range(len(df['geotag'])),df['geotag']):
    df['geotag'][index] = tuple(reversed(item))


table = pd.DataFrame(df)

def remove_non_ascii(text):
    return ''.join(i for i in text if ord(i)<128)

aws_access_key_id=''
aws_secret_access_key=''

s3= boto3.client('s3',
    aws_access_key_id=aws_access_key_id,
    aws_secret_access_key=aws_secret_access_key)

# Key is the craigslist id
for row in table.index:
     a = table.loc[row].to_json()
     a_as_byte = a.encode('utf_8')
     s3.put_object(Bucket = 'finalproject436', Key = str(table['id'].loc[row])+".json", Body = a_as_byte)
   
