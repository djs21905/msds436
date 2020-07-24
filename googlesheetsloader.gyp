import pandas as pd
import numpy as np
from craigslist import CraigslistHousing
import gspread 
from oauth2client.service_account import ServiceAccountCredentials
from gspread_dataframe import get_as_dataframe, set_with_dataframe

scope = ["https://spreadsheets.google.com/feeds",'https://www.googleapis.com/auth/spreadsheets',
"https://www.googleapis.com/auth/drive.file","https://www.googleapis.com/auth/drive"]

creds = ServiceAccountCredentials.from_json_keyfile_name("creds.json",scope)
gc= gspread.authorize(creds)
sheet = gc.open("436finalproject").sheet1

cl = CraigslistHousing(site='chicago', area='chc', category='apa', filters = {'posted_today': True})
results = cl.get_results(sort_by='price_asc', geotagged=True, limit =500

df = {'id': [],
'repost_of': [],
'name': [],
'url': [],
'datetime': [],
'last_updated': [],
'price': [],
'where': [],
'has_image': [],
'latitude': [],
'longitude':[]}

for result in results:
    print(result)
    df['id'].append(result['id'])
    df['repost_of'].append(result['repost_of'])
    df['name'].append(result['name'])
    df['url'].append(result['url'])
    df['datetime'].append(result['datetime'])
    df['last_updated'].append(result['last_updated'])
    df['price'].append(result['price'])
    df['where'].append(result['where'])
    
    df['has_image'].append(result['has_image'])
    if result['geotag'] == None:
            df['latitude'].append(0.0)
            df['longitude'].append(0.0)
    else:
        df['latitude'].append(result['geotag'][0])
        df['longitude'].append(result['geotag'][1])


b = pd.DataFrame(df)


a = get_as_dataframe(sheet)
a = a.iloc[:,0:11]
a.dropna(inplace=True)

final = pd.concat([a,b])
final['id'] = final['id'].astype('float')
final.drop_duplicates('id', inplace=True)
set_with_dataframe(sheet,final)
