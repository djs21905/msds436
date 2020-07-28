import pandas as pd
import numpy as np
from craigslist import CraigslistHousing
import gspread 
from oauth2client.service_account import ServiceAccountCredentials
from gspread_dataframe import get_as_dataframe, set_with_dataframe
import geopy 
from geopy.distance import geodesic

def get_zipcode(df, geolocator, lat_field, lon_field, indicator):
        print(df[lat_field],df[lon_field])
        location = geolocator.reverse((df[lat_field], df[lon_field]))
        try: 
                return location.raw['address'][indicator]
        except:
                return None

def nu_distance(df,lat_field,lon_field):
        return geodesic((df[lat_field],df[lon_field]), (42.055984,-87.675171)).km


geolocator = geopy.Nominatim(user_agent='http')

scope = ["https://spreadsheets.google.com/feeds",'https://www.googleapis.com/auth/spreadsheets',
"https://www.googleapis.com/auth/drive.file","https://www.googleapis.com/auth/drive"]

creds = ServiceAccountCredentials.from_json_keyfile_name("creds.json",scope)
gc= gspread.authorize(creds)
sheet = gc.open("436finalproject").get_worksheet(0)
cl = CraigslistHousing(site='chicago', area='chc', category='apa', filters = {'posted_today': True})
results = cl.get_results(sort_by='price_asc', geotagged=True, limit = 300)

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
    #print(result)
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


new_data = pd.DataFrame(df)
print("New data length " + str(len(new_data)))
google_sheets = get_as_dataframe(sheet)
print("Old data length " + str(len(google_sheets)))

google_sheets = google_sheets.iloc[:,0:14]

# Adding the zipcode,neighborhood and distancefromnu fields to new_data
zipcodes = new_data.apply(get_zipcode, axis = 1, geolocator=geolocator, lat_field= 'latitude', lon_field='longitude', indicator = 'postcode')
neighborhood  = new_data.apply(get_zipcode, axis = 1, geolocator=geolocator, lat_field= 'latitude', lon_field='longitude', indicator = 'neighbourhood')
distancefromnu  = new_data.apply(nu_distance, axis = 1, lat_field= 'latitude', lon_field='longitude')

new_data['zipcodes'] = zipcodes
new_data['neighborhood'] = neighborhood
new_data['distancefromnu'] = distancefromnu

# Merging new and old data
merged = pd.concat([google_sheets,new_data])
print("Merged data length " + str(len(merged)))
merged['id'] = merged['id'].astype('float')

# Cleaning data by removing duplicate entries and null fields
merged = merged.drop_duplicates(subset=['id','url'])
merged = merged[merged['latitude'].notna()]
merged = merged[merged['longitude'].notna()]
merged = merged[merged['longitude'].notna()]
merged = merged[merged['neighborhood'].notna()]
merged = merged[merged['zipcodes'].notna()]
merged = merged[merged['longitude'] != 0]
merged = merged[merged['latitude'] != 0]

# Handles dropping any zip code that isnt len of 5 
merged['zipcodes'] = merged['zipcodes'].astype(str)
merged = merged[merged['zipcodes'].str.len() == 5]
merged = merged[merged['zipcodes'].str.startswith('6')]

print("Merged data length after drop duplicates, nulls and broken zip codes " + str(len(merged)))
set_with_dataframe(sheet,merged,resize=True)
