# Dependencies and Setup
import matplotlib.pyplot as plt
import pandas as pd
import numpy as np
import requests
import time
import csv
import bs4 as bs
import pymongo
from sqlalchemy import create_engine
from sqlalchemy.ext.declarative import declarative_base
Base = declarative_base()

# Import API key

api_key = 'X1-ZWz1h21lfpwhe3_3fm17'
from citipy import citipy

from datetime import date
today = str(date.today().strftime('%m/%d/%y'))
print('Date today: ' + today)

# Output File (CSV)
output_data_file = "output_data/zillow.csv"

# Import libraries from sqlalchemy

from sqlalchemy import Column, Integer, String, Float


#rds_connection_string = "project2_user:project2_abc@127.0.0.1/realtor_db"
rds_connection_string = "root:Son1(Dau3$@127.0.0.1/realtor_db"
engine = create_engine(f'mysql+pymysql://{rds_connection_string}')

Base.metadata.create_all(engine)

# Create a Session Object to Connect to DB
# ----------------------------------
from sqlalchemy.orm import Session
session = Session(bind=engine)

# Check connection and table
engine.table_names()

# Import list of states from CSV file and convert to db
states = pd.read_csv("./Resources/states.csv")

# Loop through the states, create a url, scrape the data and create a data frame
zillow_list = []
for state in states['Abbreviation']:

    # Define the query URL
    query_url=f'http://www.zillow.com/webservice/GetRegionChildren.htm?zws-id={api_key}&state={state}&childtype=city'
    #print(query_url)

    # creating HTTP response object from given url 
    resp = requests.get(query_url) 

    # Create a soup object
    #soup = bs.BeautifulSoup(resp.content,'lxml-xml')
    soup = bs.BeautifulSoup(resp.content,'html.parser')
    print(soup.text)

    rows = soup.find_all('region')

    
    #Scrape the api to get specific data elements for each state
    for row in rows:

        id = row.find('id').text
        try:
            name = row.find('name').text
        except Exception as e:
            name = 'N/A'
        try:    
            url = row.find('url').text
        except Exception as e:
            url = 'N/A'   
        lat = row.find('latitude').text
        lng = row.find('longitude').text
        try:
            currency = row.find('zindex').text
       
        except Exception as e:
            currency = '0'
        
        # Dictionary to be inserted as a MongoDB document
        post = {
                'id': id,
                'city': name,
                'url': url,
                'median_price': currency,
                'lat': lat,
                'lng':lng,
                'state_cd':state
                }
        zillow_list.append(post)
       #print(f'ID: {id} State : {state} City: {name} URL : {url} curreny: {currency} Latitude:{lat}')

       #Create a zillow dataframe from the list
zillow_df=pd.DataFrame(zillow_list)
zillow_df.head(500)

# Create zillow_listing DB Class
# ----------------------------------
class ZillowListing(Base):
    __tablename__ = 'zillow_listing'
    #key_id = Column(String(255), primary_key=True)
    id= Column(String(100), primary_key=True)
    median_price= Column(String(50))
    lat = Column(String(25))
    lng = Column(String(25))
    city = Column(String(255))
    state_cd = Column(String(10))
    url= Column(String(255))

v_zillow=[]

for index, row in zillow_df.iterrows():
    v_zillow = ZillowListing(median_price=row['median_price'],id=row['id'],lat=row['lat'],
                            lng=row['lng'],city=row['city'],state_cd=row['state_cd'],url=row['url'])
    session.add(v_zillow)

    session.commit()
    


print("Database operation complete !!!")