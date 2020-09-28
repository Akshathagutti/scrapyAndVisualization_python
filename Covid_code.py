# -*- coding: utf-8 -*-
"""
Created on Fri Sep 25 15:31:27 2020

@author: Akshata Gutti
"""
import requests
import pandas as pd
import numpy as np
from bs4 import BeautifulSoup
import re
url = "https://www.mygov.in/corona-data/covid19-statewise-status"
#soup = BeautifulSoup(website, 'html.parser')
import requests
import urllib.parse
import time
import datetime
from elasticsearch import Elasticsearch
es = Elasticsearch()
def processDataIntoElasticSearchStandAlone(State_Name,total_Confirmed,  
                                           Total_Discharged,Death, active,lat_lon , id="NA"):
  currentMilliSeconds = lambda: int(round(time.time() * 1000))
  if id == "NA":
    id = currentMilliSeconds()

  dataES = {}
  dataES['State_Name'] = State_Name
  dataES['total_Confirmed'] = total_Confirmed
  dataES['Total_Discharged'] = Total_Discharged
  dataES['Death'] = Death
  dataES['active'] = active
  dataES['lat_lon'] = lat_lon
  dataES['dateTime'] = datetime.datetime.utcnow()
  dataES['timestamp'] = datetime.datetime.now()
  dataES['doc_as_upsert'] = True
  print(dataES)
  res = es.index(index="covid_data_1", id=id, doc_type='_doc', body=dataES)
  print(res['result'])

  print("processDataIntoElasticSearchStandAlone::Committed!")
from geopy.geocoders import Nominatim
geolocator = Nominatim(user_agent='myapplication')

def geoLocation(location_):
  try:
    if location_ is not "NA":
      location = geolocator.geocode(location_)
      return [location.longitude, location.latitude]
    else:
      return [0, 0]
  except Exception:
    # logger.exception("Exception!")
    print("Exception, passing")
headers = {'User-Agent': 'Mozilla/5.0'}
page = requests.get(url,headers=headers )
soup = BeautifulSoup(page.text, 'html.parser')
domains = soup.find_all("div", class_=["field-item even","field-item odd"])
data_list = []
for job_elem in domains:
    data = data_list.append(job_elem.text)
    data1= (job_elem.text)
    x = re.findall("State", data1)
    len_x = len(x)
    if len_x > 1:
       result = (data1.split(":"))
       State_Name = re.sub("Total Confirmed","",result[1])
       total_Confirmed = re.sub("[^0-9]","", result[2])
       Total_Discharged = re.sub("[^0-9]","", result[3])
       Death =  re.sub("[^0-9]","", result[4])
       active = int(total_Confirmed)-(int(Total_Discharged)+int(Death))
       #print(State_Name)
       lat_lon = geoLocation(State_Name)
       #print(lat_lon)
       #print(total_Confirmed)
       #print(Total_Discharged)
       #print(Death)
       processDataIntoElasticSearchStandAlone(State_Name,total_Confirmed,  
                                           Total_Discharged,Death, active,lat_lon)
