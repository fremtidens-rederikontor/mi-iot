# -*- coding: utf-8 -*-
import datetime as dt
import json
import urllib3
import numpy as np
from bs4 import BeautifulSoup
import urllib.request
import xarray
import paho.mqtt.client as mqtt
import time
import sys
import netCDF4
from pprint import pprint as pprint

def convertmillis(dt):
    return time.mktime(dt.timetuple()) * 1000

def unix_time_millis(dt):
    return (dt - epoch).total_seconds() * 1000.0

def publishMqtt(payload, topictype):
    id = 'dataset/boye'
    topic = id + '/e39' + '/' + topictype
    client.publish(topic, payload)
    print('Message Published @topic ' + topic)

def on_connect(client, userdata, flags, rc):
    print("Connected with result code " + str(rc))
    client.subscribe("ntnu-ocean")

def fetch_and_publish():
    # Iterating through all html entries found
    temp_array = []
    for object in meta:
        # Operate only on .nc files
        if '.nc' in str(object):
            print('File found')
            fetchurl = 'http://thredds.met.no/thredds/dodsC/' + object[1] # To visit the URL through a browser add '.html' extension at the end.

            # Test utf-8 encoding
            fetchurl = fetchurl.encode('utf-8')
            fetchurl = fetchurl.decode('utf-8')
            
            
            #  Extract station name from URL
            tempname = str(object).split('/')
            tempname = str(tempname[4]).split('.')
            tempname = str(tempname[0])
            print(fetchurl)

            # Create location name from URL
            locationname = tempname.split('_')
            locationname = '_'.join(locationname[1:])

            # Imports dataset
            data = xarray.open_dataset(fetchurl, engine='netcdf4')

            #  Removes duplicate entries of time and depth indexes to avoid
            #  conflict
            data = data.sel(time=~data.indexes['time'].duplicated())

            # Finds all variable handles for given nc file
            varkeys = data.variables.keys()

            #  Print variable data to a file
            # filename = 'data_' + tempname + '.json'
            # file = open(filename, 'w')

            # Stores all variable data for given time in a array
            kwargs = {}

            # For variables depending on depth dimensions
            # use std depth as 5 meters.
            std_depth = 5
            for key in varkeys:
                title = str('h3. ' + key)
                if title in temp_array:
                    print('KEY FOUND, SKIPPING ENTRY')
                    continue
                temp_array.append(title)
                for elem in data[key].attrs:
                    #print(data[key].attrs[elem]) 
                    con = '*'+ str(elem) + ':* ' + str(data[key].attrs[elem])
                    temp_array.append(con)
                line_seperator = '\n'
                temp_array.append(line_seperator)
            
            with open("variable_log.txt", "w") as text_file:
               text_file.write("\n".join(temp_array))
  
            

if __name__== "__main__":
    # Set up connection
    brokerAddress = "168.63.93.40"
    client = mqtt.Client()
    # client.username_pw_set("")
    client.on_connect = on_connect
    client.connect(brokerAddress, 9999)

    # In datetime format
    currenttime = dt.datetime.now()
    deltatime = dt.timedelta(minutes=20)
    # Find values for 20 minutes ago, since current values are
    # always nan due to delay from sensors.
    delaytime = currenttime - deltatime
    #  Converting to datetime64 format
    currenttime64 = np.datetime64(delaytime)
    #  Converting time to timestamp in millis
    epoch = dt.datetime.utcfromtimestamp(0)
    currentmillis = convertmillis(delaytime)

    #  Dynamically change date based on datetime now
    year = str(currenttime.year)
    month = str(currenttime.month)

    #  Checks if the month is between 1-9, and if it is
    #  adds a zero in front to force correct URL format
    if(len(month) == 1):
        month = month.zfill(2)

    day = str(currenttime.day)
    datestring = year+'/'+month+'/'+year+month

    # DAILY CATALOG
    # catalogUrl = 'http://thredds.met.no/thredds/catalog/obs/buoy-svv-e39/' + year + '/' + month + '/' + day + '/' + 'catalog.html'
    
    # MONTHLY CATALOG
    catalogUrl = 'http://thredds.met.no/thredds/catalog/obs/buoy-svv-e39/' + year + '/' + month + '/' + 'catalog.html'
    print(catalogUrl)
    
    # Fetches all the location names in the catalog
    content = urllib.request.urlopen(catalogUrl)
    soup = BeautifulSoup(content.read().decode('utf-8'), "html.parser")

    meta = []

    # Appends and split the string in two objects. To access the relevant use meta[i][1]
    for a in soup.find_all('a', href=True):
        meta.append(a['href'].split('='))

    fetch_and_publish()

    # Shut down the script after one iteration
    print('Script finished. Exiting..')
    sys.exit(0)    

