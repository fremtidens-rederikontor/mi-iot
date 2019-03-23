import datetime as dt
import json
import urllib2
import numpy as np
from bs4 import BeautifulSoup
import xarray
import paho.mqtt.client as mqtt
import time

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
    for object in meta:
        # Operate only on .nc files
        if '.nc' in str(object):
            print('File found')
            fetchurl = 'http://thredds.met.no/thredds/dodsC/' + object[1]
            #  Extract station name from URL
            tempname = str(object).split('/')
            tempname = str(tempname[4]).split('.')
            tempname = str(tempname[0])
            print(fetchurl)
            # Create location name from URL
            locationname = tempname.split('_')
            locationname = '_'.join(locationname[1:])

            # Imports dataset
            data = xarray.open_dataset(fetchurl)

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
                # Checks how many dimensions a variable is dependent on
                variabledimension = len(data[key].dims)
                try:
                    #  Some of the variables only depend on time, while others
                    #  have two dimensions, where one is depth
                    #  Checking for length of dimensions, and setting standard
                    #  depth to 5.
                    if variabledimension < 2:
                        #  Check weather the dimension is time or depth, and also forces time to be
                        #  in timestamp(millis)
                        if 'time' in key:
                            kwargs[key] = currentmillis
                        elif 'time' in str(data[key].dims):
                            kwargs[key] = float(str(data[key].sel(time=currenttime64, method='nearest').values))
                        elif 'depth' in str(data[key].dims):
                            kwargs[key] = float(str(data[key].sel(depth=std_depth, method='nearest').values))
                        else:
                            kwargs[key] = float(str(data[key].sel(time=currenttime64, method='nearest').values))
                    elif variabledimension > 1:
                        #  Pressure always returns NAN and should therefore not be included.
                        if 'Pressure' in key:
                            continue
                        else:
                            kwargs[key] = float(str(data[key].sel(depth=std_depth, time=currenttime64, method='nearest').values))
                except Exception as e:
                      print(e)

            # Add location stamp
            kwargs['deviceName'] = locationname
            json_data = json.dumps(kwargs, indent=2)
            print(locationname)
            if 'adcp' in locationname:
                publishMqtt(json_data,'acdcp')
            elif 'aquadopp' in locationname:
                publishMqtt(json_data,'aquadopp')
            elif 'ctd' in locationname:
                publishMqtt(json_data, 'ctd')
            elif 'raw_wind' in locationname:
                publishMqtt(json_data, 'raw')
            elif 'wave' in locationname:
                publishMqtt(json_data, 'wave')
            elif 'wind' in locationname:
                publishMqtt(json_data, 'wind')
            # file.write(jsondata)
            # file.close()

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
    content = urllib2.urlopen(catalogUrl)
    soup = BeautifulSoup(content, "html.parser")
    meta = []

    # Appends and split the string in two objects. To access the relevant use meta[i][1]
    for a in soup.find_all('a', href=True):
        meta.append(a['href'].split('='))

    fetch_and_publish()
