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

# Initialize error flag to detect if any errors occur during script.
# Make sure to not publish if package is corrupt.
error_flag = None

def error_occured(command):
    global error_flag
    if command == 'init':
        error_flag = False
        return error_flag
    elif command == 'set':
        error_flag = True
    elif command == 'get':
        return error_flag
    else:
        return None


def convertmillis(dt):
    return time.mktime(dt.timetuple()) * 1000


def unix_time_millis(dt):
    return (dt - epoch).total_seconds() * 1000.0


def publishMqtt(payload, topictype):
    error = error_occured('get')
    if not error:
        id = 'dataset/boye'
        topic = id + '/e39' + '/' + topictype
        try:
            client.publish(topic, payload)
            print('Message Published @topic ' + topic)
        except Exception as error:
            print('Error in publish MQTT function ', error)
            error_occured('set')
            pass
    else:
        # Exiting script if error occurs. No messaged published.
        print('Messaged not published due to error in fetching')
        pass


def on_connect(client, userdata, flags, rc):
    print("Connected with result code " + str(rc))
    client.subscribe("ntnu-ocean")


def fetch_and_publish():
    # Iterating through all html entries found
    for object in meta:
        # Operate only on .nc files
        if '.nc' in str(object):
            print('File found')
            # To visit the URL through a browser add '.html' extension at the end.
            fetchurl = 'http://thredds.met.no/thredds/dodsC/' + object[1]

            # Test utf-8 encoding.
            # fetchurl = fetchurl.encode('utf-8')
            # fetchurl = fetchurl.decode('utf-8')

            #  Extract station name from URL
            tempname = str(object).split('/')
            tempname = str(tempname[4]).split('.')
            tempname = str(tempname[0])
            print(fetchurl)

            # Create location name from URL
            locationname = tempname.split('_')
            locationname = '_'.join(locationname[1:])

            try:
            # Imports dataset
            data = xarray.open_dataset(fetchurl, engine='netcdf4', decode_times = True)
            except Exception as error:
                print('Error when trying to fetch dataset with xarray', error)
                error_occured('set')
                pass
            
            #  Removes duplicate entries of time and depth indexes to avoid
            #  conflict
            try:
                data = data.sel(time=~data.indexes['time'].duplicated())
            except Exception as error:
                print('Error when trying to remove duplicate time stamps', error)
                error_occured('set')
                pass

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
                            kwargs[key] = float(
                                str(data[key].sel(time=currenttime64, method='nearest').values))
                        elif 'depth' in str(data[key].dims):
                            kwargs[key] = float(
                                str(data[key].sel(depth=std_depth, method='nearest').values))
                        else:
                            kwargs[key] = float(
                                str(data[key].sel(time=currenttime64, method='nearest').values))
                    elif variabledimension > 1:
                        #  Pressure always returns NAN and should therefore not be included.
                        if 'Pressure' in key:
                            continue
                        else:
                            kwargs[key] = float(str(data[key].sel(
                                depth=std_depth, time=currenttime64, method='nearest').values))
                except Exception as e:
                      print('Error Occured, skipping iteration', e)
                      error_occured('set')
                      pass

            # Add location stamp
            try:
                kwargs['deviceName'] = locationname
                json_data = json.dumps(kwargs, indent=2)
                print(locationname)
                if 'adcp' in locationname:
                    publishMqtt(json_data, 'acdcp')
                elif 'aquadopp' in locationname:
                    publishMqtt(json_data, 'aquadopp')
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
            except Exception as error:
                print('Error trying to publish data', error)
                error_occured('set')
                pass


if __name__ == "__main__":
    # Set up connection
    brokerAddress = "168.63.93.40"
    client = mqtt.Client()
    # client.username_pw_set("")
    client.on_connect = on_connect
    client.connect(brokerAddress, 9999)

    # A flag to check if any error has occured. Do not publish through MQTT
    # If exception encountered to avoid clogging Thingsboard integration.
    error = error_occured('init')

    try:
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
    except Exception as error:
        print('Error formating timestamp', error)
        error_occured('set')
        pass

    # DAILY CATALOG
    # catalogUrl = 'http://thredds.met.no/thredds/catalog/obs/buoy-svv-e39/' + year + '/' + month + '/' + day + '/' + 'catalog.html'

    # MONTHLY CATALOG
    catalogUrl = 'http://thredds.met.no/thredds/catalog/obs/buoy-svv-e39/' + \
        year + '/' + month + '/' + 'catalog.html'
    print(catalogUrl)

    try:
        # Fetches all the location names in the catalog
        content = urllib.request.urlopen(catalogUrl)
        soup = BeautifulSoup(content.read().decode('utf-8'), "html.parser")

        meta = []

        # Appends and split the string in two objects. To access the relevant use meta[i][1]
        for a in soup.find_all('a', href=True):
            meta.append(a['href'].split('='))
    except Exception as error:
        print('Error parsing catalgo in BeatufulSoup', error)
        error_occured('set')
        pass

    fetch_and_publish()

    # Shut down the script after one iteration
    print('Script finished. Exiting..')
    sys.exit(0)
