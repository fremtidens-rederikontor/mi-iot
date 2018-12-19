import datetime as dt
import json
import urllib2
import numpy as np
from bs4 import BeautifulSoup
import xarray


def loop():
    #Iterating through all html entries found
    for object in meta:
        #Operate only on .nc files
        if '.nc' in str(object):
            print 'File found'
            fetchurl = 'http://thredds.met.no/thredds/dodsC/' + object[1]
            # Extract station name from URL
            tempname = str(object).split('/')
            tempname = str(tempname[4]).split('.')
            tempname = str(tempname[0])
            print fetchurl

            #Imports dataset
            data = xarray.open_dataset(fetchurl)
            
            #Finds all variable handles for given nc file
            varkeys = data.variables.keys()

            # Print variable data to a file
            filename = 'data_' + tempname + '.json'
            file = open(filename, 'w')

            #print 'DATA DIMENSION: ' + str(data.dims) + '\n' + 'DIMENSION LENGTH: '
            #print len(data.dims)

            #Stores all variable data for given time in a array
            kwargs = {}
            dimensionlength = len(data.dims)
            #For variables depending on depth dimensions
            #use std depth as 5 meters.
            std_depth = 5

            for key in varkeys:
                try:
                    # Some of the variables only depend on time, while others
                    # have two dimensions, where one is depth
                    # Checking for length of dimensions, and setting standard
                    # depth to 5.
                    counter = 0
                    if dimensionlength < 2:
                        if 'time' in str(data[key]):
                            counter +=1
                            kwargs[str(key) + str(counter)] = str(data[key].sel(time=currenttime64, method='nearest').values)
                        else:
                         kwargs[key] = str(data[key].sel(time=currenttime64, method='nearest').values)
                    elif dimensionlength > 1:
                        if 'time' in str(data[key]):
                            counter += 1
                            kwargs[str(key) + str(counter)] = str(data[key].sel(depth=std_depth, time=currenttime64, method='nearest').values)
                        else:
                            kwargs[key] = str(data[key].sel(depth=std_depth, time=currenttime64, method='nearest').values)
                except Exception as e:
                    # Some of the variables in the same dataset only have one dimension, either depth
                    # or time. Assigning values based on error message.
                    if 'time' in str(e):
                        kwargs[key] = str(data[key].sel(depth=std_depth, method='nearest').values)
                    elif 'depth' in str(e):
                        kwargs[key] = str(data[key].sel(time=currenttime64, method='nearest').values)
                    else:
                          print '---EXCEPTION---'
                          print e
                          print str(data[key])

            jsondata = json.dumps(kwargs, indent=2)
            file.write(jsondata)
            file.close()

#In datetime format
currenttime = dt.datetime.now()
deltatime = dt.timedelta(minutes=50)
#Find values for 20 minutes ago, since current values are
#always nan due to delay from sensors.
delaytime = currenttime - deltatime
#Converting to datetime64 format
currenttime64 = np.datetime64(delaytime)

#Dynamically change date based on datetime now
year = str(currenttime.year)
month = str(currenttime.month)
day = str(currenttime.day)
datestring = year+'/'+month+'/'+year+month
#DAILY CATALOG
#catalogUrl = 'http://thredds.met.no/thredds/catalog/obs/buoy-svv-e39/' + year + '/' + month + '/' + day + '/' + 'catalog.html'
#MONTHLY CATALOG
catalogUrl = 'http://thredds.met.no/thredds/catalog/obs/buoy-svv-e39/' + year + '/' + month + '/' + 'catalog.html'
print catalogUrl

#Fetches all the location names in the catalog
content = urllib2.urlopen(catalogUrl)
soup = BeautifulSoup(content, "html.parser")
meta = []

#Appends and split the string in two objects. To access the relevant use meta[i][1]
for a in soup.find_all('a', href=True):
    meta.append(a['href'].split('='))

loop()


