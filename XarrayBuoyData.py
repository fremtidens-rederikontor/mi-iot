import datetime as dt
import json
import urllib2
import numpy as np
from bs4 import BeautifulSoup
import xarray

def loop():
    #Iterating through all html entries found
    for object in metarefined:
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

            #For variables depending on depth dimensions
            #use std depth as 5 meters.
            std_depth = 5
            for key in varkeys:
                variabledimension = len(data[key].dims)
               # print 'KEY'
               # print key
                try:
                    #Fill nan-values by propogating values forward
                    data[key].ffill



                    # Some of the variables only depend on time, while others
                    # have two dimensions, where one is depth
                    # Checking for length of dimensions, and setting standard
                    # depth to 5.
                    if variabledimension < 2:
                        # Check wether the dimension is time or depth
                        if 'time' in str(data[key].dims):
                            kwargs[key] = str(data[key].sel(time=currenttime64, method='nearest').values)
                        elif 'depth' in str(data[key].dims):
                            kwargs[key] = str(data[key].sel(depth=std_depth, method='nearest').values)
                        else:
                            kwargs[key] = str(data[key].sel(time=currenttime64, method='nearest').values)
                    elif variabledimension > 1:
                        kwargs[key] = str(data[key].sel(depth=std_depth, time=currenttime64, method='nearest').values)
                except Exception as e:
                   #   print str(data[key].dims)
                      print data[key].indexes
                      print e

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

count = 0
#Appends and split the string in two objects. To access the relevant use meta[i][1]
for a in soup.find_all('a', href=True):
    meta.append(a['href'].split('='))

#Sorting out only the newest entries in the catalog.
metarefined = []
for element in meta:
    if '.nc' in str(element):
         print element
         metarefined.append(element)
         count +=1
print count
loop()


