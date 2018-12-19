import xarray
import datetime as dt
import numpy as np
import json
from bs4 import BeautifulSoup
import urllib2

def loop():
    #Iterating through all html entries found
    for object in meta:
        #Operate only on .nc files
        if '.nc' in str(object):
            print 'File found'
            fetchurl = 'http://thredds.met.no/thredds/dodsC/' + object[1]
            # Extract station name from URL
            tempname = str(object).split('/')
            tempname = str(tempname[5]).split('.')
            tempname = str(tempname[0])
            print 'Fetchurl: '
            print fetchurl

            #Imports dataset
            data = xarray.open_dataset(fetchurl)

            #Finds all variable handles for given nc file
            varkeys = data.variables.keys()

            # Print variable data to a file
            filename = 'data_' + tempname + '.json'
            file = open(filename, 'w')

            #Stores all variable data for given time in a array
            kwargs = {}
            for key in varkeys:
                try:
                    kwargs[key] = str(data[key].sel(time = currenttime64, method='nearest').values)
                except Exception as e:
                    print e


            jsondata = json.dumps(kwargs, indent=2)

            file.write(jsondata)
            file.close()

#In datetime format
currenttime = dt.datetime.now()
deltatime = dt.timedelta(minutes=500)
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
catalogUrl = 'http://thredds.met.no/thredds/catalog/obs/buoy-svv-e39/' + year + '/' + month + '/' + day + '/' + 'catalog.html'

#Fetches all the location names in the catalog
content = urllib2.urlopen(catalogUrl)
soup = BeautifulSoup(content, "html.parser")
meta = []

#Appends and split the string in two objects. To access the relevant use meta[i][1]
for a in soup.find_all('a', href=True):
    meta.append(a['href'].split('='))

loop()

#http://thredds.met.no/thredds/dodsC/obs/buoy-svv-e39/2018/12/19/20181219_080000_E39_D_Breisundet_adcp.nc
#dimensions or multi-index levels ['time'] do not exist