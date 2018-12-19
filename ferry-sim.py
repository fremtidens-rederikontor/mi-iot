import json
import paho.mqtt.client as mqtt
import datetime, time

class myData:

    def __init__(self):
        self.latitude = None
        self.longitude = None

    def createPayload(self, latitude, longitude, deviceName):
        now = datetime.datetime.now()
        data = { "deviceName": deviceName, "latitude": latitude, "longitude": longitude}
        self.json_dataPayload = json.dumps(data,indent=4, sort_keys=True, default=str)

    def returnPayload(self):
        return self.json_dataPayload

    def returnJson(self):
        return self.json_data

def on_connect(client, userdata, flags, rc):
    print("Connected with result code " + str(rc))
    client.subscribe("ntnu-ocean")

def publishMqtt():
    now = datetime.datetime.now()
    #print(now)
    # client.publish(topic, now.strftime('%H:%M:%S'))
    id = "simulation"
    topic = id + "/ferry/festoy"
    print(myData.json_dataPayload)
    client.publish(topic, myData.json_dataPayload)
    print('Message Published @topic ' + topic)

# Debug true/false
DEBUG = False

# Setting up the broker information
brokerAddress = "168.63.93.40"
client = mqtt.Client()
#client.username_pw_set("3030304040BBB", "crazyfrog123")
client.on_connect = on_connect
client.connect(brokerAddress, 9999)

# Creating data-instance
myData = myData()

# Defining the starting and ending point of latitude and
# longitude.
latFest = 62.375224
lonFest = 6.331203

latSol = 62.413568
lonSol = 6.327486

# Calculating the difference (How much the ferry has to move in total)
latDiff = latFest-latSol
lonDiff = lonFest-lonSol

# Initial starting point of the ferry.
latFerry = latFest
lonFerry = lonFest

# Iterating through 100 steps of the movement from point A to B.
n = 100
while n>0:

    # Defining the current position of the ferry
    latOutput = latFerry - (latDiff/100)
    lonOutput = lonFerry - (lonDiff/100)
    
    if(DEBUG):
        print('')
        print('Latitude Output:' + str(latOutput))
        print('Longitude Output:' + str(lonOutput))

    # Current position of the ferry
    latFerry = latOutput
    lonFerry = lonOutput

    # Configures the data and publishes it to broker.
    myData.createPayload(str(latOutput), str(lonOutput), "festoyFerry")
    publishMqtt()

    # Sleep time to provide total movement over 15 minutes.
    time.sleep(9)
    n=n-1

if(DEBUG):
    print('finished')

time.sleep(300)

# Iterating through 100 steps of the movement from point B to A.
n = 100
while n>0:

    # Defining the current position of the ferry
    latOutput = latFerry + (latDiff/100)
    lonOutput = lonFerry + (lonDiff/100)
    
    if(DEBUG):
        print('')
        print('Latitude Output:' + str(latOutput))
        print('Longitude Output:' + str(lonOutput))

    # Current position of the ferry
    latFerry = latOutput
    lonFerry = lonOutput

    # Configures the data and publishes it to broker.
    myData.createPayload(str(latOutput), str(lonOutput), "festoyFerry")
    publishMqtt()

    # Sleep time to provide total movement over 15 minutes.
    time.sleep(9)
    n=n-1

if(DEBUG):
    print('finished')
