import requests
import xml.etree.ElementTree as ET
import paho.mqtt.client as mqtt
import datetime, time
import json

class myData:
    def __init__(self):
        self.timeFrom = None
        self.timeTo = None
        self.temp = None
        self.windDir = None
        self.windSpeed = None
        self.humidity = None
        self.pressure = None

    def set_values(self, timeFrom, timeTo, temp, windDir, windSpeed, humidity, pressure):
        self.timeTo = timeTo
        self.timeFrom = timeFrom
        self.windDir = windDir
        self.windSpeed = windSpeed
        self.humidity = humidity
        self.pressure = pressure
        self.temp = temp

    def createPayload(self, temp, windSpeed, humidity, pressure):
        now = datetime.datetime.now()
        data = { "testapp.temp": temp, "testapp.windSpeed": windSpeed, "testapp.humidity": humidity, "testapp.pressure": pressure}
        self.json_dataPayload = json.dumps(data,indent=4, sort_keys=True, default=str)

    def returnPayload(self):
        return self.json_dataPayload
    def returnJson(self):
        return self.json_data
    def get_timeFrom(self):
        return self.timeFrom
    def get_timeTo(self):
        return self.timeTo
    def get_temp(self):
        return self.temp
    def get_windSpeed(self):
        return self.windSpeed
    def get_windDir(self):
        return self.windDir
    def get_humidity(self):
        return self.humidity
    def get_pressure(self):
        return self.pressure

def on_connect(client, userdata, flags, rc):
    print("Connected with result code " + str(rc))
    client.subscribe("ntnu-ocean")

def on_message(client, userdata, msg):
    print(msg.topic + " " + str(msg.payload))

def myWeatherRequest():
    apikey = "45bcb62bc6a5da5d1f3fecd9eb35ab46"
    cityId = "6453341"
    URL = "https://api.openweathermap.org/data/2.5/weather?id="+cityId+"&APPID="+apikey
    r = requests.get(URL).text
    data = json.loads(r)
    wind = data['wind']['speed']
    temp = round(data['main']['temp'] - 273, 2)
    humidity = data['main']['humidity']
    pressure = data['main']['pressure']
    print (wind,temp,humidity,pressure)
    myData.createPayload(temp, wind, humidity, pressure)

def publishMqtt():
    now = datetime.datetime.now()
    #print(now)
    # client.publish(topic, now.strftime('%H:%M:%S'))
    id = "sivert"
    topic = id + "/test"
    print(myData.json_dataPayload)
    client.publish(topic, myData.json_dataPayload)
    print('Message Published @topic ' + topic)

brokerAddress = "168.63.93.40"
client = mqtt.Client()
#client.username_pw_set("3030304040BBB", "crazyfrog123")
client.on_connect = on_connect
client.connect(brokerAddress, 9999)

myData = myData()
myWeatherRequest()
publishMqtt()

