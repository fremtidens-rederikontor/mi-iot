// Decode an uplink message from a buffer
// payload - array of bytes
// metadata - key/value object

/** Decoder **/

// decode payload to string
var data = decodeToJson(payload);
var payloadStr = decodeToString(payload);

var result = [];
// decode payload to JSON
// var data = decodeToJson(payload);

var deviceType = metadata.deviceType;



// Result object with device attributes/telemetry data
result.push({
   deviceName: data.deviceName,
   deviceType: deviceType,
   telemetry: {
       ts: data.time,
       values: {
        latitude: data.latitude,
        longitude: data.longitude,
        windGust: data.WindGust,
        windSpeed: data.WindSpeed,
        windDirection: data.WindDirection,
        rawData: payloadStr
       }
   }
});

/** Helper functions **/

function decodeToString(payload) {
   return String.fromCharCode.apply(String, payload);
}

function decodeToJson(payload) {
   // covert payload to string.
   var str = decodeToString(payload);

   // parse string to JSON
   var data = JSON.parse(str);
   return data;
}

return result;

/**
 * INPUT FROM MQTT
 * 
 * {
    "deviceName": "wind_Vartdalsfjorden",
    "WindGust": "10.78125",
    "latitude": "63.08813095",
    "WindSpeed": "7.96875",
    "WindDirection": "156.44531",
    "time": "2018-12-19T12:50:00.000000000",
    "longitude": "8.15624237"
}
 * 
 */