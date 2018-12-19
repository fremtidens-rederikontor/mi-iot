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
        currentSpeed: data.currSp001,
        currentDirection: data.currDir001,
        depth: data.depth,
        currentError: data.currErr,
        waterTemp: data.WaterTemp,
        pressure: data.Pressure,
        salinity: data.Salinity,
        conductivity: data.Conductivity,
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
 * {
    "deviceName": "ctd_Vartdalsfjorden",
    "WaterTemp": "7.062987804412842",
    "Conductivity": "32.71453857421875",
    "Salinity": "32.122802734375",
    "longitude": "8.156186103666668",
    "depth": "1",
    "Pressure": "nan",
    "time": "2018-12-19T12:59:50.000000000",
    "latitude": "63.08813095"
}
 * 
 */