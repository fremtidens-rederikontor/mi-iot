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
        waterTempAqd: data.WaterTempAqd,
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
    "deviceName": "aquadopp_Vartdalsfjorden",
    "currDir001": "340.3125",
    "latitude": "63.08813095",
    "time": "2018-12-19T12:59:00.000000000",
    "longitude": "8.156190872",
    "WaterTempAqd": "7.09375",
    "currSp001": "43.3594"
}
 * 
 * 
 */