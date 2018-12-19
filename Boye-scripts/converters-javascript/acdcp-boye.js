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
        currentSpeed: data.currSp,
        currentDirection: data.currDir,
        depth: data.depth,
        currentError: data.currErr,
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
 * 
 *    THE INPUT FROM MQTT
 * 
 *  {
    "deviceName": "acdcp_Vartdalsfjorden",
    "currSp": "31.640600204467773",
    "currDir": "343.8280944824219",
    "depth": "5",
    "longitude": "8.156190872",
    "currErr": "0.0",
    "time": "2018-12-19T12:59:00.000000000",
    "latitude": "63.08813095"
}
 * 
 * 
 */