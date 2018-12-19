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
        sprtp: data.sprtp,
        mdir: data.mdri,
        mdira: data.mdira,
        tm02: data.tm02,
        tm02a: data.tm02a,
        mdirb: data.mdirb,
        thtp: data.thtp,
        hm0a: data.hm0a,
        hmax: data.hmax,
        tm02b: data.tm02b,
        tp: data.tp,
        hm0: data.Hm0,
        hm0b: data.hm0b,
        thhf: data.thhf,
        tm01: data.tm01,
        thmax: data.thmax,
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
    "deviceName": "wave_Vartdalsfjorden",
    "sprtp": "18.9844",
    "mdir": "171.5625",
    "mdira": "93.5156",
    "tm02": "2.1484",
    "tm02a": "12.3438",
    "latitude": "63.08813095",
    "mdirb": "172.2656",
    "thtp": "169.4531",
    "time": "2018-12-19T12:50:00.000000000",
    "longitude": "8.15624237",
    "hm0a": "0.04395",
    "hmax": "0.48828",
    "tm02b": "2.1387",
    "tp": "2.3926",
    "Hm0": "0.32227",
    "hm0b": "0.30762",
    "thhf": "172.2656",
    "tm01": "2.1973",
    "thmax": "2.9785"
}
 * 
 * 
 * 
 */