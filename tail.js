var http = require('http');
var protobuf = require('protocol-buffers')

// XXX: use external file
var messages = protobuf(`
message Payload {
    required bytes data = 4;
}

message Header {
    required uint64 time_id = 1;
    required uint64 offset = 2;
    required uint64 node_id = 3;
    repeated string tags = 4;
}

message Data {
    required Header header = 1;
    required Payload payload = 2;
}
`);

// node tail.js localhost 8002 '{"tag":"a"}'
var rr = http.request(
    {
        host: process.argv[2],
        port: parseInt(process.argv[3]),
        method: 'POST',
        path: '/?tlv=1',
    },function (res) {

        var data = new Buffer(0);
        var need = -1;

        res.on('data', function(chunk) {
            data = Buffer.concat([data,chunk]);

            AGAIN: while(true) {
                if (need == -1 && data.length >= 4) {
                    need = data.readUInt32BE(0);
                    data = data.slice(4);
                }
                if (data.length >= need) {
                    var slice = data.slice(0, need);
                    var decoded = messages.Data.decode(slice);
                    console.log(decoded);
                    data = data.slice(need);
                    need = -1;
                    if (data.length >= 4) {
                        console.log("AGAIN");
                        continue AGAIN;
                    }
                }
                break;
            }
        });
        res.on('end', function() {
            console.log("END");
        });
    });

rr.write(process.argv[4]);
rr.end();

