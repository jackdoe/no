var http = require('http');
var protobuf = require('protocol-buffers')
var fs = require('fs');
var path = require('path');
var messages = protobuf(fs.readFileSync(path.resolve(__dirname, 'data.proto')));

// node tail.js localhost 8002 '{"tag":"a"}'
var rr = http.request(
    {
        host: process.argv[2],
        port: parseInt(process.argv[3]),
        method: 'POST',
        path: '/?tlv=1&' + (process.argv[5] || ''),
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
                if (need != -1 && data.length >= need) {
                    console.log(messages.Data.decode(data.slice(0,need)));
                    data = data.slice(need);
                    need = -1;
                    if (data.length >= 4)
                        continue AGAIN;
                }
                break;
            }
        });
        res.on('end', function() {
        });
    });

rr.write(process.argv[4]);
rr.end();

