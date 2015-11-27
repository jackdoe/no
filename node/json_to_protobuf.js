var protobuf = require('protocol-buffers')
var fs = require('fs');
var path = require('path');
var messages = protobuf(fs.readFileSync(path.resolve(__dirname, 'data.proto')));

var obj = JSON.parse(process.argv[2]);
process.stdout.write(messages.Data.encode(obj));
