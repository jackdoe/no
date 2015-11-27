var fs = require("fs");
var net = require("net");
var url = require("url");
var protobuf = require('protocol-buffers')
var path = require('path');
var messages = protobuf(fs.readFileSync(path.resolve(__dirname, 'data.proto')));

var message_types = {};

Object.keys(messages.Ensamble_Header.Type).forEach(function(m) {
    message_types[messages.Ensamble_Header.Type[m]] = m;
});

function protocol_decoder() {
    this.messages = messages;
    this.message_types = message_types;
}

protocol_decoder.prototype.decode_socket = function(socket) {
    // Identify this client
    socket.name = socket.remoteAddress + ":" + socket.remotePort

    console.log('Connected by: ' + socket.name);

    var data = new Buffer(0);
    var read_data;
    var expected_length = -1;

    socket.on('data', function(chunk) {
        data = Buffer.concat([ data, chunk ]);
        console.log('got %d bytes of data', data.length);

        while (data.length > 0) {
            if (data.length >= 4) {
                expected_length = data.readInt32BE(0);
            } else {
                break;
            }

            console.log('got a message of %d bytes', expected_length);

            if (data.length >= expected_length) {
                read_data = data.slice(4, 4 + expected_length);
                data = data.slice(4 + expected_length);

                var message = messages.Ensamble_Message.decode(read_data);
                socket.emit('decoded_message', message, socket);
            }
        }
    });

    socket.on('drain', function() {
        console.log("drained...");
    });

    socket.on('close', function() {
        console.log("Connection closed by remote: " + socket.name);
    });
}

module.exports = new protocol_decoder();