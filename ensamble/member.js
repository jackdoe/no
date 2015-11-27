var fs = require("fs");
var net = require("net");
var url = require("url");
var protobuf = require('protocol-buffers')
var path = require('path');

const util = require('util');
const EventEmitter = require('events');

var protocol_decoder = require('./protocol_decoder.js');

function Member(node, client) {

    this.node = node;
    this.alive = false;
    this.client = undefined;

    if (client) {
        this.init_client(this, client);
    } else {
        this.open_connection();
    }

    EventEmitter.call(this);
}

util.inherits(Member, EventEmitter);

Member.prototype.getName = function() {
    return this.node;
}

Member.prototype.init_client = function(me, client) {

    client.on('close', function() {
        console.log("seding bye!");
        me.emit('bye');
    });

    client.on('decoded_message', me.onMessage.bind(me));

    me.name = client.name
    me.client = client;
}

Member.prototype.onMessage = function(message, socket) {
    console.log("I got a decoded message from node: " + this.node);

    this.emit(protocol_decoder.message_types[message.header.type], message);

    console.log(message);
}

Member.prototype.open_connection = function() {
    console.log("Opening connection to: " + this.node);

    var me = this;
    var up = url.parse(this.node);
    console.dir(up);

    var client = net.connect({
        host : up.hostname,
        port : up.port
    }, function() {
        // 'connect' listener
        console.log('connected to server!');
    });

    client.on('connect', function(conn) {
        console.log('connection establ');
        me.init_client(me, client);
        protocol_decoder.decode_socket(client);

        this.send("HELLO");

    }.bind(this));

    client.on('error', function(err) {
        console.log("ERROR!!!")
        console.dir(err);
    });
}

Member.prototype.send = function(type, payload) {
    var send_buffer = protocol_decoder.encode_message(6, type, payload);

    this.client.write(send_buffer, function() {
        console.log("I have sent my message of type: " + type);
    });

}

Member.prototype.stop = function() {
    console.log("Stop member: " + this.host);
}

module.exports = Member;