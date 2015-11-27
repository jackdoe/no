/**
 * 
 */
'use strict';

var fs = require("fs");
var net = require("net");
var url = require("url");
var protobuf = require('protocol-buffers')
var path = require('path');
var messages = protobuf(fs.readFileSync(path.resolve(__dirname, 'data.proto')));

const
util = require('util');
const
EventEmitter = require('events');

var Member = require("./member.js");

var protocol_decoder = require('./protocol_decoder.js');

function Me(ensamble, port) {
    console.log("Starting ensamble listener");
    this.ensamble = ensamble;
    this.startListener(port);
    this.name = "My own damn listener";

    this.messageQueue = [];

    EventEmitter.call(this);
}
util.inherits(Me, EventEmitter);

Me.prototype.listener_function = function() {
    var me = this;

    return function(socket) {
        var listener;

        var listen_function;
        listen_function = function(message, sock) {
            console.log("GOTCHA!");

            if (message.header.type == messages.Ensamble_Header.Type.HELLO) {
                console.log("I can see a hello, I am promoting this socket to a Member..!!!");

                var hello_data = {
                    'header' : {
                        'node_id' : 6,
                        'type' : messages.Ensamble_Header.Type.HELLO
                    }
                };

                var hello = messages.Ensamble_Message.encode(hello_data);

                var blen = new Buffer(4);
                blen.fill(0);
                blen.writeUInt32BE(hello.length, 0);

                var hello_buf = Buffer.concat([ blen, hello ]);

                socket.write(hello_buf, function() {
                    console.log("I have sent my helloz...");
                });

                me.emit('HELLO', socket);

                socket.removeListener('decoded_message', listen_function);
            } else {
                socket.end();
            }
        }

        socket.on('decoded_message', listen_function)

        socket.hello_received = false;

        // This is where we start ACTUALLY decoding messages coming over the
        // wire!
        protocol_decoder.decode_socket(socket);
    }
};

Me.prototype.startListener = function(port) {
    console.log("Starting ensamble listener");

    var server = net.createServer(this.listener_function());

    server.on('connection', function(socket) {
        console.log("Received connection...");
        // console.dir(socket);
    })

    server.listen({
        'port' : port
    }, function() {
        // address = server.address();
        // console.log("opened server on %j", address);
    });
}

function Ensamble() {
    this.members = [];
    this.me;
    this.is_master = false;
    this.master;

    EventEmitter.call(this);
}

util.inherits(Ensamble, EventEmitter);

Ensamble.prototype.start = function(port) {
    this.me = new Me(this, port);

    this.me.on('HELLO',
            function(socket) {
                console.log("I received a hello from a new connection: "
                        + socket.name);

                this._addMember(new Member(socket.name, socket));
            }.bind(this));
}

Ensamble.prototype.setMaster = function(member) {
    if (member == undefined) {
        this.master = undefined;
        this.is_master = true;
    }
}

Ensamble.prototype.reset = function() {
    console.log("Resetting ensamble state...");

    Object.keys(this.members).forEach(function(m) {
        delete this.members[m];
    });
}

Ensamble.prototype._addMember = function(member) {

    member.on("bye", function() {
        console.log("I shall remove this member...");
        this._removeMember(member);
    }.bind(this));

    member.on("MASTER_OFFER", function(message) {
        console.log("I got a master OFFER: " + message.payload);
    });

    member.on("MASTER_DISCOVER", function(message) {
        member.send("MASTER_OFFER", this.master);
    }.bind(this));

    member.send("MASTER_DISCOVER");

    this.members[member.getName()] = member;
}

Ensamble.prototype._removeMember = function(member) {
    member.stop();
    delete this.members[member.getName()];
}

Ensamble.prototype.addMember = function(node) {
    console.log("Adding node:" + node);
    try {
        var member = new Member(node);

        member.once('HELLO', function() {
            console.log("Adding member to the ensamble...");
            this._addMember(member);
        }.bind(this));
    } catch (e) {
        console.log(e);
    }
}

Ensamble.prototype.broadcast_tick = function(tick_time) {
    var tick_data = new Buffer(4);
    tick_data.fill(0);
    tick_data.writeUInt32BE(tick_time,0)

    Object.keys(this.members).forEach(function(member_key) {;
        //console.log("Sending tick to " + this.members[member_key].node);
        this.members[member_key].send('TICK',tick_data);
    }.bind(this));

}

Ensamble.prototype.getMembers = function() {
    return this.members;
}

Ensamble.prototype.save = function() {
    console.log("Saving ensamble state");
}

Ensamble.prototype.load = function() {
    console.log("Loading ensamble");
}

module.exports = new Ensamble();
