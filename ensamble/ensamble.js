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

var message_types = {}; 
Object.keys(messages.Ensamble_Header.Type).forEach(function(m){
	message_types[messages.Ensamble_Header.Type[m]] = m;
});

const util = require('util');
const EventEmitter = require('events');

function read_messages(socket) {
	// Identify this client
	socket.name = socket.remoteAddress + ":" + socket.remotePort
	
	console.log('Connected by: ' + socket.name);
	
	var data = new Buffer(0);
	var read_data;
	var expected_length = -1;
	
	socket.on('data', function(chunk) {
		  data = Buffer.concat([data, chunk]);
		  console.log('got %d bytes of data', data.length);
		  
		  while(data.length > 0) {
			  if(data.length >= 4) {
				  expected_length = data.readInt32BE(0);
			  } else {
				  break;
			  }
			  
			  console.log('got a message of %d bytes', expected_length);
			  
			  if(data.length >= expected_length) {
				  read_data = data.slice(4, 4+expected_length);
				  data = data.slice(4+expected_length);
				  
				  var message = messages.Ensamble_Message.decode(read_data);
				  socket.emit('decoded_message', message, socket);
			  }
		  }
	});
	
	socket.on('drain', function(){
	  console.log("drained...");
	});
	
	socket.on('close', function() {
		console.log("Connection closed by remote: " + socket.name);
	});
}


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
			
			if(message.header.type == messages.Ensamble_Header.Type.HELLO) {
				console.log("I can see a hello, I am promoting this socket to a Member..!!!");


				var hello_data = {
					'header': {
						'node_id': 6,
						'type': messages.Ensamble_Header.Type.HELLO
					}
				};

				var hello = messages.Ensamble_Message.encode(hello_data);

				var blen = new Buffer(4);
				blen.fill(0);
				blen.writeUInt32BE(hello.length, 0);

				var hello_buf = Buffer.concat([blen, hello]);

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
		
		//This is where we start ACTUALLY decoding messages coming over the wire!
		read_messages(socket);
	}
};

Me.prototype.startListener = function(port) {
	console.log("Starting ensamble listener");

	var server = net.createServer(this.listener_function());
	
	server.on('connection', function(socket) {
		console.log("Received connection...");
//		console.dir(socket);
	})

	server.listen({'port': port},function() {
//	  address = server.address();
//	  console.log("opened server on %j", address);
	});
}


function Member(node, client) {
	
	this.node = node;
	this.alive = false;
	this.client = undefined;
	
	if(client) {
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
	var me = me;
	

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
	
	this.emit(message_types[message.header.type], message);
	
//	if(message.header.type == messages.Ensamble_Header.Type.HELLO) {
//		console.log("Member emitting hello...");
//		this.emit('hello');
//	}
//	if(message.header.type == messages.Ensamble_Header.Type.TICK) {
//		this.emit('tick');
//	}
	console.log(message);
}

Member.prototype.open_connection = function() {
	console.log("Opening connection to: " + this.node);
	
	var me = this;
	var up = url.parse(this.node);
	console.dir(up);
	
	var client = net.connect({host: up.hostname, port: up.port},
	  function() { //'connect' listener
		  console.log('connected to server!');
		  
	});

	client.on('connect', function(conn) {
		console.log('connection establ');
		me.init_client(me, client);
		read_messages(client);

		this.send("HELLO");
		
		}.bind(this));
	
	client.on('error', function( err ) {
		console.log("ERROR!!!")
		console.dir(err);
	});
}

Member.prototype.send = function(type, payload) {

	var data = {
		'header': {
			'node_id': 6,
			'type': messages.Ensamble_Header.Type[type]
		}
	};
	
	if(payload != undefined) {
		data["payload"] = {"data": payload};
	}
	
	var encoded_data = messages.Ensamble_Message.encode(data);

	var blen = new Buffer(4);
	blen.fill(0);
	blen.writeUInt32BE(encoded_data.length, 0);

	var send_buffer = Buffer.concat([blen, encoded_data]);

	this.client.write(send_buffer, function() {
		console.log("I have sent my message of type: " + type);
	});

}

Member.prototype.stop = function() {
	console.log("Stop member: " + this.host);
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

	this.me.on('HELLO', function(socket) {
		console.log("I received a hello from a new connection: " + socket.name);

		this._addMember( new Member(socket.name, socket) );
	}.bind(this));
}

Ensamble.prototype.setMaster = function(member) {
	if(member == undefined) {
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
	
	member.on("bye", function(){
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
	} catch(e) {
		console.log(e);
	}
}

Ensamble.prototype.broadcast = function(message) {
	
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