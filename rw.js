// curl -XGET -d '{"and":[{"or":[{"tag":"a"}]},{"or":[{"tag":"b"},{"tag":"a"}]}]}' http://localhost:8001/' # query AND(OR(a),OR(b,a))
// curl -XGET -d '{"tag":"any"}' http://localhost:8001/' # query a
// node json_to_protobuf.js '{"header":{"node_id": 0, "time_id": 0, "offset": 0, "tags":["a","b"]},"frames":[{"data": "AAAAAAAAAAA"},{"data": "BBBBBBBBBB","id":"b"}]}' > example.pb
// curl -silent -XGET --data-binary @example.pb 'http://localhost:8000/' # send messages with tags a and b
// cat example.pb >/dev/udp/localhost/8003

var protobuf = require('protocol-buffers')
var http = require('http');
var fs = require('fs');
var url = require('url');
var timers = require('timers');
var url = require('url');
var dgram = require('dgram');
var udp = dgram.createSocket('udp4');
var path = require('path');
var messages = protobuf(fs.readFileSync(path.resolve(__dirname, 'data.proto')));
var ensamble = require('./ensamble');

var time_inc = function(from) {
    return from + 1;
}

var time_dec = function(from) {
    return from - 1;
}

var time = function() {
    return Math.floor(Date.now() / 1000);
}

var tick = time();

var argv = require('minimist')(process.argv.slice(2));

ensamble.start(parseInt(argv.intercom));

if (argv.ensamble) {
    ensamble.reset();

    var ensamble_members = argv.ensamble.split(",");
    ensamble_members.forEach(function(m) {

        ensamble.addMember("rw://" + m);
    });

    ensamble.save();
} else {
    ensamble.load();
}

var on_tick = function(data) {
    console.log(data);
    var tick_id = data.readUInt32BE(0);
    tick = tick_id;

    console.log("I've set my time to: " + tick);
}

ensamble.on('TICK', function(message) {on_tick(message.payload.data);});

var MASTER = argv.master; // XXX: temp

var master_tick = time();
if (MASTER) {
    setInterval(function(){
        var tick_data = new Buffer(4);
        tick_data.fill(0);
        tick_data.writeUInt32BE(++master_tick,0)

        on_tick(tick_data);
        ensamble.broadcast('TICK', tick_data);
    }, 1000);
}


var WCOUNTER = 0;
var RCOUNTER = 0;
var NAME_TO_STORE = {};
var ROOT = argv.root || '/tmp/messages/';


var WRITER_PORT = argv.writer || 8001;
var NODE_ID = argv.node_id || 0;
var WRITER_UDP_PORT = argv.udp || 0;
var SEARCHER_PORT = argv.searcher || 8002;
var POOL = (argv.pool instanceof Array ? argv.pool : [ argv.pool ]).filter(
        function(e) {
            return e
        }).map(function(e) {
    if (!e.startsWith("http://"))
        e = "http://" + e;

    var u = url.parse(e)
    return {
        host : u.hostname,
        port : u.port || WRITER_UDP_PORT
    }
});

var Store = require('./store.js')(ROOT, NODE_ID);

var TERMINATED = new Buffer(1);
TERMINATED.fill(0);

Array.prototype.random = function() {
    return this[Math.floor((Math.random() * this.length))];
}

var fn_for_tag = function(time_id, tag) {
    return ROOT + time_id + '/tag#' + tag + '.txt';
}

var get_store_obj = function(time_id, cache) {
    if (!cache)
        return new Store(time_id);
    if (!(time_id in cache))
        cache[time_id] = new Store(time_id)
    return cache[time_id];
}


var err_handler = function(response, e, interval, do_not_terminate) {
    var msg = (e instanceof Error ? e.stack : e);
    if (!do_not_terminate)
        response.write(TERMINATED)
    response.end(msg)
    if (interval)
        timers.clearInterval(interval);
    console.log(msg);
};





var acceptor = http.createServer(function(request, response) {
    var url_parts = url.parse(request.url, true);
    var body = new Buffer(0);
    request.on('data', function(data) {
        body = Buffer.concat([ body, data ])
    });
    request.on('end', function() {
        try {
            var decoded = messages.Data.decode(body)

            var t = tick;

            var s = get_store_obj(t, NAME_TO_STORE);
            var encoded = s.append(decoded);
            WCOUNTER++;

            response.writeHead(200, {
                "Content-Type" : "application/json"
            });
            response.end(JSON.stringify({
                offset : s.position,
                fn : s.time_id
            }));
        } catch (e) {
            err_handler(response, e, undefined, true);
        }
    });
});

process.on('uncaughtException', function(e) {
    console.log((e instanceof Error ? e.stack : e));
});

if (WRITER_PORT > 0)
    acceptor.listen(WRITER_PORT);

if (SEARCHER_PORT > 0) {
    var searcher = require('./searcher')(Store, messages);
    searcher.error_handler = err_handler;
    searcher.start(SEARCHER_PORT);
}

if (WRITER_UDP_PORT > 0) {
    udp.on('message', function(message, remote) {
        WCOUNTER++;
        get_store_obj(time(), NAME_TO_STORE).append(
                messages.Data.decode(message), function() {
                });
    });
    udp.bind(WRITER_UDP_PORT);
}

console.log("running on writer: http@" + WRITER_PORT + "/udp@"
        + WRITER_UDP_PORT + ", searcher: http@" + SEARCHER_PORT + " POOL: "
        + JSON.stringify(POOL) + " NODE_ID: " + NODE_ID);
setInterval(function() {
    cleaned = 0;
    // NAME_TO_STORE is only used for writers
    for ( var k in NAME_TO_STORE) {
        if (k < time_dec(time())) {
            console.log("cleaning up: " + k);
            NAME_TO_STORE[k].cleanup();
            delete NAME_TO_STORE[k];
            cleaned++;
        }
    }
    console.log(time() + " written: " + WCOUNTER + "/s, searched: " + RCOUNTER
            + "/s, cleaned: " + cleaned);
    RCOUNTER = 0;
    WCOUNTER = 0;

    console.log(Object.keys(ensamble.getMembers()));
}, 1000);
