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

function DocumentIdentifier() {
    this.time_id = -1;
    this.offset = -1;

    this.equals = function(other) {
        return this.time_id == other.time_id && this.offset == other.offset;
    }

    this.cmp = function(other) {
        if (this.time_id === other.time_id)
            return this.offset - other.offset
        return this.time_id - other.time_id;
    }

    this.reset = function() {
        this.time_id = Number.MAX_VALUE;
        this.offset = Number.MAX_VALUE;
    }

    this.set = function(other) {
        this.time_id = other.time_id;
        this.offset = other.offset;
    }

    this.no_more = function() {
        return this.time_id == Number.MAX_VALUE;
    }

    this.set_from_hash = function(other) {
        this.time_id = parseInt(other.time_id);
        this.offset = parseInt(other.offset);
    }

    this.to_string = function() {
        if (this.no_more())
            return "NO_MORE";
        if (this.time_id == -1)
            return "NO_INIT";
        return "time_id: " + this.time_id + ",offset: " + this.offset;
    }
}

function Term(tag) {
    this.doc_id = new DocumentIdentifier();

    this.offset = 0;
    this.tag = tag;
    this.time_id = time_dec(time());
    this.fd = -1;
    this.buffer = new Buffer(6);
    this.from = undefined;
    this.to = undefined;
    this.size = 0;
    this.not_initialized = true;

    this.set_time_id_range = function(from, to) {
        this.from = parseInt(from) || time_dec(time());
        this.time_id = this.from;
        this.to = to ? parseInt(to) : time();
    }

    this.left = function() {
        return this.size - this.offset
    };

    this.jump = function(to_doc_id) {
        if (this.doc_id.equals(to_doc_id))
            return this.doc_id;

        this.time_id = to_doc_id.time_id;
        this.offset = to_doc_id.offset;
        return this.next();
    };

    this.has_something_left_in_current_file = function() {
        if (this.fd <= 0)
            return false;

        return (this.left() > 0)
    }

    this.pick_closest_non_zero_file = function(temp_time_id) {
        var end = this.to;
        while (temp_time_id <= end) {
            if (this.open_time_id(temp_time_id))
                return true;
            temp_time_id = time_inc(temp_time_id);
        }

        return false;
    }

    this.open_time_id = function(tid) {
        if (this.fd > 0) {
            fs.closeSync(this.fd);
            this.fd = -1;
        }

        var name = Store.fn_for_tag(tid, this.tag);
        if (fs.existsSync(name)) {
            this.time_id = tid;
            this.fd = fs.openSync(name, 'r');
            this.size = fs.fstatSync(this.fd).size;
            this.offset = 0;
            return true;
        }
        return false;
    }

    this.reopen_if_needed = function() {
        if (this.not_initialized) {
            if (!this.pick_closest_non_zero_file(this.from))
                return false;
            this.not_initialized = false;
        }

        if (this.has_something_left_in_current_file())
            return true;

        return this.pick_closest_non_zero_file(time_inc(this.time_id));
    }

    this.next = function() {
        if (this.doc_id.no_more())
            return this.doc_id;
        if (this.reopen_if_needed()) {
            if (this.left() >= 6) {
                this.buffer.fill(0);

                var n_read = fs.readSync(this.fd, this.buffer, 0, 6,
                        this.offset);
                if (n_read != 6)
                    throw (new Error("failed to read 6 bytes, got:" + n_read
                            + " size: " + size + " at offset: " + this.offset));

                this.offset += 6;
                this.doc_id.time_id = this.time_id;

                this.doc_id.offset = (this.buffer.readUInt32BE(2) << 16)
                        | this.buffer.readUInt16BE(0);
                return this.doc_id;
            }
        }

        this.doc_id.reset();
        return this.doc_id;
    }
    this.to_string = function() {
        return "tag:" + this.tag + "@" + (this.from || 0) + ":"
                + (this.to || 0) + "#" + this.doc_id.to_string();
    }
}

function TermOffsetTimeId(list) {
    this.list = list.sort(function(a, b) {
        var v = parseInt(a.time_id) - parseInt(b.time_id);
        if (v != 0)
            return v;
        return parseInt(a.offset) - parseInt(b.offset);
    });

    this.doc_id = new DocumentIdentifier();
    this.cursor = -1;
    this.set_time_id_range = function(from, to) {
        // XXX: does it make sense to honor from/to here?
    }

    this.jump = function(to_doc_id) {
        if (this.doc_id.no_more())
            return this.doc_id;

        // XXX: bsearch
        while (cursor < this.list.length) {
            this.doc_id.set_from_hash(this.list[cursor]);
            if (this.doc_id.equals(to_doc_id))
                return this.doc_id;
            if (this.doc_id.cmp(to_doc_id) >= 0)
                return this.doc_id;
            cursor++;
        }

        this.doc_id.reset();
        return this.doc_id;
    }

    this.next = function() {
        if (this.doc_id.no_more())
            return this.doc_id;

        if (this.cursor + 1 >= this.list.length) {
            this.doc_id.reset();
            return this.doc_id;
        }

        this.cursor++;
        this.doc_id.set_from_hash(this.list[this.cursor]);
        return this.doc_id;
    }

    this.to_string = function() {
        return "{list:" + JSON.stringify(this.list) + "@" + (this.from || 0)
                + ":" + (this.to || 0) + "#" + this.doc_id.to_string() + "}";
    }
}

function BoolOr() {
    this.queries = [];
    this.doc_id = new DocumentIdentifier();
    this.new_doc = new DocumentIdentifier();

    this.add = function(query) {
        this.queries.push(query);
    }

    this.jump = function(to_doc_id) {
        if (to_doc_id.no_more()) {
            this.doc_id.reset();
            return this.doc_id;
        }

        while (true) {
            if (this.doc_id.no_more())
                return this.doc_id;

            if (this.doc_id.equals(to_doc_id))
                return this.doc_id;

            if (this.doc_id.cmp(to_doc_id) >= 0)
                return this.doc_id;

            this.next();
        }
    }

    this.set_time_id_range = function(from, to) {
        for (var i = 0; i < this.queries.length; i++)
            this.queries[i].set_time_id_range(from, to);
    }

    this.next = function() {
        if (this.doc_id.no_more())
            return this.doc_id;

        this.new_doc.reset();
        for (var i = 0; i < this.queries.length; i++) {
            var cur_doc = this.queries[i].doc_id;
            if (cur_doc.equals(this.doc_id) || cur_doc.time_id == -1)
                cur_doc = this.queries[i].next();

            if (cur_doc.cmp(this.new_doc) < 0)
                this.new_doc.set(cur_doc);
        }
        this.doc_id.set(this.new_doc);
        return this.doc_id;
    }

    this.to_string = function() {
        return "{OR(" + this.queries.map(function(e) {
            return e.to_string()
        }).join(",") + ")" + "#" + this.doc_id.to_string() + "}";
    }
}

function BoolAnd() {
    this.or = new BoolOr();
    this.doc_id = new DocumentIdentifier();

    this.add = function(query) {
        this.or.add(query);
    }

    this.set_time_id_range = function(from, to) {
        this.or.set_time_id_range(from, to);
    }

    this.jump = function(to_doc_id) {
        return this.next_with_target(this.or.jump(to_doc_id));
    }

    this.next = function() {
        return this.next_with_target(this.or.next());
    }

    this.next_with_target = function(to_doc_id) {
        if (to_doc_id.no_more()) {
            this.doc_id.reset();
            return this.doc_id;
        }

        if (this.doc_id.no_more())
            return this.doc_id;

        var new_doc = to_doc_id;
        while (true) {
            if (new_doc.no_more()) {
                this.doc_id.set(new_doc);
                return this.doc_id;
            }
            var n = 0;
            var biggest = undefined;
            for (var i = 0; i < this.or.queries.length; i++) {
                var qdoc_id = this.or.queries[i].doc_id;
                if (qdoc_id.equals(new_doc))
                    n++;
                if (!biggest || qdoc_id.cmp(biggest) > 0)
                    biggest = qdoc_id
            }

            if (n == this.or.queries.length) {
                this.doc_id.set(new_doc);
                return this.doc_id;
            } else {
                new_doc = this.or.jump(biggest);
            }
        }
    }

    this.to_string = function() {
        return "{AND[" + this.or.to_string() + "]" + "#"
                + this.doc_id.to_string() + "}";
    }
}

var parse = function(obj) {
    var q;
    if (obj.tag) {
        q = new Term(obj.tag);
    } else if (obj.list) {
        q = new TermOffsetTimeId(obj.list);
    } else {
        if (!obj.and && !obj.or)
            throw (new Error("dont know what to do with " + JSON.stringify(obj)));

        var arr = obj.and ? obj.and : obj.or;
        if (arr.length == 1) {
            q = parse(arr[0]);
        } else {
            q = obj.and ? new BoolAnd() : new BoolOr();
            for (var i = 0; i < arr.length; i++) {
                q.add(parse(arr[i]));
            }
        }
    }
    if (obj.from || obj.to)
        q.set_time_id_range(obj.from, obj.to);
    return q;
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

var searcher = http.createServer(function(request, response) {
    response.writeHead(200, {
        'Content-Type' : 'text/event-stream',
        'Connection' : 'keep-alive',
        'Cache-Control' : 'no-cache',
    });
    var url_parts = url.parse(request.url, true);
    var qs = url_parts.query;

    var body = '';
    request.on('data', function(data) {
        body += data;
    });

    var cache = {};
    var cleanup = function() {
        for ( var k in cache) {
            cache[k].cleanup();
            delete cache[k];
        }
    };

    var send = function(bytes) {
        if (qs.sub) {
            var sub = qs.sub;
            if (!(sub instanceof Array))
                sub = [ sub ];
            var frames = [];
            var decoded = messages.Data.decode(bytes);
            var seen = {}
            for (var i = 0; i < sub.length; i++) {
                for (var j = 0; j < decoded.frames.length; j++) {
                    if (!seen[j] && decoded.frames[j].id == sub[i]) {
                        frames.push(decoded.frames[j]);
                        seen[j] = true;
                    }
                }
            }

            var output = {
                header : decoded.header,
                frames : frames
            };
            bytes = messages.Data.encode(output);
        }

        RCOUNTER++;

        if (qs.tlv) {
            var lbuf = new Buffer(4);
            lbuf.fill(0);
            lbuf.writeUInt32BE(bytes.length);
            response.write(lbuf, 'binary');
        }
        response.write(bytes, 'binary');
    };

    request.on('end', function() {
        try {
            obj = JSON.parse(body);
            var q = parse(obj);
            var n;
            while (true) {
                var n = q.next()
                if (n.no_more()) {
                    response.end();
                    cleanup();
                    break;
                }
                send(get_store_obj(n.time_id, cache).get(n.offset));
            }
            ;
        } catch (e) {
            err_handler(response, e, undefined);
        }
        ;
        response.on('error', function() {
            cleanup()
        });
        response.on('end', function() {
            cleanup()
        });
    });
    request.connection.on('close', function() {
        cleanup()
    });

});

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

if (SEARCHER_PORT > 0)
    searcher.listen(SEARCHER_PORT);

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
