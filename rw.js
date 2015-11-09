// curl -XGET -d '{"and":[{"or":[{"tag":"a"}]},{"or":[{"tag":"b"},{"tag":"a"}]}]}' http://localhost:8001/' # query AND(OR(a),OR(b,a))
// curl -XGET -d '{"tag":"any"}' http://localhost:8001/' # query a
// curl -XGET -d '{blablabla}' 'http://localhost:8000/?tags=a&tags=b' # send messages with tags a and b
// echo -n "hello" >/dev/udp/localhost/8003

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

var argv = require('optimist')
    .default('root','/tmp/messages/')
    .default('writer',0)
    .default('udp',0)
    .default('searcher',0)
    .default('node_id',0)
    .argv;

var WCOUNTER = 0;
var RCOUNTER = 0;
var NAME_TO_STORE = {};
var ROOT = argv.root;
var PAUSE = -1;

var WRITER_PORT = argv.writer;
var NODE_ID = argv.node_id;
var WRITER_UDP_PORT = argv.udp
var SEARCHER_PORT = argv.searcher;
var POOL = (argv.pool instanceof Array ? argv.pool : [argv.pool] )
    .filter(function(e) { return e })
    .map(function(e) {
        if (!e.startsWith("http://"))
            e = "http://" + e;

        var u = url.parse(e)
        return { host: u.hostname, port: u.port || WRITER_UDP_PORT }
    });

var TERMINATED = new Buffer(1);
TERMINATED.fill(0);

Array.prototype.random = function () {
  return this[Math.floor((Math.random()*this.length))];
}

function Store(time_id) {
    this.time_id = time_id;
    if (!fs.existsSync(ROOT + time_id))
        fs.mkdirSync(ROOT + time_id);
    this.fn = ROOT + time_id + '/main.txt';
    this.fd = fs.openSync(this.fn, 'a+')
    this.position = fs.statSync(this.fn).size;
    this.fd_tags = {}
}

Store.prototype.fsyncSync = function() {
    for (var k in this.fd_tags) {
        fs.fsyncSync(this.fd_tags[k]);
    }
    fs.fsyncSync(this.fd);
}

Store.prototype.cleanup = function() {
    for (var k in this.fd_tags) {
        fs.closeSync(this.fd_tags[k]);
    }
    fs.closeSync(this.fd);
}

Store.prototype.log = function(msg, level) {
    msg = this.fn + ": " + msg;
    if (level == 0)
        throw(new Error(msg));
    else
        console.log(msg);
}

Store.prototype.append = function(data, replica) {
    if (data.length > 0xFFFFFF || data.length == 0)
        this.log("data.length("+data.length+") > 0xFFFFFF",0);
    var encoded;
    if (replica) {
        encoded = data;
    } else {
        encoded = messages.Data.encode({
            header: {time_id: this.time_id, offset: this.position, node_id: NODE_ID},
            payload: data,
        });
    }
    // XXX: make the protobuf decoder understand streams and offsets, instead of writing the length here
    var blen = new Buffer(4);
    blen.fill(0);
    blen.writeUInt32BE(encoded.length, 0);

    var n_written = fs.writeSync(this.fd, blen, 0, blen.length, this.position);
    if (n_written != blen.length)
        this.log("failed to write "+blen.length+" bytes, got: " + n_written, 0);

    if (fs.writeSync(this.fd, encoded, 0, encoded.length, this.position + blen.length) != encoded.length)
        this.log("failed to write " + encoded.length + " bytes", 0);

    var buf = new Buffer(6);
    buf.fill(0);
    buf.writeUInt16BE(this.position & 0xFFFF, 0);
    buf.writeUInt32BE(this.position >> 16, 2);

    this.position += encoded.length + blen.length;

    var tags = [];
    for(var i=0; i<data.length; i++) {
        tags = tags.concat(data[i]["tags"]);
    }

    // XXX: Encode the payload index in each tag to make sub-addressing easy.
    for (var i = 0; i < tags.length; i++) {
        var tag = tags[i];
        if (tag) {
            fd = this.fd_tags[tag];
            if (!fd) {
                fd = fs.openSync(fn_for_tag(this.time_id, tag),'a');
                this.fd_tags[tag] = fd;
            }
            fs.writeSync(fd, buf, 0, buf.length); // A write that's under the size of 'PIPE_BUF' is supposed to be atomic
        }
    }

    return encoded;
};

Store.prototype.get = function(offset) {
    var blen = new Buffer(4);
    var n_read = fs.readSync(this.fd, blen, 0, 4, offset);
    if (n_read != 4)
        this.log("failed to read 4 bytes, got: " + n_read + " at offset: " + offset,0);

    var len = blen.readUInt32BE(0);
    var buffer = new Buffer(len);
    n_read = fs.readSync(this.fd, buffer, 0, len, offset + 4);
    if (n_read != len)
        this.log("failed to read " + len + " bytes, got: " + n_read + ", from offset: " + offset, 0);
    return buffer;
};

var fn_for_tag = function(time_id,tag) {
    return ROOT + time_id + '/tag#' + tag + '.txt';
}

var get_store_obj = function(time_id, cache) {
    if (!cache)
        return new Store(time_id);
    if (!(time_id in cache))
        cache[time_id] = new Store(time_id)
    return cache[time_id];
}

var ts_to_id = function (ts) {
    return Math.floor(ts / 60);
}
var time = function() {
    return ts_to_id(Math.floor(Date.now() / 1000));
}

var time_inc = function(from) {
    return from + 1;
}

var time_dec = function(from) {
    return from - 1;
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
            return  "NO_MORE";
        if (this.time_id == -1)
            return "NO_INIT";
        return "time_id: " + this.time_id + ",offset: " + this.offset;
    }
}

function Term(tag) {
    this.doc_id = new DocumentIdentifier();

    this.offset = 0;
    this.tag = tag;
    this.time_id = time();
    this.fd = -1;
    this.buffer = new Buffer(6);
    this.from = undefined;
    this.to = undefined;
    this.size = 0;
    this.not_initialized = true;
    this.is_pausable = true;

    this.set_time_id_range = function(from,to) {
        this.from = parseInt(from) || time();
        this.time_id = this.from;
        if (to) {
            this.to = parseInt(to);
        }
    }
    this.set_pausable = function() {
        if (this.to && this.to < time())
            this.is_pausable = false;
    }

    this.left = function() { return this.size - this.offset };

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

        if (this.left() > 0)
            return true;

        this.size = fs.fstatSync(this.fd).size;
        return this.left() > 0;
    }

    this.pick_closest_non_zero_file = function(temp_time_id) {
        var end = this.to || time_inc(time());

        while(temp_time_id < end) {
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

        var name = fn_for_tag(tid, this.tag);
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
            if (!this.pick_closest_non_zero_file(this.time_id)) {
                this.time_id = time();
                return false;
            }
            this.not_initialized = false;
        }

        if (this.has_something_left_in_current_file())
            return true;

        return this.pick_closest_non_zero_file(time_inc(this.time_id));
    }

    this.next = function () {
        if (this.doc_id.no_more())
            return this.doc_id;

        if (this.to && this.time_id >= this.to) {
            this.doc_id.reset();
            return this.doc_id;
        }

        if (this.reopen_if_needed()) {
            if (this.left() >= 6) {
                this.buffer.fill(0);

                var n_read = fs.readSync(this.fd,this.buffer,0,6,this.offset);
                if (n_read != 6) throw(new Error("failed to read 6 bytes, got:" + n_read + " size: " + size + " at offset: " + this.offset));

                this.offset += 6;
                this.doc_id.time_id = this.time_id;

                this.doc_id.offset = (this.buffer.readUInt32BE(2) << 16) | this.buffer.readUInt16BE(0);
                return this.doc_id;
            }
        }

        if (!this.is_pausable) {
            this.doc_id.reset();
            return this.doc_id;
        }
        return PAUSE;
    }
    this.to_string = function () {
        return "tag:" + this.tag + "@" + (this.from || 0) + ":" + (this.to || 0) + "#" + this.doc_id.to_string();
    }
}

function TermOffsetTimeId(list) {
    this.is_pausable = false;
    this.list = list.sort(function(a,b) {
        var v = parseInt(a.time_id) - parseInt(b.time_id);
        if (v != 0)
            return v;
        return parseInt(a.offset) - parseInt(b.offset);
    });

    this.doc_id = new DocumentIdentifier();
    this.cursor = -1;
    this.set_pausable = function() {}
    this.set_time_id_range = function(from,to) {
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

    this.next = function () {
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

    this.to_string = function () {
        return "{list:" + JSON.stringify(this.list) + "@" + (this.from || 0) + ":" + (this.to || 0)+ "#" + this.doc_id.to_string() + "}";
    }
}

function BoolOr() {
    this.queries = [];
    this.doc_id = new DocumentIdentifier();
    this.new_doc = new DocumentIdentifier();
    this.is_pausable = false;
    this.set_pausable = function() {
        if (this.queries.length == 0) {
            this.is_pausable = false;
        } else {
            var n_not_pausable = 0;
            for (var i = 0; i < this.queries.length; i++) {
                this.queries[i].set_pausable();
                if (!this.queries[i].is_pausable)
                    n_not_pausable++;
            }
            this.is_pausable = !(n_not_pausable == this.queries.length);
        }
    }

    this.add = function(query) {
        this.queries.push(query);
    }

    this.jump = function(to_doc_id) {
        if (to_doc_id.no_more()) {
            this.doc_id.reset();
            return this.doc_id;
        }

        while(true) {
            if (this.doc_id.no_more())
                return this.doc_id;

            if (this.doc_id.equals(to_doc_id))
                return this.doc_id;

            if (this.doc_id.cmp(to_doc_id) >= 0)
                return this.doc_id;

            this.next();
        }
    }

    this.set_time_id_range = function(from,to) {
        for(var i = 0; i < this.queries.length; i++)
            this.queries[i].set_time_id_range(from,to);
    }

    this.next = function () {
        if (this.doc_id.no_more())
            return this.doc_id;

        // XXX: this blocks until all of the queries are not pausing
        this.new_doc.reset();
        var has_one_pause = false;
        for (var i = 0; i < this.queries.length; i++) {
            var cur_doc = this.queries[i].doc_id;

            if (cur_doc.equals(this.doc_id)) {
                var tmp = this.queries[i].next();

                // in case we have one query that must pause, advance the other queries anyway
                // so we group the pauses
                if (tmp == PAUSE) {
                    has_one_pause = true;
                    continue;
                } else {
                    cur_doc = tmp;
                }
            }
            if (cur_doc.cmp(this.new_doc) <= 0)
                this.new_doc.set(cur_doc);
        }

        if (has_one_pause) {
            if (!this.is_pausable) {
                this.doc_id.reset();
                return this.doc_id;
            }

            return PAUSE;
        }
        this.doc_id.set(this.new_doc);
        return this.doc_id;
    }

    this.to_string = function () {
        return "{OR(" + this.queries.map(function(e) { return e.to_string() }).join(",") + ")" + "#" + this.doc_id.to_string() +"}";
    }
}

function BoolAnd() {
    this.or = new BoolOr();
    this.doc_id = new DocumentIdentifier();

    this.set_pausable = function() {
        this.or.set_pausable();
    }

    this.add = function(query) {
        this.or.add(query);
    }

    this.set_time_id_range = function(from,to) {
        this.or.set_time_id_range(from,to);
    }

    this.jump = function(to_doc_id) {
        return this.next_with_target(this.or.jump(to_doc_id));
    }

    this.next = function () {
        return this.next_with_target(this.or.next());
    }

    this.next_with_target = function (to_doc_id) {
        if (to_doc_id == PAUSE)
            return to_doc_id;

        if (to_doc_id.no_more()) {
            this.doc_id.reset();
            return this.doc_id;
        }

        if (this.doc_id.no_more())
            return this.doc_id;

        var new_doc = to_doc_id;
        while(true) {
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

    this.to_string = function () {
        return "{AND[" + this.or.to_string() +"]" + "#" + this.doc_id.to_string() +"}";
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
            throw(new Error("dont know what to do with " + JSON.stringify(obj)));

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

var searcher = http.createServer(function (request, response) {
    response.writeHead(200, {
        'Content-Type': 'text/event-stream',
        'Connection': 'keep-alive',
        'Cache-Control': 'no-cache',
    });
    var url_parts = url.parse(request.url, true);
    var qs = url_parts.query;

    var body = '';
    request.on('data', function (data) { body += data; });
    var done = false;
    var i = undefined;
    var cache = {};
    var cleanup = function() {
        if (i) {
            timers.clearInterval(i);
            i = undefined;
        }
        for (var k in cache) {
            cache[k].cleanup();
            delete cache[k];
        }
    };

    var send = function(bytes) {
        RCOUNTER++;
        if (qs.json) {
            response.write(JSON.stringify(messages.Data.decode(bytes)));
        } else {
            if (qs.tlv) {
                var lbuf = new Buffer(4);
                lbuf.fill(0);
                lbuf.writeUInt32BE(bytes.length);
                response.write(lbuf, 'binary');
            }
            response.write(bytes, 'binary');
        }
    };

    request.on('end', function () {
        try {
            obj = JSON.parse(body);
            var q = parse(obj);
            q.set_pausable();
            i = setInterval(function() {
                try {
                    var n;
                    do {
                        var n = q.next()
                        if (n != PAUSE) {
                            if (n.no_more()) {
                                response.end();
                                cleanup();
                                break;
                            }
                            send(get_store_obj(n.time_id,cache).get(n.offset));
                        }
                    } while(n != PAUSE);
                } catch (e) {
                    err_handler(response,e,i);
                }
            },1000);
        } catch(e) {
            err_handler(response,e,undefined);
        };
        response.on('error', function() { cleanup() });
        response.on('end', function() { cleanup() });
    });
    request.connection.on('close',function(){ cleanup() });


});

var acceptor = http.createServer(function (request, response) {
    var url_parts = url.parse(request.url, true);
    var query = url_parts.query;
    var body = new Buffer(0);
    var is_receiving_replica = parseInt(query.replica || 0);
    var wait_for_n_replicas = parseInt(query.wait_for_n_replicas || 0);
    var do_n_replicas = parseInt(query.do_n_replicas || 0);
    var per_replica_timeout_ms = parseInt(query.per_replica_timeout_ms || 1000);
    request.on('data', function (data) { body = Buffer.concat([body,data]) });
    request.on('end', function () {
        try {
            var tags,t;

            if (is_receiving_replica) {
                var decoded = messages.Data.decode(body);
                t = decoded.header.time_id
                tags = decoded.header.tags;
                tags = tags.filter(function(e) { return e != "__r0" });
                tags.push("__r" + is_receiving_replica);
            } else {
                var myparsed = JSON.parse(body)

                t = time();
                tags = (query.tags instanceof Array ? query.tags : [query.tags] ).filter(function(e) { return e });
                tags.push("__r0");
            }

            var s = get_store_obj(t, NAME_TO_STORE);
            var encoded = s.append(myparsed, is_receiving_replica);
            WCOUNTER++;
            var errors = [], connections = [];
            var timeout_timer = undefined;
            var ack = function() {
                response.writeHead(200, {"Content-Type": "application/json"});
                response.end(JSON.stringify({offset: s.position, fn: s.fn, errors: errors, encoded_length: encoded.length}));
                if (timeout_timer)
                    clearTimeout(timeout_timer);
            }

            if (query.ack_before_replication)
                ack();

            if (!is_receiving_replica && POOL.length > 0 && do_n_replicas > 0) {
                var need = Math.min(wait_for_n_replicas, POOL.length, do_n_replicas);
                var left = Math.min(POOL.length, do_n_replicas);
                timeout_timer = setTimeout(function() {
                    connections.forEach(function(c) {
                        c.abort()
                    });
                }, per_replica_timeout_ms + 10);

                for (var idx = 0; idx < do_n_replicas && idx < POOL.length; idx++) {
                    // always send to the same items from the pool
                    // must randomize the pool arguments per box in order to balance
                    var rr = http.request({
                        host: POOL[idx].host,
                        port: POOL[idx].port,
                        method: 'POST',
                        path: '/?replica=' + (idx + 1) }, function (replica_response) {
                            var data = '';
                            replica_response.on('data', function(chunk) { data += chunk; });
                            replica_response.on('end', function() {
                                left--;
                                try {
                                    var rlen = JSON.parse(data).encoded_length;
                                    if (rlen != encoded.length)
                                        throw(new Error("remote encoded length("+rlen+") != local encoded length("+encoded.length+")"));
                                    need--;
                                } catch (e) {
                                    errors.push(e.message)
                                }

                                if ((need == 0 || left == 0) && !query.ack_before_replication)
                                    ack();
                            });
                        });
                    connections.push(rr);
                    rr.on('socket', function (socket) {
                        socket.setTimeout(per_replica_timeout_ms);
                        socket.on('timeout', function() {
                            rr.abort();
                        });
                    });

                    rr.on('error', function (err) {
                        errors.push(err.message)
                        if (--left == 0 && !query.ack_before_replication)
                            ack();
                    });

                    rr.write(encoded, 'binary');
                    rr.end();
                }
            } else {
                if (!query.ack_before_replication)
                    ack();
            }

        } catch (e) {
            err_handler(response, e, undefined, true);
        }
    });
});

process.on('uncaughtException', function(e){
    console.log((e instanceof Error ? e.stack : e));
});

if (WRITER_PORT > 0)
    acceptor.listen(WRITER_PORT);

if (SEARCHER_PORT > 0)
    searcher.listen(SEARCHER_PORT);

if (WRITER_UDP_PORT > 0) {
    udp.on('message', function (message, remote) { get_store_obj(time(), NAME_TO_STORE).append(message, ["any"], function() {}); });
    udp.bind(WRITER_UDP_PORT);
}

console.log("running on writer: http@" + WRITER_PORT + "/udp@" + WRITER_UDP_PORT +", searcher: http@" + SEARCHER_PORT + " POOL: " + JSON.stringify(POOL) + " NODE_ID: " + NODE_ID);
setInterval(function() {
    cleaned = 0;
    // NAME_TO_STORE is only used for writers
    for (var k in NAME_TO_STORE) {
        if (k < time_dec(time())) {
            console.log("cleaning up: " + k);
            NAME_TO_STORE[k].cleanup();
            delete NAME_TO_STORE[k];
            cleaned++;
        }
    }
    console.log(time() + " written: " + WCOUNTER + "/s, searched: " + RCOUNTER + "/s, cleaned: " + cleaned);
    RCOUNTER = 0;
    WCOUNTER = 0;
},1000);
