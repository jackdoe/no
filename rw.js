// curl -XGET -d '{"and":[{"or":[{"tag":"a"}]},{"or":[{"tag":"b"},{"tag":"a"}]}]}' http://localhost:8001/' # query AND(OR(a),OR(b,a))
// curl -XGET -d '{"tag":"any"}' http://localhost:8001/' # query a
// curl -XGET -d '{blablabla}' 'http://localhost:8000/?tags=a&tags=b' # send messages with tags a and b
// echo -n "hello" >/dev/udp/localhost/8003

var http = require('http');
var fs = require('fs');
var timers = require('timers');
var url = require('url');
var dgram = require('dgram');

var COUNTER = 0;
var NAME_TO_STORE = {}
var ROOT = "/tmp/messages/";
var PAUSE = -1;
var WRITER_PORT = 8000;
var WRITER_UDP_PORT = 8002;
var SEARCHER_PORT = 8001;

function Store(time_id) {
    this.time_id = time_id;
    try {
        fs.mkdirSync(ROOT + time_id);
    } catch(e) {

    };
    this.fn = ROOT + time_id + '/main.txt';
    this.fd = fs.openSync(this.fn, 'a+')
    this.position = fs.statSync(this.fn).size;
    this.fd_tags = {}
}

Store.prototype.log = function(msg, level) {
    msg = this.fn + ": " + msg;
    if (level == 0)
        throw(new Error(msg));
    else
        console.log(msg);
}

Store.prototype.append = function(data, tags, callback) {
    if (data.length > 0xFFFFFF || data.length == 0)
        this.log("data.length("+data.length+") > 0xFFFFFF",0);

    var blen = new Buffer(4);
    blen.fill(0);
    blen.writeUInt32BE(data.length, 0);
    var n_written = fs.writeSync(this.fd,blen,0,4,this.position);
    if (n_written != 4)
        this.log("failed to write 4 bytes, got: " + n_written, 0);

    if (fs.writeSync(this.fd,data,0,data.length,this.position + 4) != data.length)
        this.log("failed to write " + data.length + " bytes", 0);

    COUNTER++;

    var buf = new Buffer(4);
    buf.fill(0);
    buf.writeUInt32BE(this.position, 0);
    this.position += data.length + 4;
    var store = this;
    tags.forEach(function(tag) {
        if (tag) {
            fd = store.fd_tags[tag];
            if (!fd) {
                fd = fs.openSync(fn_for_tag(store.time_id, tag),'a');
                store.fd_tags[tag] = fd;
            }
            fs.writeSync(fd,buf,0, 4); // A write that's under the size of 'PIPE_BUF' is supposed to be atomic
        }
    });

    callback();
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

var get_store_obj = function(time_id) {
    if (!(time_id in NAME_TO_STORE))
        NAME_TO_STORE[time_id] = new Store(time_id)
    return NAME_TO_STORE[time_id];
}

var ts_to_id = function (ts) {
    return Math.floor(ts / 10);
}
var time = function() {
    return ts_to_id(Math.floor(Date.now() / 1000)); // in 10s of time_ids
}

var time_inc = function(from) {
    return from + 1;
}

var time_dec = function(from) {
    return from + 1;
}

function DocumentIdentifier() {
    this.time_id = Number.MAX_VALUE;
    this.offset = Number.MAX_VALUE;

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
}

function Term(tag) {
    this.doc_id = new DocumentIdentifier();

    this.offset = 0;
    this.tag = tag;
    this.time_id = time();
    this.fd = -1;
    this.buffer = new Buffer(4);
    this.from = undefined;
    this.to = undefined;
    this.size = 0;

    this.set_time_id_range = function(from,to) {
        this.from = parseInt(from) || time();
        this.time_id = this.from;
        if (to)
            this.to = parseInt(to);
    }
    this.left = function() { return this.size - this.offset };
    this.reopen_if_needed = function() {
        if (this.fd > 0) {
            if (this.left() == 0) {
                this.size = fs.fstatSync(this.fd).size;
                if (this.left() > 0)
                    return true;
            }
            if (this.time_id == time())
                return true;
        }

        var end = this.to || time();
        var old_time_id = this.time_id;
        while(this.time_id <= end) {
            var name = fn_for_tag(this.time_id, this.tag);
            if (fs.existsSync(name)) {
                if (this.fd > 0)
                    fs.closeSync(this.fd);
                this.fd = fs.openSync(name, 'r');
                this.size = fs.fstatSync(this.fd).size;
                if (this.time_id != old_time_id)
                    this.offset = 0;
                if (this.left() > 0)
                    return true;
            }
            this.time_id = time_inc(this.time_id);
        }
        return false;
    }

    this.next = function () {
        if (this.reopen_if_needed()) {
            if (this.left() >= 4) {
                this.buffer.fill(0);

                var n_read = fs.readSync(this.fd,this.buffer,0,4,this.offset);
                if (n_read != 4) throw(new Error("failed to read 4 bytes, got:" + n_read + " size: " + size + " at offset: " + this.offset));

                this.offset += 4;
                this.doc_id.time_id = this.time_id;
                this.doc_id.offset = this.buffer.readUInt32BE(0);
                return this.doc_id;
            }
        }

        if (this.to && this.time_id >= this.to) {
            this.doc_id.reset();
            return this.doc_id;
        }
        return PAUSE;
    }
    this.to_string = function () {
        return "tag:" + this.tag + "@" + (this.from || 0) + ":" + (this.to || 0);
    }
}

function BoolOr() {
    this.queries = [];
    this.doc_id = new DocumentIdentifier();

    this.add = function(query) {
        this.queries.push(query);
    }

    this.set_time_id_range = function(from,to) {
        for(var i = 0; i < this.queries.length; i++)
            this.queries[i].set_time_id_range(from,to);
    }

    this.next = function () {
        // XXX: this blocks until all of the queries are not pausing
        var new_doc = new DocumentIdentifier();
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

            if (cur_doc.cmp(new_doc) <= 0) new_doc = cur_doc;
        }
        if (has_one_pause)
            return PAUSE;
        return this.doc_id = new_doc;
    }

    this.to_string = function () {
        return "OR(" + this.queries.map(function(e) { return e.to_string() }).join(",") + ")";
    }
}

function BoolAnd() {
    this.or = new BoolOr();
    this.doc_id = new DocumentIdentifier();

    this.add = function(query) {
        this.or.add(query);
    }

    this.set_time_id_range = function(from,to) {
        this.or.set_time_id_range(from,to);
    }

    this.next = function () {
        while(true) {
            var new_doc = this.or.next();
            if (new_doc == PAUSE) return new_doc;

            var n = 0;
            for (var i = 0; i < this.or.queries.length; i++) {
                if (this.or.queries[i].doc_id.equals(new_doc))
                    n++;
            }
            if (n = this.or.queries.length)
                return this.doc_id = new_doc;
        }
    }

    this.to_string = function () {
        return "AND[" + this.or.to_string() +"]";
    }
}


var parseAnd = function(obj) {
    if (obj.length == 1)
        return parse(obj[0]);

    var b = new BoolAnd();
    for (var i = 0; i < obj.length; i++) {
        b.add(parse(obj[i]));
    }
    return b;
}
var parseOr = function(obj) {
    if (obj.length == 1)
        return parse(obj[0]);

    var b = new BoolOr();
    for (var i = 0; i < obj.length; i++) {
        b.add(parse(obj[i]));
    }
    return b;
}

var parse = function(obj) {
    var b = new BoolOr();
    if (obj.tag) {
        b.add(new Term(obj.tag));
    } else {
        if (obj.and) {
            b.add(parseAnd(obj.and))
        } else if (obj.or) {
            b.add(parseOr(obj.or));
        }
    }
    if (b.queries.length == 1)

        return b.queries[0];
    return b;
}

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
    var i;
    request.on('end', function () {
        obj = JSON.parse(body);
        var q = parse(obj);
        q.set_time_id_range(qs.from,qs.to);
        console.log(q.to_string());
        var i = setInterval(function() {
            var n;
            do {
                var n = q.next()
                if (n.time_id == Number.MAX_VALUE) {
                    response.end();
                    timers.clearInterval(i);
                    break;
                }

                if (n != PAUSE) {
                    var bytes = get_store_obj(n.time_id).get(n.offset);
                    if (qs.json) {
                        response.write(JSON.stringify({time_id: n.time_id, offset: n.offset, data: bytes }));
                    } else {
                        response.write(bytes);
                    }
                }
            } while(n != PAUSE);
        },1000);

        response.on('end', function() {
            timers.clearInterval(i);
        })
    });
});



var acceptor = http.createServer(function (request, response) {
    var url_parts = url.parse(request.url, true);
    var query = url_parts.query;
    var body = '';
    request.on('data', function (data) { body += data; });
    request.on('end', function () {
        var s = get_store_obj(time());
        var tags = query.tags;
        if (!(tags instanceof Array))
            tags = [tags];

        s.append(new Buffer(body), tags || [], function() {
            response.writeHead(200, {"Content-Type": "text/plain"});
            response.end("ok " + s.position + "\n");
        });
    });
});

acceptor.listen(WRITER_PORT);
searcher.listen(SEARCHER_PORT);

var udp = dgram.createSocket('udp4');
udp.on('message', function (message, remote) {
    get_store_obj(time()).append(message, ["any"], function() {});
});
udp.bind(WRITER_UDP_PORT);

console.log("running on writer: http@" + WRITER_PORT + "/udp@" + WRITER_UDP_PORT +", searcher: http@" + SEARCHER_PORT);
setInterval(function() {
    console.log(time() + " written so far: " + COUNTER);
},1000);
