/**
 * New node file
 */
var fs = require('fs');
var protobuf = require('protocol-buffers')
var path = require('path');
var messages = protobuf(fs.readFileSync(path.resolve(__dirname, 'data.proto')));

var ROOT = '/tmp/messages/';
var NODE_ID = 0;

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

Store.prototype.append = function(data) {
    if (data.length > 0xFFFFFF || data.length == 0)
        this.log("data.length("+data.length+") > 0xFFFFFF",0);

    data.header.time_id = this.time_id;
    data.header.offset = this.position;
    data.header.node_id = NODE_ID;

    var encoded = messages.Data.encode(data);

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
    var tags = data.header.tags;
    for (var i = 0; i < tags.length; i++) {
        var tag = tags[i];
        if (tag) {
            fd = this.fd_tags[tag];
            if (!fd) {
                fd = fs.openSync(Store.fn_for_tag(this.time_id, tag),'a');
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

Store.fn_for_tag = function(time_id,tag) {
    return ROOT + time_id + '/tag#' + tag + '.txt';
}

module.exports = function(root, node_id) {
	ROOT = root;
	NODE_ID = node_id;
	return Store;
}