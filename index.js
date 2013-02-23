/* simple-basex - Node.JS client for BaseX
 * http://docs.basex.org/wiki/Server_Protocol
 */

/* The BaseX protocol uses zero terminated strings with escaping for
 * zero bytes in strings.
 *
 * The decoding of incoming messages needs to be deferred until the
 * type of the data is known, which is dependent on the type of
 * exchange being executed.  In order to support this, two buffering
 * levels are used - Incoming data buffers are stored until they are
 * consumed and a second decoding buffer for strings is used so that
 * UTF-8 decoding can be performed on one buffer.  When reading a
 * string, bytes are pulled from the input buffers until a zero byte
 * is read.  When an escape byte (0xff) is seen, the next byte is put
 * into the string buffer verbatim.  As soon as the zero terminator is
 * encountered, the rest of the buffer is kept in the input buffer
 * chain, the string is decoded and the handler is invoked.
 *
 * This convoluted mechanism is used to make sure that UTF-8 multi
 * byte sequences which are split across incoming packet boundaries
 * are correctly decoded.
 */

var net = require('net');
var events = require('events');
var util = require('util');
var crypto = require('crypto');

exports.sessionDefaults = {
    host: 'localhost',
    port: 1984,
    user: 'admin',
    password: 'admin',
    initialStringBufferSize: 0x10000
};

function md5(str) {
    return crypto.createHash('md5').update(str).digest("hex");
}

function Session(options) {
    events.EventEmitter.call(this);

    this.options = options || {};
    this.options.__proto__ = exports.sessionDefaults;

    this.stringBuffer = new Buffer(this.options.initialStringBufferSize);
    this.stringBufferOffset = 0;
    this.inEscape = false;
    this.buffers = []; // list of buffers with unconsumed data
    this.queue = [];
    this.handlerArguments = [];

    this.socket = net.createConnection(this.options.port, this.options.host);
    this.socket.setNoDelay();

    var session = this;
    this.socket.on('connect', function () { session.performHandshake(); });
    this.socket.on('data', function (data) { session.handleData(data); });
    this.socket.on('end', function () { session.busy = true; });
}

util.inherits(Session, events.EventEmitter);

Session.prototype.READ_STRING = 1;
Session.prototype.READ_BYTE = 2;

Session.prototype.writeMessage = function(items) {
    var bufferSize = 0;
    for (var i = 0; i < items.length; i++) {
        var arg = items[i];
        switch (typeof arg) {
        case 'string':
            bufferSize += Buffer.byteLength(arg) + 1;
            break;
        case 'number':
            if (arg < 0 || arg > 255) {
                throw new Error('numeric protocol argument out range (needs to be between 0 and 255');
            }
            bufferSize += 1;
            break;
        case 'function':
            handler = arg;
            break;
        default:
            throw new Error("unexpected argument type (at 1)");
        }
    }
    var buffer = new Buffer(bufferSize);
    var offset = 0;
    for (var i = 0; i < items.length; i++) {
        var arg = items[i];
        switch (typeof arg) {
        case 'string':
            offset += buffer.write(arg, offset);
            buffer[offset++] = 0;
            break;
        case 'number':
            buffer.writeInt8(arg, offset++);
            break;
        default:
            throw new Error("unexpected argument type (at 2)");
        }
    }
    if (!this.socket.write(buffer)) {
        throw new Error('could not write (write buffering not implemented)');
    }
}

Session.prototype.pushToStringBuffer = function (byte) {
    if (this.stringBuffer.length == this.stringBufferOffset) {
        var newBuffer = new Buffer(this.stringBufferOffset * 2);
        this.stringBuffer.copy(newBuffer);
        this.stringBuffer = newBuffer;
    }
    this.stringBuffer[this.stringBufferOffset++] = byte;
}

Session.prototype.getStringFromBuffers = function() {
    while (this.buffers.length > 0) {
        var buffer = this.buffers.shift();
        for (var i = 0; i < buffer.length; i++) {
            var byte = buffer[i];
            if (this.inEscape) {
                this.pushToStringBuffer(byte);
                this.inEscape = false;
            } else if (byte == 0xff) {
                this.inEscape = true;
            } else if (byte == 0x00) {
                var retval = this.stringBuffer.toString('utf8', 0, this.stringBufferOffset);
                this.stringBufferOffset = 0;
                i++;                                        // skip over terminating 0
                if (buffer.length > i) {
                    this.buffers.unshift(buffer.slice(i)); // return unconsumed part of buffer to buffer list
                }
                return retval;
            } else {
                this.pushToStringBuffer(byte);
            }
        }
    }
    return null;
}

Session.prototype.getByteFromBuffers = function () {
    if (this.buffers.length) {
        var byte = this.buffers[0][0];
        if (this.buffers[0].length > 1) {
            this.buffers[0] = this.buffers[0].slice(1);
        } else {
            this.buffers.shift();
        }
        return byte;
    } else {
        return null;
    }
}

Session.prototype.handleData = function(data) {
    this.buffers.push(data);

    while (this.queue.length > 0) {
        switch (this.queue[0]) {
        case this.READ_STRING:
            var string = this.getStringFromBuffers();
            if (string !== null) {
                this.handlerArguments.push(string);
                this.queue.shift();
                break;
            } else {
                return;
            }
        case this.READ_BYTE:
            var byte = this.getByteFromBuffers();
            if (byte !== null) {
                this.handlerArguments.push(byte);
                this.queue.shift();
                break;
            } else {
                return;
            }
        default:
            var handler = this.queue.shift();
            var arguments = this.handlerArguments;
            this.handlerArguments = [];
            handler.apply(this, arguments);
        }
    }
}

Session.prototype.transaction = function(sendData, receiveData, handler) {
    for (var i = 0; i < receiveData.length; i++) {
        this.queue.push(receiveData[i]);
    }
    this.queue.push(handler);
    this.writeMessage(sendData);
}

Session.prototype.readError = function(handler) {
    this.queue.unshift(handler);
    this.queue.unshift(this.READ_STRING);
}

Session.prototype.performHandshake = function() {
    this.transaction([],
                     [ this.READ_STRING ],
                     this.login);
}

Session.prototype.login = function(timestamp) {
    this.transaction([ this.options.user, md5(md5(this.options.password) + timestamp) ],
                     [ this.READ_BYTE ],
                     this.getLoginStatus);
}

Session.prototype.getLoginStatus = function (loginStatus) {
    if (loginStatus == 0) {
        this.emit('loggedIn');
    } else {
        this.emit('error', new Error('authorization failed'));
    };
}

Session.prototype.defaultHandler = function(result, info, code) {
    this.emit('result', { result: result, info: info, code: code });
}

Session.prototype.execute = function (query, handler) {
    handler = handler || this.defaultHandler;
    this.transaction([ query ],
                     [ this.READ_STRING, this.READ_STRING, this.READ_BYTE ],
                     function (result, info, code) {
                         if (code == 0) {
                             handler.call(this, result, info);
                         } else {
                             this.emit('error', new Error('BaseX query failed\nquery: ' + query + '\n' + 'message: ' + info));
                         }
                     });
}

Session.prototype.query = function(queryString) {
    var retval = new Query(this);

    this.transaction([ 0, queryString ],
                     [ this.READ_STRING, this.READ_BYTE ],
                     function saveQueryId(id, status) {
                         if (status != 0) {
                             this.emit('error', new Error('unexpected status ' + status + ' received from server when allocating query ID'));
                         } else {
                             retval.id = id;
                             this.emit('queryIdAllocated', retval);
                         }
                     });
    return retval;
}

function Query(session) {
    this.session = session;
}

Query.prototype.bind = function(name, value, type) {
    if (this.id == undefined) {
        throw new Error('cannot bind to query that has no ID allocated yet');
    }

    if (type == undefined) {
        switch (typeof value) {
        case 'string':
            type = 'xs:string';
            break;
        case 'number':
            type = 'xs:decimal';
            break;
        default:
            type = 'xs:string';
        }
    }

    this.session.transaction([ 3, this.id, name, value.toString(), type ],
                             [ this.session.READ_STRING, this.session.READ_BYTE ],
                             function (empty, status) {
                                 if (status) {
                                     this.readError(function (message) {
                                         this.emit('error', new Error('bind error: ' + message));
                                     });
                                 }
                             });
}

Query.prototype.close = function(handler) {
}

Query.prototype.execute = function(handler) {
    if (this.id == undefined) {
        throw new Error('cannot bind to query that has no ID allocated yet');
    }

    this.session.transaction([ 5, this.id ],
                             [ this.session.READ_STRING, this.session.READ_BYTE ],
                             function (result, status) {
                                 console.log('prepared query ran, result:', result, 'status', status);
                             });
}

Query.prototype.info = function(handler) {
}

exports.Session = Session;
