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
    host: process.env.BASEX_HOST || 'localhost',
    port: process.env.BASEX_PORT || 1984,
    username: process.env.BASEX_USERNAME || 'admin',
    password: process.env.BASEX_PASSWORD || 'admin',
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
    this.buffers = []; // list of buffers with unconsumed input data
    // The inputActions field is pre-filled with the operations needed
    // for the login sequence.  Any user transactions will be queued
    // after these operations.
    this.inputActions = [ this.READ_STRING, this.sendLogin, this.READ_BYTE, this.getLoginStatus ];
    this.handlerArguments = [];
    // Start with queued output.  All user writes are queued until
    // after the login has been completed.
    this.outputQueue = [];

    this.socket = net.createConnection(this.options.port, this.options.host);
    this.socket.setNoDelay();

    var session = this;
    this.socket.on('data', function (data) { session.handleData(data); });
}

util.inherits(Session, events.EventEmitter);

Session.prototype.READ_STRING = 1;
Session.prototype.READ_BYTE = 2;

Session.prototype.CMD_QUERY = 0;
Session.prototype.CMD_CLOSE = 2;
Session.prototype.CMD_BIND = 3;
Session.prototype.CMD_RESULTS = 4;
Session.prototype.CMD_EXECUTE = 5;
Session.prototype.CMD_INFO = 6;
Session.prototype.CMD_OPTIONS = 7;

Session.prototype.flushOutputQueue = function () {
    while (this.outputQueue.length) {
        if (!this.socket.write(this.outputQueue.shift())) {
            this.socket.once('drain', this.flushOutputQueue.bind(this));
            return;                                         // stay in output queue mode
        }
    }
    this.outputQueue = null;
}

Session.prototype.writeMessage = function(items, force) {
    // Calculate size of message buffer (which is costly, but less
    // costly than sending out multiple messages)
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
        default:
            throw new Error("unexpected argument " + i + " in Session.writeMessage, must be string or number, found " + typeof arg + " (" + arg + ")");
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
        }
    }

    if (this.outputQueue && !force) {
        this.outputQueue.push(buffer);
    } else {
        if (!this.socket.write(buffer)) {
            this.outputQueue = [];
            this.socket.once('drain', this.flushOutputQueue.bind(this));
        }
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
    // Try to read a string from the buffers read so far.  First, the
    // characters that make up the string are collected into the
    // stringBuffer of the session.  That buffer is then decoded using
    // the Buffer.toString() function.

    // This function either returns the decoded string or null if no
    // complete string was found in the input buffer yet.
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

    while (this.inputActions.length > 0) {
        switch (this.inputActions[0]) {
        case this.READ_STRING:
            var string = this.getStringFromBuffers();
            if (string !== null) {
                this.handlerArguments.push(string);
                this.inputActions.shift();
                break;
            } else {
                return;
            }
        case this.READ_BYTE:
            var byte = this.getByteFromBuffers();
            if (byte !== null) {
                this.handlerArguments.push(byte);
                this.inputActions.shift();
                break;
            } else {
                return;
            }
        default:
            var handler = this.inputActions.shift();
            var arguments = this.handlerArguments;
            this.handlerArguments = [];
            handler.apply(this, arguments);
        }
    }
}

Session.prototype.transaction = function(sendData, receiveData, handler) {
    for (var i = 0; i < receiveData.length; i++) {
        this.inputActions.push(receiveData[i]);
    }
    this.inputActions.push(handler);
    this.writeMessage(sendData);
}

Session.prototype.readError = function(handler) {
    // If an error occurs, the error message will follow the error
    // status byte.  Deal with this by reading the error message next,
    // before processing any queued input operations.
    this.inputActions.unshift(handler);
    this.inputActions.unshift(this.READ_STRING);
}

Session.prototype.sendLogin = function(timestamp) {
    this.writeMessage([ this.options.username, md5(md5(this.options.password) + timestamp) ], true);
}

Session.prototype.getLoginStatus = function (loginStatus) {
    if (loginStatus == 0) {
        this.emit('loggedIn');
        if (this.outputQueue) {
            this.flushOutputQueue();
        }
    } else {
        this.emit('error', new Error('authorization failed'));
    };
}

Session.prototype.defaultHandler = function(err, result) {
    if (err) {
        this.emit('error', err);
    } else {
        this.emit('result', result);
    }
}

Session.prototype.execute = function (command, handler) {
    handler = handler || this.defaultHandler.bind(this);
    this.transaction([ command ],
                     [ this.READ_STRING, this.READ_STRING, this.READ_BYTE ],
                     function (result, info, code) {
                         if (code == 0) {
                             handler(null, { result: result, info: info });
                         } else {
                             handler(new Error('BaseX command failed\ncommand: ' + command + '\n' + 'message: ' + info));
                         }
                     });
}

Session.prototype.getValueType = function(value) {
    switch (typeof value) {
    case 'string':
        return 'xs:string';
        break;
    case 'number':
        return 'xs:decimal';
        break;
    default:
        return 'xs:string';
    }
}

Session.prototype.executeBoundQuery = function(id, bindings, handler) {
    var output = [];
    var input = [];
    for (var key in bindings) {
        var value = bindings[key];
        output = output.concat([ this.CMD_BIND, id, key, value.toString(), this.getValueType(value) ]);
        input = input.concat([ this.READ_STRING, this.READ_BYTE,
                               function (empty, status) {
                                   if (status != 0) {
                                       this.emit('error', new Error("binding of variable " + key + " failed"));
                                   }
                               } ]);
    }
    output = output.concat([ this.CMD_EXECUTE, id ]);
    input = input.concat([ this.READ_STRING, this.READ_BYTE,
                           function (result, status) {
                               if (status != 0) {
                                   this.readError(function (message) {
                                       handler(new Error(message));
                                   });
                               } else {
                                   handler(null, result);
                               }
                           } ]);
    this.inputActions = this.inputActions.concat(input);
    this.writeMessage(output);
}

Session.prototype.prepareQuery = function(queryString, handler) {
    this.transaction([ this.CMD_QUERY, queryString ],
                     [ this.READ_STRING, this.READ_BYTE ],
                     handler);
}

Session.prototype.query = function(queryString, bindings, handler) {
    if (typeof bindings == 'function') {
        handler = bindings;
        bindings = {};
    }
    if (handler) {
        this.prepareQuery(queryString,
                          function (id, status) {
                              if (status != 0) {
                                  handler(new Error('unexpected status ' + status + ' received from server when allocating query ID'));
                                  return;
                              }
                              this.executeBoundQuery(id, bindings, handler);
                          });
    } else {
        return new Query(this, queryString);
    }
}

function Query(session, queryString) {
    events.EventEmitter.call(this);

    this.session = session;
    this.queryString = queryString;

    var query = this;
    this.session.prepareQuery(queryString, 
                              function (id, status) {
                                  if (status != 0) {
                                      query.emit('error', new Error('unexpected status ' + status + ' received from server when allocating query ID'));
                                  } else {
                                      query.id = id;
                                      query.emit('queryParsed');
                                  }
                              });
}

util.inherits(Query, events.EventEmitter);

Query.prototype.execute = function(bindings, handler) {
    if (typeof bindings == 'function') {
        handler = bindings;
        bindings = {};
    }

    handler = handler || this.session.defaultHandler.bind(this);

    function execute() {
        this.session.executeBoundQuery(this.id, bindings, handler);
    }

    if (this.id == undefined) {
        this.once('queryParsed', execute.bind(this));
    } else {
        execute.bind(this)();
    }
}

exports.Session = Session;
