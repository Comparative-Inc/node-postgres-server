'use strict';

const net = require('net');
const EventEmitter = require('events').EventEmitter;
const util = require('util');
const PacketReader = require('./packet_reader.js');
const BufferWriter = require('./buffer_writer.js');

module.exports = PgServer;

function PgServer(options) {
  if (this instanceof PgServer) {
    this._server = net.createServer();
    this._server.on('connection',this._onServerConnection.bind(this));
    this._server.on('error',this._onServerError.bind(this));
    this._server.on('close',this._onServerClose.bind(this));
    this._server.on('listening',this._onServerListening.bind(this));
  } else {
    return new PgServer(options);
  }
  return this;
}

util.inherits(PgServer,EventEmitter);

PgServer.prototype.listen = function(port,done) {
  if (!port) {
    port = 5432;
  }
  if (!done) {
    done = function() {};
  }

  this._server.listen(port,(err) => {
    done(err);
  });
};

PgServer.prototype._onServerConnection = function(socket) {
  const client = new PgClient({ socket, server: this });

  this.emit('connection',client);
};
PgServer.prototype._onServerError = function(err) {
  console.error("_onServerError:",err);
  this.emit('error',err);
};
PgServer.prototype._onServerClose = function() {
  console.error("_onServerClose:");
  this.emit('close');
};
PgServer.prototype._onServerListening = function() {
  this.emit('listening');
};

function PgClient(options) {
  if (this instanceof PgClient) {
    this._socket = options.socket;
    this._server = options.server;
    this._reader = new PacketReader();
    this._buffer_writer = new BufferWriter();
    this._handshake_complete = false;

    this._socket.on('connect',this._onSocketConnect.bind(this));
    this._socket.on('close',this._onSocketClose.bind(this));
    this._socket.on('end',this._onSocketEnd.bind(this));
    this._socket.on('error',this._onSocketError.bind(this));
    this._socket.on('data',this._onSocketData.bind(this));
    this._socket.on('timeout',this._onSocketTimeout.bind(this));
  } else {
    return new PgClient(options);
  }
  return this;
}

PgClient.prototype._onSocketConnect = function() {
  this._server.emit('client_connect',this);
};
PgClient.prototype._onSocketClose = function() {
  this._server.emit('client_close',this);
};
PgClient.prototype._onSocketEnd = function() {
  this._server.emit('client_end',this);
};
PgClient.prototype._onSocketError = function(err) {
  this._server.emit('client_error',err,this);
};
PgClient.prototype._onSocketData = function(buf) {
  this._reader.addChunk(buf);
  this._pumpPackets();
};
PgClient.prototype._onSocketTimeout = function() {
  this._server.emit('client_timeout',this);
};
PgClient.prototype._pumpPackets = function() {
  if (!this._handshake_complete) {
    const ret = this._reader.read();
    if (ret) {
      const { buffer } = ret;
      const ver = buffer.readUInt32BE();
      if (ver == 80877103) {
        // Deny SSL, no handling yet.
        this._socket.write('N');
      } else if (ver == 196608) {
        const key_value_buffer = buffer.slice(4);
        const params = buf_to_kv(key_value_buffer);
        this._reader.headerSize = 1;
        this._handshake_complete = true;
        this._server.emit('client_connect',this,params);
      } else {
        console.error("PgClient: handshake unknown client version");
        this._server.emit('protocol_error',this,buffer);
      }
    }
  } else {
    let ret;
    while (ret = this._reader.read()) {
      const { header, buffer } = ret;
      this._handlePacket(header,buffer);
    }
  }
};
PgClient.prototype._handlePacket = function(header,buffer) {
  switch(header) {
    case 'p':
      const password = buffer.toString('utf8');
      this._server.emit('client_password',this,password);
      break;
    case 'Q':
      const query = buffer.toString('utf8');
      this._server.emit('client_query',this,query);
      break;
    case 'X':
      this._server.emit('client_terminate',this);
      break;
    default:
      console.error("PgServer: unhandled packet type:",header);
      break;
  }
};
PgClient.prototype.sendAuthenticationOk = function() {
  this._buffer_writer.addInt32(0);
  const buf = this._buffer_writer.flush('R');
  this._socket.write(buf);
};
PgClient.prototype.sendAuthenticationCleartextPassword = function() {
  this._buffer_writer.addInt32(3);
  const buf = this._buffer_writer.flush('R');
  this._socket.write(buf);
};
PgClient.prototype.sendReadyForQuery = function(status) {
  if (!status) {
    status = 'I';
  }

  this._buffer_writer.addChar(status);
  const buf = this._buffer_writer.flush('Z');
  this._socket.write(buf);
};
PgClient.prototype.sendErrorResponse = function(error_map) {
  write_error_map(error_map,this._buffer_writer);
  this._buffer_writer.addByte(0);
  const buf = this._buffer_writer.flush('E');
  this._socket.write(buf);
};

const ERROR_CODE_MAP = {
  severity: 'S',
  code: 'C',
  message: 'M',
  detail: 'D',
  hint: 'H',
  position: 'P',
  internal_position: 'p',
  internal_query: 'q',
  where: 'W',
  schema: 's',
  table: 't',
  column: 'c',
  data_type: 'd',
  contraint: 'n',
  file: 'F',
  line: 'L',
  routine: 'R',
};

function write_error_map(error_map,writer) {
  Object.keys(error_map).forEach(k => {
    const v = error_map[k].toString();
    const code = k.length == 1 ? k : ERROR_CODE_MAP[k];
    if (!code) {
      throw new Error("PgServer: bad error key: " + k);
    }
    writer.addChar(code);
    writer.addCString(v);
  });
}

function buf_to_kv(buf) {
  const map = {};
  const string_list = [];

  let index = 0;
  while (index < buf.length) {
    const next_zero = buf.indexOf(0,index);
    if (next_zero == -1) {
      break;
    }
    const s = buf.toString('ascii',index,next_zero);
    string_list.push(s);

    index = next_zero + 1;
  }
  for (let i = 0 ; i < string_list.length ; i += 2) {
    const k = string_list[i];
    const v = string_list[i+1];
    if (k) {
      map[k] = v;
    }
  }

  return map;
}
