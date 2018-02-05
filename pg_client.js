'use strict';

const util = require('util');
const EventEmitter = require('events').EventEmitter;
const PacketReader = require('./packet_reader.js');
const BufferWriter = require('./buffer_writer.js');
const BufferReader = require('./buffer_reader.js');
const { SQLSTATE_ERROR_CODE_MAP, ERROR_FIELD_ID_MAP, PG_TYPE_MAP } = require('./constants.js');
const pg_type_writer = require('./pg_type_writer.js');

module.exports = PgClient;

function PgClient(options) {
  if (this instanceof PgClient) {
    this._socket = options.socket;
    this._server = options.server;
    this._reader = new PacketReader();
    this._buffer_writer = new BufferWriter();
    this._handshake_complete = false;
    this._prepared_statement_map = {};
    this._portal_map = {};

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
util.inherits(PgClient,EventEmitter);

PgClient.prototype.getStatementByName = function(name) {
  if (!name) {
    name = "";
  }
  return this._prepared_statement_map[name];
}
PgClient.prototype.getStatementByPortal = function(portal) {
  if (!portal) {
    portal = "";
  }
  return this._portal_map[portal];
}
PgClient.prototype.end = function() {
  this._socket.end();
}

PgClient.prototype._onSocketConnect = function() {
  this.emit('socket_connect');
};
PgClient.prototype._onSocketClose = function() {
  this.emit('socket_close');
};
PgClient.prototype._onSocketEnd = function() {
  this.emit('socket_end');
};
PgClient.prototype._onSocketError = function(err) {
  this.emit('socket_error',err);
};
PgClient.prototype._onSocketData = function(buf) {
  this._reader.addChunk(buf);
  this._pumpPackets();
};
PgClient.prototype._onSocketTimeout = function() {
  this.emit('socket_timeout');
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
        this.emit('connect',params);
      } else {
        console.error("PgClient: handshake unknown client version");
        this.emit('protocol_error',buffer);
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
  const reader = new BufferReader(buffer);
  switch(header) {
    case 'p': {
      const password = reader.readCString();
      this.emit('password',password);
      break;
    }
    case 'Q': {
      const query = reader.readCString();
      this.emit('query',query);
      break;
    }
    case 'X': {
      this.emit('terminate');
      break;
    }
    case 'P': {
      const name = reader.readCString();
      const query = reader.readCString();
      const parameter_count = reader.readInt16();
      const parameter_data_type_list = reader.readInt32List(parameter_count);

      const statement = {
        name,
        query,
        parameter_data_type_list,
      };

      this._prepared_statement_map[name] = statement;
      this.emit('parse',statement);
      this.sendParseComplete();
      break;
    }
    case 'B': {
      const portal = reader.readCString();
      const name = reader.readCString();
      const statement = this.getStatementByName(name);

      if (statement) {
        statement.portal = portal;
        this._portal_map[portal] = statement;

        const parameter_format_count = reader.readInt16();
        statement.parameter_format_code_list = reader.readInt16List(parameter_format_count);
        const parameter_value_count = reader.readInt16();
        statement.parameter_value_list = reader.readPStringList(parameter_value_count);
        const result_format_count = reader.readInt16();
        statement.result_format_list = reader.readInt16List(result_format_count);

        this.emit('bind',statement);
        this.sendBindComplete();
      } else {
        this.sendErrorResponse({
          severity: 'ERROR',
          code: SQLSTATE_ERROR_CODE_MAP.invalid_sql_statement_name,
          message: "Unknown prepared statement",
        });
      }
      break;
    }
    case 'D': {
      const type = reader.readChar();
      const name = reader.readCString();
      if (type == 'S') {
        const statement = this.getStatementByName(name);
        if (statement) {
          this.emit('describe_statement',statement);
        } else {
          this.sendErrorResponse({
            severity: 'ERROR',
            code: SQLSTATE_ERROR_CODE_MAP.invalid_sql_statement_name,
            message: "Unknown prepared statement",
          });
        }
      } else if (type == 'P') {
        const statement = this.getStatementByPortal(name);
        if (statement) {
          this.emit('describe_portal',statement);
        } else {
          this.sendErrorResponse({
            severity: 'ERROR',
            code: SQLSTATE_ERROR_CODE_MAP.invalid_sql_statement_name,
            message: "Unknown portal",
          });
        }
      } else {
        this.sendErrorResponse({
          severity: 'ERROR',
          code: SQLSTATE_ERROR_CODE_MAP.protocol_violation,
          message: "Unknown describe command",
        });
      }
      break;
    }
    case 'E': {
      const portal = reader.readCString();
      const max_rows = reader.readInt32();
      const statement = this.getStatementByPortal(portal);
      if (statement) {
        this.emit('execute',statement,max_rows);
      } else {
        this.sendErrorResponse({
          severity: 'ERROR',
          code: SQLSTATE_ERROR_CODE_MAP.invalid_sql_statement_name,
          message: "Unknown portal",
        });
      }
      break;
    }
    case 'H':
      this.emit('flush');
      break;
    case 'S':
      this.emit('sync')
      break;
    default: {
      console.error("PgClient: unhandled packet type:",header);
      this.emit('error',new Error("Unhanded packet type: " + header));
      break;
    }
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
PgClient.prototype.sendParseComplete = function() {
  const buf = this._buffer_writer.flush('1');
  this._socket.write(buf);
};
PgClient.prototype.sendParseComplete = function() {
  const buf = this._buffer_writer.flush('1');
  this._socket.write(buf);
};
PgClient.prototype.sendBindComplete = function() {
  const buf = this._buffer_writer.flush('2');
  this._socket.write(buf);
};
PgClient.prototype.sendCloseComplete = function() {
  const buf = this._buffer_writer.flush('3');
  this._socket.write(buf);
};
PgClient.prototype.sendRowDescription = function(field_list) {
  this._buffer_writer.addInt16(field_list.length);
  field_list.forEach(f => {
    const pg_type = PG_TYPE_MAP[f.type] || {};
    const type_id = f.type_id || pg_type.oid || 0;
    const type_size = f.type_size || pg_type.size || -1;

    this._buffer_writer.addCString(f.name);
    this._buffer_writer.addInt32(f.table_id || 0);
    this._buffer_writer.addInt16(f.column_id || 0);
    this._buffer_writer.addInt32(type_id);
    this._buffer_writer.addInt16(type_size);
    this._buffer_writer.addInt32(f.type_modifier || 0);
    this._buffer_writer.addInt16(f.format_code || 0);
  });

  const buf = this._buffer_writer.flush('T');
  this._socket.write(buf);
};
PgClient.prototype.sendDataRowList = function(row_list,field_list) {
  if (!Array.isArray(row_list)) {
    row_list = [row_list];
  }
  if (!Array.isArray(field_list)) {
    field_list = [field_list];
  }
  const count = field_list.length;
  const format_list = field_list.map(f => {
    const pg_type = PG_TYPE_MAP[f.type] || {};
    const type_id = f.type_id || pg_type.oid || 0;
    const writer = pg_type_writer.getWriter(type_id);

    return {
      name: f.name,
      writer,
    };
  });

  row_list.forEach(r => {
    const is_array = Array.isArray(r);

    this._buffer_writer.addInt16(count);
    format_list.forEach((f,i) => {
      const val = is_array ? r[i] : r[f.name];
      f.writer(val,this._buffer_writer);
    });

    const buf = this._buffer_writer.flush('D');
    this._socket.write(buf);
  });
};
PgClient.prototype.sendCommandComplete = function(tag,a,b) {
  let s = tag || "SELECT";
  if (a) {
    s += " " + a;
  }
  if (b) {
    s += " " + b;
  }
  this._buffer_writer.addCString(s);
  const buf = this._buffer_writer.flush('C');
  this._socket.write(buf);
};

function write_error_map(error_map,writer) {
  Object.keys(error_map).forEach(k => {
    const v = error_map[k].toString();
    const code = k.length == 1 ? k : ERROR_FIELD_ID_MAP[k];
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
