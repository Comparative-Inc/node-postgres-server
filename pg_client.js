'use strict';

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
  this._server.emit('socket_connect',this);
};
PgClient.prototype._onSocketClose = function() {
  this._server.emit('socket_close',this);
};
PgClient.prototype._onSocketEnd = function() {
  this._server.emit('socket_end',this);
};
PgClient.prototype._onSocketError = function(err) {
  this._server.emit('socket_error',this,err);
};
PgClient.prototype._onSocketData = function(buf) {
  this._reader.addChunk(buf);
  this._pumpPackets();
};
PgClient.prototype._onSocketTimeout = function() {
  this._server.emit('socket_timeout',this);
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
  const reader = new BufferReader(buffer);
  switch(header) {
    case 'p': {
      const password = reader.readCString();
      this._server.emit('client_password',this,password);
      break;
    }
    case 'Q': {
      const query = reader.readCString();
      this._server.emit('client_query',this,query);
      break;
    }
    case 'X': {
      this._server.emit('client_terminate',this);
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
      this._server.emit('client_parse',this,statement);
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

        this._server.emit('client_bind',this,statement);
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
          this._server.emit('client_describe_statement',this,statement);
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
          this._server.emit('client_describe_portal',this,statement);
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
        this._server.emit('client_execute',this,statement,max_rows);
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
      this._server.emit('client_flush',this);
      break;
    case 'S':
      this._server.emit('client_sync',this)
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
PgClient.prototype.sendCommandComplete = function(tag,oid,rows) {
  let s = tag;
  if (tag == 'INSERT') {
    s += " " + (oid || "0");
  }
  s += " rows";
  this._buffer_writer.addCString(s);
  const buf = this._buffer_writer.flush('C');
  this._socket.write(buf);
}

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
