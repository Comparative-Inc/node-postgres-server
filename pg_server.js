'use strict';

const net = require('net');
const EventEmitter = require('events').EventEmitter;
const util = require('util');
const PgClient = require('./pg_client.js');

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

  this.emit('server_connection',client);
};
PgServer.prototype._onServerError = function(err) {
  this.emit('server_error',err);
};
PgServer.prototype._onServerClose = function() {
  console.error("_onServerClose:");
  this.emit('server_close');
};
PgServer.prototype._onServerListening = function() {
  this.emit('server_listening');
};
