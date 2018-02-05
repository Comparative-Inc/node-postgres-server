'use strict';

const PgServer = require('../pg_server.js');

const server = new PgServer();

const format_list = [
  { name: "message", type: 'text' },
];

server.on('listening',() => {
  console.log("Server listening")
});
server.on('new_client',_handleNewClient);
server.on('close',() => {
  console.log("Server close");
});
server.on('error',err => {
  console.error("Server error:",err);
});

server.listen(5432,(err) => {
  console.log("Postgres Server listen started on :5432");
});

function _handleNewClient(client) {
  client.on('connect',params => {
    console.log("client_connect:",params);
    client.sendAuthenticationCleartextPassword();
  });
  client.on('password',password => {
    console.log("client_password:",password);
    client.sendAuthenticationOk();
    client.sendReadyForQuery();
  });
  client.on('query',query => {
    console.log("client_query:",query);
    client.sendErrorResponse({ severity: 'ERROR', code: 58000, message: "Not Implemented" });
    client.sendReadyForQuery();
  });
  client.on('parse',statement => {
    console.log("client_parse:",statement);
  });
  client.on('bind',statement => {
    console.log("client_bind:",statement);
  });
  client.on('describe_statement',statement => {
    console.log("client_describe_statement:",statement);
  });
  client.on('describe_portal',statement => {
    console.log("client_describe_portal:",statement);
    client.sendRowDescription(format_list);
  });
  client.on('execute',(statement,max_rows) => {
    console.log("client_execute:",statement, { max_rows });
    const rows = [["foo"]];
    client.sendDataRowList(rows,format_list);
    client.sendCommandComplete("SELECT",null,1);
  });
  client.on('flush',() => {
    console.log("client_flush");
  });
  client.on('sync',() => {
    console.log("client_sync");
    client.sendReadyForQuery();
  });
  client.on('terminate',() => {
    console.log("client_terminate");
    client.end();
  });
  client.on('socket_connect',client => {
    console.log("socket_connect");
  });
  client.on('socket_close',client => {
    console.log("socket_close");
  });
  client.on('socket_error',(client,err) => {
    console.log("socket_error:",err);
  });
  client.on('socket_end',client => {
    console.log("socket_end");
  });
}
