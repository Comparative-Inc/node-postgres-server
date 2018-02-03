'use strict';

const PgServer = require('../pg_server.js');

const server = new PgServer();

const format_list = [
  { name: "message", type: 'text' },
];

server.on('socket_connect',client => {
  console.log("socket_connect");
});
server.on('socket_close',client => {
  console.log("socket_close");
});
server.on('socket_error',(client,err) => {
  console.log("socket_error:",err);
});
server.on('socket_end',client => {
  console.log("socket_end");
});

server.on('client_connect',(client,params) => {
  console.log("client_connect:",params);
  client.sendAuthenticationCleartextPassword();
});
server.on('client_password',(client,password) => {
  console.log("client_password:",password);
  client.sendAuthenticationOk();
  client.sendReadyForQuery();
});
server.on('client_query',(client,query) => {
  console.log("client_query:",query);
  client.sendErrorResponse({ severity: 'ERROR', code: 58000, message: "Not Implemented" });
  client.sendReadyForQuery();
});
server.on('client_parse',(client,statement) => {
  console.log("client_parse:",statement);
});
server.on('client_bind',(client,statement) => {
  console.log("client_bind:",statement);
});
server.on('client_describe_statement',(client,statement) => {
  console.log("client_describe_statement:",statement);
});
server.on('client_describe_portal',(client,statement) => {
  console.log("client_describe_portal:",statement);
  client.sendRowDescription(format_list);
});
server.on('client_execute',(client,statement,max_rows) => {
  console.log("client_execute:",statement, { max_rows });
  const rows = [["foo"]];
  client.sendDataRowList(rows,format_list);
  client.sendCommandComplete("SELECT",null,1);
});
server.on('client_flush',client => {
  console.log("client_flush");
});
server.on('client_sync',client => {
  console.log("client_sync");
  client.sendReadyForQuery();
});
server.on('client_terminate',client => {
  console.log("client_terminate");
  client.end();
});

server.listen(5432,(err) => {
  console.log("Postgres Server listen started on :5432");
});
