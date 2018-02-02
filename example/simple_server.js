'use strict';

const PgServer = require('../pg_server.js');

const server = new PgServer();

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
  client.sendErrorResponse({ severity: 'ERROR', code: 58000, message: 'Not Implemented' });
  client.sendReadyForQuery();
});

server.listen(5432,(err) => {
  console.log("Postgres Server listen started on :5432");
});
