'use strict';

const PgServer = require('../pg_server.js');
const lcdb = require('lightning-db');

const server = new PgServer();

const resultMatrix = {format: [], rows: []};

function _lcdbQuery(query) {
  const result = lcdb.query(query);
  const rowMatrix = [];
  const formatList = [];
  result.columns.forEach((col) => {
    formatList.push({
      name: col.name,
      type: typeof col.data[0],
    });
  });
  for (let i = 0; i < result.rows; i++) {
    const row = [];
    result.columns.forEach((col) => {
      row.push(col.data[i]);
    });
    rowMatrix.push(row);
  }
  return {format: formatList, rows: rowMatrix};
}

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

const inputArgs = require('minimist')(process.argv.slice(2));

const port = inputArgs.port || inputArgs.p || 5432;
server.listen(port,(err) => {
  console.log("Postgres Server listen started on :", port);
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
    const qresult = _lcdbQuery(statement.query);
    resultMatrix.format = qresult.format;
    resultMatrix.rows = qresult.rows;
    console.log("query result:\n", qresult);
  });
  client.on('describe_statement',statement => {
    console.log("client_describe_statement:",statement);
  });
  client.on('describe_portal',statement => {
    console.log("client_describe_portal:",statement);
    client.sendRowDescription(resultMatrix.format);
  });
  client.on('execute', (statement,max_rows) => {
    console.log("client_execute:\n",statement, "\nmax rows: ", max_rows, "\nresult:\n", resultMatrix);
    client.sendDataRowList(resultMatrix.rows,resultMatrix.format);
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
