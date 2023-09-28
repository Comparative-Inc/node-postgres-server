'use strict';

const { Client, Query } = require('pg');

const inputArgs = require('minimist')(process.argv.slice(2));

const connectConfig = {
  user: inputArgs.user || inputArgs.u || 'test',
  password: inputArgs.password || inputArgs.p || 'test',
  database: inputArgs.database || inputArgs.db || 'test',
  host: inputArgs.server || inputArgs.s || 'localhost',
  port: inputArgs.port || 5432,
};

const client = new Client(connectConfig);

client.connect();

client.query(inputArgs.sql,[''], (err, res) => {
  if (err) {
    console.error(err);
  } else {
    console.log(res);
  }
  client.end();
});
