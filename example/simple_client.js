'use strict';

const { Client } = require('pg');
const client = new Client({
  user: 'test',
  password: 'test',
  database: 'test',
});

client.connect();

client.query("SELECT $1::text as message",["Hello world!"],(err,res) => {
  if (err) {
    console.error(err);
  } else {
    console.log(res);
  }
  client.end();
});
