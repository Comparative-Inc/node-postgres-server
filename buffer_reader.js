'use strict';

module.exports = BufferReader;

function BufferReader(buffer) {
  if (this instanceof BufferReader) {
    this._buffer = buffer;
    this._offset = 0;
  } else {
    return new BufferReader(buffer);
  }
  return this;
}

BufferReader.prototype.readCString = function() {
  const next_zero = this._buffer.indexOf(0,this._offset);
  let ret;
  if (next_zero == this._offset) {
    ret = "";
    this._offset++;
  } else if (next_zero > this._offset) {
    ret = this._buffer.toString('utf8',this._offset,next_zero);
    this._offset = next_zero + 1;
  }
  return ret;
};
BufferReader.prototype.readChar = function() {
  const byte = this.readByte();
  let ret;
  if (byte !== undefined) {
    ret = String.fromCharCode(byte);
  }
  return ret;
};
BufferReader.prototype.readByte = function() {
  let ret;
  if (this.hasBytesAvailable(1)) {
    ret = this._buffer.readUInt8(this._offset++);
  }
  return ret;
};
BufferReader.prototype.readInt16 = function() {
  let ret;
  if (this.hasBytesAvailable(2)) {
    ret = this._buffer.readInt16BE(this._offset);
    this._offset += 2;
  }
  return ret;
};
BufferReader.prototype.readInt32 = function() {
  let ret;
  if (this.hasBytesAvailable(4)) {
    ret = this._buffer.readInt32BE(this._offset);
    this._offset += 4;
  }
  return ret;
};
BufferReader.prototype.readPString = function() {
  const len = this.readInt32();
  let ret;
  if (len == -1) {
    ret = null;
  } else if (len == 0) {
    ret = "";
  } else if (len > 0) {
    ret = this._buffer.toString('utf8',this._offset,this._offset + len)
    this._offset += len;
  }
  return ret;
};
BufferReader.prototype.readInt16List = function(count) {
  let ret = [];
  for (let i = 0 ; i < count ; i++) {
    ret.push(this.readInt16());
  }
  return ret;
};
BufferReader.prototype.readInt16List = function(count) {
  let ret = [];
  for (let i = 0 ; i < count ; i++) {
    ret.push(this.readInt16());
  }
  return ret;
};
BufferReader.prototype.readInt32List = function(count) {
  let ret = [];
  for (let i = 0 ; i < count ; i++) {
    ret.push(this.readInt32());
  }
  return ret;
};
BufferReader.prototype.readPStringList = function(count) {
  let ret = [];
  for (let i = 0 ; i < count ; i++) {
    ret.push(this.readPString());
  }
  return ret;
};

BufferReader.prototype.hasBytesAvailable = function(num) {
  return this._offset + num <= this._buffer.length;
};
