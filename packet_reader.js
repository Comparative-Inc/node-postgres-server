'use strict';

// Originally from github.com/brianc/node-packet-reader
// modified for server functionality

module.exports = Reader

function Reader(options) {
  options = options || {}
  this.offset = 0
  this.lastChunk = false
  this.chunk = null
  this.chunkLength = 0
  this.headerSize = options.headerSize || 0
  this.lengthPadding = options.lengthPadding || 0
  if (this.headerSize > 1) {
    throw new Error('pre-length header of more than 1 byte length not currently supported');
  }
}

Reader.prototype.addChunk = function(chunk) {
  if (!this.chunk || this.offset === this.chunkLength) {
    this.chunk = chunk
    this.chunkLength = chunk.length
    this.offset = 0
    return
  }

  const newChunkLength = chunk.length
  const newLength = this.chunkLength + newChunkLength

  if (newLength > this.chunk.length) {
    let newBufferLength = this.chunk.length * 2
    while (newLength >= newBufferLength) {
      newBufferLength *= 2
    }
    var newBuffer = new Buffer(newBufferLength)
    this.chunk.copy(newBuffer)
    this.chunk = newBuffer
  }
  chunk.copy(this.chunk, this.chunkLength)
  this.chunkLength = newLength
}

Reader.prototype.read = function() {
  if (this.chunkLength < (this.headerSize + 4 + this.offset)) {
    return false
  }

  let header;
  if (this.headerSize) {
    header = String.fromCharCode(this.chunk[this.offset]);
  }

  const length = this.chunk.readUInt32BE(this.offset + this.headerSize) + this.lengthPadding;

  const remaining = this.chunkLength - (this.offset + this.headerSize)
  if (length > remaining) {
    return false
  }

  const data_length = length - 4;
  const packet_start = this.offset + this.headerSize + 4 + this.lengthPadding;
  const buffer = this.chunk.slice(packet_start,packet_start + data_length);
  this.offset = packet_start + data_length;
  return { header, buffer };
}
