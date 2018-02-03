'use strict';

exports.getWriter = getWriter;

function getWriter(type_id) {
  return default_writer;
}

function default_writer(val,writer) {
  if (val === null) {
    writer.addInt32(-1);
  } else {
    writer.addPString(val.toString());
  }
}
