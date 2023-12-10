'use strict';

exports.getWriter = getWriter;

function getWriter(type_id) {
  switch (type_id) {
    case 1082: // date
      return date_writer;
    default:
      return default_writer;
  }
}

function default_writer(val,writer) {
  if (val === null) {
    writer.addInt32(-1);
  } else {
    writer.addPString(val.toString());
  }
}

function date_writer(val,writer) {
  if (val === null) {
    writer.addInt32(-1);
  } else {
    writer.addPString(val.toISOString().slice(0,10));
  }
}