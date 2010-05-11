var redis = require('./redis'),
    sys = require('sys'),
    http = require('http');


var client = redis.createClient();

function loaddoc (doc, callback) {
  client.set(doc._id, JSON.stringify(doc), function (error, value) {
    callback(error, value);
  });
}

client.flushdb(function () {
  var i = 0;
  var loader = function () {
    if (i < 100) {
      i += 1;
      loaddoc({_id:i.toString(), k:"v"}, loader);
    } else {
      client.dbsize(function (error, l)  {sys.puts(l)} );
      client.all(function (k,v) {
        sys.puts(k+' '+v)
      } );
    }
  }
  loader()
})



