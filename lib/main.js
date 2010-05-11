var redis = require('./redis'),
    sys = require('sys'),
    couchdb = require('node-couchdb'),
    http = require('http');

var db = couchdb.createClient(5984, 'localhost').db('redistest');

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
    }
  }
  loader()
})

db.changes({include_docs:true}, function (error, changes) {
  changes.results.forEach(function (f) {
    client.set(f.doc._id, f.doc, function () {
      client.dbsize(function (error, l)  {sys.puts(l)} );
    });
  })
  sys.puts(sys.inspect(changes));
});



