var redis = require('./redis'),
    sys = require('sys'),
    url = require('url'),
    couchdb = require('node-couchdb'),
    http = require('http');

// var db = couchdb.createClient(5984, 'localhost').db('redistest');
// 
// var client = redis.createClient();

var dprint = function (d, string) {
  if (d) {sys.puts(string)}
} 

// function loaddoc (doc, callback) {
//   client.set(doc._id, JSON.stringify(doc), function (error, value) {
//     callback(error, value);
//   });
// }
// 
// client.flushdb(function () {
//   var i = 0;
//   var loader = function () {
//     if (i < 100) {
//       i += 1;
//       loaddoc({_id:i.toString(), k:"v"}, loader);
//     } else {
//       client.dbsize(function (error, l)  {sys.puts(l)} );
//     }
//   }
//   loader()
// })
// 
// db.changes({include_docs:true}, function (error, changes) {
//   changes.results.forEach(function (f) {
//     client.set(f.doc._id, f.doc, function () {
//       client.dbsize(function (error, l)  {sys.puts(l)} );
//     });
//   })
//   sys.puts(sys.inspect(changes));
// });

function pull (options, callback) {
  var redisClient = redis.createClient(options.redisport, options.redishost);
  var couchClient = couchdb.createClient(options.couchport, options.couchhost).db(options.couchdb)
  
  dprint(options.debug, 'Getting current _changes feed.')
  
  var finish = function (changes) {
    redisClient.get('__replication_info', function (error, doc) {
      if (error) {doc = {remotes:{}}}
      else {doc = JSON.parse(doc)}
      if (!doc.remotes[options.couchuri]) {
        doc.remotes[options.couchuri] = {}
      }
      doc.remotes[options.couchuri].seq = changes.last_seq; 
      redisClient.set('__replication_info', JSON.stringify(doc), function (error) {
        if (error) {throw new Error(error)};
        callback(null, doc, changes)
      })
    })
  }
  
  redisClient.get('__replication_info', function (error, repinfo) {
    if (error) {repinfo = {remotes:{}}; repinfo.remotes[options.couchuri] = {seq:0}}
    else {repinfo = JSON.parse(repinfo)}
    couchClient.changes({include_docs:true,seq:repinfo.remotes[options.couchuri].seq}, function (error, changes) {
      if (error) {throw new Error(error)}

      var i = 0;
      changes.results.forEach(function (result) {
        var doc = result.doc;
        redisClient.get(doc._id, function (error, redisDoc) {
          if (error) {
            redisClient.set(doc._id, JSON.stringify(doc), function (error) {
              if (error) {throw new Error(error)};
              i += 1; if (i === changes.results.length) { finish(changes); };
            });
          } else {
            if (error) {throw new Error(error)};
            doc = options.onConflict(JSON.parse(redisDoc), doc, true);
            redisClient.set(doc._id, JSON.stringify(doc), function (error) {
              if (error) {throw new Error(error)};
              i += 1; if (i === changes.results.length) { finish(changes); };
            })
          }

        })
      })
    })
  })
  
  
  
}

function createRedplicator (options) {
  
  var couchuri = url.parse(options.couchuri);
  options.couchport = couchuri.port ? couchuri.port : {'http:':80,'https:':443}[couchuri.scheme];
  options.couchhost = couchuri.hostname;
  options.couchdb = couchuri.pathname.replace('/','');
  
  options.couchpush = options.couchpush ? options.couchpush : 'interval';
  if (options.couchpush === 'interval') {
    options.couchpushInterval = options.couchpushInterval ? options.couchpushInterval : 10 * 1000;
  }
  
  options.onConflict = options.onConflict ? options.onConflict : function (redisDoc, couchDoc, pull) {return couchDoc} 
   
  var redisClient = redis.createClient(options.redisport, options.redishost);
  var couchClient = couchdb.createClient(options.couchport, options.couchhost).db(options.couchdb)
    
  if (options.couchpull !== 'disabled') {
    pull(options, function () {dprint(options, 'Pull finished')})
  }
  
}

createRedplicator({couchuri:'http://localhost:5984/redistest', debug:true});
