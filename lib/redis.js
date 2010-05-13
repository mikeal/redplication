var net = require('net'),
    sys = require('sys');

var end = '\r\n';

function createNetClient (port, host) {
  var client = net.createConnection(port, host);
  client.addListener("connect", function () {
    if (client.onConnect) {
      client.onConnect();
    }
  })
  return client;
}

function statusReply (client, callback) {
  var errorListener = function (error) {
    error = error ? error : 'Unexpectedly hung up'; 
    callback(error);
  }
  client.addListener("error", errorListener);
  client.addListener("end", errorListener);
  var teardown = function () {
    client.removeListener("error", errorListener);
    client.removeListener("end", errorListener);
    client.removeListener("data", dataListener);
  }
  var buffer = '';
  var dataListener = function (chunk) {
    buffer += chunk.toString();
    if (buffer.indexOf(end) !== -1) {
      status = buffer.slice(1, buffer.indexOf(end));
      teardown();
      callback(null, status);
    }
  }
  client.addListener("data", dataListener);
}

function intReply (client, callback) {
  statusReply(client, function (error, status) {
    if (!error) { callback(null, parseInt(status)) }
    else {callback(error, status)};
  })
}

function bulkReply (client, callback) {
  var errorListener = function (error) {
    error = error ? error : 'Unexpectedly hung up'; 
    callback(error);
  }
  client.addListener("error", errorListener);
  client.addListener("end", errorListener);
  var teardown = function () {
    client.removeListener("error", errorListener);
    client.removeListener("end", errorListener);
    client.removeListener("data", dataListener);
  }
  var buffer = '';
  var size;
  var dataListener = function (chunk) {
    buffer += chunk.toString();
    if (size) {
      if (buffer.length - 2 == size) {
        teardown();
        callback(null, buffer.slice(0, buffer.length - 2));
      }
    } else {
      if (buffer.indexOf(end) != -1) {
        size = parseInt(buffer.slice(1, buffer.indexOf(end)));
        if (size === -1) {
          teardown();
          callback("Key does not exist.")
        }
        buffer = buffer.slice(buffer.indexOf(end) + end.length);
        if (buffer.length - 2 == size) {
          teardown();
          callback(null, buffer.slice(0, buffer.length - 2));
        }
      }
    }
  }
  client.addListener("data", dataListener);
}

function multiBulkReply (client, callback) {
  var buffer = '';
  var errorListener = function (error) {
    error = error ? error : 'Unexpectedly hung up'; 
    callback(error);
  }
  if (!client) {throw new Error("No client")}
  client.addListener("error", errorListener);
  client.addListener("end", errorListener);
  var teardown = function () {
    client.removeListener("error", errorListener);
    client.removeListener("end", errorListener);
    client.removeListener("data", dataListener);
  }
  
  var size;
  var responses = 0;
  var expected;
  
  var dataListener = function (chunk) {
    if (buffer === undefined) {
      buffer = '';
    }
    if (chunk) {
      buffer += chunk.toString();
    }
    if (buffer === undefined) {throw new Error('what')}
    if (!expected) { 
      if (buffer.indexOf(end) !== -1) {
        expected = parseInt(buffer.slice(1, buffer.indexOf(end)));
        buffer = buffer.slice(buffer.indexOf(end) + end.length);
      } else { return; }
    } 
    
    if (size || size === 0) {
      if (buffer.length - 2 >= size) {
        client.emit('reply', buffer.slice(0, size));
        responses += 1;
        if (responses === expected) {
          teardown();
          callback();
        } else {
          buffer = buffer.slice(size + end.length);
          if (buffer === undefined) {
            throw new Error('what2!')
          }
          size = null;
          dataListener();
        }
      }
    } else if (buffer.indexOf(end) !== -1) {
      size = parseInt(buffer.slice(1, buffer.indexOf(end)));
      buffer = buffer.slice(buffer.indexOf(end) + end.length);
      if (buffer === undefined) {
        throw new Error('what3!')
      }
      dataListener();
    }
  }
  client.addListener("data", dataListener);
}

function Client (port, host) {
  this.port = port ? port : 6379;
  this.host = host ? host : 'localhost';
  this.clients = [];
}
Client.prototype.getClient = function (callback) {
  var client = createNetClient(this.port, this.host);
  client.busy = true;
  this.clients.push(client);
  client.onConnect = function () {callback(client)}
  return client
}
Client.prototype.all = function (keyValueCallback, endCallback) {
  var self = this;
  self.keys('*', function (error, keys) {
    if (error && endCallback) {endCallback(error)}
    var client = self.mget(keys, function (error) {
      if (endCallback) {endCallback(error)};
      client.destroy();
    })
    var i = 0;
    // client.addListener('data', function (chunk){sys.puts(chunk.toString())})
    client.addListener("reply", function (value) {
      keyValueCallback(keys[i], value);
      i += 1;
    })
  })
}
Client.prototype.keys = function (pattern, callback) {
  var client = this.getClient(function (client) {
    bulkReply(client, function (error, keysString) {
      client.busy = false;
      if (error) {client.destroy(); return callback(error)};
      callback(null, keysString.split(' '));
      client.destroy();
    })
    client.write('KEYS '+pattern+end);
  });
  return client;
}
Client.prototype.mget = function (keys, endCallback) {
  var client = this.getClient(function (client) {
    multiBulkReply(client, function (error) {
      endCallback(error);
      client.destroy();
    })
    client.write('MGET '+keys.join(' ')+end);
  });
  return client;
}
Client.prototype.get = function (key, callback) {
  var client = this.getClient(function (client) {
    bulkReply(client, function (error, value) {
      client.busy = false;
      if (error) {client.destroy(); return callback(error)};
      callback(null, value);
      client.destroy();
    })
    client.write('GET '+key+end);
  });
  return client;
}

Client.prototype.flushdb = function (callback) {
  var client = this.getClient(function (client) {
    statusReply(client, function (error, status) {
      if (error){
        client.destroy();
        return callback(error)
      }
      if (status !== 'OK') {
        callback('Status was not OK, Status was '+status);
      } else {
        callback(null)
      }
      client.destroy();
    })
    client.write('FLUSHDB'+end)
  })
}
Client.prototype.set = function (key, value, callback) {
  var client = this.getClient(function (client) {
    statusReply(client, function (error, status) {
      if (error){
        client.destroy();
        return callback(error)
      }
      if (status !== 'OK') {
        callback('Status was not OK, Status was '+status);
      } else {
        callback(null)
      }
      client.destroy()
    })
    client.write('SET '+key+' '+value.length+end+value+end);
  })
}
Client.prototype.dbsize = function (callback) {
  var client = this.getClient(function (client) {
    intReply(client, function (error, status){client.destroy(); callback(error, status);});
    client.write('DBSIZE'+end);
  })
}

exports.createClient = function (port, host) {return new Client(port, host)};
exports.Client = Client;

// var c = new Client();
// c.all(function (k, v) {
//   sys.puts(k + ' ' + v)
// }, function (error) {if (error) {throw new Error(error)}})

