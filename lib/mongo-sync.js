var Fiber = require('fibers');
var mongodb = require('mongodb');
var objects = require('common-utils/objects');

exports.ObjectId = mongodb.ObjectID;

function sync(obj, fn) {
  return function() {
    var args = Array.prototype.slice.call(arguments);
    var result;
    var fiber;
    args.push(function(error, value) {
      result = error || value;
      if (fiber) {
        fiber.run(result);
      } else {
        fiber = true;
      }
    });
    var o = this[obj];
    o[fn].apply(o, args);
    if (!fiber) {
      fiber = Fiber.current;
      Fiber.yield();
    }
    if (result instanceof Error) {
      throw new Error(result.stack + '\nFollowed by:');
    }
    return result;
  };
}

function Server(server, options) {
  server = (server || '127.0.0.1').split(':');
  this._host = server[0]
  this._port = server[1] || 27017;
  this._server = new mongodb.Server(this._host, this._port, objects.merge(options, {
    poolSize:2,
    socketOptions:{
      noDelay:true
    },
    auto_reconnect:true,
    disableDriverBSONSizeCheck:true
  }));
  
}

function Client(server, options) {
  this._server = server
  this._client = new mongodb.MongoClient(server, options)
}

function DB(client, name) {
  this._server = client._server;
  this._client = client;
  this._db = client.db(name);
}

function Collection(db, name) {
  if (arguments.length < 2) {
    throw new Error('too few arguments');
  }
  this._db = db;
  this._collection = db._db.collection(name);
}

function Cursor(cursor) {
  this._cursor = cursor;
  this._mapper = function(value) {
    return value;
  };
}

exports.Server = Server;
exports.Client = Client;
exports.DB = DB;
exports.Collection = Collection;

Server.prototype.close = function() {
  this._client.close();
};

Client.prototype.connect = function(uri) {
  var client = sync('_client', 'connect').call(this, uri);
  var dbname = uri.split('/').slice(-1)[0]
  if (dbname === undefined) {
    throw new Error('Database name is missing')
  }
  this._db = new DB(client, dbname);
  return this._db;
};

DB.prototype.getMongo = function() {
  return this._server;
};

DB.prototype.getName = function() {
  return this._db.databaseName;
};

DB.prototype.getCollection = function(name) {
  return new Collection(this, name);
};

DB.prototype.runCommand = sync('_db', 'command');
// DB.prototype.auth = sync('_db', 'authenticate');

// Just to be backward compatible
DB.prototype.getCollection = function() {
  return this.listCollections();
}

DB.prototype.listCollections = function() {
  this._res = this._db.listCollections();
  cols = sync('_res', 'toArray').call(this);
  return cols.map(function(entry) { 
    return entry.name;
  });
};

[
  'addUser',
  'dropDatabase',
  'eval',
  'open',
  'removeUser'
].forEach(function(name) {
  DB.prototype[name] = sync('_db', name);
});

Collection.prototype.getDB = function() {
  return this._db;
};

// Keep backward compatibility
Collection.prototype.insert = function(doc) {
  if (Array.isArray(doc)) {
    return this.insertMany(doc);
  } else {
    return this.insertOne(doc);
  }
};

Collection.prototype.insertMany = (function() {
  var insert = sync('_collection', 'insertMany');
  return function(query, docs) {
    return insert.call(this, query, docs);
  };
})();

Collection.prototype.insertOne = (function() {
  var insert = sync('_collection', 'insertOne');
  return function(query, single) {
    return insert.call(this, query, {single:single});
  };
})();

// Keep backward compatibility
Collection.prototype.remove = function(doc, flag) {
  // if flag justOne is set
  if (flag) {
    return this.removeOne(doc);
  }
  // If nothing is set
  else {
    return this.removeMany(doc);
  }
};

Collection.prototype.removeMany = (function() {
  var remove = sync('_collection', 'removeMany');
  return function(query, docs) {
    return remove.call(this, query, docs);
  };
})();

Collection.prototype.removeOne = (function() {
  var remove = sync('_collection', 'removeOne');
  return function(query, single) {
    return remove.call(this, query, {single:single});
  };
})();

// Keep backward compatibility
Collection.prototype.update = function(query, doc) {
  if (Array.isArray(doc)) {
    return this.updateMany(query, doc);
  } else {
    return this.updateOne(query, doc);
  }
};

Collection.prototype.updateOne = (function() {
  var update = sync('_collection', 'updateOne');
  return function() {
    if (arguments.length < 2) {
      throw new Error('too few arguments');
    }
    return update.apply(this, arguments);
  };
})();

Collection.prototype.updateMany = (function() {
  var update = sync('_collection', 'updateMany');
  return function() {
    if (arguments.length < 2) {
      throw new Error('too few arguments');
    }
    return update.apply(this, arguments);
  };
})();

Collection.prototype.findAndModify = (function() {
  var findAndModify = sync('_collection', 'findAndModify');
  return function(document) {
    return findAndModify.call(this, document.query, document.sort, document.update, document);
  };
})();

Collection.prototype.save = function() {
  var save = sync('_collection', 'save');
  return function(doc) {
    console.log(doc);
    return save.call(this, doc);
  };
};

Collection.prototype.find = function() {
  return new Cursor(this._collection.find.apply(this._collection, arguments));
};

Collection.prototype.getIndexes = sync('_collection', 'indexes');

[
  'aggregate',
  'count',
  'distinct',
  'drop',
  'dropIndex',
  'ensureIndex',
  'findOne',
  'mapReduce'
].forEach(function(name) {
  Collection.prototype[name] = sync('_collection', name);
});

(function() {
  var count = sync('_cursor', 'count');
  Cursor.prototype.count = function() {
    return count.call(this, false);
  };
  Cursor.prototype.size = function() {
    return count.call(this, true);
  };
})();

Cursor.prototype.forEach = function(fn) {
  this.toArray().forEach(fn);
  return this;
};

Cursor.prototype.explain = sync('_cursor', 'explain');

Cursor.prototype.map = function(mapper) {
  this._mapper = mapper;
  return this;
};

Cursor.prototype.next = (function() {
  var next = sync('_cursor', 'nextObject');
  return function() {
    return this._mapper(next.apply(this, arguments));
  };
})();

Cursor.prototype.toArray = (function() {
  var toArray = sync('_cursor', 'toArray');
  return function() {
    return toArray.apply(this, arguments).map(this._mapper);
  };
})();

Cursor.prototype.close = sync('_cursor', 'close');

[
  'limit',
  'skip',
  'sort'
].forEach(function(name) {
  Cursor.prototype[name] = function() {
    this._cursor[name].apply(this._cursor, arguments);
    return this;
  };
});

