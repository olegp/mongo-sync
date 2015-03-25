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
  var host = server[0], port = server[1] || 27017;
  this._server = new mongodb.Server(host, port, objects.merge(options, {
    poolSize:2,
    socketOptions:{
      noDelay:true
    },
    auto_reconnect:true,
    disableDriverBSONSizeCheck:true
  }));
}

exports.Server = Server;

Server.prototype.db = function(name) {
  var result = new DB(this, new mongodb.Db(name, this._server, {w:1}));
  result.open();
  return result;
};

Server.prototype.connect = function(uri) {
  return new DB(this, sync('MongoClient', 'connect').call(mongodb, uri));
};

Server.prototype.close = function() {
  this._server.close();
};

function DB(server, db) {
  this._server = server;
  this._db = db;
}

exports.DB = DB;

DB.prototype.getMongo = function() {
  return this._server;
};

DB.prototype.getName = function() {
  return this._db.databaseName;
};

DB.prototype.getCollection = function(name) {
  return new Collection(this, name);
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

DB.prototype.runCommand = sync('_db', 'command');
DB.prototype.auth = sync('_db', 'authenticate');

function Collection(db, name) {
  if (arguments.length < 2) {
    throw new Error('too few arguments');
  }
  this._db = db;
  this._collection = db._db.collection(name);
}

exports.Collection = Collection;

Collection.prototype.getDB = function() {
  return this._db;
};

[
  'aggregate',
  'count',
  'distinct',
  'drop',
  'dropIndex',
  'ensureIndex',
  'findOne',
  'insert',
  'mapReduce',
  'save'
].forEach(function(name) {
  Collection.prototype[name] = sync('_collection', name);
});

Collection.prototype.remove = (function() {
  var remove = sync('_collection', 'remove');
  return function(query, single) {
    return remove.call(this, query, {single:single});
  };
})();

Collection.prototype.findAndModify = (function() {
  var findAndModify = sync('_collection', 'findAndModify');
  return function(document) {
    return findAndModify.call(this, document.query, document.sort, document.update, document);
  };
})();

Collection.prototype.getIndexes = sync('_collection', 'indexes');

Collection.prototype.update = (function() {
  var update = sync('_collection', 'update');
  return function() {
    if (arguments.length < 2) {
      throw new Error('too few arguments');
    }
    return update.apply(this, arguments);
  };
})();

Collection.prototype.find = function() {
  return new Cursor(this._collection.find.apply(this._collection, arguments));
};

function Cursor(cursor) {
  this._cursor = cursor;
  this._mapper = function(value) {
    return value;
  };
}

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