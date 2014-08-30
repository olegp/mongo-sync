var Fiber = global.Fiber || require("fibers");
var mongodb = require("mongodb");

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

function Server(server) {
  server = (server || '127.0.0.1').split(':');
  var host = server[0], port = server[1] || 27017;
  this._server = new mongodb.Server(host, port, {
    poolSize:2,
    socketOptions:{
      noDelay:true
    },
    auto_reconnect:true,
    disableDriverBSONSizeCheck:true
  });
}

exports.Server = Server;

Server.prototype.db = function(name) {
  return new DB(this, name);
};

Server.prototype.connect = function(uri) {
  return new DB(this, null, uri);
};

Server.prototype.close = function() {
  this._server.close();
};

function DB(server, name, uri) {
  this._server = server;
  this._name = name;
  // TODO: reimplement this the proper way
  if (uri) {
    this._db = mongodb.Db;
    this._db = sync('_db', 'connect').call(this, uri);
  } else {
    this._db = new mongodb.Db(name, server._server, {
      w: 1
    });
    sync('_db', 'open').call(this);
  }
}

exports.DB = DB;

DB.prototype.getMongo = function() {
  return this._server;
};

DB.prototype.getName = function() {
  return this._name;
};

DB.prototype.getCollection = function(name) {
  return new Collection(this, name);
};

DB.prototype.collectionNames = (function() {
  var collectionNames = sync('_db', 'collectionNames');
  return function() {
    return collectionNames.apply(this, arguments).map(function(collection) {
      return collection.name.substr(collection.name.indexOf('.') + 1);
    });
  };
})();

[
  'addUser',
  'dropDatabase',
  'eval',
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
  'count',
  'distinct',
  'drop',
  'dropIndex',
  'ensureIndex',
  'findOne',
  'mapReduce',
  'insert',
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

Collection.prototype.find = (function() {
  var find = sync('_collection', 'find');
  return function() {
    return new Cursor(find.apply(this, arguments));
  };
})();

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
  var fn = sync('_cursor', name);
  Cursor.prototype[name] = function() {
    fn.apply(this, arguments);
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
