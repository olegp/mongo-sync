var Fiber = global.Fiber || require("fibers");
var mongodb = require("mongodb");

exports.ObjectId = mongodb.ObjectId;

//TODO avoid modifying prototype
Function.prototype.sync = function(that, args) {
	args = args ? Array.prototype.slice.call(args) : [];
	var fiber = Fiber.current;
	args.push(function(error, value) {
		fiber.run(error || value);
	});
	this.apply(that, args);
	var result = Fiber.yield();
	if (result instanceof Error) {
		throw new Error(result.message);
	}
	return result;
}

var Server = exports.Server = function(server) {
  server = (server || "127.0.0.1").split(':');
  var host = server[0], port = server[1] || 27017;
  this._server = new mongodb.Server(host, port, {});
}

Server.prototype.db = function(name) {
	return new DB(this, name);
}

Server.prototype.close = function() {
	this._server.close();
}

var DB = exports.DB = function(server, name) {
	this._server = server;
	this._name = name;
	this._db = new mongodb.Db(name, server._server, { w : 1 });
  this._db.open.sync(this._db);
}

DB.prototype.getMongo = function() {
	return this._server;
}

DB.prototype.getName = function() {
	return this._name;
}

DB.prototype.getCollection = function(name) {
	var collection = new Collection();
	collection._db = this;
	collection._collection = this._db.collection(name);
	return collection;
}

DB.prototype.collectionNames = function() {
	return this._db.collectionNames.sync(this._db, arguments).map(function(collection) {
    return collection.name.substr(collection.name.indexOf('.') + 1);
  });
}

DB.prototype.addUser = function() {
	return this._db.addUser.sync(this._db, arguments);
}

DB.prototype.auth = function() {
	return this._db.auth.sync(this._db, arguments);
}

DB.prototype.dropDatabase = function() {
	return this._db.dropDatabase.sync(this._db, arguments);
}

DB.prototype.eval = function() {
	return this._db.eval.sync(this._db, arguments);
}

DB.prototype.removeUser = function() {
	return this._db.removeUser.sync(this._db, arguments);
}

DB.prototype.runCommand = function() {
	return this._db.runCommand.sync(this._db, arguments);
}

DB.prototype.getLastErrorObj = function() {
	return this._db.lastError.sync(this._db, arguments);
}

var Collection = exports.Collection = function(db, name) {
	if (db && name) {
		this._db = db;
		this._collection = new MongolianCollection(db._db, name);
	}
}

Collection.prototype.getDB = function() {
	return this._db;
}

Collection.prototype.count = function() {
	return this._collection.count.sync(this._collection, arguments);
}

Collection.prototype.distinct = function() {
	return this._collection.distinct.sync(this._collection, arguments);
}

Collection.prototype.drop = function() {
	return this._collection.drop.sync(this._collection, arguments);
}

Collection.prototype.dropIndex = function() {
	return this._collection.dropIndex.sync(this._collection, arguments);
}

Collection.prototype.ensureIndex = function() {
	return this._collection.ensureIndex.sync(this._collection, arguments);
}

Collection.prototype.findOne = function() {
	return this._collection.findOne.sync(this._collection, arguments);
}

Collection.prototype.findAndModify = function(document) {
	return this._collection.findAndModify.sync(this._collection, [document.query, document.sort, document.update, document]);
}

Collection.prototype.getIndexes = function() {
	return this._collection.indexes.sync(this._collection, arguments);
}

Collection.prototype.mapReduce = function() {
	return this._collection.mapReduce.sync(this._collection, arguments);
}

Collection.prototype.remove = function() {
	return this._collection.remove.sync(this._collection, arguments);
}

Collection.prototype.insert = function() {
	return this._collection.insert.sync(this._collection, arguments);
}

Collection.prototype.save = function() {
	return this._collection.save.sync(this._collection, arguments);
}

Collection.prototype.update = function() {
  if(arguments.length < 2) throw new Error("too few arguments");
	return this._collection.update.sync(this._collection, arguments);
}

Collection.prototype.find = function(query, fields) {
	return new Cursor(this._collection.find.apply(this._collection, arguments));
}

function Cursor(cursor) {
	this._cursor = cursor;
}

Cursor.prototype.toArray = function() {
	return this._cursor.toArray.sync(this._cursor);
}

Cursor.prototype.forEach = function(f) {
	return this.toArray().forEach(f);
}

Cursor.prototype.sort = function(sort) {
	this._cursor.sort(sort);
	return this;
}

Cursor.prototype.limit = function(limit) {
	this._cursor.limit(limit);
	return this;
}

Cursor.prototype.skip = function(skip) {
	this._cursor.skip(skip);
	return this;
}

Cursor.prototype.count = function() {
	return this._cursor.count.sync(this._cursor);
}

Cursor.prototype.size = function() {
	return this._cursor.size.sync(this._cursor);
}

Cursor.prototype.explain = function() {
	this._cursor.explain();
	return this.toArray().pop();
}

Cursor.prototype.map = function(mapper) {
	this._cursor.map(mapper);
	return this;
}

Cursor.prototype.next = function() {
	return this._cursor.nextObject.sync(this._cursor);
}
