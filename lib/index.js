var Mongolian = require("mongolian");
var MongolianDB = require("mongolian/lib/db");
var MongolianCollection = require("mongolian/lib/collection");
var MongolianCursor = require("mongolian/lib/cursor");

Function.prototype.sync = function(that, args) {
	args = args ? Array.prototype.slice.call(args) : [];
	var fiber = Fiber.current;
	args.push(function(error, value) {
		fiber.run(error || value);
	});
	this.apply(that, args);
	var result = yield();
	if(result instanceof Error) {
		throw new Error(result.message);
	}
	return result;
};

function Server(server) {
	this._server = new Mongolian(server);
}

Server.prototype.db = function(name) {
	return new DB(this._server.db(name));
};

function DB(server, name) {
	this._server = server;
	this._name = name;
	this._db = new MongolianDB(server._server, name);
}

DB.prototype.getMongo = function() {
	return this._server;
};

DB.prototype.getName = function() {
	return this._name;
};

DB.prototype.getCollection = function(name) {
	var collection = new Collection();
	collection._collection = this._db.collection(name);
	return collection;
};

DB.prototype.collectionNames = function() {
	return this._db.collectionNames.sync(this._db, arguments);
};

// TODO DB: 'addUser', 'auth', 'dropDatabase', 'eval', 'collectionNames',
// 'removeUser', 'runCommand'

function Collection(db, name) {
	if(db && name) {
		this._collection = new MongolianCollection(db._db, name);
	}
}

Collection.prototype.count = function() {
	return this._collection.count.sync(this._collection, arguments);
};

/*
 * sync(Collection, ['count', 'distinct', 'drop', 'dropIndex', 'ensureIndex',
 * 'find', 'findOne', 'findAndModify', 'indexes', 'mapReduce', 'remove',
 * 'runCommand', 'save', 'update']);
 * 
 * Collection.prototype.getDB = function() { return this.db; };
 * 
 * Collection.prototype.getIndexes = Collection.prototype.indexes; // TODO
 * hasNext sync(Cursor, ['close', 'count', 'next', 'size', 'toArray']);
 * 
 * Cursor.prototype.next = function() { throw new Error(); }
 */

var server = new Server('127.0.0.1');
var db = new DB(server, "test");
console.log(db.getCollection("posts").count());
