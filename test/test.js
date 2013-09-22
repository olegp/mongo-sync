var assert = require("assert");
var test = require('test');
var Server = require("../lib/mongo-sync").Server;

var db = new Server('127.0.0.1').db("test");
db.dropDatabase();
var collection = db.getCollection("tests");

exports.testGetCollection = function() {
  assert(collection);
};

exports.testCollectionNames = function() {
  collection.insert({});
  assert.notEqual(db.collectionNames().indexOf("tests"), -1);
};

//exports.testAddUser = function() {
//}

//exports.testAuth = function() {
//}

exports.testDropDatabase = function() {
  collection.insert({});
  db.dropDatabase();
  assert.equal(db.collectionNames().indexOf("tests"), -1);
};

exports.testEval = function() {
  assert.strictEqual(db.eval("return 42"), 42);
};

//exports.testRemoveUser = function() {
//}

//exports.testRunCommand = function() {
//  assert.strictEqual(
//    db.runCommand({
//    eval:function(x) {
//      return x;
//    },
//    args:[42]
//  }).retval, 42);
//};

exports.testGetLastErrorObj = function() {
  assert.equal(db.getLastErrorObj().err, null);
};

exports.testCollectionCount = function() {
  collection.remove();
  collection.insert({});
  collection.insert({});
  assert(collection.count(), 2);
};

exports.testDistinct = function() {
  collection.remove();
  collection.insert({name:"John"});
  collection.insert({name:"John"});
  assert.equal(collection.distinct("name", {})[0], "John");
};

exports.testDrop = function() {
  collection.insert({});
  collection.drop();
  assert.equal(db.collectionNames().indexOf("tests"), -1);
};

exports.testInsert = function() {
  collection.remove();
  collection.insert({test:"test"});
  assert.equal(collection.find({}).next().test, "test");
};

exports.testSave = function() {
  collection.remove();
  var test = collection.save({test:"test"});
  assert.equal(collection.count(), 1);
  test.test = "test2";
  collection.save(test);
  assert.equal(collection.count(), 1);
  assert.equal(collection.find({}).next().test, "test2");
};

exports.testUpdate = function() {
  collection.remove();
  collection.insert({test:"test"});
  assert.equal(collection.count(), 1);
  collection.update({}, {$set:{test:"test2"}});
  //NOTE: we don't test all update operators here http://docs.mongodb.org/manual/reference/operator/nav-update/#id1
  assert.equal(collection.count(), 1);
  assert.equal(collection.find({}).next().test, "test2");
};

exports.testEnsureIndex = function() {
  var indexed = db.getCollection("indexed");
  indexed.ensureIndex({name:1});
  assert.equal(indexed.getIndexes()[1].key.name, 1);
};

exports.testFind = function() {
  collection.remove();
  collection.insert({expression:"2 + 2", result:4});
  collection.insert({expression:"1 + 1", result:2});

  assert.equal(collection.find({result:2}).next().expression, "1 + 1");
  assert.equal(collection.find({result:2}, {expression:1}).next().expression, "1 + 1");
  assert.equal(collection.find({result:2}, {result:1}).next().expression, undefined);
};

exports.testFindOne = function() {
  collection.remove();
  collection.insert({expression:"2 + 2", result:4});
  collection.insert({expression:"1 + 1", result:2});

  assert.equal(collection.findOne({result:2}).expression, "1 + 1");
  assert.equal(collection.findOne({result:2}, {expression:1}).expression, "1 + 1");
  assert.equal(collection.findOne({result:2}, {result:1}).expression, undefined);
};

exports.testFindAndModify = function() {
  collection.remove();
  collection.insert({test:"test"});
  assert.equal(collection.findAndModify({
    query:{test:"test"},
    update:{$set:{test:"test2"}}
  }).test, "test");
  assert.equal(collection.find({}).next().test, "test2");
  //TODO test that options work as well
};

exports.testGetIndexes = function() {
  collection.insert({test:"test"});
  assert.equal(collection.getIndexes()[0].key._id, 1);
};

//exports.testMapReduce = function() {
//};

exports.testRemove = function() {
  collection.remove();
  collection.insert({name:"John"});
  collection.insert({name:"John"});
  collection.insert({name:"John"});
  collection.insert({name:"Smith"});

  assert.equal(collection.count(), 4);

  collection.remove({name:"Smith"});
  assert.equal(collection.count(), 3);

  collection.remove({name:"John"}, true);
  assert.equal(collection.count(), 2);

  collection.remove({name:"John"});
  assert.equal(collection.count(), 0);
};

exports.testToArray = function() {
  collection.remove();
  collection.insert({name:"John"});
  collection.insert({name:"Smith"});
  collection.insert({name:"Adam"});

  var array = collection.find({}).toArray();
  assert.equal(array.length, 3);
  assert.equal(array[0].name, "John");
  assert.equal(array[1].name, "Smith");
  assert.equal(array[2].name, "Adam");
};

exports.testForEach = function() {
  collection.remove();
  collection.insert({name:"John"});
  collection.insert({name:"Smith"});
  collection.insert({name:"Adam"});

  var array = [];
  collection.find({}).forEach(function(item) {
    array.push(item);
  });
  assert.equal(array.length, 3);
  assert.equal(array[0].name, "John");
  assert.equal(array[1].name, "Smith");
  assert.equal(array[2].name, "Adam");
};

exports.testSort = function() {
  collection.remove();
  collection.insert({name:"John"});
  collection.insert({name:"Smith"});
  collection.insert({name:"Adam"});

  var array = collection.find({}).sort({name:1}).toArray();
  assert.equal(array[0].name, "Adam");
  assert.equal(array[1].name, "John");
  assert.equal(array[2].name, "Smith");

  array = collection.find({}).sort({name:-1}).toArray();
  assert.equal(array[0].name, "Smith");
  assert.equal(array[1].name, "John");
  assert.equal(array[2].name, "Adam");
};

exports.testLimit = function() {
  collection.remove();
  collection.insert({name:"John"});
  collection.insert({name:"Smith"});
  collection.insert({name:"Adam"});

  var array = collection.find({}).limit(2).toArray();
  assert.equal(array.length, 2);
  assert.equal(array[0].name, "John");
  assert.equal(array[1].name, "Smith");
};

exports.testSkip = function() {
  collection.remove();
  collection.insert({name:"John"});
  collection.insert({name:"Smith"});
  collection.insert({name:"Adam"});

  var array = collection.find({}).skip(1).toArray();
  assert.equal(array.length, 2);
  assert.equal(array[0].name, "Smith");
  assert.equal(array[1].name, "Adam");
};

exports.testCount = function() {
  collection.remove();
  collection.insert({});
  collection.insert({});
  assert.equal(collection.find({}).count(), 2);
  assert.equal(collection.find({}).skip(1).count(), 2);
  assert.equal(collection.find({}).limit(1).count(), 2);
};

exports.testSize = function() {
  collection.remove();
  collection.insert({});
  collection.insert({});
  assert.equal(collection.find({}).size(), 0);
  assert.equal(collection.find({}).skip(1).size(), 0);
  assert.equal(collection.find({}).limit(1).size(), 1);
};

exports.testExplain = function() {
  assert.equal(collection.find({}).explain().cursor, 'BasicCursor');
};

exports.testMap = function() {
  collection.remove();
  collection.insert({name:"John"});
  collection.insert({name:"Smith"});
  collection.insert({name:"Adam"});
  var array = collection.find({}).map(function(user) {
    return user.name;
  }).toArray();
  assert.equal(array[0], "John");
  assert.equal(array[1], "Smith");
  assert.equal(array[2], "Adam");
};

exports.testNext = function() {
  collection.remove();
  collection.insert({name:"John"});
  collection.insert({name:"Smith"});
  collection.insert({name:"Adam"});

  var cursor = collection.find({});
  assert.equal(cursor.next().name, "John");
  assert.equal(cursor.next().name, "Smith");
  assert.equal(cursor.next().name, "Adam");
};

if (require.main === module) {
  test.run(exports);
}