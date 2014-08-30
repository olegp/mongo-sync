var assert = require("assert");
var test = require('test');
var Server = require("../lib/mongo-sync").Server;

var db = new Server('127.0.0.1').db("test");
var collection = db.getCollection("tests");

exports.testGetCollection = function() {
  assert.ok(collection);
};

exports.testCollectionNames = function() {
  collection.insert({});
  assert.notStrictEqual(db.collectionNames().indexOf("tests"), -1);
};

exports.testAddUser = function() {
  db.addUser('tester', 'testee');
};

exports.testRemoveUser = function() {
  db.removeUser('tester');
};

exports.testAuth = function() {
  db.addUser('tester', 'testee');
  assert.ok(db.auth('tester', 'testee'));
  db.removeUser('tester');
  assert.throws(function() {
    db.auth('tester', 'testee');
  });
};

exports.testDropDatabase = function() {
  collection.insert({});
  db.dropDatabase();
  assert.strictEqual(db.collectionNames().indexOf("tests"), -1);
};

exports.testEval = function() {
  assert.strictEqual(db.eval("return 42"), 42);
};

//exports.testRemoveUser = function() {
//}

exports.testRunCommand = function() {
  assert.strictEqual(
    db.runCommand({
    $eval:"function(x) { return x; }",
    args:[42]
  }).retval, 42);
};

exports.testCollectionCount = function() {
  collection.remove();
  collection.insert({});
  collection.insert({});
  assert.strictEqual(collection.count(), 2);
};

exports.testDistinct = function() {
  collection.remove();
  collection.insert({name:"John"});
  collection.insert({name:"John"});
  assert.strictEqual(collection.distinct("name", {})[0], "John");
};

exports.testDrop = function() {
  collection.insert({});
  collection.drop();
  assert.strictEqual(db.collectionNames().indexOf("tests"), -1);
};

exports.testInsert = function() {
  collection.remove();
  collection.insert({test:"test"});
  assert.strictEqual(collection.find().next().test, "test");
};

exports.testSave = function() {
  collection.remove();
  var test = collection.save({test:"test"});
  assert.strictEqual(collection.count(), 1);
  test.test = "test2";
  collection.save(test);
  assert.strictEqual(collection.count(), 1);
  assert.strictEqual(collection.find().next().test, "test2");
};

exports.testUpdate = function() {
  collection.remove();
  collection.insert({test:"test"});
  assert.strictEqual(collection.count(), 1);
  collection.update({}, {$set:{test:"test2"}});
  //NOTE: we don't test all update operators here http://docs.mongodb.org/manual/reference/operator/nav-update/#id1
  assert.strictEqual(collection.count(), 1);
  assert.strictEqual(collection.find().next().test, "test2");
};

exports.testEnsureIndex = function() {
  var indexed = db.getCollection("indexed");
  indexed.ensureIndex({name:1});
  assert.strictEqual(indexed.getIndexes()[1].key.name, 1);
};

exports.testFind = function() {
  collection.remove();
  collection.insert({expression:"2 + 2", result:4});
  collection.insert({expression:"1 + 1", result:2});

  assert.strictEqual(collection.find({result:2}).next().expression, "1 + 1");
  assert.strictEqual(collection.find({result:2}, {expression:1}).next().expression, "1 + 1");
  assert.strictEqual(collection.find({result:2}, {result:1}).next().expression, undefined);
};

exports.testFindOne = function() {
  collection.remove();
  collection.insert({expression:"2 + 2", result:4});
  collection.insert({expression:"1 + 1", result:2});

  assert.strictEqual(collection.findOne({result:2}).expression, "1 + 1");
  assert.strictEqual(collection.findOne({result:2}, {expression:1}).expression, "1 + 1");
  assert.strictEqual(collection.findOne({result:2}, {result:1}).expression, undefined);
};

exports.testFindAndModify = function() {
  collection.remove();
  collection.insert({test:"test"});
  assert.strictEqual(collection.findAndModify({
    query:{test:"test"},
    update:{$set:{test:"test2"}}
  }).test, "test");
  assert.strictEqual(collection.find().next().test, "test2");

  assert.strictEqual(collection.findAndModify({
    query:{test:"test2"},
    update:{$set:{test:"test3"}},
    'new':true
  }).test, "test3");
  assert.strictEqual(collection.find().next().test, "test3");

  collection.remove();
  assert.deepEqual(collection.findAndModify({
    query:{test:"test"},
    update:{$set:{test:"test2"}},
    upsert:true
  }), null);
  assert.strictEqual(collection.find().next().test, "test2");

  collection.remove();
  assert.strictEqual(collection.findAndModify({
    query:{test:"test"},
    update:{$set:{test:"test2"}},
    upsert:true,
    'new':true
  }).test, "test2");
  assert.strictEqual(collection.find().next().test, "test2");
};

exports.testGetIndexes = function() {
  collection.insert({test:"test"});
  assert.strictEqual(collection.getIndexes()[0].key._id, 1);
};

//exports.testMapReduce = function() {
//};

exports.testRemove = function() {
  collection.remove();
  collection.insert({name:"John"});
  collection.insert({name:"John"});
  collection.insert({name:"John"});
  collection.insert({name:"Smith"});

  assert.strictEqual(collection.count(), 4);

  collection.remove({name:"Smith"});
  assert.strictEqual(collection.count(), 3);

  collection.remove({name:"John"}, true);
  assert.strictEqual(collection.count(), 2);

  collection.remove({name:"John"});
  assert.strictEqual(collection.count(), 0);
};

exports.testToArray = function() {
  collection.remove();
  collection.insert({name:"John"});
  collection.insert({name:"Smith"});
  collection.insert({name:"Adam"});

  var cursor = collection.find();
  var array = cursor.toArray();
  assert.strictEqual(array.length, 3);
  assert.strictEqual(array[0].name, "John");
  assert.strictEqual(array[1].name, "Smith");
  assert.strictEqual(array[2].name, "Adam");
  assert.strictEqual(cursor._cursor.isClosed(), true);
};

exports.testForEach = function() {
  collection.remove();
  collection.insert({name:"John"});
  collection.insert({name:"Smith"});
  collection.insert({name:"Adam"});

  var array = [];
  var cursor = collection.find();
  assert.strictEqual(cursor.forEach(function(item) {
    array.push(item);
  }), cursor);
  assert.strictEqual(array.length, 3);
  assert.strictEqual(array[0].name, "John");
  assert.strictEqual(array[1].name, "Smith");
  assert.strictEqual(array[2].name, "Adam");
  assert.strictEqual(cursor._cursor.isClosed(), true);
};

exports.testSort = function() {
  collection.remove();
  collection.insert({name:"John"});
  collection.insert({name:"Smith"});
  collection.insert({name:"Adam"});

  var array = collection.find().sort({name:1}).toArray();
  assert.strictEqual(array[0].name, "Adam");
  assert.strictEqual(array[1].name, "John");
  assert.strictEqual(array[2].name, "Smith");

  array = collection.find().sort({name:-1}).toArray();
  assert.strictEqual(array[0].name, "Smith");
  assert.strictEqual(array[1].name, "John");
  assert.strictEqual(array[2].name, "Adam");
};

exports.testLimit = function() {
  collection.remove();
  collection.insert({name:"John"});
  collection.insert({name:"Smith"});
  collection.insert({name:"Adam"});

  var array = collection.find().limit(2).toArray();
  assert.strictEqual(array.length, 2);
  assert.strictEqual(array[0].name, "John");
  assert.strictEqual(array[1].name, "Smith");
};

exports.testSkip = function() {
  collection.remove();
  collection.insert({name:"John"});
  collection.insert({name:"Smith"});
  collection.insert({name:"Adam"});

  var array = collection.find().skip(1).toArray();
  assert.strictEqual(array.length, 2);
  assert.strictEqual(array[0].name, "Smith");
  assert.strictEqual(array[1].name, "Adam");
};

exports.testCount = function() {
  collection.remove();
  collection.insert({name:"John"});
  collection.insert({name:"John"});
  collection.insert({name:"Smith"});
  assert.strictEqual(collection.count(), 3);
  assert.strictEqual(collection.count({name:"John"}), 2);
  assert.strictEqual(collection.find().count(), 3);
  assert.strictEqual(collection.find().skip(1).count(), 3);
  assert.strictEqual(collection.find().limit(1).count(), 3);
  assert.strictEqual(collection.find().skip(1).limit(1).count(), 3);
};

exports.testSize = function() {
  collection.remove();
  collection.insert({});
  collection.insert({});
  assert.strictEqual(collection.find().size(), 2);
  assert.strictEqual(collection.find().skip(1).size(), 1);
  assert.strictEqual(collection.find().limit(1).size(), 1);
  assert.strictEqual(collection.find().skip(1).limit(1).size(), 1);
};

exports.testExplain = function() {
  assert.strictEqual(collection.find().explain().cursor, 'BasicCursor');
};

exports.testMap = function() {
  collection.remove();
  collection.insert({name:"John"});
  collection.insert({name:"Smith"});
  collection.insert({name:"Adam"});
  var cursor = collection.find();
  var array = cursor.map(function(user) {
    return user.name;
  }).toArray();
  assert.strictEqual(array[0], "John");
  assert.strictEqual(array[1], "Smith");
  assert.strictEqual(array[2], "Adam");
  assert.strictEqual(cursor._cursor.isClosed(), true);
};

exports.testNext = function() {
  collection.remove();
  collection.insert({name:"John"});
  collection.insert({name:"Smith"});
  collection.insert({name:"Adam"});

  var cursor = collection.find();
  assert.strictEqual(cursor.next().name, "John");
  assert.strictEqual(cursor._cursor.isClosed(), false);
  assert.strictEqual(cursor.next().name, "Smith");
  assert.strictEqual(cursor._cursor.isClosed(), false);
  assert.strictEqual(cursor.next().name, "Adam");
  assert.strictEqual(cursor._cursor.isClosed(), false);
  assert.strictEqual(cursor.next(), null);
  assert.strictEqual(cursor._cursor.isClosed(), true);
};

if (require.main === module) {
  test.run(exports);
}

