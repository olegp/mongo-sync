var assert = require("assert");
var test = require('test');
var Server = require("../lib/mongo-sync").Server;
var Client = require("../lib/mongo-sync").Client;

var server = new Server('127.0.0.1');
var client = new Client(server);
var db = client.connect("mongodb://127.0.0.1:27017/test");
var collection = db.getCollection("tests");

exports.testGetCollection = function() {
  assert.ok(collection);
};

exports.testCollectionNames = function() {
  collection.insertOne({});
  assert.notStrictEqual(db.listCollections().indexOf("tests"), -1);
};

// exports.testAddUser = function() {
//   db.addUser('tester', 'testee', []);
// };

// exports.testRemoveUser = function() {
//   db.removeUser('tester');
// };

// exports.testAuth = function() {
//   client.addUser('tester', 'testee', []);
//   assert.ok(db.auth('tester', 'testee'));
//   db.removeUser('tester');
//   assert.throws(function() {
//     db.auth('tester', 'testee');
//   });
// };

exports.testDropDatabase = function() {
  collection.insertOne({});
  db.dropDatabase();
  assert.strictEqual(db.listCollections().indexOf("tests"), -1);
};

exports.testEval = function() {
  assert.strictEqual(db.eval("return 42"), 42);
};

exports.testRunCommand = function() {
  assert.strictEqual(
    db.runCommand({
    $eval:"function(x) { return x; }",
    args:[42]
  }).retval, 42);
};

exports.testCollectionCount = function() {
  collection.remove();
  collection.insertOne({});
  collection.insertOne({});
  assert.strictEqual(collection.count(), 2);
};

exports.testDistinct = function() {
  collection.remove();
  collection.insertOne({name:"John"});
  collection.insertOne({name:"John"});
  assert.strictEqual(collection.distinct("name", {})[0], "John");
};

exports.testDrop = function() {
  collection.insertOne({});
  collection.drop();
  assert.strictEqual(db.listCollections().indexOf("tests"), -1);
};

exports.testInsertOne = function() {
  collection.remove();
  collection.insertOne({test:"test"});
  assert.strictEqual(collection.find().next().test, "test");
};

exports.testInsertOld = function() {
  collection.remove();
  collection.insert({test:"test"});
  assert.strictEqual(collection.find().next().test, "test");
};

exports.testInsertMany = function() {
  collection.remove();
  collection.insertMany([{test:"test"},{test:"test2"}]);
  assert.strictEqual(collection.find().next().test, "test");
  assert.strictEqual(collection.count(), 2);
};

// exports.testSave = function() {
//   collection.remove();
//   var test = collection.save({test:"test"});
//   assert.strictEqual(collection.count(), 1);
//   test.test = "test2";
//   collection.save(test);
//   assert.strictEqual(collection.count(), 1);
//   assert.strictEqual(collection.find().next().test, "test2");
// };

exports.testUpdate = function() {
  collection.remove();
  collection.insertOne({test:"test"});
  assert.strictEqual(collection.count(), 1);
  collection.update({}, {$set:{test:"test2"}});
  //NOTE: we don't test all update operators here http://docs.mongodb.org/manual/reference/operator/nav-update/#id1
  assert.strictEqual(collection.count(), 1);
  assert.strictEqual(collection.find().next().test, "test2");
};

exports.testUpdateMany = function() {
  collection.remove();
  collection.insertMany([{test:"test"},{test:"test"},{test:"test"}]);
  assert.strictEqual(collection.count(), 3);
  collection.updateMany({}, {$set:{test:"test2"}});
  //NOTE: we don't test all update operators here http://docs.mongodb.org/manual/reference/operator/nav-update/#id1
  assert.strictEqual(collection.count(), 3);
  assert.strictEqual(collection.find().next().test, "test2");
};

exports.testEnsureIndex = function() {
  var indexed = db.getCollection("indexed");
  indexed.ensureIndex({name:1});
  assert.strictEqual(indexed.getIndexes()[1].key.name, 1);
};

exports.testFind = function() {
  collection.remove();
  collection.insertOne({expression:"2 + 2", result:4});
  collection.insertOne({expression:"1 + 1", result:2});

  assert.strictEqual(collection.find({result:2}).next().expression, "1 + 1");
  assert.strictEqual(collection.find({result:2}, {expression:1}).next().expression, "1 + 1");
  assert.strictEqual(collection.find({result:2}, {result:1}).next().expression, undefined);
};

exports.testFindOne = function() {
  collection.remove();
  collection.insertOne({expression:"2 + 2", result:4});
  collection.insertOne({expression:"1 + 1", result:2});

  assert.strictEqual(collection.findOne({result:2}).expression, "1 + 1");
  assert.strictEqual(collection.findOne({result:2}, {expression:1}).expression, "1 + 1");
  assert.strictEqual(collection.findOne({result:2}, {result:1}).expression, undefined);
};

exports.testFindAndModify = function() {
  collection.remove();
  collection.insertOne({test:"test"});
  assert.strictEqual(collection.find().next().test, "test");
  collection.findAndModify({
    query:{test:"test"},
    update:{$set:{test:"test2"}}
  });
  assert.strictEqual(collection.find().next().test, "test2");

  collection.findAndModify({
    query:{test:"test2"},
    update:{$set:{test:"test3"}},
    'new':true
  });
  assert.strictEqual(collection.find().next().test, "test3");

  collection.remove();
  collection.findAndModify({
    query:{test:"test"},
    update:{$set:{test:"test2"}},
    upsert:true
  });
  // assert.deepEqual(collection.find(), null);
  assert.strictEqual(collection.find().next().test, "test2");

  collection.remove();
  collection.findAndModify({
    query:{test:"test"},
    update:{$set:{test:"test2"}},
    upsert:true,
    'new':true
  });
  assert.strictEqual(collection.find().next().test, "test2");
};

exports.testGetIndexes = function() {
  collection.insertOne({test:"test"});
  assert.strictEqual(collection.getIndexes()[0].key._id, 1);
};

//exports.testMapReduce = function() {
//};

exports.testRemove = function() {
  collection.remove();
  collection.insert([{name:"John"},{name:"John"},{name:"John"},{name:"Smith"}]);
  assert.strictEqual(collection.count(), 4);

  collection.remove({name:"Smith"});
  assert.strictEqual(collection.count(), 3);

  collection.remove({name:"John"}, true);
  assert.strictEqual(collection.count(), 2);

  collection.remove({name:"John"});
  assert.strictEqual(collection.count(), 0);
};

exports.testRemoveMany = function() {
  collection.remove();
  collection.insert([{name:"John"},{name:"John"},{name:"John"},{name:"Smith"}]);
  assert.strictEqual(collection.count(), 4);

  collection.remove({name:"Smith"});
  assert.strictEqual(collection.count(), 3);

  collection.removeOne({name:"John"}, true);
  assert.strictEqual(collection.count(), 2);

  collection.removeMany({name:"John"});
  assert.strictEqual(collection.count(), 0);
};

exports.testToArray = function() {
  collection.remove();
  collection.insertOne({name:"John"});
  collection.insertOne({name:"Smith"});
  collection.insertOne({name:"Adam"});

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
  collection.insertOne({name:"John"});
  collection.insertOne({name:"Smith"});
  collection.insertOne({name:"Adam"});

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
  collection.insertOne({name:"John"});
  collection.insertOne({name:"Smith"});
  collection.insertOne({name:"Adam"});

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
  collection.insertOne({name:"John"});
  collection.insertOne({name:"Smith"});
  collection.insertOne({name:"Adam"});

  var array = collection.find().limit(2).toArray();
  assert.strictEqual(array.length, 2);
  assert.strictEqual(array[0].name, "John");
  assert.strictEqual(array[1].name, "Smith");
};

exports.testSkip = function() {
  collection.remove();
  collection.insertOne({name:"John"});
  collection.insertOne({name:"Smith"});
  collection.insertOne({name:"Adam"});

  var array = collection.find().skip(1).toArray();
  assert.strictEqual(array.length, 2);
  assert.strictEqual(array[0].name, "Smith");
  assert.strictEqual(array[1].name, "Adam");
};

exports.testCount = function() {
  collection.remove();
  collection.insertOne({name:"John"});
  collection.insertOne({name:"John"});
  collection.insertOne({name:"Smith"});
  assert.strictEqual(collection.count(), 3);
  assert.strictEqual(collection.count({name:"John"}), 2);
  assert.strictEqual(collection.find().count(), 3);
  assert.strictEqual(collection.find().skip(1).count(), 3);
  assert.strictEqual(collection.find().limit(1).count(), 3);
  assert.strictEqual(collection.find().skip(1).limit(1).count(), 3);
};

exports.testSize = function() {
  collection.remove();
  collection.insertOne({});
  collection.insertOne({});
  assert.strictEqual(collection.find().size(), 2);
  assert.strictEqual(collection.find().skip(1).size(), 1);
  assert.strictEqual(collection.find().limit(1).size(), 1);
  assert.strictEqual(collection.find().skip(1).limit(1).size(), 1);
};

exports.testExplain = function() {
  var cur = collection.find().explain();
  assert.strictEqual(cur.executionStats.executionSuccess, true);
};

exports.testMap = function() {
  collection.remove();
  collection.insertOne({name:"John"});
  collection.insertOne({name:"Smith"});
  collection.insertOne({name:"Adam"});
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
  collection.insertOne({name:"John"});
  collection.insertOne({name:"Smith"});
  collection.insertOne({name:"Adam"});

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

