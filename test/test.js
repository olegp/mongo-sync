var Server = require("../lib/mongo-sync").Server;

var server = new Server('127.0.0.1');
console.log(server.db("test").getCollection("posts").find().limit(1).next());
server.close();