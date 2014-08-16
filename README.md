# Mongo Sync

Mongo Sync is a synchronous MongoDB driver for use with [Common Node](http://olegp.github.com/common-node/) that attempts to closely approximate the [MongoDB shell](http://www.mongodb.org/display/DOCS/dbshell+Reference).

It is a thin wrapper around the official MongoDB driver for Node. Here is a quick usage example that you can use with Common Node:

    var Server = require("mongo-sync").Server;
    var server = new Server('127.0.0.1');
    var result = server.db("test").getCollection("posts").find().toArray();
    console.log(result);
    server.close();
    
For a complete application using this driver, check out [Notes](https://github.com/olegp/notes/). 

If you want to use Mongo Sync directly with [node-fibers](https://github.com/laverdet/node-fibers) (i.e. without Common Node), make sure all database requests occur within a fiber, e.g.:

    var Fiber = require('fibers');
    Fiber(function() {
      // set up goes here ...
      var result = server.db("LashDB").getCollection("users").find().toArray();
    }).run();

### Tests

[![Build Status](https://travis-ci.org/olegp/mongo-sync.svg?branch=master)](https://travis-ci.org/olegp/mongo-sync)

Run the unit tests with the following command:

    common-node test/test.js

### Community

If you have any questions about using `mongo-sync` by itself or in conjunction with Common Node, please post them to the [Common Node mailing list](https://groups.google.com/forum/#!forum/common-node).
    
### License 

(The MIT License)

Copyright (c) 2011+ Oleg Podsechin

Permission is hereby granted, free of charge, to any person obtaining a copy of this software and associated documentation files (the 'Software'), to deal in the Software without restriction, including without limitation the rights to use, copy, modify, merge, publish, distribute, sublicense, and/or sell copies of the Software, and to permit persons to whom the Software is furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in all copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED 'AS IS', WITHOUT WARRANTY OF ANY KIND, EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.
