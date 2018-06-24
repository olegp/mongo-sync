var Fiber = require('fibers');
Fiber(function() {
    require("test").run(require("./test"));
}).run();
