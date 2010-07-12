var assert = require('assert'),
    sys = require('sys'),
    redis = require('redis-client'),
    EventEmitter = require('events').EventEmitter;

var _fresnel = require('fresnel');
var Fresnel = _fresnel.Fresnel;

_fresnel.AUTO_CLOSE = true;

process.addListener('uncaughtException', function(err) {
    console.log("Caught exception: " + err);
});

function randomTask() {
    return {
        "class": randomString(),
        "args": []
    };
}

var CHARACTERS = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789";

function randomString(length) {
    length = length || 16;
    var text = "";

    for (var i=0; i < length; i++) {
        text += CHARACTERS.charAt(Math.floor(Math.random() * CHARACTERS.length));
    }

    return text;
}

function range(end) {
    var range = [];
    for (var i = 0; i < end; i++) {
        range.push(i);
    }

    return range;
}


function replaceClientMethod(fresnel, method, func) {
    var _getClient = fresnel._getClient;

    fresnel._getClient = function() {
        var client = _getClient.apply(this);
        client[method] = function() {
            var callback = arguments[arguments.length - 1];
            callback(null, func.apply(null, arguments));
        };
        return client;
    }
}

module.exports = {
    'should be an EventEmitter': function(assert) {
        var fresnel = new Fresnel();
        assert.ok(fresnel instanceof EventEmitter);
    },
    'should buffer tasks into a local queue': function(assert, beforeExit) {
        var fresnel = new Fresnel(randomString());

        // create some tasks in Redis
        var task = randomTask();

        fresnel.createTask(task, function() {
            fresnel.bufferTasks(function() {
                fresnel.getUnbufferedQueueLength(function(length) {
                    assert.equal(0, length);
                });
            });
        });

        beforeExit(function() {
            assert.equal(task.toString(), fresnel.BUFFERED_TASKS[0].toString());
        });
    },
    'should mark buffered tasks as pending': function(assert, beforeExit) {
        var fresnel = new Fresnel(randomString());

        fresnel.createTask(randomTask(), function() {
            fresnel.bufferTasks(function() {
                fresnel.getPendingCount(function(count) {
                    assert.equal(1, count);
                });
            });
        });
    },
    'should buffer tasks with no callback': function(assert) {
        var fresnel = new Fresnel(randomString());

        fresnel.createTask(randomTask(), function() {
            fresnel.bufferTasks();
        });
    },
    'should succeed when no tasks are available to be buffered': function(assert) {
        var fresnel = new Fresnel(randomString());

        fresnel.bufferTasks();
    },
    'should succeed when no tasks are available to be buffered and no callback was provided': function(assert) {
        var fresnel = new Fresnel(randomString());

        fresnel.bufferTasks();
    },
    'Duplicate tasks should only be inserted once': function(assert) {
        var fresnel = new Fresnel(randomString());

        var task = randomTask();

        fresnel.createTask(task, function() {
            fresnel.createTask(task, function() {
                fresnel.getUnbufferedQueueLength(function(length) {
                    assert.equal(1, length);
                });
            });
        });
    },
    'Locally buffered tasks should be run': function(assert, beforeExit) {
        var fresnel = new Fresnel(randomString());

        var task = randomTask();
        task.id = "1234";

        fresnel.BUFFERED_TASKS = [task];

        fresnel.runBufferedTasks();

        beforeExit(function() {
            assert.equal(0, fresnel.BUFFERED_TASKS.length);
        });
    },
    'should create tasks with no callback': function(assert) {
        var fresnel = new Fresnel(randomString());

        fresnel.createTask(randomTask());
    },
    "shouldn't fail when creating a duplicate task with no callback": function(assert) {
        var fresnel = new Fresnel(randomString());

        var task = randomTask();

        fresnel.createTask(task, function() {
            fresnel.createTask(task);
        });
    },
    "should add to the 'tasks' set when creating tasks": function(assert, beforeExit) {
        var fresnel = new Fresnel(randomString());
        
        var calledWithKey;

        replaceClientMethod(fresnel, 'sadd', function(key, value, callback) {
            calledWithKey = key;
        });
        
        fresnel.createTask(randomTask());

        beforeExit(function() {
            assert.equal(fresnel._namespace("tasks"), calledWithKey);
        });
    },
    "should add to the 'queue' sorted set when creating tasks": function(assert, beforeExit) {
        var fresnel = new Fresnel(randomString());
        
        var calledWithKey;

        replaceClientMethod(fresnel, 'zadd', function(key, score, value, callback) {
            calledWithKey = key;
        });
        
        fresnel.createTask(randomTask());

        beforeExit(function() {
            assert.equal(fresnel._namespace("queue"), calledWithKey);
        });
    },
    "should form a task definition when creating tasks": function(assert, beforeExit) {
        var fresnel = new Fresnel(randomString());
        
        var calledWithKey;

        replaceClientMethod(fresnel, 'set', function(key, value, callback) {
            calledWithKey = key;
        });

        var task = randomTask();
        
        fresnel.createTask(task);

        beforeExit(function() {
            assert.equal(fresnel._namespace("tasks:" + task.id), calledWithKey);
        });
    },
    "update definition should store an internal definition": function(assert, beforeExit) {
        var fresnel = new Fresnel(randomString());

        var resultTask;

        var task = randomTask();
        task.id = 42;

        fresnel._getClient = function() {
            return {
                "set": function(key, value) {
                    resultTask = JSON.parse(value);
                }
            };
        }

        fresnel._updateDefinition(task);

        beforeExit(function() {
            assert.equal(task.id, resultTask.id);
        });
    },
    "_getDefinitions should load multiple task definitions": function(assert, beforeExit) {
        var fresnel = new Fresnel(randomString());

        var tasks = [];
        var taskIds = [];
        var taskDefs = [];

        var insertCount = 0;

        for (var i = 0; i < 5; i++) {
            tasks.push(randomTask());
        }

        tasks.forEach(function(task) {
            var taskId;
            do {
                taskId = Math.floor(Math.random() * 10);
            } while (taskIds.indexOf(taskId) >= 0);

            taskIds.push(taskId);
            task.id = taskId;

            fresnel._updateDefinition(task, function() {
                if (++insertCount == tasks.length) {
                    fresnel._getDefinitions(taskIds.slice(0, 2), function(defs) {
                        taskDefs = defs;
                    });
                }
            });
        });

        beforeExit(function() {
            assert.eql(tasks.slice(0, 2), taskDefs);
        });
    },
    "_getDefinitions should load individual task definitions": function(assert, beforeExit) {
        var fresnel = new Fresnel(randomString());

        var task = randomTask();
        var taskDefs;
        task.id = Math.floor(Math.random() * 10);

        fresnel._updateDefinition(task, function() {
            fresnel._getDefinitions(task.id, function(defs) {
                taskDefs = defs;
            });
        });

        beforeExit(function() {
            assert.eql(task, taskDefs[0]);
        });
    },
    "hash function should only consider public fields": function(assert) {
        var fresnel = new Fresnel(randomString());

        var task = randomTask();
        var hash = fresnel._hash(task);

        task.id = "1234";

        assert.equal(hash, fresnel._hash(task));
    },
    "when tasks execute successfully, they should be removed from the 'tasks' set": function(assert, beforeExit) {
        var fresnel = new Fresnel(randomString());

        var removedKeys = [];

        var task = randomTask();

        replaceClientMethod(fresnel, 'srem', function(key, value, callback) {
            removedKeys.push(key);
        });

        fresnel.createTask(task, function() {
            fresnel._executeTask(task);
        });

        beforeExit(function() {
            assert.ok(removedKeys.indexOf(fresnel._namespace("tasks")) >= 0);
        });
    },
    "when tasks execute successfully, they should be removed from the 'pending' set": function(assert, beforeExit) {
        var fresnel = new Fresnel(randomString());

        var removedKeys = [];

        var task = randomTask();

        replaceClientMethod(fresnel, 'srem', function(key, value, callback) {
            removedKeys.push(key);
        });

        fresnel.createTask(task, function() {
            fresnel._executeTask(task);
        });

        beforeExit(function() {
            assert.ok(removedKeys.indexOf(fresnel._namespace("pending")) >= 0);
        });
    },
    "when tasks execute successfully, their task definition should be removed": function(assert, beforeExit) {
        var fresnel = new Fresnel(randomString());

        var removedKeys = [];

        var task = randomTask();

        replaceClientMethod(fresnel, 'del', function(key, callback) {
            removedKeys.push(key);
        });

        fresnel.createTask(task, function() {
            fresnel._executeTask(task);
        });

        beforeExit(function() {
            assert.ok(removedKeys.indexOf(fresnel._namespace("tasks:" + task.id)) >= 0);
        });
    },
}
