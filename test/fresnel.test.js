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

function replaceClientMethod(fresnel, method, func) {
    var _getClient = fresnel._getClient;

    fresnel._getClient = function() {
        var client = _getClient.apply(this);
        client[method] = func;
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

        fresnel.BUFFERED_TASKS = [randomTask()];

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
            callback();
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
            callback();
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
            callback();
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

        var taskId = 42;
        var task = randomTask();
        assert.ok(task.id == null);

        fresnel._getClient = function() {
            return {
                "set": function(key, value) {
                    resultTask = JSON.parse(value);
                }
            };
        }

        fresnel._updateDefinition(taskId, task);

        beforeExit(function() {
            assert.equal(taskId, resultTask.id);
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

        var removedKey;

        var task = randomTask();

        replaceClientMethod(fresnel, 'srem', function(key, value, callback) {
            removedKey = key;

            if (callback) {
                callback();
            }
        });

        fresnel.createTask(task, function() {
            fresnel._executeTask(task);
        });

        beforeExit(function() {
            assert.equal(fresnel._namespace("tasks"), removedKey);
        });
    },
}
