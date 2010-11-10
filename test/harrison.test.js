var assert = require('assert'),
    sys = require('sys'),
    redis = require('redis'),
    EventEmitter = require('events').EventEmitter;

var _harrison = require('harrison');
var Harrison = _harrison.Harrison;

_harrison.AUTO_CLOSE = true;

/**
 * Assert that `expected` is within `epsilon` of `actual`.
 *
 * @param {Number} expected
 * @param {Number} actual
 * @param {Number} epsilon
 * @param {String} msg
 * @api public
 */
assert.almostEqual = function(expected, actual, epsilon, msg) {
    assert.ok(Math.abs(expected - actual) <= epsilon, msg);
};

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

function pending(test) {
    // console.log("PENDING");
    // console.log(test);
}

function replaceClientMethod(harrison, method, func) {
    var _getClient = harrison._getClient;

    harrison._getClient = function() {
        var client = _getClient.apply(this);
        var originalMethod = client[method];
        client[method] = function() {
            var callback = arguments[arguments.length - 1];
            var args = [client, originalMethod];

            // copy arguments into a proper array
            for (arg in arguments) {
                args.push(arguments[arg]);
            }

            callback(null, func.apply(null, args));
        };
        return client;
    };
}

var failTask = function(task, callback) {
    // simulate a failed task
    callback(task, false, "Failed intentionally.");
};

var successTask = function(task, callback) {
    // simulate a successful task execution
    callback(task, true, "Succeeded.");
};

module.exports = {
    'should be an EventEmitter': function(assert) {
        var harrison = new Harrison();
        assert.ok(harrison instanceof EventEmitter);
    },
    "should remove buffered tasks from the 'queue' set": function(assert, beforeExit) {
        var harrison = new Harrison(randomString());
        harrison._runTask = successTask;

        var queueLength;

        harrison.createTask(randomTask(), function() {
            harrison.bufferTasks(function() {
                harrison.getUnbufferedQueueLength(function(length) {
                    queueLength = length;
                });
            });
        });

        beforeExit(function() {
            assert.equal(0, queueLength);
        });
    },
    'should mark buffered tasks as pending': function(assert, beforeExit) {
        var harrison = new Harrison(randomString());
        harrison._runTask = successTask;

        harrison.createTask(randomTask(), function() {
            harrison.bufferTasks(function() {
                harrison.getPendingCount(function(count) {
                    assert.equal(1, count);
                });
            });
        });
    },
    "bufferTasks() should update the task definition with 'reservedAt'": function(assert, beforeExit) {
        var harrison = new Harrison(randomString());
        harrison._runTask = successTask;

        var reservedAt = new Date().getTime();
        var taskDef;

        harrison.createTask(randomTask(), function(task) {
            harrison.bufferTasks(function() {
                harrison._getDefinition(task.id, function(def) {
                    taskDef = def;
                });
            });
        });

        beforeExit(function() {
            assert.almostEqual(reservedAt, Date.parse(taskDef.reservedAt), 500);
        });
    },
    "bufferTasks() should set the task state to 'reserved'": function(assert, beforeExit) {
        var harrison = new Harrison(randomString());

        var taskDef;

        // mock out the chain to prevent it from running immediately
        harrison._getLocalQueue = function(taskType) {
            return {
                add: function() {}
            };
        };

        harrison.createTask(randomTask(), function(task) {
            harrison.bufferTasks(function() {
                harrison._getDefinition(task.id, function(def) {
                    taskDef = def;
                });
            });
        });

        beforeExit(function() {
            assert.equal("reserved", taskDef.state);
        });
    },
    'should buffer tasks with no callback': function(assert) {
        var harrison = new Harrison(randomString());

        harrison._runTask = successTask;

        harrison.createTask(randomTask(), function() {
            harrison.bufferTasks();
        });
    },
    'should succeed when no tasks are available to be buffered': function(assert) {
        var harrison = new Harrison(randomString());

        harrison.bufferTasks();
    },
    'should succeed when no tasks are available to be buffered and no callback was provided': function(assert) {
        var harrison = new Harrison(randomString());

        harrison.bufferTasks();
    },
    'Duplicate tasks should only be inserted once': function(assert) {
        var harrison = new Harrison(randomString());

        var task = randomTask();

        harrison.createTask(task, function() {
            harrison.createTask(task, function() {
                harrison.getUnbufferedQueueLength(function(length) {
                    assert.equal(1, length);
                });
            });
        });
    },
    "migrateTasks() should move tasks from the reservoir into the main queue": function(assert, beforeExit) {
        var harrison = new Harrison(randomString());

        var futureTasks;
        var queuedTasks;
        var taskId;

        harrison._createDefinition(randomTask(), function(task) {
            taskId = task.id;

            harrison._addToReservoir(taskId, new Date().getTime(), function() {
                harrison.migrateTasks(function() {
                    harrison._getQueuedTasks(0, 0, function(tasks) {
                        queuedTasks = tasks;
                    });

                    harrison._getFutureTasks(0, 0, function(tasks) {
                        futureTasks = tasks;
                    });
                });
            });
        });

        beforeExit(function() {
            assert.eql([], futureTasks);
            assert.equal(1, queuedTasks.length);
            assert.equal(taskId, queuedTasks[0][0]);
        });
    },
    'should create tasks with no callback': function(assert) {
        var harrison = new Harrison(randomString());

        harrison.createTask(randomTask());
    },
    "createTask() should set the task state to 'ready'": function(assert, beforeExit) {
        var harrison = new Harrison(randomString());

        var taskDef;

        harrison.createTask(randomTask(), function(task) {
            harrison._getDefinition(task.id, function(def) {
                taskDef = def;
            });
        });

        beforeExit(function() {
            assert.equal("ready", taskDef.state);
        });
    },
    "should yield the task id when creating a task": function(assert, beforeExit) {
        var harrison = new Harrison(randomString());

        var taskId;

        harrison._generateId(function() {
            // task will be assigned the 2nd generated id
            harrison.createTask(randomTask(), function(task) {
                taskId = task.id;
            });
        });

        beforeExit(function() {
            assert.equal(2, taskId);
        });
    },
    "shouldn't fail when creating a duplicate task with no callback": function(assert) {
        var harrison = new Harrison(randomString());

        var task = randomTask();

        harrison.createTask(task, function() {
            harrison.createTask(task);
        });
    },
    "should yield 'false' when creating a duplicate task": function(assert, beforeExit) {
        var harrison = new Harrison(randomString());

        var taskId;
        var task = randomTask();

        harrison.createTask(task, function() {
            harrison.createTask(task, function(id) {
                taskId = id;
            });
        });

        beforeExit(function() {
            assert.equal(false, taskId);
        });
    },
    "should add to the 'tasks' set when creating tasks": function(assert, beforeExit) {
        var harrison = new Harrison(randomString());
        
        var calledWithKey;

        replaceClientMethod(harrison, 'sadd', function(client, method, key, value, callback) {
            calledWithKey = key;
        });
        
        harrison.createTask(randomTask());

        beforeExit(function() {
            assert.equal(harrison._namespace("tasks"), calledWithKey);
        });
    },
    "should add to the 'queue' sorted set when creating tasks": function(assert, beforeExit) {
        var harrison = new Harrison(randomString());
        
        var calledWithKey;

        replaceClientMethod(harrison, 'zadd', function(client, method, key, score, value, callback) {
            calledWithKey = key;
        });
        
        harrison.createTask(randomTask());

        beforeExit(function() {
            assert.equal(harrison._namespace("queue"), calledWithKey);
        });
    },
    "should form a task definition when creating tasks": function(assert, beforeExit) {
        var harrison = new Harrison(randomString());
        
        var taskId;
        var taskDef;

        var task = randomTask();
        
        harrison.createTask(task, function(task) {
            taskId = task.id;
            harrison._getDefinition(task.id, function(def) {
                taskDef = def;
            });
        });

        beforeExit(function() {
            assert.equal(taskId, taskDef.id);
            assert.equal(task['class'], taskDef['class']);
            assert.eql(task.args, taskDef.args);
        });
    },
    "update definition should store an internal definition": function(assert, beforeExit) {
        var harrison = new Harrison(randomString());

        var taskDef;

        var task = randomTask();
        task.id = 42;

        harrison._updateDefinition(task, function() {
            harrison._getDefinition(task.id, function(def) {
                taskDef = def;
            });
        });

        beforeExit(function() {
            assert.equal(task.id, taskDef.id);
        });
    },
    "_getDefinitions should load multiple task definitions": function(assert, beforeExit) {
        var harrison = new Harrison(randomString());

        var tasks = [];
        var taskIds = [];
        var taskDefs = [];

        var insertCount = 0;

        for (var i = 0; i < 5; i++) {
            tasks.push(randomTask());
        }

        var getDefs = function() {
            harrison._getDefinitions(taskIds.slice(0, 2), function(defs) {
                taskDefs = defs;
            });
        }.barrier(tasks.length);

        tasks.forEach(function(task) {
            var taskId;
            do {
                taskId = Math.floor(Math.random() * 10);
            } while (taskIds.indexOf(taskId) >= 0);

            taskIds.push(taskId);
            task.id = taskId;

            harrison._updateDefinition(task, getDefs);
        });

        beforeExit(function() {
            assert.eql(tasks.slice(0, 2), taskDefs);
        });
    },
    "_getDefinition should load individual task definitions": function(assert, beforeExit) {
        var harrison = new Harrison(randomString());

        var task = randomTask();
        var taskDef;
        task.id = Math.floor(Math.random() * 10);

        harrison._updateDefinition(task, function() {
            harrison._getDefinition(task.id, function(def) {
                taskDef = def;
            });
        });

        beforeExit(function() {
            assert.eql(task, taskDef);
        });
    },
    "_getDefinitions should handle empty task definitions gracefully": function(assert, beforeExit) {
        var harrison = new Harrison(randomString());

        var taskDefs = [];

        harrison._getDefinitions(Math.floor(Math.random() * 10), function(defs) {
            taskDefs = defs;
        });

        beforeExit(function() {
            assert.eql([], taskDefs);
        });
    },
    "hash function should only consider public fields": function(assert) {
        var harrison = new Harrison(randomString());

        var task = randomTask();
        var hash = harrison._hash(task);

        task.id = "1234";

        assert.equal(hash, harrison._hash(task));
    },
    "shutdown should close the Redis client connection": function(assert, beforeExit) {
        pending(function() {
            var harrison = new Harrison(randomString());

            var closed = false;

            replaceClientMethod(harrison, 'close', function(client, method) {
                closed = true;
                method.apply(client);
            });

            // open a client connection
            harrison._getClient();
            harrison.shutdown();
            console.log("shut down");

            beforeExit(function() {
                assert.ok(closed);
            });
        });
    },
    "_setFailureAttempts should update the task definition": function(assert, beforeExit) {
        var harrison = new Harrison(randomString());

        var taskDef;
        var attempts = 5;

        harrison.createTask(randomTask(), function(task) {
            harrison._setFailureAttempts(task.id, attempts, function() {
                harrison._getDefinition(task.id, function(def) {
                    taskDef = def;
                });
            });
        });

        beforeExit(function() {
            assert.equal(attempts, taskDef.attempts);
        });
    },
    "_incrementFailureAttempts should update the task definition": function(assert, beforeExit) {
        var harrison = new Harrison(randomString());

        var taskDef;
        var attempts = 5;

        harrison.createTask(randomTask(), function(task) {
            harrison._setFailureAttempts(task.id, attempts, function() {
                harrison._incrementFailureAttempts(task.id, function() {
                    harrison._getDefinition(task.id, function(def) {
                        taskDef = def;
                    });
                });
            });
        });

        beforeExit(function() {
            assert.equal(attempts + 1, taskDef.attempts);
        });
    },
    "_queueTask should update the task definition with 'queuedAt'": function(assert, beforeExit) {
        var harrison = new Harrison(randomString());

        var taskDef;
        var queuedAt = new Date().getTime();

        harrison._createDefinition(randomTask(), function(task) {
            harrison._queueTask(task, null, function() {
                harrison._getDefinition(task.id, function(def) {
                    taskDef = def;
                });
            });
        });

        beforeExit(function() {
            // queuedAt should be within 500ms
            assert.almostEqual(queuedAt, Date.parse(taskDef.queuedAt), 500);
        });
    },
    "_queueTask should update the task definition with 'firstQueuedAt' if it wasn't already set": function(assert, beforeExit) {
        var harrison = new Harrison(randomString());

        var taskDef;
        var queuedAt = new Date();

        harrison._createDefinition(randomTask, function(task) {
            harrison._queueTask(task, null, function() {
                harrison._getDefinition(task.id, function(def) {
                    taskDef = def;
                });
            });
        });

        beforeExit(function() {
            assert.almostEqual(queuedAt, Date.parse(taskDef.firstQueuedAt), 500);
        });
    },
    "_queueTask should leave 'firstQueuedAt' alone if it was already set": function(assert, beforeExit) {
        var harrison = new Harrison(randomString());

        var taskDef;
        var firstQueuedAt = new Date(Math.floor(Math.random() * new Date().getTime())).toISOString();
        var task = randomTask();
        task.firstQueuedAt = firstQueuedAt;

        harrison._createDefinition(task, function(task) {
            harrison._queueTask(task, null, function() {
                harrison._getDefinition(task.id, function(def) {
                    taskDef = def;
                });
            });
        });

        beforeExit(function() {
            assert.equal(firstQueuedAt, taskDef.firstQueuedAt);
        });
    },
    "_queueTask should update the task definition with 'scheduledFor'": function(assert, beforeExit) {
        var harrison = new Harrison(randomString());

        var taskDef;
        var scheduledFor = new Date(Math.floor(Math.random() * new Date().getTime()));

        harrison._createDefinition(randomTask(), function(task) {
            harrison._queueTask(task, scheduledFor, function() {
                harrison._getDefinition(task.id, function(def) {
                    taskDef = def;
                });
            });
        });

        beforeExit(function() {
            assert.equal(scheduledFor.toISOString(), taskDef.scheduledFor);
        });
    },
    "_queueTask should update the task definition with 'firstScheduledFor' if it wasn't already set": function(assert, beforeExit) {
        var harrison = new Harrison(randomString());

        var taskDef;
        var scheduledFor = new Date(Math.floor(Math.random() * new Date().getTime()));

        harrison._createDefinition(randomTask(), function(task) {
            harrison._queueTask(task, scheduledFor, function() {
                harrison._getDefinition(task.id, function(def) {
                    taskDef = def;
                });
            });
        });

        beforeExit(function() {
            assert.equal(scheduledFor.toISOString(), taskDef.firstScheduledFor);
        });
    },
    "_queueTask should leave 'firstScheduledFor' alone if it was already set": function(assert, beforeExit) {
        var harrison = new Harrison(randomString());

        var taskDef;
        var scheduledFor = new Date(Math.floor(Math.random() * new Date().getTime()));
        var firstScheduledFor = new Date().toISOString();
        var task = randomTask();
        task.firstScheduledFor = firstScheduledFor;

        harrison._createDefinition(task, function(task) {
            harrison._queueTask(task, scheduledFor, function() {
                harrison._getDefinition(task.id, function(def) {
                    taskDef = def;
                });
            });
        });

        beforeExit(function() {
            assert.equal(firstScheduledFor, taskDef.firstScheduledFor);
        });
    },
    "_queueTask should put tasks scheduled for the future in the 'reservoir'": function(assert, beforeExit) {
        var harrison = new Harrison(randomString());

        var taskId;
        var queue;
        var futureTasks;
        var scheduledFor = new Date(new Date().getTime() + (10 * 60 * 1000)); // 10 minutes from now

        harrison._createDefinition(randomTask(), function(task) {
            taskId = task.id;
            harrison._queueTask(task, scheduledFor, function() {
                harrison._getQueuedTasks(0, 0, function(tasks) {
                    queue = tasks;
                });

                harrison._getFutureTasks(0, 0, function(tasks) {
                    futureTasks = tasks;
                });
            });
        });

        beforeExit(function() {
            assert.eql([], queue);
            assert.equal(taskId, futureTasks[0][0]);
        });
    },
    "when executing a task, its state should be set to 'running'": function(assert, beforeExit) {
        var harrison = new Harrison(randomString());

        var taskDef;
        var task = randomTask();

        harrison._runTask = function(task, callback) {
            harrison._getDefinition(task.id, function(def) {
                taskDef = def;
            });
            callback(task, true);
        };

        harrison.createTask(task, function(taskId) {
            harrison._executeTask(task);
        });

        beforeExit(function() {
            assert.equal("running", taskDef.state);
        });
    },
    "when executing a task for the first time, 'firstRunAt' should be set": function(assert, beforeExit) {
        var harrison = new Harrison(randomString());

        var taskDef;
        var firstRunAt = new Date();
        var task = randomTask();

        harrison._runTask = function(task, callback) {
            harrison._getDefinition(task.id, function(def) {
                taskDef = def;
            });
            callback(task, true);
        };

        harrison.createTask(task, function(taskId) {
            harrison._executeTask(task);
        });

        beforeExit(function() {
            assert.almostEqual(firstRunAt, Date.parse(taskDef.firstRunAt), 500);
        });
    },
    "when executing a task that was previously run, 'firstRunAt' should be left alone": function(assert, beforeExit) {
        var harrison = new Harrison(randomString());

        var taskDef;
        var firstRunAt = new Date(Math.floor(Math.random() * new Date().getTime())).toISOString();
        var task = randomTask();
        task.firstRunAt = firstRunAt;

        harrison._runTask = function(task, callback) {
            harrison._getDefinition(task.id, function(def) {
                taskDef = def;
            });
            callback(task, true);
        };

        harrison.createTask(task, function(taskId) {
            harrison._executeTask(task);
        });

        beforeExit(function() {
            assert.equal(firstRunAt, taskDef.firstRunAt);
        });
    },
    "when executing a task, 'lastRunAt' should be set": function(assert, beforeExit) {
        var harrison = new Harrison(randomString());

        var taskDef;
        var lastRunAt = new Date();
        var task = randomTask();

        harrison._runTask = function(task, callback) {
            harrison._getDefinition(task.id, function(def) {
                taskDef = def;
            });
            callback(task, true);
        };

        harrison.createTask(task, function(taskId) {
            harrison._executeTask(task);
        });

        beforeExit(function() {
            assert.almostEqual(lastRunAt, Date.parse(taskDef.lastRunAt), 500);
        });
    },
    "when a task returns, it should have 'lastRunBy' set": function(assert, beforeExit) {
        // TODO
    },
    "when tasks execute successfully, they should be removed from the 'tasks' set": function(assert, beforeExit) {
        var harrison = new Harrison(randomString());

        var removedKeys = [];

        var task = randomTask();

        replaceClientMethod(harrison, 'srem', function(client, method, key, value, callback) {
            removedKeys.push(key);
        });

        harrison._runTask = successTask;

        harrison.createTask(task, function() {
            harrison._executeTask(task);
        });

        beforeExit(function() {
            assert.ok(removedKeys.indexOf(harrison._namespace("tasks")) >= 0);
        });
    },
    "when tasks execute successfully, they should be removed from the 'pending' set": function(assert, beforeExit) {
        var harrison = new Harrison(randomString());

        var pendingCount;

        var task = randomTask();

        harrison._runTask = successTask;

        harrison.createTask(task, function() {
            harrison._executeTask(task, function() {
                harrison.getPendingCount(function(count) {
                    pendingCount = count;
                });
            });
        });

        beforeExit(function() {
            assert.equal(0, pendingCount);
        });
    },
    "when tasks execute successfully, their task definition should be removed": function(assert, beforeExit) {
        var harrison = new Harrison(randomString());
        harrison._runTask = successTask;

        var taskDef;
        var task = randomTask();

        harrison.createTask(task, function(task) {
            harrison._executeTask(task, function() {
                harrison._getDefinition(task.id, function(def) {
                    taskDef = def;
                });
            });
        });

        beforeExit(function() {
            assert.isNull(taskDef);
        });
    },
    "task execution should be limited to N simultaneous tasks": function(assert) {
        // TODO
    },
    "when tasks fail, their task definitions should remain": function(assert, beforeExit) {
        var harrison = new Harrison(randomString());

        var taskDef;

        var task = randomTask();

        harrison._runTask = failTask;

        harrison.createTask(task, function() {
            harrison._executeTask(task, function() {
                harrison._getDefinition(task.id, function(def) {
                    taskDef = def;
                });
            });
        });

        beforeExit(function() {
            assert.ok(taskDef);
        });
    },
    "when tasks fail, their state should be set to 'error'": function(assert, beforeExit) {
        var harrison = new Harrison(randomString());

        var taskDef;
        var task = randomTask();

        harrison._runTask = failTask;

        harrison.createTask(task, function(task) {
            harrison._executeTask(task, function() {
                harrison._getDefinition(task.id, function(def) {
                    taskDef = def;
                });
            });
        });

        beforeExit(function() {
            assert.equal("error", taskDef.state);
        });
    },
    "when tasks fail, they should be removed from the 'pending' set": function(assert, beforeExit) {
        var harrison = new Harrison(randomString());

        var pendingCount;

        var task = randomTask();

        harrison._runTask = failTask;

        harrison.createTask(task, function() {
            harrison._executeTask(task, function() {
                harrison.getPendingCount(function(count) {
                    pendingCount = count;
                });
            });
        });

        beforeExit(function() {
            assert.equal(0, pendingCount);
        });
    },
    "when tasks fail, they should be added to the 'reservoir', scheduled in the future": function(assert, beforeExit) {
        var harrison = new Harrison(randomString());

        var added = [];
        var task = randomTask();

        harrison._runTask = failTask;

        harrison.createTask(task, function() {
            replaceClientMethod(harrison, 'zadd', function(client, method, key, score, value, callback) {
                added.push([key, score, value]);
            });

            harrison._executeTask(task);
        });

        beforeExit(function() {
            added = added.filter(function(x) {
                return x[0] === harrison._namespace('reservoir');
            });

            assert.equal(1, added.length);
            assert.ok(new Date().getTime() < added[0][1]);
            assert.equal(task.id, added[0][2]);
        });
    },
    "when tasks fail, their error message should be set in the 'errors:<id>' string value": function(assert, beforeExit) {
        var harrison = new Harrison(randomString());

        var lastError;
        var errorString = randomString();

        var task = randomTask();

        harrison._runTask = function(task, callback) {
            // simulate a failed test
            callback(task, false, errorString);
        };

        harrison.createTask(task, function() {
            harrison._executeTask(task, function(task) {
                harrison.getLastError(task.id, function(error) {
                    lastError = error;
                });
            });
        });

        beforeExit(function() {
            assert.equal(errorString, lastError);
        });
    },
    "when tasks fail for the first time, they should be added to the 'failed' set with an attempt count of 1": function(assert, beforeExit) {
        var harrison = new Harrison(randomString());

        var failedTasks = [];
        var task = randomTask();

        harrison._runTask = failTask;

        harrison.createTask(task, function() {
            harrison._executeTask(task, function() {
                harrison.getFailedTasks(0, 0, function(tasks) {
                    failedTasks = tasks;
                });
            });
        });

        beforeExit(function() {
            assert.equal(1, failedTasks.length);
            assert.equal(task.id, failedTasks[0][0]);
            assert.equal(1, failedTasks[0][1]);
        });
    },
    "when tasks fail more than once, they should be scheduled according to the number of failures": function(assert) {
        // TODO
    },
    "when tasks fail and subsequently succeed, their error message in 'errors:<id>' should be cleared": function(assert, beforeExit) {
        var harrison = new Harrison(randomString());

        var error;
        var attempts = 5;
        var task = randomTask();

        harrison._runTask = successTask;

        harrison.createTask(task, function() {
            harrison._updateError(task.id, randomString(), function() {
                harrison._setFailureAttempts(task.id, attempts, function() {
                    harrison._executeTask(task, function() {
                        harrison.getLastError(task.id, function(err) {
                            error = err;
                        });
                    });
                });
            });
        });

        beforeExit(function() {
            assert.isNull(error);
        });
    },
    "when tasks fail subsequently, their attempt count in the 'failed' set should be incremented": function(assert, beforeExit) {
        var harrison = new Harrison(randomString());

        var failedTasks = [];
        var attempts = 5;
        var task = randomTask();
        task.attempts = attempts;

        harrison._runTask = failTask;

        harrison.createTask(task, function() {
            harrison._setFailureAttempts(task.id, attempts, function() {
                harrison._executeTask(task, function() {
                    harrison.getFailedTasks(0, 0, function(tasks) {
                        failedTasks = tasks;
                    });
                });
            });
        });

        beforeExit(function() {
            assert.equal(1, failedTasks.length);
            assert.equal(task.id, failedTasks[0][0]);
            assert.equal(attempts + 1, failedTasks[0][1]);
        });
    },
    "when tasks fail for the Nth time, they should be added to the 'error' set": function(assert, beforeExit) {
        var harrison = new Harrison(randomString());

        var erroredTasks;
        var attempts = _harrison.MAX_RETRIES;
        var task = randomTask();
        // TODO _setFailureAttempts() etc. should handle this
        task.attempts = attempts;

        harrison._runTask = failTask;

        harrison.createTask(task, function() {
            harrison._setFailureAttempts(task.id, attempts, function() {
                harrison._executeTask(task, function() {
                    harrison.getErroredOutTasks(0, 0, function(tasks) {
                        erroredTasks = tasks;
                    });
                });
            });
        });

        beforeExit(function() {
            assert.equal(1, erroredTasks.length);
            assert.equal(task.id, erroredTasks[0][0]);
        });
    },
    "when tasks fail for the Nth time, they should be removed from the 'pending' set": function(assert, beforeExit) {
        var harrison = new Harrison(randomString());

        var pendingCount;

        var attempts = _harrison.MAX_RETRIES;
        var task = randomTask();
        task.attempts = attempts;

        harrison._runTask = failTask;

        harrison.createTask(task, function() {
            harrison._setFailureAttempts(task.id, attempts, function() {
                harrison._executeTask(task, function() {
                    harrison.getPendingCount(function(count) {
                        pendingCount = count;
                    });
                });
            });
        });

        beforeExit(function() {
            assert.equal(0, pendingCount);
        });
    },
    "when tasks fail for the Nth time, their state should be set to 'failed'": function(assert, beforeExit) {
        var harrison = new Harrison(randomString());

        var taskDef;
        var attempts = _harrison.MAX_RETRIES;
        var task = randomTask();
        task.attempts = attempts;

        harrison._runTask = failTask;

        harrison.createTask(task, function(task) {
            harrison._setFailureAttempts(task.id, attempts, function() {
                harrison._executeTask(task, function() {
                    harrison._getDefinition(task.id, function(def) {
                        taskDef = def;
                    });
                });
            });
        });

        beforeExit(function() {
            assert.equal("failed", taskDef.state);
        });
    },
    "when tasks fail, their error message should be set in the 'errors:<id>' string value": function(assert, beforeExit) {
        var harrison = new Harrison(randomString());

        var lastError;
        var errorString = randomString();

        var attempts = _harrison.MAX_RETRIES;
        var task = randomTask();
        task.attempts = attempts;

        harrison._runTask = function(task, callback) {
            // simulate a failed test
            callback(task, false, errorString);
        };

        harrison.createTask(task, function() {
            harrison._setFailureAttempts(task.id, attempts, function() {
                harrison._executeTask(task, function(task) {
                    harrison.getLastError(task.id, function(error) {
                        lastError = error;
                    });
                });
            });
        });

        beforeExit(function() {
            assert.equal(errorString, lastError);
        });
    },
    "when tasks fail for the Nth time, they should be removed from the 'failed' set": function(assert, beforeExit) {
        var harrison = new Harrison(randomString());

        var failedTasks;
        var attempts = _harrison.MAX_RETRIES;
        var task = randomTask();
        task.attempts = attempts;

        harrison._runTask = function(task, callback) {
            // simulate a failed test
            callback(task, false);
        };

        harrison.createTask(task, function() {
            harrison._setFailureAttempts(task.id, attempts, function() {
                harrison._executeTask(task, function() {
                    harrison.getFailedTasks(0, 0, function(tasks) {
                        failedTasks = tasks;
                    });
                });
            });
        });

        beforeExit(function() {
            assert.equal(0, failedTasks.length);
        });
    }
};
