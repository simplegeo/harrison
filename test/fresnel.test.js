var assert = require('assert'),
    sys = require('sys'),
    redis = require('redis-client'),
    EventEmitter = require('events').EventEmitter;

var _fresnel = require('fresnel');
var Fresnel = _fresnel.Fresnel;

_fresnel.AUTO_CLOSE = true;

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

function replaceClientMethod(fresnel, method, func) {
    var _getClient = fresnel._getClient;

    fresnel._getClient = function() {
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
            fresnel.bufferTasks();
        });

        beforeExit(function() {
            assert.equal(task.toString(), fresnel.BUFFERED_TASKS[0].toString());
        });
    },
    "should remove buffered tasks from the 'queue' set": function(assert, beforeExit) {
        var fresnel = new Fresnel(randomString());

        var queueLength;

        fresnel.createTask(randomTask(), function() {
            fresnel.bufferTasks(function() {
                fresnel.getUnbufferedQueueLength(function(length) {
                    queueLength = length;
                });
            });
        });

        beforeExit(function() {
            assert.equal(0, queueLength);
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
    "should yield the task id when creating a task": function(assert, beforeExit) {
        var fresnel = new Fresnel(randomString());

        var taskId;

        fresnel._generateId(function() {
            // task will be assigned the 2nd generated id
            fresnel.createTask(randomTask(), function(id) {
                taskId = id;
            });
        });

        beforeExit(function() {
            assert.equal(2, taskId);
        });
    },
    "shouldn't fail when creating a duplicate task with no callback": function(assert) {
        var fresnel = new Fresnel(randomString());

        var task = randomTask();

        fresnel.createTask(task, function() {
            fresnel.createTask(task);
        });
    },
    "should yield 'false' when creating a duplicate task": function(assert, beforeExit) {
        var fresnel = new Fresnel(randomTask());

        var taskId;
        var task = randomTask();

        fresnel.createTask(task, function() {
            fresnel.createTask(task, function(id) {
                taskId = id;
            });
        });

        beforeExit(function() {
            assert.equal(false, taskId);
        });
    },
    "should add to the 'tasks' set when creating tasks": function(assert, beforeExit) {
        var fresnel = new Fresnel(randomString());
        
        var calledWithKey;

        replaceClientMethod(fresnel, 'sadd', function(client, method, key, value, callback) {
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

        replaceClientMethod(fresnel, 'zadd', function(client, method, key, score, value, callback) {
            calledWithKey = key;
        });
        
        fresnel.createTask(randomTask());

        beforeExit(function() {
            assert.equal(fresnel._namespace("queue"), calledWithKey);
        });
    },
    "should form a task definition when creating tasks": function(assert, beforeExit) {
        var fresnel = new Fresnel(randomString());
        
        var taskDef;

        var task = randomTask();
        
        fresnel.createTask(task, function() {
            fresnel._getDefinition(task.id, function(def) {
                taskDef = def;
            });
        });

        beforeExit(function() {
            assert.eql(task, taskDef);
        });
    },
    "update definition should store an internal definition": function(assert, beforeExit) {
        var fresnel = new Fresnel(randomString());

        var taskDef;

        var task = randomTask();
        task.id = 42;

        fresnel._updateDefinition(task, function() {
            fresnel._getDefinition(task.id, function(def) {
                taskDef = def;
            });
        });

        beforeExit(function() {
            assert.equal(task.id, taskDef.id);
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
    "_getDefinition should load individual task definitions": function(assert, beforeExit) {
        var fresnel = new Fresnel(randomString());

        var task = randomTask();
        var taskDef;
        task.id = Math.floor(Math.random() * 10);

        fresnel._updateDefinition(task, function() {
            fresnel._getDefinition(task.id, function(def) {
                taskDef = def;
            });
        });

        beforeExit(function() {
            assert.eql(task, taskDef);
        });
    },
    "_getDefinitions should handle empty task definitions gracefully": function(assert, beforeExit) {
        var fresnel = new Fresnel(randomString());

        var taskDefs = [];

        fresnel._getDefinitions(Math.floor(Math.random() * 10), function(defs) {
            taskDefs = defs;
        });

        beforeExit(function() {
            assert.eql([], taskDefs);
        });
    },
    "hash function should only consider public fields": function(assert) {
        var fresnel = new Fresnel(randomString());

        var task = randomTask();
        var hash = fresnel._hash(task);

        task.id = "1234";

        assert.equal(hash, fresnel._hash(task));
    },
    "shutdown should close the Redis client connection": function(assert, beforeExit) {
        pending(function() {
            var fresnel = new Fresnel(randomString());

            var closed = false;

            replaceClientMethod(fresnel, 'close', function(client, method) {
                closed = true;
                method.apply(client);
            });

            // open a client connection
            fresnel._getClient();
            fresnel.shutdown();
            console.log("shut down");

            beforeExit(function() {
                assert.ok(closed);
            });
        });
    },
    "_setFailureAttempts should update the task definition": function(assert, beforeExit) {
        var fresnel = new Fresnel(randomString());

        var taskDef;
        var attempts = 5;

        fresnel.createTask(randomTask(), function(taskId) {
            fresnel._setFailureAttempts(taskId, attempts, function() {
                fresnel._getDefinition(taskId, function(def) {
                    taskDef = def;
                });
            });
        });

        beforeExit(function() {
            assert.equal(attempts, taskDef.attempts);
        });
    },
    "_incrementFailureAttempts should update the task definition": function(assert, beforeExit) {
        var fresnel = new Fresnel(randomString());

        var taskDef;
        var attempts = 5;

        fresnel.createTask(randomTask(), function(taskId) {
            fresnel._setFailureAttempts(taskId, attempts, function() {
                fresnel._incrementFailureAttempts(taskId, function() {
                    fresnel._getDefinition(taskId, function(def) {
                        taskDef = def;
                    });
                });
            });
        });

        beforeExit(function() {
            assert.equal(attempts + 1, taskDef.attempts);
        });
    },
    "when tasks execute successfully, they should be removed from the 'tasks' set": function(assert, beforeExit) {
        var fresnel = new Fresnel(randomString());

        var removedKeys = [];

        var task = randomTask();

        replaceClientMethod(fresnel, 'srem', function(client, method, key, value, callback) {
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

        replaceClientMethod(fresnel, 'srem', function(client, method, key, value, callback) {
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

        replaceClientMethod(fresnel, 'del', function(client, method, key, callback) {
            removedKeys.push(key);
        });

        fresnel.createTask(task, function() {
            fresnel._executeTask(task);
        });

        beforeExit(function() {
            assert.ok(removedKeys.indexOf(fresnel._namespace("tasks:" + task.id)) >= 0);
        });
    },
    "when tasks fail, their task definitions should remain": function(assert, beforeExit) {
        var fresnel = new Fresnel(randomString());

        var taskDef;

        var task = randomTask();

        fresnel._runTask = function(task, callback) {
            // simulate a failed test
            callback(task, false);
        }

        fresnel.createTask(task, function() {
            fresnel._executeTask(task, function() {
                fresnel._getDefinition(task.id, function(def) {
                    taskDef = def;
                });
            });
        });

        beforeExit(function() {
            assert.ok(taskDef);
        });
    },
    "when tasks fail, they should be removed from the 'pending' set": function(assert, beforeExit) {
        var fresnel = new Fresnel(randomString());

        var pendingCount;
        var removedKeys = [];

        var task = randomTask();

        replaceClientMethod(fresnel, 'srem', function(client, method, key, value, callback) {
            removedKeys.push(key);
        });

        fresnel._runTask = function(task, callback) {
            // simulate a failed test
            callback(task, false);
        }

        fresnel.createTask(task, function() {
            fresnel._executeTask(task, function() {
                fresnel.getPendingCount(function(count) {
                    pendingCount = count;
                });
            });
        });

        beforeExit(function() {
            assert.equal(0, pendingCount);
            assert.ok(removedKeys.indexOf(fresnel._namespace("pending")) >= 0);
        });
    },
    "when tasks fail, they should be added to the 'reservoir', scheduled in the future": function(assert, beforeExit) {
        var fresnel = new Fresnel(randomString());

        var added = [];
        var task = randomTask();

        fresnel._runTask = function(task, callback) {
            // simulate a failed test
            callback(task, false);
        }

        fresnel.createTask(task, function() {
            replaceClientMethod(fresnel, 'zadd', function(client, method, key, score, value, callback) {
                added.push([key, score, value]);
            });

            fresnel._executeTask(task);
        });

        beforeExit(function() {
            added = added.filter(function(x) {
                return x[0] == fresnel._namespace('reservoir');
            });

            assert.equal(1, added.length);
            assert.ok(new Date().getTime() < added[0][1]);
            assert.equal(task.id, added[0][2]);
        });
    },
    "when tasks fail, their error message should be set in the 'errors:<id>' string value": function(assert, beforeExit) {
        var fresnel = new Fresnel(randomString());

        var lastError;
        var errorString = randomString();

        var task = randomTask();

        fresnel._runTask = function(task, callback) {
            // simulate a failed test
            callback(task, false, errorString);
        }

        fresnel.createTask(task, function() {
            fresnel._executeTask(task, function(task) {
                fresnel.getLastError(task.id, function(error) {
                    lastError = error;
                });
            });
        });

        beforeExit(function() {
            assert.equal(errorString, lastError);
        });
    },
    "when tasks fail for the first time, they should be added to the 'failed' set with an attempt count of 1": function(assert, beforeExit) {
        var fresnel = new Fresnel(randomString());

        var failedTasks = [];
        var task = randomTask();

        fresnel._runTask = function(task, callback) {
            // simulate a failed test
            callback(task, false);
        }

        fresnel.createTask(task, function() {
            fresnel._executeTask(task, function() {
                fresnel.getFailedTasks(function(tasks) {
                    failedTasks = tasks;
                });
            });
        });

        beforeExit(function() {
            assert.equal(1, failedTasks.length);
            assert.equal(task.id, failedTasks[0][0]);
            assert.equal(1, failedTasks[0][1]);
        });
    "when tasks fail and subsequently succeed, their error message in 'errors:<id>' should be cleared": function(assert, beforeExit) {
        var fresnel = new Fresnel(randomString());

        var removed = [];
        var attempts = 5;
        var task = randomTask();

        replaceClientMethod(fresnel, 'del', function(client, method, key, callback) {
            removed.push(key);
        });

        fresnel.createTask(task, function() {
            fresnel._updateError(task.id, randomString(), function() {
                fresnel._setFailureAttempts(task.id, attempts, function() {
                    fresnel._executeTask(task);
                });
            });
        });

        beforeExit(function() {
            removed = removed.filter(function(x) {
                return x == fresnel._namespace("errors:" + task.id);
            });

            assert.equal(1, removed.length);
        });
    },
    "when tasks fail subsequently, their attempt count in the 'failed' set should be incremented": function(assert, beforeExit) {
        var fresnel = new Fresnel(randomString());

        var failedTasks = [];
        var attempts = 5;
        var task = randomTask();
        task.attempts = attempts;

        fresnel._runTask = function(task, callback) {
            // simulate a failed test
            callback(task, false);
        }

        fresnel.createTask(task, function() {
            fresnel._setFailureAttempts(task.id, attempts, function() {
                fresnel._executeTask(task, function() {
                    fresnel.getFailedTasks(function(tasks) {
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
        var fresnel = new Fresnel(randomString());

        var erroredTasks;
        var attempts = _fresnel.MAX_RETRIES;
        var task = randomTask();
        // TODO _setFailureAttempts() etc. should handle this
        task.attempts = attempts;

        fresnel._runTask = function(task, callback) {
            // simulate a failed test
            callback(task, false);
        }

        fresnel.createTask(task, function() {
            fresnel._setFailureAttempts(task.id, attempts, function() {
                fresnel._executeTask(task, function() {
                    fresnel.getErroredOutTasks(function(tasks) {
                        erroredTasks = tasks;
                    });
                });
            });
        });

        beforeExit(function() {
            assert.equal(1, erroredTasks.length);
            assert.equal(task.id, erroredTasks[0]);
        });
    },
    "when tasks fail for the Nth time, they should be removed from the 'pending' set": function(assert, beforeExit) {
        var fresnel = new Fresnel(randomString());

        var pendingCount;
        var removedKeys = [];

        var attempts = _fresnel.MAX_RETRIES;
        var task = randomTask();
        task.attempts = attempts;

        replaceClientMethod(fresnel, 'srem', function(client, method, key, value, callback) {
            removedKeys.push(key);
        });

        fresnel._runTask = function(task, callback) {
            // simulate a failed test
            callback(task, false);
        }

        fresnel.createTask(task, function() {
            fresnel._setFailureAttempts(task.id, attempts, function() {
                fresnel._executeTask(task, function() {
                    fresnel.getPendingCount(function(count) {
                        pendingCount = count;
                    });
                });
            });
        });

        beforeExit(function() {
            assert.equal(0, pendingCount);
            assert.ok(removedKeys.indexOf(fresnel._namespace("pending")) >= 0);
        });
    },
    "when tasks fail, their error message should be set in the 'errors:<id>' string value": function(assert, beforeExit) {
        var fresnel = new Fresnel(randomString());

        var lastError;
        var errorString = randomString();

        var attempts = _fresnel.MAX_RETRIES;
        var task = randomTask();
        task.attempts = attempts;

        fresnel._runTask = function(task, callback) {
            // simulate a failed test
            callback(task, false, errorString);
        }

        fresnel.createTask(task, function() {
            fresnel._setFailureAttempts(task.id, attempts, function() {
                fresnel._executeTask(task, function(task) {
                    fresnel.getLastError(task.id, function(error) {
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
        var fresnel = new Fresnel(randomString());

        var failedTasks;
        var attempts = _fresnel.MAX_RETRIES;
        var task = randomTask();
        task.attempts = attempts;

        fresnel._runTask = function(task, callback) {
            // simulate a failed test
            callback(task, false);
        }

        fresnel.createTask(task, function() {
            fresnel._setFailureAttempts(task.id, attempts, function() {
                fresnel._executeTask(task, function() {
                    fresnel.getFailedTasks(function(tasks) {
                        failedTasks = tasks;
                    });
                });
            });
        });

        beforeExit(function() {
            assert.equal(0, failedTasks.length);
        });
    },

    },
}
