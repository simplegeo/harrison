var sys = require('sys'),
    EventEmitter = require('events').EventEmitter,
    hashlib = require('hashlib'),
    redis = require('redis-client');

require('ext/date').extend(Date);
require('ext/function').extend(Function);

exports.AUTO_CLOSE = false; // auto-close Redis connections when drained.
exports.DEFAULT_NAMESPACE = 'fresnel';
exports.BATCH_SIZE = 1;
exports.BASE_RETRY_DELAY = 15 * 1000;
exports.MAX_RETRIES = 10;

/** @constructor */
function Fresnel(namespace) {
    EventEmitter.call(this);

    this.namespace = namespace || exports.DEFAULT_NAMESPACE;
    this.BUFFERED_TASKS = [];
}

sys.inherits(Fresnel, EventEmitter);
exports.Fresnel = Fresnel;

/**
 * Buffer tasks from Redis into a local queue and mark them as pending.
 */
Fresnel.prototype.bufferTasks = function(callback) {
    var self = this;

    self._getClient().zrange(self._namespace("queue"), 0, exports.BATCH_SIZE - 1, x(function(reply) {
        redis.convertMultiBulkBuffersToUTF8Strings(reply);
        var taskIds = reply;

        if (taskIds) {
            self._getDefinitions(taskIds, function(taskDefs) {
                taskDefs.forEach(function(task) {
                    // add to the local buffer
                    self.BUFFERED_TASKS.unshift(task);
                });

                var onCompletion = callback && callback.barrier(2 * taskIds.length);
                taskIds.forEach(function(taskId) {
                    // add to the set of pending tasks
                    self._getClient().sadd(self._namespace("pending"), taskId, x(function(reply) {
                        // unqueue this task
                        self._unqueueTask(taskId, onCompletion);

                        self._getClient().hmset(self._namespace("tasks:" + taskId), "reservedAt", new Date().toISOString(), "state", "reserved", x(onCompletion));
                    }));
                });
            });
        } else {
            if (callback) {
                callback();
            }
        }
    }));
};

/**
 * Add a new task to the queue.
 */
Fresnel.prototype.createTask = function(task, callback) {
    var self = this;

    self._isQueued(task, function(isQueued) {
        if (isQueued) {
            if (callback) {
                callback(false);
            }
        } else {
            task.state = "ready";
            self._createDefinition(task, function(task) {
                self._queueTask(task, null, function() {
                    if (callback) {
                        callback(task);
                    }
                });
            });
        }
    });
};

/**
 * Get a list of tasks that have exceeded the maximum number of retries.
 */
Fresnel.prototype.getErroredOutTasks = function(callback) {
    this._getClient().zrevrange(this._namespace("error"), 0, 10, x(function(reply) {
        if (!reply) {
            callback([]);
        } else {
            callback(reply);
        }
    }));
};

/**
 * Get a list of tasks that have failed, including the number of failures.
 */
Fresnel.prototype.getFailedTasks = function(callback) {
    this._getClient().zrevrange(this._namespace("failed"), 0, 10, "WITHSCORES", x(function(reply) {
        var tasks = [];

        if (reply) {
            for (var i = 0; i < reply.length; i += 2) {
                tasks.push([reply[i], reply[i + 1]]);
            }
        }

        callback(tasks);
    }));
};

/**
 * Get the last error message for a specified task id.
 */
Fresnel.prototype.getLastError = function(taskId, callback) {
    this._getClient().get(this._namespace("errors:" + taskId), x(callback));
};

/**
 * Get the number of tasks currently marked as pending.
 */
Fresnel.prototype.getPendingCount = function(callback) {
    this._getClient().scard(this._namespace("pending"), x(callback));
};

/**
 * Get the number of tasks that are ready to run and have not already been
 * subsumed into the internal buffer.
 */
Fresnel.prototype.getUnbufferedQueueLength = function(callback) {
    this._getClient().zcard(this._namespace("queue"), x(callback));
};

/**
 * Run tasks that have been buffered locally.
 */
Fresnel.prototype.runBufferedTasks = function() {
    while (this.BUFFERED_TASKS.length > 0) {
        var task = this.BUFFERED_TASKS.pop();
        this._executeTask(task);
    }
};

/**
 * Close outstanding connections and whatnot.
 */
Fresnel.prototype.shutdown = function() {
    if (this._client) {
        this._client.close();
    }
};

/**
 * Create a new task definition.
 */
Fresnel.prototype._createDefinition = function(task, callback) {
    var self = this;

    self._generateId(function(taskId) {
        task.id = taskId;
        self._updateDefinition(task, callback);
    });
};

/**
 * Execute a task.
 *
 * If present, callback will be run when the task has been completed.
 */
Fresnel.prototype._executeTask = function(task, callback) {
    var self = this;

    var runTask = function() {
        self._runTask(task, self._taskCompleted.bind(self, callback));
    }.barrier(2);

    var now = new Date().toISOString();
    self._getClient().hmset(self._namespace("tasks:" + task.id), "state", "running", "lastRunAt", now, x(runTask));
    self._getClient().hsetnx(self._namespace("tasks:" + task.id), "firstRunAt", now, x(runTask));
};

/**
 * Actually run the task.
 */
Fresnel.prototype._runTask = function(task, callback) {
    // TODO implement for real

    // pretend they succeeded for now
    callback(task, true);
};

/**
 * Called when a task has returned.
 */
Fresnel.prototype._taskCompleted = function(callback, task, status, response) {
    var self = this;

    response = response || "";
    var onCompletion;

    if (status) {
        onCompletion = callback && callback.barrier(3, [task, response]);

        self._getClient().srem(self._namespace("tasks"), self._hash(task), x(function() {

            // only delete the task definition if it no longer exists in 'tasks';
            // this way if removal from 'tasks' fails, when it gets re-run, its
            // definition will still be available.

            self._getClient().del(self._namespace("tasks:" + task.id), x(onCompletion));
        }));

        self._getClient().srem(self._namespace("pending"), task.id, x(onCompletion));
        self._getClient().del(self._namespace("errors:" + task.id), x(onCompletion));
    } else {
        if (task.attempts >= exports.MAX_RETRIES) {
            onCompletion = callback && callback.barrier(5, [task, response]);

            self._getClient().hset(self._namespace("tasks:" + task.id), "state", "failed", x(onCompletion));
            self._getClient().zadd(self._namespace("error"), new Date().getTime(), task.id, x(onCompletion));
            self._getClient().zrem(self._namespace("failed"), task.id, x(onCompletion));
        } else {
            onCompletion = callback && callback.barrier(5, [task, response]);
            var scheduledFor = new Date().getTime() + exports.BASE_RETRY_DELAY;

            self._incrementFailureAttempts(task.id, onCompletion);
            self._getClient().hset(self._namespace("tasks:" + task.id), "state", "error", x(onCompletion));
            self._getClient().zadd(self._namespace("reservoir"), scheduledFor, task.id, x(onCompletion));
        }

        self._getClient().srem(self._namespace("pending"), task.id, x(onCompletion));
        self._updateError(task.id, response, onCompletion);
    }
};

/**
 * Generate an id for a new task.
 */
Fresnel.prototype._generateId = function(callback) {
    this._getClient().incr(this._namespace("next.task.id"), x(callback));
};

/**
 * Get a Redis client connection.
 */
Fresnel.prototype._getClient = function() {
    if (!this._client) {
        this._client = redis.createClient();
        if (exports.AUTO_CLOSE) {
            this._client.addListener("drained", function() {
                this.close();
            });
        }
    }

    return this._client;
};

/**
 * Get the task definition associated with a particular task id.
 */
Fresnel.prototype._getDefinition = function(taskId, callback) {
    this._getClient().hgetall(this._namespace("tasks:" + taskId), x(function(reply) {
        redis.convertMultiBulkBuffersToUTF8Strings(reply);

        if (reply && reply.args === null) {
            reply.args = [];
        }

        callback(reply);
    }));
};

/**
 * Get task definitions for a list of task ids.
 */
Fresnel.prototype._getDefinitions = function(taskIds, callback) {
    var self = this;

    if (!Array.isArray(taskIds)) {
        taskIds = [taskIds];
    }

    var taskDefs = [];
    var onCompletion = callback && callback.barrier(taskIds.length, function(cb) {
        cb(taskDefs.filter(function(def) {
            return def !== null;
        }));
    });

    taskIds.forEach(function(taskId) {
        // TODO add this collection pattern into Function.barrier
        self._getDefinition(taskId, function(taskDef) {
            // collect definitions into taskDefs
            taskDefs.push(taskDef);

            // the callback, with an added barrier
            onCompletion();
        });
    });
};

/**
 * Get a list of tasks from the reservoir.
 */
Fresnel.prototype._getFutureTasks = function(count, callback) {
    this._getClient().zrange(this._namespace("reservoir"), 0, count, "WITHSCORES", x(function(reply) {
        var tasks = [];

        if (reply) {
            redis.convertMultiBulkBuffersToUTF8Strings(reply);
            for (var i = 0; i < reply.length; i += 2) {
                tasks.push([reply[i], reply[i + 1]]);
            }
        }

        callback(tasks);
    }));
};

/**
 * Get a list of queued tasks.
 */
Fresnel.prototype._getQueuedTasks = function(count, callback) {
    this._getClient().zrevrange(this._namespace("queue"), 0, count, "WITHSCORES", x(function(reply) {
        var tasks = [];

        if (reply) {
            redis.convertMultiBulkBuffersToUTF8Strings(reply);
            for (var i = 0; i < reply.length; i += 2) {
                tasks.push([reply[i], reply[i + 1]]);
            }
        }

        callback(tasks);
    }));
};

/**
 * Generate a hash for a public-facing task definition.
 */
Fresnel.prototype._hash = function(task) {
    var publicTask = {
        "class": task['class'],
        "args": task.args
    };
    return hashlib.sha1(JSON.stringify(publicTask));
};

/**
 * Increment the number of failures for a specific task id.
 */
Fresnel.prototype._incrementFailureAttempts = function(taskId, callback) {
    var onCompletion = callback && callback.barrier(2);

    this._getClient().zincrby(this._namespace("failed"), 1, taskId, x(onCompletion));
    this._getClient().hincrby(this._namespace("tasks:" + taskId), "attempts", 1, x(onCompletion));
};

/**
 * Check whether a particular task is already scheduled.
 */
Fresnel.prototype._isQueued = function(task, callback) {
    this._getClient().sismember(this._namespace("tasks"), this._hash(task), x(function(reply) {
        callback(!!reply);
    }));
};

/**
 * Wrap a key with the configured namespace.
 */
Fresnel.prototype._namespace = function(key) {
    return this.namespace + ":" + key;
};

/**
 * Schedule a task.
 *
 * Note: this does not handle associated housekeeping; use createTask() instead.
 */
Fresnel.prototype._queueTask = function(task, scheduledFor, callback) {
    var now = new Date();
    scheduledFor = scheduledFor || now;

    var onCompletion = callback && callback.barrier(5);

    this._getClient().sadd(this._namespace("tasks"), this._hash(task), x(onCompletion));
    this._getClient().hmset(this._namespace("tasks:" + task.id), "scheduledFor", scheduledFor.toISOString(), "queuedAt", now.toISOString(), x(onCompletion));
    this._getClient().hsetnx(this._namespace("tasks:" + task.id), "firstScheduledFor", scheduledFor.toISOString(), x(onCompletion));
    this._getClient().hsetnx(this._namespace("tasks:" + task.id), "firstQueuedAt", now.toISOString(), x(onCompletion));

    if (scheduledFor.getTime() > new Date().getTime() + 60 * 1000) {
        this._getClient().zadd(this._namespace("reservoir"), scheduledFor.getTime(), task.id, x(onCompletion));
    } else {
        this._getClient().zadd(this._namespace("queue"), scheduledFor.getTime(), task.id, x(onCompletion));
    }
};

/**
 * Set the number of attempts a failed task has had.
 *
 * Note: use _incrementFailureAttempts() if that's what you intend to do.
 */
Fresnel.prototype._setFailureAttempts = function(taskId, attempts, callback) {
    var onCompletion = callback && callback.barrier(2);

    this._getClient().zadd(this._namespace("failed"), attempts, taskId, x(onCompletion));
    this._getClient().hset(this._namespace("tasks:" + taskId), "attempts", attempts, x(onCompletion));
};

/**
 * Remove a task from the queue.
 *
 * Note: this does not handle associated housekeeping.
 */
Fresnel.prototype._unqueueTask = function(taskId, callback) {
    this._getClient().zrem(this._namespace("queue"), taskId, x(function(reply) {
        if (callback) {
            callback(!!reply);
        }
    }));
};

/**
 * Insert/update an internal task definition.
 */
Fresnel.prototype._updateDefinition = function(task, callback) {
    // assemble an appropriate set of arguments for an HMSET
    var args = [];
    args.push(this._namespace("tasks:" + task.id));

    for (key in task) {
        args.push(key, task[key]);
    }

    args.push(x(function(reply) {
        if (callback) {
            callback(task, reply);
        }
    }));

    var client = this._getClient();
    client.hmset.apply(client, args);
};

/**
 * Update the error message for a specific task id.
 */
Fresnel.prototype._updateError = function(taskId, error, callback) {
    this._getClient().set(this._namespace("errors:" + taskId), error, x(callback));
};

/**
 * Exception wrapper for Redis errors.
 * @private
 */
function x(callback) {
    return function(err, reply) {
        if (err) {
            throw err;
        }

        if (callback) {
            callback(reply);
        }
    };
}
