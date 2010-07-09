var sys = require('sys'),
    EventEmitter = require('events').EventEmitter,
    hashlib = require('hashlib'),
    redis = require('redis-client');

exports.AUTO_CLOSE = false; // auto-close Redis connections when drained.
exports.DEFAULT_NAMESPACE = 'fresnel';
exports.BATCH_SIZE = 1;

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

                var remaining = taskIds.length;
                taskIds.forEach(function(taskId) {
                    // add to the set of pending tasks
                    self._getClient().sadd(self._namespace("pending"), taskId, x(function(reply) {
                        // unqueue this task
                        self._unqueueTask(taskId, function() {
                            if (--remaining <= 0) {
                                if (callback) {
                                    callback();
                                }
                            }
                        });
                    }));
                });
            });
        } else {
            if (callback) {
                callback();
            }
        }
    }));
}

/**
 * Add a new task to the queue.
 */
Fresnel.prototype.createTask = function(task, callback) {
    var self = this;

    self._isQueued(task, function(isQueued) {
        if (isQueued) {
            if (callback) {
                callback();
            }
        } else {
            self._generateId(function(taskId) {
                self._updateDefinition(taskId, task, function() {
                    self._queueTask(taskId, null, function(queued) {

                        // technically speaking, if queued is true, the insert
                        // should be rolled back to avoid the task running more
                        // than once, but that's complicated and running a task
                        // more than once is fine.
                        self._getClient().sadd(self._namespace("tasks"), self._hash(task), x(callback));
                    });
                });
            });
        }
    });
}

/**
 * Get the number of tasks currently marked as pending.
 */
Fresnel.prototype.getPendingCount = function(callback) {
    this._getClient().scard(this._namespace("pending"), x(callback));
}

/**
 * Get the number of tasks that are ready to run and have not already been
 * subsumed into the internal buffer.
 */
Fresnel.prototype.getUnbufferedQueueLength = function(callback) {
    this._getClient().zcard(this._namespace("queue"), x(callback));
}

/**
 * Run tasks that have been buffered locally.
 */
Fresnel.prototype.runBufferedTasks = function() {
    while (this.BUFFERED_TASKS.length > 0) {
        var task = this.BUFFERED_TASKS.pop();
        this._executeTask(task);
    }
}

/**
 * Close outstanding connections and whatnot.
 */
Fresnel.prototype.shutdown = function() {
    if (this._client) {
        this._client.close();
    }
}

/**
 * Execute a task.
 */
Fresnel.prototype._executeTask = function(task) {
    this._getClient().srem(this._namespace("tasks"), this._hash(task), x());
}

/**
 * Generate an id for a new task.
 */
Fresnel.prototype._generateId = function(callback) {
    this._getClient().incr(this._namespace("next.task.id"), x(callback));
}

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
}

/**
 * Get task definitions for a list of task ids.
 */
Fresnel.prototype._getDefinitions = function(taskIds, callback) {
    var self = this;

    if (!Array.isArray(taskIds)) {
        taskIds = [taskIds];
    }

    self._getClient().mget(taskIds.map(function(taskId) { return self._namespace("tasks:" + taskId); }), x(function(reply) {
        redis.convertMultiBulkBuffersToUTF8Strings(reply);

        var taskDefs = reply.map(function(taskDef) {
            try {
                return JSON.parse(taskDef.toString());
            } catch (e) {
                console.log(e);
                return null;
            }
        });

        callback(taskDefs);
    }));
}

/**
 * Generate a hash for a public-facing task definition.
 */
Fresnel.prototype._hash = function(task) {
    var publicTask = {
        "class": task.class,
        "args": task.args
    }
    return hashlib.sha1(JSON.stringify(publicTask));
}

/**
 * Check whether a particular task is already scheduled.
 */
Fresnel.prototype._isQueued = function(task, callback) {
    this._getClient().sismember(this._namespace("tasks"), this._hash(task), x(function(reply) {
        callback(!!reply);
    }));
}

/**
 * Wrap a key with the configured namespace.
 */
Fresnel.prototype._namespace = function(key) {
    return this.namespace + ":" + key;
}

/**
 * Schedule a task.
 *
 * Note: this does not handle associated housekeeping; use createTask() instead.
 */
Fresnel.prototype._queueTask = function(taskId, timestamp, callback) {
    timestamp = timestamp || new Date().getTime();

    this._getClient().zadd(this._namespace("queue"), timestamp, taskId, x(function(reply) {
        if (callback) {
            callback(!!reply);
        }
    }));
}

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
}

/**
 * Insert/update an internal task definition.
 */
Fresnel.prototype._updateDefinition = function(taskId, task, callback) {
    task['id'] = taskId;
    this._getClient().set(this._namespace("tasks:" + taskId), JSON.stringify(task), x(function(reply) {
        if (callback) {
            callback(taskId, reply);
        }
    }));
}

/**
 * Exception wrapper for Redis errors.
 */
function x(callback) {
    return function(err, reply) {
        if (err) {
            throw err;
        }

        if (callback) {
            callback(reply);
        }
    }
}
