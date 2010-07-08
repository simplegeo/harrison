// buffering waiting tasks into `pending` and Fresnel's local queue

var sys = require('sys'),
    EventEmitter = require('events').EventEmitter,
    hashlib = require('hashlib'),
    redis = require('redis-client');

exports.DEFAULT_NAMESPACE = 'fresnel';

function Fresnel(namespace) {
    EventEmitter.call(this);

    this.namespace = namespace || exports.DEFAULT_NAMESPACE;
    this.BATCH_SIZE = 1;
    this.BUFFERED_TASKS = [];
}

sys.inherits(Fresnel, EventEmitter);
exports.Fresnel = Fresnel;

/**
 * Buffer tasks from Redis into a local queue and mark them as pending.
 */
Fresnel.prototype.bufferTasks = function(callback) {
    var self = this;

    self._getClient().zrange(self._namespace("queue"), 0, this.BATCH_SIZE - 1, function(err, reply) {
        if (err) {
            throw err;
        }

        redis.convertMultiBulkBuffersToUTF8Strings(reply);
        var taskIds = reply;

        self._getDefinitions(taskIds, function(taskDefs) {
            taskDefs.forEach(function(task) {
                // add to the local buffer
                self.BUFFERED_TASKS.unshift(task);
            });

            var remaining = taskIds.length;
            taskIds.forEach(function(taskId) {
                // add to the set of pending tasks
                self._getClient().sadd(self._namespace("pending"), taskId, function(err, reply) {
                    if (err) {
                        throw err;
                    }

                    // unqueue this task
                    self._unqueueTask(taskId, function() {
                        if (--remaining <= 0) {
                            callback();
                        }
                    });
                });
            });
        });
    });
}

/**
 * add a new task to the queue
 */
Fresnel.prototype.createTask = function(task, callback) {
    var self = this;

    self._isQueued(task, function(isQueued) {
        if (isQueued) {
            callback();
        } else {
            self._generateId(function(taskId) {
                self._updateDefinition(taskId, task, function() {
                    self._queueTask(taskId, null, function(queued) {
                        self._getClient().sadd(self._namespace("tasks"), self._hash(task), function(err, reply) {
                            if (err) {
                                throw err;
                            }

                            // technically speaking, if queued is true, the
                            // insert should be rolled back to avoid the task
                            // running more than once, but that's complicated
                            // and running a task more than once is fine.

                            callback();
                        });
                    });
                });
            });
        }
    });
}

Fresnel.prototype.getPendingCount = function(callback) {
    this._getClient().scard(this._namespace("pending"), function(err, reply) {
        if (err) {
            throw err;
        }

        return callback(reply);
    });
}

/**
 * Close outstanding connections and whatnot.
 */
Fresnel.prototype.shutdown = function() {
    if (this._client) {
        this._client.close();
    }
}

Fresnel.prototype._isQueued = function(task, callback) {
    this._getClient().sismember(this._namespace("tasks"), this._hash(task), function(err, reply) {
        if (err) {
            throw err;
        }

        callback(!!reply);
    });
}

Fresnel.prototype._generateId = function(callback) {
     this._getClient().incr(this._namespace("next.task.id"), function(err, reply) {
        if (err) {
            throw err;
        }

        callback(reply);
    });
}

/**
 * Get a Redis client connection.
 */
Fresnel.prototype._getClient = function() {
    if (!this._client) {
        this._client = redis.createClient();
    }

    return this._client;
}

Fresnel.prototype._getDefinitions = function(taskIds, callback) {
    var self = this;

    if (!Array.isArray(taskIds)) {
        taskIds = [taskIds];
    }

    self._getClient().mget(taskIds.map(function(taskId) { return self._namespace("tasks:" + taskId); }), function(err, reply) {
        if (err) {
            throw err;
        }

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
    });
}

Fresnel.prototype.getUnbufferedQueueLength = function(callback) {
    this._getClient().zcard(this._namespace("queue"), function(err, reply) {
        if (err) {
            throw err;
        }

        callback(reply);
    });
}

Fresnel.prototype._hash = function(task) {
    return hashlib.sha1(JSON.stringify(task));
}

/**
 * Wrap a key with the configured namespace.
 */
Fresnel.prototype._namespace = function(key) {
    return this.namespace + ":" + key;
}

Fresnel.prototype._queueTask = function(taskId, timestamp, callback) {
    timestamp = timestamp || new Date().getTime();

    this._getClient().zadd(this._namespace("queue"), timestamp, taskId, function(err, reply) {
        if (err) {
            throw err;
        }

        if (callback) {
            callback(!!reply);
        }
    });
}

Fresnel.prototype._unqueueTask = function(taskId, callback) {
    this._getClient().zrem(this._namespace("queue"), taskId, function(err, reply) {
        if (err) {
            throw err;
        }

        if (callback) {
            callback(!!reply);
        }
    });
}

Fresnel.prototype._updateDefinition = function(taskId, task, callback) {
    this._getClient().set(this._namespace("tasks:" + taskId), JSON.stringify(task), function(err, reply) {
        if (err) {
            throw err;
        }

        if (callback) {
            callback(taskId, reply);
        }
    });
}
