
/**
 * Module dependencies.
 */

var express = require('express'),
    Fresnel = require('fresnel').Fresnel;

var app = module.exports = express.createServer();
var fresnel = new Fresnel();

// Configuration

app.configure(function() {
    app.set('views', __dirname + '/views');
    app.use(express.bodyDecoder());
    app.use(express.logger());
    app.use(app.router);
    app.use(express.staticProvider(__dirname + '/public'));
});

app.configure('development', function() {
    app.use(express.errorHandler({ dumpExceptions: true, showStack: true })); 
});

app.configure('production', function() {
   app.use(express.errorHandler()); 
});

// Routes

app.get('/', function(req, res) {
    var pendingCount, queuedCount, futureCount, failedCount;
    var pendingTasks, queuedTasks, futureTasks, failedTasks;
    var render = function () {
        res.render('index.ejs', {
            locals: {
                pendingCount: pendingCount,
                queuedCount: queuedCount,
                futureCount: futureCount,
                failedCount: failedCount,
                pendingTasks: pendingTasks,
                queuedTasks: queuedTasks,
                futureTasks: futureTasks,
                failedTasks: failedTasks
            }
        });
    }.barrier(8);

    fresnel.getPendingCount(function(count) {
        pendingCount = count;
        render();
    });

    fresnel.getUnbufferedQueueLength(function(count) {
        queuedCount = count;
        render();
    });

    fresnel.getReservoirSize(function(count) {
        futureCount = count;
        render();
    });

    fresnel.getFailCount(function(count) {
        failedCount = count;
        render();
    });

    fresnel._getPendingTasks(10, function(tasks) {
        var taskIds = tasks.map(function(task) {
            return task[0];
        });

        if (taskIds.length > 0) {
            fresnel._getDefinitions(taskIds, function(defs) {
                pendingTasks = defs;
                render();
            });
        } else {
            pendingTasks = [];
            render();
        }
    });

    fresnel._getQueuedTasks(10, function(tasks) {
        var taskIds = tasks.map(function(task) {
            return task[0];
        });

        if (taskIds.length > 0) {
            fresnel._getDefinitions(taskIds, function(defs) {
                queuedTasks = defs;
                render();
            });
        } else {
            queuedTasks = [];
            render();
        }
    });

    fresnel._getFutureTasks(10, function(tasks) {
        var taskIds = tasks.map(function(task) {
            return task[0];
        });

        if (taskIds.length > 0) {
            fresnel._getDefinitions(taskIds, function(defs) {
                futureTasks = defs;
                render();
            });
        } else {
            futureTasks = [];
            render();
        }
    });

    fresnel.getErroredOutTasks(10, function(tasks) {
        var taskIds = tasks.map(function(task) {
            return task[0];
        });

        if (taskIds.length > 0) {
            fresnel._getDefinitions(taskIds, function(defs) {
                // TODO wrap this into the task definition when a task errors out
                for (var i=0; i < defs.length; i++) {
                    defs[i].failedAt = new Date(new Number(tasks[i][1])).toISOString();
                }

                failedTasks = defs;
                render();
            });
        } else {
            failedTasks = [];
            render();
        }
    });
});

app.get('/tasks/:id', function(req, res) {
    fresnel._getDefinition(req.params.id, function(task) {
        var lastError;

        var render = function() {
            res.render("task.ejs", {
                locals: {
                    task: task,
                    lastError: lastError
                }
            });
        };

        if (task.attempts > 0) {
            fresnel.getLastError(req.params.id, function(error) {
                lastError = error;
                render();
            });
        } else {
            render();
        }
    });
});

// Only listen on $ node app.js

if (!module.parent) app.listen(3000);
