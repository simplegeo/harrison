var assert = require('assert'),
    redis = require('redis-client'),
    EventEmitter = require('events').EventEmitter;

var Fresnel = require('fresnel').Fresnel;

process.addListener('uncaughtException', function(err) {
    console.log("Caught exception: " + err);
});

function randomTask() {
    return {
        "class": "class:" + new Date().getTime(), // TODO random string helper
        "args": []
    };
}

module.exports = {
    'should be an EventEmitter': function(assert) {
        var fresnel = new Fresnel();
        assert.ok(fresnel instanceof EventEmitter);
    },
     'should buffer tasks into a local queue': function(assert, beforeExit) {
        var fresnel = new Fresnel('test:' + new Date().getTime());

        // create some tasks in Redis
        var task = randomTask();

        fresnel.createTask(task, function() {
            fresnel.bufferTasks(function() {
                fresnel.getUnbufferedQueueLength(function(length) {
                    try {
                        assert.equal(0, length);
                    } finally {
                        fresnel.shutdown();
                    }
                });
            });
        });

        beforeExit(function() {
            assert.equal(task.toString(), fresnel.BUFFERED_TASKS[0].toString());
        });
     },
    'should mark buffered tasks as pending': function(assert, beforeExit) {
        var fresnel = new Fresnel('test:' + new Date().getTime());

        console.log("creating tasks");
        fresnel.createTask(randomTask(), function() {
            console.log("buffering tasks");
            fresnel.bufferTasks(function() {
                fresnel.getPendingCount(function(count) {
                    try {
                        assert.equal(1, count);
                    } finally {
                        fresnel.shutdown();
                    }
                });
            });
        });
    },
    'create task adds shaw for uniqueness': function(assert) {
    }
}
