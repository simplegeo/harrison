var assert = require('assert'),
    redis = require('redis-client'),
    EventEmitter = require('events').EventEmitter;

var Fresnel = require('fresnel').Fresnel;

process.addListener('uncaughtException', function(err) {
    console.log("Caught exception: " + err);
});

module.exports = {
    'should be an EventEmitter': function(assert) {
        var fresnel = new Fresnel();
        assert.ok(fresnel instanceof EventEmitter);
    },
    'should buffer tasks into a local queue': function(assert, beforeExit) {
        var fresnel = new Fresnel('test:' + new Date().getTime());

        // create some tasks in Redis
        var task = {
            "class": "Sample",
            "args": []
        }

        fresnel.createTask(task, function() {
            fresnel.bufferTasks(function() {
                fresnel.shutdown();
            });
        });

        beforeExit(function() {
            assert.equal(task.toString(), fresnel.BUFFERED_TASKS[0].toString());

            fresnel.getUnbufferedQueueLength(function(length) {
                assert.equal(0, length);
            });

            fresnel.shutdown();
        });

    },
    'should mark buffered tasks as pending': function(assert) {

    },
    'create task adds shaw for uniqueness': function(assert) {
    }
}
