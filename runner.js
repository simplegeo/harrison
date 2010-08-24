var Fresnel = require('fresnel').Fresnel;

var fresnel = new Fresnel();

fresnel.WORKER_MAP = {
    "Class": "http://localhost:8080/jobs/sample",
    "Backfill": "http://localhost:8080/jobs/backfill",
    "Load": "http://localhost:8081/",
    "Walk": "http://simplegeo.local:8989/_tasks/walk.json",
    "*": "http://localhost:8081/"
};

fresnel.addListener('task-completed', function(task, response, time) {
    console.log(task.id + " completed in " + time + "ms.");
});

fresnel.addListener('task-error', function(task, error, time) {
    console.log("task " + task.id + " errored " + task.attempts + " time(s): " + error);
});

fresnel.addListener('task-failed', function(task, error, time) {
    console.log("task " + task.id + " failed: " + error);
});

fresnel.getUnbufferedQueueLength(function(length) {
    console.log("Unbuffered tasks: " + length);
});

fresnel.getReservoirSize(function(length) {
    console.log("Reservoir size: " + length);
});

fresnel.processTasks();
console.log("Task runner started.");
