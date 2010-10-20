var Harrison = require('harrison').Harrison;

var harrison = new Harrison();

harrison.WORKER_MAP = {
    "Class": "http://localhost:8080/jobs/sample",
    "Backfill": "http://localhost:8080/jobs/backfill",
    "Load": "http://localhost:8081/",
    "Walk": "http://simplegeo.local:8989/_tasks/walk.json",
    "*": "http://localhost:8081/"
};

harrison.addListener('task-completed', function(task, response, time) {
    console.log(task.id + " completed in " + time + "ms.");
});

harrison.addListener('task-error', function(task, error, time) {
    console.log("task " + task.id + " errored " + task.attempts + " time(s): " + error);
});

harrison.addListener('task-failed', function(task, error, time) {
    console.log("task " + task.id + " failed: " + error);
});

harrison.getUnbufferedQueueLength(function(length) {
    console.log("Unbuffered tasks: " + length);
});

harrison.getReservoirSize(function(length) {
    console.log("Reservoir size: " + length);
});

harrison.processTasks();
console.log("Task runner started.");
