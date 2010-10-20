var Harrison = require('harrison').Harrison;

var harrison = new Harrison();

harrison.addListener('task-created', function(task) {
    console.log(task.id + " created: " + JSON.stringify(task));
});

harrison.getUnbufferedQueueLength(function(length) {
    console.log("Unbuffered tasks: " + length);
});

harrison.getReservoirSize(function(length) {
    console.log("Reservoir size: " + length);
});

harrison.acceptTasks();
console.log("Task listener started.");
