var Fresnel = require('fresnel').Fresnel;

var fresnel = new Fresnel();

fresnel.addListener('task-created', function(task) {
    console.log(task.id + " created: " + JSON.stringify(task));
});

fresnel.getUnbufferedQueueLength(function(length) {
    console.log("Unbuffered tasks: " + length);
});

fresnel.getReservoirSize(function(length) {
    console.log("Reservoir size: " + length);
});

fresnel.acceptTasks();
console.log("Task listener started.");
