var Fresnel = require('fresnel').Fresnel;

var fresnel = new Fresnel();

fresnel.WORKER_MAP = {
    "Class": "http://localhost:8080/jobs/sample",
    "Backfill": "http://localhost:8080/jobs/backfill",
    "Load": "http://localhost:8081/",
    "*": "http://localhost:8081/"
};

fresnel.getUnbufferedQueueLength(function(length) {
    console.log("Unbuffered tasks: " + length);
});

fresnel.processTasks();
