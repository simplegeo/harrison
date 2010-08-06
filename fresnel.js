var Fresnel = require('fresnel').Fresnel;

var fresnel = new Fresnel();

fresnel.getUnbufferedQueueLength(function(length) {
    console.log("Unbuffered tasks: " + length);
});

fresnel.processTasks();
