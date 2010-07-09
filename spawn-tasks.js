var redis = require('redis-client'),
    Fresnel = require('fresnel').Fresnel;

redis.debugMode = true;

var fresnel = new Fresnel();

var task = {
    "class": "echo",
    "args": []
}
fresnel.createTask(task, function() {
    fresnel.shutdown();
});

