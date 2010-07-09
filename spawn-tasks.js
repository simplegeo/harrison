var redis = require('redis-client'),
    Fresnel = require('fresnel').Fresnel;

redis.debugMode = false;

var fresnel = new Fresnel();

function randomTask() {
    return {
        "class": randomString(),
        "args": []
    };
}

var CHARACTERS = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789";

function randomString(length) {
    length = length || 16;
    var text = "";

    for (var i=0; i < length; i++) {
        text += CHARACTERS.charAt(Math.floor(Math.random() * CHARACTERS.length));
    }

    return text;
}

for (var i = 0; i < 10; i++) {
    fresnel.createTask(randomTask(), function() {
        fresnel.shutdown();
    });
}

