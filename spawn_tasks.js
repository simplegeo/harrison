var Harrison = require('harrison').Harrison;

var harrison = new Harrison();

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

var taskCount;

if (process.argv.length > 2) {
    taskCount = process.argv[2];
} else {
    taskCount = 100;
}

var onCompletion = function() {
    harrison.shutdown();
}.barrier(taskCount);

for (var i = 0; i < taskCount; i++) {
    harrison.createTask(randomTask(), onCompletion);
}
