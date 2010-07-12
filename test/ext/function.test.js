require('ext/function').extend(Function);

function assertArraysEqual(assert, a1, a2) {
    assert.equal(a1.length, a2.length);
    a1.forEach(function(e, i) { assert.equal(e, a2[i]) });
}

module.exports = {
    '.bind should bind "this" in a function': function(assert) {
        var f = function() {
            return this;
        }

        assert.equal("this", f.bind("this")());
    },
    '.bind should allow you to pre-load arguments': function(assert) {
        var f = function(arg1, arg2) {
            return [this, arg1, arg2];
        }
        
        assertArraysEqual(assert, ["this", 1, 2], f.bind("this", 1)(2));
    },
    '.curry should pre-load arguments': function(assert) {
        var f = function(arg1, arg2) {
            return [arg1, arg2];
        }

        assertArraysEqual(assert, [1, 2], f.curry()(1, 2));
        assertArraysEqual(assert, [1, 2], f.curry(1)(2));
        assertArraysEqual(assert, [1, 2], f.curry(1, 2)());
    },
    '.delay should run the function later': function(assert, beforeExit) {
        var ran = false;
        var f = function() {
            ran = true;
        }

        f.delay(100);

        assert.equal(false, ran);
        beforeExit(function() {
            assert.equal(true, ran);
        });
    },
    '.defer should run the function later': function(assert, beforeExit) {
        var ran = false;
        var f = function() {
            ran = true;
        }

        f.defer();

        assert.equal(false, ran);
        beforeExit(function() {
            assert.equal(true, ran);
        });
    }
}
