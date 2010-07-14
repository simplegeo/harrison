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
    },
    '.wrap should wrap a function with a wrapper, and bind "this" correctly': function(assert) {
        var f = function(arg) {
            return arg;
        }

        var f2 = f.wrap(function(super, arg) {
            return super(arg + 1);
        });

        assert.equal(2, f2(1));
    },
    '.wrap should bind "this" correctly': function(assert) {
        var f = function() {
            return this;
        }

        var f2 = f.wrap(function(super) {
            return super();
        });

        assert.equal("this", f2.call("this"));
    },
    '.barrier should wait until N calls before calling the callback': function(assert) {
        var called = false;

        var f = function() {
            called = true;
        }

        var b = f.barrier(2);

        b();
        assert.equal(false, called);
        b();
        assert.equal(true, called);
    },
    '.barrier should accept a wrapper function that will wrap around the original function': function (assert) {
        var f = function(arg) {
            return arg;
        }

        var b = f.barrier(1, function(super, arg) { return super(arg + 1) });

        assert.equal(2, b(1));
    }
}
