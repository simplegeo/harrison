var slice = Array.prototype.slice;

function update(array, args) {
    var arrayLength = array.length, length = args.length;
    while (length--) array[arrayLength + length] = args[length];
    return array;
}

function merge(array, args) {
    array = slice.call(array, 0);
    return update(array, args);
}

exports.extend = function(Function) {
    /** from prototype.js */
    Function.prototype.bind = function(context) {
        // if (arguments.length < 2 && Object.isUndefined(arguments[0])) return this;
        var __method = this, args = slice.call(arguments, 1);
        return function() {
            var a = merge(args, arguments);
            return __method.apply(context, a);
        };
    };

    /** from prototype.js */
    Function.prototype.curry = function() {
        if (!arguments.length) return this;
        var __method = this, args = slice.call(arguments, 0);
        return function() {
          var a = merge(args, arguments);
          return __method.apply(this, a);
        };
    };

    /** from prototype.js */
    Function.prototype.delay = function(timeout) {
        var __method = this, args = slice.call(arguments, 1);
        // timeout = timeout * 1000;
        return setTimeout(function() {
            return __method.apply(__method, args);
        }, timeout);
    };

    /** from prototype.js */
    Function.prototype.defer = function() {
        var args = update([0.01], arguments);
        return this.delay.apply(this, args);
    };

    /** from prototype.js */
    Function.prototype.wrap = function(wrapper) {
        var __method = this;
        return function() {
            var a = update([__method.bind(this)], arguments);
            return wrapper.apply(this, a);
        };
    };

    /**
     * Generates a function that waits for certain number of calls
     * before firing the original function
     * @param {Number} countdown the number of calls to wait for
     * @param {Function|Array} [wrapper]
     *      When a function, used in .wrap around the original function.
     *      When an array, passed to the function as the arguments once fired.
     */
    Function.prototype.barrier = function(countdown, wrapper) {
        var __method = this;

        if (Array.isArray(wrapper)) {
            var args = wrapper;
            wrapper = function(wrapped) { return wrapped.apply(__method, args); };
        }

        return function() {
            countdown--;
            if (countdown === 0) {
                if (wrapper) {
                    return __method.wrap(wrapper).apply(__method, arguments);
                } else {
                    return __method.apply(__method, arguments);
                }
            }
        };
    };
};
