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
        }
    }

    /** from prototype.js */
    Function.prototype.curry = function() {
        if (!arguments.length) return this;
        var __method = this, args = slice.call(arguments, 0);
        return function() {
          var a = merge(args, arguments);
          return __method.apply(this, a);
        }
    }

    /** from prototype.js */
    Function.prototype.delay = function(timeout) {
        var __method = this, args = slice.call(arguments, 1);
        // timeout = timeout * 1000;
        return setTimeout(function() {
            return __method.apply(__method, args);
        }, timeout);
    }

    /** from prototype.js */
    Function.prototype.defer = function() {
        var args = update([0.01], arguments);
        return this.delay.apply(this, args);
    }
}
