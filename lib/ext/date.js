function pad(n) {
    return n < 10 ? '0' + n : n;
}

exports.extend = function(Date) {
    Date.prototype.toISOString = function() {
        return this.getUTCFullYear() + '-' + 
               pad(this.getUTCMonth() + 1) + '-' +
               pad(this.getUTCDate()) + 'T' +
               pad(this.getUTCHours()) + ':' +
               pad(this.getUTCMinutes()) + ':' +
               pad(this.getUTCSeconds()) + '.' +
               pad(this.getUTCMilliseconds()) + 'Z';
    }
}
