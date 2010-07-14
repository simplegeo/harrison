function pad(n) {
    return n < 10 ? '0' + n : n;
}

exports.extend = function(Date) {
    /**
     * Returns a ISO 8601 version of this date (millisecond accuracy)
     */
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
