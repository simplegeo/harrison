require('ext/date').extend(Date);

module.exports = {
    'test date formatting': function(assert) {
        var date = new Date(1278612143347);
        assert.equal('2010-07-08T18:02:23.347Z', date.toISOString());
    },
    'test date parsing': function(assert) {
        assert.equal(1278612143347, Date.parse('2010-07-08T18:02:23.347Z'));
    },
    'test non-UTC date parsing': function(assert) {
        assert.equal(1278612143347, Date.parse('2010-07-08T12:02:23.347-600'));
    }
}
