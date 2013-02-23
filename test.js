var basex = require('./index.js');
var s = new basex.Session({ user: 'ballhaus', password: 'xmx111re' }); /* ({ host: 'ballhaus.netzhansa.com', user: 'ballhaus', password: 'xmx111re'}); */

s.on('loggedIn', function() {
    s.execute('open ballhaus', function() {
        this.emit('databaseOpen');
    });
});

s.on('databaseOpen', function() {
    var query1 = s.query('<bar foo="{$foo}" bar="{$bar}"/>');

    this.on('queryIdAllocated', function () {
        query1.bind('foo', '30');
        query1.bind('bar', 31);

        console.log('database is open');
        console.log('query1', query1.id);
        query1.execute(function (result) {
            console.log('prepared query executed:', result);
        });
        /*
        s.execute('xquery /ballhaus/repertoire/piece[1]/name');
        s.execute('xquery /ballhaus/repertoire/piece[2]/name', function (result) {
            console.log('specific handler for second query, result:', result);
        });
        s.execute('xquery /ballhaus/repertoire/piece[3]/name');
        */
        s.execute('exit');
    });
});

s.on('result', function (result) {
    console.log('result event', result);
});

