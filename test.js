var basex = require('./index.js');
var s = new basex.Session({ user: 'ballhaus', password: 'xmx111re' }); /* ({ host: 'ballhaus.netzhansa.com', user: 'ballhaus', password: 'xmx111re'}); */

s.on('loggedIn', function() {
    s.execute('open ballhaus', function() {
        this.emit('databaseOpen');
    });
});

s.on('databaseOpen', function() {
    var query1 = s.query('/ballhaus/repertoire/piece[1]/name');
    var query2 = s.query('/ballhaus/repertoire/piece[2]/name');

    query1.bind('foo', '1', 'xs:string');
    query2.bind('bar', '2', 'xs:string');

    console.log('database is open');
    console.log('query1', query1.id);
    console.log('query2', query2.id);
    s.execute('xquery /ballhaus/repertoire/piece[1]/name');
    s.execute('xquery /ballhaus/repertoire/piece[2]/name', function (result) {
        console.log('specific handler for second query, result:', result);
    });
    s.execute('xquery /ballhaus/repertoire/piece[3]/name');
    s.execute('exit');
});

s.on('result', function (result) {
    console.log(result);
});

