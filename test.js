var basex = require('./index.js');
var s = new basex.Session({ user: 'ballhaus', password: 'xmx111re' }); /* ({ host: 'ballhaus.netzhansa.com', user: 'ballhaus', password: 'xmx111re'}); */

s.on('loggedIn', function() {
    s.execute('open ballhaus', function() {
        this.emit('databaseOpen');
    });
});

var query1 = s.query('/ballhaus/repertoire/piece[1]/name');
var query2 = s.query('/ballhaus/repertoire/piece[2]/name');

s.on('databaseOpen', function() {
    console.log('database is open');
    console.log('query1', query1.id);
    console.log('query2', query2.id);
    s.execute('xquery /ballhaus/repertoire/piece[1]/name');
    s.execute('xquery /ballhaus/repertoire/piece[2]/name');
    s.execute('exit');
});

s.on('result', function (result) {
    console.log(result);
});

