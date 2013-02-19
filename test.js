var basex = require('./index.js');
var s = new basex.Session({ host: 'ballhaus.netzhansa.com', user: 'ballhaus', password: 'xmx111re'});

s.on('loggedIn', function() {
    s.execute('open ballhaus', function() {
        this.emit('databaseOpen');
    });
});

s.on('databaseOpen', function() {
    console.log('database is open');
    s.execute('xquery /ballhaus/people', function(result) { console.log(result.length); });
});
