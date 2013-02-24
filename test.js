var basex = require('./index.js');
var s = new basex.Session();

s.on('loggedIn', function() {
    s.execute('open ballhaus', function() {
        this.emit('databaseOpen');
    });
});

s.on('databaseOpen', function() {
    var query1 = s.query('<bar foo="{$foo}" bar="{$bar}"/>',
                         { foo: 123, bar: '456' },
                         function (err, result) {
                             if (err) {
                                 s.emit('error', new Error(err));
                             } else {
                                 console.log('got query1 result:', result);
                             }
                         });
    var query2 = s.query('<bar foo="{$foo}" bar="{$bar}"/>',
                         { foo: 123, bar: '456' },
                         function (err, result) {
                             if (err) {
                                 s.emit('error', new Error(err));
                             } else {
                                 console.log('got query2 result:', result);
                             }
                         });
    s.execute('xquery /ballhaus/repertoire/piece[1]/name');
    s.execute('xquery /ballhaus/repertoire/piece[2]/name', function (result) {
        console.log('specific handler for second query, result:', result);
    });
    s.execute('xquery /ballhaus/repertoire/piece[3]/name');
});

s.on('result', function (result) {
    console.log('result event', result);
});

