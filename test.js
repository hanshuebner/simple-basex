var basex = require('./index.js');
var s = new basex.Session();
s.execute('xquery <foo/>');
s.query('<bar foo="{$foo}" bar="{$bar}"/>',
        { foo: 123, bar: '456' },
        function (err, result) {
            if (err) {
                s.emit('error', new Error(err));
            } else {
                console.log('got query2 result:', result);
            }
            s.execute('exit', function () {
                console.log('session exited');
            });
        });
s.on('result', function (result) {
    console.log('result event', result);
});

