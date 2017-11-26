const express = require('express');
const http = require('http');
const client = require('redis').createClient();
const async = require('async');
const geohash = require('ngeohash');


client.on('connect',function() {
    console.log('connected');
});

setInterval(() => {
    client.keys('*', function (err, keys) {
      if (err) return console.log(err);
      if(keys){
            async.map(keys, function(key, cb) {
               client.get(key, function (error, value) {
                    if (error) return cb(error);
                    var latlon = geohash.decode(key);
                    io.emit('coords', {
                      lat: latlon.latitude,
                      lng: latlon.longitude
                    });
                    //console.log('Latitude : ' + latlon.latitude + ' , Longitude : ' + latlon.longitude + ' and Value : ' + value);
                });
            }, function (error, results) {
               if (error) return console.log(error);
               console.log(results);
            });
      }
    });
}, 10000);


http.get({
  hostname: 'localhost',
  port: 3002,
  path: '/index.html',
  agent: false
}, callback);

var response;
var callback;

callback = function (res) {
  response = res;
  console.log(response); // Here `response` will return an object.
}

console.log(response); // But here, even if code is written after `http.get`, `response ` will return `undefined` because response is used before the callback (same as callback of `addEventListener`) was called

const app = express();
const httpServer = http.createServer(app);
const io = require('socket.io')(httpServer);

app.use(express.static(`${__dirname}/public`));

httpServer.listen(3002, () => {
  console.log('Listening on 0.0.0.0:3002')
});
