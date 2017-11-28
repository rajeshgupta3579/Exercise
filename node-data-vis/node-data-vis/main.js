const express = require('express');
const http = require('http');
const client = require('redis').createClient();
const async = require('async');
const geohash = require('ngeohash');

client.on('connect',function() {
    console.log('connected');
});

var timer = 60000;

setInterval(() => {
  updateView();
}, timer);

updateView = function(){
  io.emit('clearView');
  var SURGE_PRICING_KEY_PREFIX = 'surge_';
  updateView = function(){
    io.emit('clearView');
    client.keys(SURGE_PRICING_KEY_PREFIX + '*', function (err, keys) {
      if (err) return console.log(err);
      if(keys){
            async.map(keys, function(key, cb) {
                client.get(key, function (error, value) {
                    if (error) return cb(error);
                    var latlon = geohash.decode(key.substr(SURGE_PRICING_KEY_PREFIX.length));
                    io.emit('coords', {
                      lat: latlon.latitude,
                      lng: latlon.longitude,
                      value: value
                    });
                });
            });
      }
    });
  }
}

const app = express();
const httpServer = http.createServer(app);
const io = require('socket.io')(httpServer);
io.sockets.on('connection', function(socket){
   socket.on('updateView', updateView);
});


app.use(express.static(`${__dirname}/public`));

httpServer.listen(3002, () => {
  console.log('Listening on 0.0.0.0:3002')
});
