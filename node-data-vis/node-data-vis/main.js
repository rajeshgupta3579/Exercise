const express = require('express');
const http = require('http');
const client = require('redis').createClient();
const async = require('async');
const geohash = require('ngeohash');


client.on('connect',function() {
    updateView();
    console.log('connected');
});

setInterval(() => {
  updateView();
}, 1000);

updateView = function(){
  io.emit('clearView',{});
  client.keys('*', function (err, keys) {
    if (err) return console.log(err);
    if(keys){
          async.map(keys, function(key, cb) {
             client.get(key, function (error, value) {
                  if (error) return cb(error);
                  var latlon = geohash.decode(key);
                  io.emit('coords', {
                    lat: latlon.latitude,
                    lng: latlon.longitude,
                    value: value
                  });
                  // console.log('Latitude : ' + latlon.latitude + ' , Longitude : ' + latlon.longitude + ' and Value : ' + value);
              });
          }, function (error, results) {
             if (error) return console.log('error'+error);
             console.log(results);
          });
    }
  });
}

const app = express();
const httpServer = http.createServer(app);
const io = require('socket.io')(httpServer);
io.sockets.on('connection', function(socket){
socket.on('updateView', function(data){
        updateView();
    });
  }
);


app.use(express.static(`${__dirname}/public`));

httpServer.listen(3002, () => {
  console.log('Listening on 0.0.0.0:3002')
});
