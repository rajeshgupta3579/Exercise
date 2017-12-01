const express = require('express');
const http = require('http');
const client = require('redis').createClient();
const async = require('async');
const geohash = require('ngeohash');

client.on('connect',function() {
    console.log('connected');
});

var timer = 10000;
var intervalId = false;
updateViewScheduled = function() {
  console.log("updateViewScheduled");
  if(intervalId){
    clearIntervalId();
  }
  intervalId = setInterval(updateView,timer);
}

checkAndUpdateView = function() {
  console.log("checkAndUpdateView");
  if(intervalId==false){
    updateView();
    updateViewScheduled();
  }
}

clearIntervalId = function() {
  console.log("clearIntervalId");
  clearInterval(intervalId);
  intervalId = false;
}


updateView = function(){
  io.emit('clearView');
  console.log("updateView");
  var SURGE_PRICING_KEY_PREFIX = 'surge_';
  var SPEED_KEY_PREFIX = 'speed_';
  client.keys(SURGE_PRICING_KEY_PREFIX + '*', function (err, keys) {
    if (err) return console.log(err);
    if(keys){
          async.map(keys, function(key, cb) {
              client.get(key, function (error, value) {
                  if (error) return cb(error);
                  var latlon = geohash.decode(key.substr(SURGE_PRICING_KEY_PREFIX.length));
                  io.emit('surgePriceCoordinates', {
                    lat: latlon.latitude,
                    lng: latlon.longitude,
                    value: parseFloat(value).toFixed(2)
                  });
              });
          });
    }
  });
  client.keys(SPEED_KEY_PREFIX + '*', function (err, keys) {
    if (err) return console.log(err);
    if(keys){
          async.map(keys, function(key, cb) {
              client.get(key, function (error, value) {
                  if (error) return cb(error);
                  var latlon = geohash.decode(key.substr(SPEED_KEY_PREFIX.length));
                  var count = parseFloat(value.split(",")[0]);
                  var sumSpeed = parseFloat(value.split(",")[1]);
                  var averageSpeed = sumSpeed/count;
                  io.emit('trafficCongestionCoordinates', {
                    lat: latlon.latitude,
                    lng: latlon.longitude,
                    value: parseFloat(averageSpeed).toFixed(2)
                  });
              });
          });
    }
  });
}


const app = express();
const httpServer = http.createServer(app);
const io = require('socket.io')(httpServer);
io.sockets.on('connection', function(socket){
   socket.on('updateView', updateView);
   socket.on('updateViewScheduled', updateViewScheduled );
   socket.on('checkAndUpdateView', checkAndUpdateView);
   socket.on('clearIntervalId', clearIntervalId);
});


app.use(express.static(`${__dirname}/public`));

httpServer.listen(3002, () => {
  console.log('Listening on 0.0.0.0:3002')
});
