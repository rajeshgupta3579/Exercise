const express = require('express');
const http = require('http');
const client = require('redis').createClient();
const async = require('async');
const geohash = require('ngeohash');
const mysql = require('mysql');

client.on('connect',function() {
    console.log('connected');
});

var connection = mysql.createConnection({
  userName: 'grabDB',
  password: 'mainkyodassa',
  host: 'grab-exercise-db-instance.coooavv2qtha.eu-central-1.rds.amazonaws.com',
  port:3306,
  database: 'GrabExercise'
  });
connection.connect(function(err) {
  if (err) {
    console.error('Database connection failed: ' + err.stack);
    return;
  }

  console.log('Connected to database.');
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

updateBatchView = function(){
  io.emit('clearView');
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
