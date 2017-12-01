var map;
var sock = io();
var layers = [];
var intervalId;
var SURGE_PRICE_API_URL = 'https://8b8luijpq5.execute-api.eu-central-1.amazonaws.com/prod/getSurgePriceDataFromRDS';
var TRAFFIC_CONGESTION_API_URL = 'https://8b8luijpq5.execute-api.eu-central-1.amazonaws.com/prod/getTrafficCongestionDataFromRDS';
var AVERAGE_SPEED_UPPER_BOUND = 25;
var AVERAGE_SPEED_LOWER_BOUND = 20;
var lat_error = 0.0027;
var long_error = 0.0055;
(function init() {
  initMap();
  sock.emit('updateView',function(){});
  sock.emit('updateViewScheduled',function(){});
  sock.on('surgePriceCoordinates', function(c) {
    drawMarker(c.lat,c.lng,c.value,'surgePrice');
  });
  sock.on('trafficCongestionCoordinates', function(c) {
    drawMarker(c.lat,c.lng,c.value,'trafficCongestion');
  });
  sock.on('clearView', function() {
    for(var i=0;i<layers.length;i++){
      map.removeLayer(layers[i]);
    }
  });
})();

function updateViewBatch() {
    if($('#batch').prop("checked")){
        for(var i=0;i<layers.length;i++){
          map.removeLayer(layers[i]);
        }
        sock.emit('clearIntervalId',function(){});
        $.ajax({
          type: 'GET',
          url: SURGE_PRICE_API_URL,
          data: { "timestamp" : $('#timeField').val() },
          contentType: "application/json",
          success: function(data){
            data.forEach(function(databaseEntryItem){
              var latlon = geohash.decode(databaseEntryItem.GeoHash);
              drawMarker(latlon.latitude,latlon.longitude,parseFloat(databaseEntryItem.SurgePrice).toFixed(2),'surgePrice');
            })
          }
        });
        $.ajax({
          type: 'GET',
          url: TRAFFIC_CONGESTION_API_URL,
          data: { "timestamp" : $('#timeField').val() },
          contentType: "application/json",
          success: function(data){
            data.forEach(function(databaseEntryItem){
              var latlon = geohash.decode(databaseEntryItem.GeoHash);
              drawMarker(latlon.latitude,latlon.longitude,parseFloat(databaseEntryItem.AverageSpeed).toFixed(2),'trafficCongestion');
            })
          }
        });
    }
    else{
        sock.emit('checkAndUpdateView',function(){});
    }
}

function drawMarker(lat, lng, value, type) {
  var bounds = [[lat + lat_error, lng + long_error], [lat - lat_error, lng + long_error], [lat + lat_error, lng - long_error], [lat - lat_error, lng - long_error]];
  if(type=='surgePrice') {
    layers.push(L.rectangle(bounds, {
      weight: 2,
      fill : false
    }).bindTooltip("<label class = \"labelText\"><b>" + value + "x" + "</br></label>", {
          permanent: true, className: "labelContainer", direction : "top", offset : [0,20]
       }).addTo(map));
  }
  else {
    if(value > AVERAGE_SPEED_UPPER_BOUND)
      color = "green";
    else if(value>=AVERAGE_SPEED_LOWER_BOUND && value<=AVERAGE_SPEED_UPPER_BOUND)
      color = "blue";
    else
      color = "red";

    layers.push(L.rectangle(bounds, {
      weight: 2,
      color: color,
    }).addTo(map));
  }
}

function initMap() {

  console.log('Initializing map');
  map = L.map('map').setView([40.7,-73.9], 13);

  // Set up map source
  L.tileLayer(
    'http://{s}.tile.openstreetmap.org/{z}/{x}/{y}.png', {
      attribution: 'Open Street Map',
      maxZoom: 18
    }).addTo(map);
}
