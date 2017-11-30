var map;
var sock = io();
var layers = [];
var intervalId;
var api_url = 'https://8b8luijpq5.execute-api.eu-central-1.amazonaws.com/prod/getDataFromRDS';
(function init() {
  initMap();
  sock.emit('updateView',function(){});
  sock.emit('updateViewScheduled',function(){});
  sock.on('coords', function(c) {
    drawMarker(c.lat,c.lng,c.value);
  });
  sock.on('clearView', function() {
    for(var i=0;i<layers.length;i++){
      map.removeLayer(layers[i]);
    }
  });
})();

function updateViewBatch() {
    if($('#batch').prop("checked")){
        sock.emit('clearIntervalId',function(){});
        $.ajax({
          type: 'GET',
          url: api_url,
          data: { "timestamp" : $('#timeField').val() },
          contentType: "application/json",
          success: function(data){
            for(var i=0;i<layers.length;i++){
              map.removeLayer(layers[i]);
            }
            data.forEach(function(databaseEntryItem){
              var latlon = Geohash.decode(databaseEntryItem.GeoHash);
              console.log(parseFloat(databaseEntryItem.SurgePrice).toFixed(2));
              drawMarker(latlon.lat,latlon.lon,parseFloat(databaseEntryItem.SurgePrice).toFixed(2));
            })
          }
        });
    }
    else{
        sock.emit('checkAndUpdateView',function(){});
    }
}

function drawMarker(lat, lng, value) {
  layers.push(L.circle([lat,lng], 300, {
     fill: false
   }).bindTooltip("<label class = \"labelText\"><b>" + value + "x" + "</br></label>", {
        permanent: true, className: "labelContainer", direction : "top", offset : [0,20]
      }).addTo(map));
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
