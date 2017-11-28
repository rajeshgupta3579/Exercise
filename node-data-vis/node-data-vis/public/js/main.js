var map;
var sock = io();
var layers = [];

(function init() {
  initMap();
  sock.emit('updateView',function(){});
  sock.on('coords', function(c) {
    drawMarker(c.lat, c.lng,c.value);
  });
  sock.on('clearView', function() {
    for(var i=0;i<layers.length;i++){
      map.removeLayer(layers[i]);
    }
  });
})();

function drawMarker(lat, lng,value) {

  layers.push(L.marker([lat,lng]).bindLabel(value + 'x', {
    noHide: true
  }).addTo(map).showLabel());
  // var circle = L.circle([lat,lng], 1000, {
  //   fill: false
  // }).addTo(map);
}

function initMap() {
  console.log('Initializing map');
  map = L.map('map').setView([40.7, -73.8], 11);

  // Set up map source
  L.tileLayer(
    'http://{s}.tile.openstreetmap.org/{z}/{x}/{y}.png', {
      attribution: 'Open Street Map',
      maxZoom: 18
    }).addTo(map);
}
