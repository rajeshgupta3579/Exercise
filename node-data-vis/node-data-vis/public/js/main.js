var map;
var sock = io();

(function init() {
  initMap();

  sock.on('coords', function(c) {
    drawMarker(c.lat, c.lng);
  });
})();

function drawMarker(lat, lng) {
  //L.marker([lat, lng]).addTo(map);
  L.circle([lat, lng], {
    color: 'steelblue',
    fillColor: 'steelblue',
    fillOpacity: 0.5,
    radius: 300
  }).addTo(map);
  
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
