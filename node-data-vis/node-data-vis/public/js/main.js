var map;
var sock = io();

(function init() {
  initMap();
  sock.emit('updateView', function(data) {
    sock.send('updateView');
  });
  sock.on('coords', function(c) {
    drawMarker(c.lat, c.lng,c.value);
  });
  sock.on('clearView', function(c) {
    var x = document.getElementsByClassName('leaflet-popup-close-button');
    for(var i=0;i<x.length;i++){
      x[i].click();
    }
  });
})();

function drawMarker(lat, lng,value) {
  // L.marker(new L.LatLng(lat, lng)).bindPopup('Look revealing label!').openPopup().addTo(map);
  // L.marker([lat,long]).bindPopup('Look revealing label!').openPopup().addTo(map);
  L.circle([lat, lng], {
    color: 'steelblue',
    fillColor: 'steelblue',
    fillOpacity: 0.5,
    radius: 300
  }).addTo(map);
  // var popup = L.popup().setLatLng([lat,lng]).setContent(value.toString());
  // L.popup().setLatLng([lat,lng]).setContent("1.2x").openOn(map);
  // map.addLayer(popup);
  // L.marker([lat,lng]).addTo(map).bindPopup("<b>1.2x</b>").openPopup();
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
