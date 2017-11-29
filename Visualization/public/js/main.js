var map;
var sock = io();
var layers = [];
(function init() {
  initMap();
  sock.emit('updateView',function(){});
  sock.on('coords', function(c) {
    drawMarker(c.lat,c.lng,c.value);
  });
  sock.on('clearView', function() {
    for(var i=0;i<layers.length;i++){
      map.removeLayer(layers[i]);
    }
  });
})();

function updateMapView() {
  console.log("Hello!");
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
