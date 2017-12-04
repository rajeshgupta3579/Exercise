var WEATHER_DATA_API_URL = "https://8b8luijpq5.execute-api.eu-central-1.amazonaws.com/prod/getWeatherDataFromRDS";
var scale_x=[],average_speed=[],average_supply=[],average_demand=[];
(function() {
  var data = $.ajax({
    type: 'GET',
    url: WEATHER_DATA_API_URL,
    async: false
  }).responseJSON;
  console.log(data);
  data.forEach(function(databaseEntryItem){
    scale_x.push(databaseEntryItem.Weather);
    average_speed.push(databaseEntryItem.AverageSpeed);
    average_supply.push(databaseEntryItem.AverageSupply);
    average_demand.push(databaseEntryItem.AverageDemand);
  })
})();
var myConfig = {
  backgroundColor: "white",
 	type: "bar",
 	title:{
 	  text:'Weather Analysis of Supply, Demand and Traffic Average Speed (Congestion)'
 	},
 	plot:{
 	  borderRadius: "0 0 5 5",
    animation: {
      effect: 3,
      sequence: 1
    }
 	},
   tooltip : {
    visible:true,
    text: "%kl<br><span style='color:%color'>%t: </span>%v<br>",
    backgroundColor : "white",
    borderColor : "#e3e3e3",
    borderRadius : "5px",
    bold : true,
    fontSize : "12px",
    fontColor : "#2f2f2f",
    textAlign : 'left',
    padding : '15px',
    shadow : true,
    shadowAlpha : 0.2,
    shadowBlur : 5,
    shadowDistance : 4,
    shadowColor : "#a1a1a1"
  },
 	scaleX:{
    values:scale_x,
 	},
 	scaleY:{
    maxItems:5,
 	  tick:{visible:true},
 	  guide: {
 	    visible: true
 	  }
 	},
	series : [
		{
			values : average_supply,
			backgroundColor: "#43B195",
			text:'Average Supply'
		},
		{
			values : average_demand,
			backgroundColor: "#EA4352",
			text:'Average Demand'
		},
		{
			values : average_speed,
			backgroundColor: "#FAE935",
			text:'Average Traffic Speed'
		},
	]
};

zingchart.render({
	id : 'myChart',
	data : myConfig,
	height: '100%',
	width: '100%'
});
