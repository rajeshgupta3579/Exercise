const mysql = require('mysql');
const config = require('./config.json')

var pool  = mysql.createPool({
    host     : config.dbhost,
    port     : config.dbport,
    user     : config.dbuser,
    password : config.dbpassword,
    database : config.dbname,
    connectionTimeout : 60000
});
exports.handler =  (event, context, callback) => {
  //prevent timeout from waiting event loop
  let timestamp = event.timestamp;
  context.callbackWaitsForEmptyEventLoop = false;
  pool.getConnection(function(err, connection) {
      var sqlQuery = "select t1.Weather,t1.AverageSupply,t1.AverageDemand,t2.AverageSpeed from (select avg(Supply) as AverageSupply,avg(Demand) as AverageDemand,Weather from GrabExercise.SurgePriceTable join GrabExercise.WeatherDataTable on GrabExercise.SurgePriceTable.TimeStampStart = GrabExercise.WeatherDataTable.TimeStampStart group by Weather) as t1 join (select avg(AverageSpeed) as AverageSpeed, Weather from GrabExercise.TrafficCongestionTable join GrabExercise.WeatherDataTable on GrabExercise.TrafficCongestionTable.TimeStampStart = GrabExercise.WeatherDataTable.TimeStampStart group by Weather) as t2 on t1.Weather = t2.Weather;";
      connection.query(sqlQuery, function (error, results, fields) {
      connection.release();
      if (error) callback(error);
      else callback(null,results);
    });
  });
};
