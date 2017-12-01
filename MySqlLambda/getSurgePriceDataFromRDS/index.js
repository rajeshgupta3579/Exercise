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
      connection.query('SELECT * from SurgePriceTable where TimeStampStart <=\'' + timestamp + '\' and TimeStampEnd >\'' + timestamp + '\'', function (error, results, fields) {
      connection.release();
      if (error) callback(error);
      else callback(null,results);
    });
  });
};
