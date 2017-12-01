package exercise.processor;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Timestamp;
import java.text.DecimalFormat;
import java.text.SimpleDateFormat;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.stream.Collectors;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import exercise.util.Constants;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;

public class ScheduledProcessor {

  private static final Logger log = LoggerFactory.getLogger(ScheduledProcessor.class);
  private static final ScheduledExecutorService EXECUTOR = Executors.newScheduledThreadPool(1);
  private static final JedisPool jedisPool =
      new JedisPool(Constants.REDIS_HOST, Constants.REDIS_PORT);

  public static void main(String[] args) {

    final String jdbcUrl = "jdbc:" + Constants.DB_DRIVER_NAME + "://" + Constants.DB_HOST + ":"
        + Constants.DB_PORT + "/" + Constants.DB_NAME + "?user=" + Constants.DB_USER_NAME
        + "&password=" + Constants.DB_PASSWORD;
    Connection conn = null;
    try {
      conn = DriverManager.getConnection(jdbcUrl, Constants.DB_USER_NAME, Constants.DB_PASSWORD);
    } catch (SQLException e) {
      log.info("Database Connection Error");
      e.printStackTrace();
      System.exit(1);
    }
    log.info("Connection to Database Established Successfully.");
    final Connection finalConn = conn;


    final Jedis jedis = jedisPool.getResource();
    EXECUTOR.scheduleAtFixedRate(new Runnable() {
      @Override
      public void run() {
        log.info("-----------------Calculating Surge Price and Congestion------------------------------");
        String timeStampStart = new SimpleDateFormat("yyyy-MM-dd HH-mm-ss").format(new Timestamp(System.currentTimeMillis()));
        
        //Surge Price
        List<String> driverLocations = jedis.keys(Constants.SUPPLY_KEY_PREFIX + "*").stream()
            .map(key -> jedis.get(key)).collect(Collectors.toList());

        Map<String, Integer> supplyCountMap = new HashMap<>();

        for (String location : driverLocations) {
          Integer count = supplyCountMap.get(location) == null ? 0 : supplyCountMap.get(location);
          supplyCountMap.put(location, ++count);
        }

        Set<String> geohashes = jedis.keys(Constants.DEMAND_KEY_PREFIX + "*").stream()
            .map(key -> key.substring(Constants.DEMAND_KEY_PREFIX.length()))
            .collect(Collectors.toSet());

        for (String geohash : geohashes) {
          String demandValueString = jedis.get(Constants.DEMAND_KEY_PREFIX + geohash);

          Double demand =
              StringUtils.isNotEmpty(demandValueString) ? Double.valueOf(demandValueString)
                  : 0.0;

          Double supply = supplyCountMap.get(geohash) != null ? supplyCountMap.get(geohash) : 0.0;

          if (demand != 0) {
            Double surgePrice = 0.0;
            if (supply != 0) {
              surgePrice = Double.min(Double.max(Constants.MIN_SURGE_MULTIPLIER, demand / supply),
                  Constants.MAX_SURGE_MULTIPLIER);
            } else {
              surgePrice = Constants.MAX_SURGE_MULTIPLIER;
            }
            Double newDemand = Double.max(0.0, demand - supply);
            System.out.println("GeoHash : " + geohash + " , Demand : " + demand + " , Supply : "
                + supply + " , Surge Price : " + surgePrice + " , " + "New Demand : " + newDemand);

            jedis.set(Constants.DEMAND_KEY_PREFIX + geohash, newDemand.toString());
            jedis.set(Constants.SURGE_PRICING_KEY_PREFIX + geohash,
                new DecimalFormat("#0.00").format(surgePrice).toString());

            String sqlQuery = "insert into " + Constants.SURGE_PRICE_TABLE_NAME
                + "(TimeStampStart, TimeStampEnd, GeoHash, Demand, Supply, SurgePrice) values(timestamp(\""
                + timeStampStart + "\"), timestamp(\"" + timeStampStart + "\") + INTERVAL \'"
                + Constants.SCHEDULED_TIME_INTERVAL.toString() + "\' "
                + Constants.SCHEDULED_TIME_UNIT.toString().substring(0,
                    Constants.SCHEDULED_TIME_UNIT.toString().length() - 1)
                + ", \'" + geohash + "\', " + demand.toString() + ", " + supply.toString() + ", "
                + surgePrice.toString() + ");";

            // log.info(sqlQuery);
            try {
              Statement st = finalConn.createStatement();
              st.executeUpdate(sqlQuery);
            } catch (SQLException e) {
              log.info("SQL Insert Error");
              e.printStackTrace();
            }
          }
        }
        
        //Traffic Congestion 
        geohashes.clear();
        geohashes = jedis.keys(Constants.SPEED_KEY_PREFIX + "*").stream()
                .map(key -> key.substring(Constants.SPEED_KEY_PREFIX.length()))
                .collect(Collectors.toSet());
        for (String geohash : geohashes) {
        	String averageSpeedString = jedis.get(Constants.SPEED_KEY_PREFIX + geohash);
        	if(StringUtils.isNotEmpty(averageSpeedString)) {
	        	Double count = Double.valueOf(averageSpeedString.split(",")[0]);
	        	Double sumSpeed = Double.valueOf(averageSpeedString.split(",")[1]);
	        	Double averageSpeed = sumSpeed/count;
	        	String Congestion;
	        	
	        	if(averageSpeed > Constants.AVERAGE_SPEED_LOWER_BOUND)
	        		Congestion = "LOW";
	        	else if(averageSpeed <= Constants.AVERAGE_SPEED_UPPER_BOUND && averageSpeed >= Constants.AVERAGE_SPEED_LOWER_BOUND)
	        		Congestion = "MODERATE";
	        	else
	        		Congestion = "HIGH";
	        	
	        	System.out.println("GeoHash : " + geohash + " , Sum Speed : " + sumSpeed + " , Count : "
	                    + count + " , Average Speed : " + averageSpeed + " , " + "Congestion : " + Congestion);
	        	
				String sqlQuery = "insert into " + Constants.TRAFFIC_CONGESTION_TABLE_NAME
				     + "(TimeStampStart, TimeStampEnd, GeoHash, AverageSpeed, Congestion) values(timestamp(\""
				     + timeStampStart + "\"), timestamp(\"" + timeStampStart + "\") + INTERVAL \'"
				     + Constants.SCHEDULED_TIME_INTERVAL.toString() + "\' "
				     + Constants.SCHEDULED_TIME_UNIT.toString().substring(0,
				         Constants.SCHEDULED_TIME_UNIT.toString().length() - 1)
				     + ", \'" + geohash + "\', " + averageSpeed.toString() + ",\'" + Congestion + "\');";
				
				 //log.info(sqlQuery);
				 try {
				   Statement st = finalConn.createStatement();
				   st.executeUpdate(sqlQuery);
				 } catch (SQLException e) {
				   log.info("SQL Insert Error");
				   e.printStackTrace();
				 }
	        	
        	}
        }
        
        
        log.info("Data Inserted into Database Successfully.");
        log.info("--------------------------Finished--------------------------------");
      }
    }, 0, Constants.SCHEDULED_TIME_INTERVAL, Constants.SCHEDULED_TIME_UNIT);
  }
}
