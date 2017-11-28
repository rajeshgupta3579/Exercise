package exercise.processor;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import exercise.util.Constants;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;


public class SurgePricingProcessor {
	
  private static final Logger log = LoggerFactory.getLogger(SurgePricingProcessor.class);
  private static final ScheduledExecutorService EXECUTOR = Executors.newScheduledThreadPool(1);
  private static final JedisPool jedisPool =
      new JedisPool(Constants.REDIS_HOST, Constants.REDIS_PORT);
         
  public static void main(String[] args) {
    final Jedis jedis = jedisPool.getResource();
    EXECUTOR.scheduleAtFixedRate(new Runnable() {
      @Override
      public void run() {
    	log.info("------------------------Calculating Surge Price------------------------------");
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
              StringUtils.isNotEmpty(demandValueString) ? Double.parseDouble(demandValueString) : 0.0;
        
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
            System.out.println("GeoHash : " + geohash + " , Demand : " + demand + " , Supply : " + supply + " , Surge Price : " + surgePrice + " , New Demand : " + newDemand );
            jedis.set(Constants.DEMAND_KEY_PREFIX + geohash, newDemand.toString());
			jedis.set(Constants.SURGE_PRICING_KEY_PREFIX + geohash, String.format("%2f",surgePrice.toString()));
          }
        }
        log.info("--------------------------Finished--------------------------------");
      }
    }, 0, 10, TimeUnit.MINUTES);
  }
}
