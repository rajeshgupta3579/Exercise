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
import exercise.util.Constants;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;


public class SurgePricingProcessor {
  private static final ScheduledExecutorService EXECUTOR = Executors.newScheduledThreadPool(1);
  private static final JedisPool jedisPool =
      new JedisPool(Constants.REDIS_HOST, Constants.REDIS_PORT);

  public static void main(String[] args) {
    final Jedis jedis = jedisPool.getResource();
    EXECUTOR.scheduleAtFixedRate(new Runnable() {
      @Override
      public void run() {
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
          if (StringUtils.isNumeric(demandValueString) && supplyCountMap.get(geohash) != null) {

          }
        }
      }
    }, 2, 10, TimeUnit.MINUTES);
  }
}
