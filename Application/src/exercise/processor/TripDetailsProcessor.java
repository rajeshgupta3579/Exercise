package exercise.processor;

import java.util.Arrays;
import java.util.List;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.amazonaws.auth.DefaultAWSCredentialsProviderChain;
import com.amazonaws.services.kinesis.clientlibrary.exceptions.InvalidStateException;
import com.amazonaws.services.kinesis.clientlibrary.exceptions.ShutdownException;
import com.amazonaws.services.kinesis.clientlibrary.exceptions.ThrottlingException;
import com.amazonaws.services.kinesis.clientlibrary.interfaces.IRecordProcessor;
import com.amazonaws.services.kinesis.clientlibrary.interfaces.IRecordProcessorCheckpointer;
import com.amazonaws.services.kinesis.clientlibrary.interfaces.IRecordProcessorFactory;
import com.amazonaws.services.kinesis.clientlibrary.lib.worker.InitialPositionInStream;
import com.amazonaws.services.kinesis.clientlibrary.lib.worker.KinesisClientLibConfiguration;
import com.amazonaws.services.kinesis.clientlibrary.lib.worker.ShutdownReason;
import com.amazonaws.services.kinesis.clientlibrary.lib.worker.Worker;
import com.amazonaws.services.kinesis.model.Record;
import exercise.util.Constants;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;


public class TripDetailsProcessor implements IRecordProcessorFactory {
  private static final Logger log = LoggerFactory.getLogger(TripDetailsProcessor.class);
  private static final JedisPool jedisPool =
      new JedisPool(Constants.REDIS_HOST, Constants.REDIS_PORT);

  private class RecordProcessor implements IRecordProcessor {
    private String kinesisShardId;
    private static final long CHECKPOINT_INTERVAL_MILLIS = 60000L; // 1 minute
    private long nextCheckpointTimeInMillis;

    @Override
    public void initialize(String shardId) {
      log.info("Initializing record processor for shard: " + shardId);
      this.kinesisShardId = shardId;
      nextCheckpointTimeInMillis = System.currentTimeMillis() + CHECKPOINT_INTERVAL_MILLIS;
    }

    @Override
    public void processRecords(List<Record> records, IRecordProcessorCheckpointer checkpointer) {
      Jedis jedis = jedisPool.getResource();
      for (Record r : records) {
        try {
          byte[] b = new byte[r.getData().remaining()];
          r.getData().get(b);
          String tripDetails = new String(b, "UTF-8");
          System.out.println(tripDetails);
          Double tripSpeed = Double.valueOf(tripDetails.split(Constants.DELIMITER)[0]);
          List<String> routeGeohashes =
              Arrays.asList(tripDetails.split(Constants.DELIMITER)[1].split(","));
          for (String geohash : routeGeohashes) {
            updateSpeedForGeoHash(Constants.SPEED_KEY_PREFIX + geohash, tripSpeed, jedis);
          }
        } catch (Exception e) {
          log.error("Error parsing record", e);
          System.exit(1);
        }
      }

      if (System.currentTimeMillis() > nextCheckpointTimeInMillis) {
        checkpoint(checkpointer);
        nextCheckpointTimeInMillis = System.currentTimeMillis() + CHECKPOINT_INTERVAL_MILLIS;
      }

    }

    private void updateSpeedForGeoHash(String geohash, Double tripSpeed, Jedis jedis) {
      String value = jedis.get(geohash);
      if (StringUtils.isEmpty(value)) {
        jedis.set(geohash, "1" + "," + tripSpeed.toString());
      } else {
        Integer countInt = Integer.valueOf(value.split(",")[0]);
        Double existingSpeed = Double.valueOf(value.split(",")[1]);
        tripSpeed = existingSpeed + tripSpeed;
        jedis.set(geohash,
            String.valueOf(countInt + 1) + "," + tripSpeed.toString());
      }
      log.info("Written to Redis");
    }

    @Override
    public void shutdown(IRecordProcessorCheckpointer checkpointer, ShutdownReason reason) {
      log.info(
          "Shutting down record processor for shard: " + kinesisShardId + " , reason: " + reason);
      checkpoint(checkpointer);
    }

    private void checkpoint(IRecordProcessorCheckpointer checkpointer) {
      log.info("Checkpointing shard " + kinesisShardId);
      try {
        checkpointer.checkpoint();
      } catch (ShutdownException se) {
        // Ignore checkpoint if the processor instance has been shutdown (fail over).
        log.info("Caught shutdown exception, skipping checkpoint.", se);
      } catch (ThrottlingException e) {
        // Skip checkpoint when throttled. In practice, consider a backoff and retry policy.
        log.error("Caught throttling exception, skipping checkpoint.", e);
      } catch (InvalidStateException e) {
        // This indicates an issue with the DynamoDB table (check for table, provisioned IOPS).
        log.error(
            "Cannot save checkpoint to the DynamoDB table used by the Amazon Kinesis Client Library.",
            e);
      } catch (Exception e) {
        log.error("Error while trying to checkpoint during ProcessRecords", e);
      }
    }
  }

  @Override
  public IRecordProcessor createProcessor() {
    return this.new RecordProcessor();
  }

  public static void main(String[] args) {
    KinesisClientLibConfiguration config =
        new KinesisClientLibConfiguration(Constants.TRIP_DETAIL_APPLICATION_NAME,
            Constants.TRIP_DETAIL_STREAM_NAME, new DefaultAWSCredentialsProviderChain(),
            Constants.TRIP_DETAIL_APPLICATION_NAME).withRegionName(Constants.REGION)
                .withInitialPositionInStream(InitialPositionInStream.TRIM_HORIZON);

    final TripDetailsProcessor consumer = new TripDetailsProcessor();
    new Worker.Builder().recordProcessorFactory(consumer).config(config).build().run();
  }
}
