package exercise.processor;

import java.util.List;
import java.util.Set;
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


public class IncomingRequestProcessor implements IRecordProcessorFactory {
  private static final Logger log = LoggerFactory.getLogger(IncomingRequestProcessor.class);
  private static final JedisPool jedisPool =
      new JedisPool(Constants.REDIS_HOST, Constants.REDIS_PORT);
  Jedis jedis = jedisPool.getResource();

  private class RecordProcessor implements IRecordProcessor {
    private String kinesisShardId;
    private static final long REPORTING_INTERVAL_MILLIS = 30000L; // 1 minute
    private long nextReportingTimeInMillis;
    private static final long CHECKPOINT_INTERVAL_MILLIS = 60000L; // 1 minute
    private long nextCheckpointTimeInMillis;

    @Override
    public void initialize(String shardId) {
      log.info("Initializing record processor for shard: " + shardId);
      this.kinesisShardId = shardId;
      nextReportingTimeInMillis = System.currentTimeMillis() + REPORTING_INTERVAL_MILLIS;
      nextCheckpointTimeInMillis = System.currentTimeMillis() + CHECKPOINT_INTERVAL_MILLIS;
    }

    @Override
    public void processRecords(List<Record> records, IRecordProcessorCheckpointer checkpointer) {


      for (Record r : records) {
        try {
          byte[] b = new byte[r.getData().remaining()];
          r.getData().get(b);
          String geohash = new String(b, "UTF-8");
          incrementCountForGeoHash(geohash);
        } catch (Exception e) {
          log.error("Error parsing record", e);
          System.exit(1);
        }
      }

      if (System.currentTimeMillis() > nextReportingTimeInMillis) {
        publishDataFromJedis();
        nextReportingTimeInMillis = System.currentTimeMillis() + REPORTING_INTERVAL_MILLIS;
      }
      if (System.currentTimeMillis() > nextCheckpointTimeInMillis) {
        checkpoint(checkpointer);
        nextCheckpointTimeInMillis = System.currentTimeMillis() + CHECKPOINT_INTERVAL_MILLIS;
      }

    }

    private void publishDataFromJedis() {
      System.out.println(
          "--------------------------------------STATS----------------------------------------");
      Set<String> setOfKeys = jedis.keys("*");
      for (String key : setOfKeys) {
        System.out.println(key + "->" + jedis.get(key));
      }
      System.out.println(
          "------------------------------------FINISHED----------------------------------------");
    }

    private void incrementCountForGeoHash(String geohash) {
      String count = jedis.get(geohash);
      if (StringUtils.isEmpty(count)) {
        jedis.set(geohash, "1");
      } else if (StringUtils.isNumeric(count)) {
        Integer countInt = Integer.parseInt(count);
        jedis.set(geohash, String.valueOf(countInt + 1));
      }
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
        new KinesisClientLibConfiguration(Constants.INCOMING_REQUEST_APPLICATION_NAME,
            Constants.INCOMING_REQUEST_STREAM_NAME, new DefaultAWSCredentialsProviderChain(),
            Constants.INCOMING_REQUEST_APPLICATION_NAME).withRegionName(Constants.REGION)
                .withInitialPositionInStream(InitialPositionInStream.TRIM_HORIZON);

    final IncomingRequestProcessor consumer = new IncomingRequestProcessor();
    new Worker.Builder().recordProcessorFactory(consumer).config(config).build().run();
  }
}
