package exercise.processor;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
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



public class DriverLocationProcessor implements IRecordProcessorFactory {
  private static final Logger log = LoggerFactory.getLogger(DriverLocationProcessor.class);
  public static HashMap<String, Integer> hm;

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
          Integer x = hm.get(geohash);
          if (x == null) {
            DriverLocationProcessor.hm.put(geohash, 1);
          } else {
            DriverLocationProcessor.hm.put(geohash, x + 1);
          }
        } catch (Exception e) {
          log.error("Error parsing record", e);
          System.exit(1);
        }
      }
      if (System.currentTimeMillis() > nextReportingTimeInMillis) {
        System.out.println(
            "--------------------------------------STATS----------------------------------------");
        for (Map.Entry<String, Integer> entry : DriverLocationProcessor.hm.entrySet()) {
          System.out.println(entry.getKey() + " : " + entry.getValue());
        }
        nextReportingTimeInMillis = System.currentTimeMillis() + REPORTING_INTERVAL_MILLIS;
        System.out.println(
            "------------------------------------FINISHED----------------------------------------");
      }
      if (System.currentTimeMillis() > nextCheckpointTimeInMillis) {
        checkpoint(checkpointer);
        nextCheckpointTimeInMillis = System.currentTimeMillis() + CHECKPOINT_INTERVAL_MILLIS;
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
        new KinesisClientLibConfiguration(Constants.DRIVER_LOCATION_APPLICATION_NAME,
            Constants.DRIVER_LOCATION_STREAM_NAME, new DefaultAWSCredentialsProviderChain(),
            Constants.DRIVER_LOCATION_APPLICATION_NAME).withRegionName(Constants.REGION)
                .withInitialPositionInStream(InitialPositionInStream.TRIM_HORIZON);

    final DriverLocationProcessor consumer = new DriverLocationProcessor();

    DriverLocationProcessor.hm = new HashMap<String, Integer>();

    new Worker.Builder().recordProcessorFactory(consumer).config(config).build().run();
  }
}