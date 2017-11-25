package exercise.writer;

import java.nio.ByteBuffer;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import com.amazonaws.AmazonClientException;
import com.amazonaws.regions.Region;
import com.amazonaws.regions.RegionUtils;
import com.amazonaws.services.kinesis.AmazonKinesis;
import com.amazonaws.services.kinesis.AmazonKinesisClientBuilder;
import com.amazonaws.services.kinesis.model.DescribeStreamResult;
import com.amazonaws.services.kinesis.model.PutRecordRequest;
import com.amazonaws.services.kinesis.model.ResourceNotFoundException;
import exercise.util.ConfigurationUtils;
import exercise.util.CredentialUtils;
import exercise.util.DriverLocation;
import exercise.util.DriverLocationGenerator;

/**
 * Continuously sends simulated stock trades to Kinesis
 *
 */
public class DriverLocationWriter {

  private static final Log LOG = LogFactory.getLog(DriverLocationWriter.class);

  /**
   * Checks if the stream exists and is active
   *
   * @param kinesisClient Amazon Kinesis client instance
   * @param streamName Name of stream
   */
  private static void validateStream(AmazonKinesis kinesisClient, String streamName) {
    try {
      DescribeStreamResult result = kinesisClient.describeStream(streamName);
      if (!"ACTIVE".equals(result.getStreamDescription().getStreamStatus())) {
        System.err.println(
            "Stream " + streamName + " is not active. Please wait a few moments and try again.");
        System.exit(1);
      }
    } catch (ResourceNotFoundException e) {
      System.err
          .println("Stream " + streamName + " does not exist. Please create it in the console.");
      System.err.println(e);
      System.exit(1);
    } catch (Exception e) {
      System.err.println("Error found while describing the stream " + streamName);
      System.err.println(e);
      System.exit(1);
    }
  }

  /**
   * Uses the Kinesis client to send the stock trade to the given stream.
   *
   * @param trade instance representing the stock trade
   * @param kinesisClient Amazon Kinesis client
   * @param streamName Name of stream
   */
  private static void sendStockTrade(DriverLocation trade, AmazonKinesis kinesisClient,
      String streamName) {
    byte[] bytes = trade.toJsonAsBytes();
    // The bytes could be null if there is an issue with the JSON serialization by the Jackson JSON
    // library.
    if (bytes == null) {
      LOG.warn("Could not get JSON bytes for stock trade");
      return;
    }

    LOG.info("Putting trade: " + trade.toString());
    PutRecordRequest putRecord = new PutRecordRequest();
    putRecord.setStreamName(streamName);
    // We use the ticker symbol as the partition key, as explained in the tutorial.
    putRecord.setPartitionKey(trade.getTickerSymbol());
    putRecord.setData(ByteBuffer.wrap(bytes));

    try {
      kinesisClient.putRecord(putRecord);
    } catch (AmazonClientException ex) {
      LOG.warn("Error sending record to Amazon Kinesis.", ex);
    }
  }

  public static void main(String[] args) throws Exception {
    String streamName = args[0];
    String regionName = args[1];
    Region region = RegionUtils.getRegion(regionName);
    if (region == null) {
      System.err.println(regionName + " is not a valid AWS region.");
      System.exit(1);
    }
    AmazonKinesisClientBuilder clientBuilder = AmazonKinesisClientBuilder.standard();
    clientBuilder.setRegion(regionName);
    clientBuilder.setCredentials(CredentialUtils.getCredentialsProvider());
    clientBuilder.setClientConfiguration(ConfigurationUtils.getClientConfigWithUserAgent());

    AmazonKinesis kinesisClient = clientBuilder.build();

    // Validate that the stream exists and is active
    validateStream(kinesisClient, streamName);

    // Repeatedly send stock trades with a 100 milliseconds wait in between
    DriverLocationGenerator driverLocationGenerator = new DriverLocationGenerator();
    while (true) {
      DriverLocation trade = driverLocationGenerator.getRandomTrade();
      sendStockTrade(trade, kinesisClient, streamName);
      Thread.sleep(100);
    }
  }

}
