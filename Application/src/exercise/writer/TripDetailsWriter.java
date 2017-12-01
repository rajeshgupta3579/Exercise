package exercise.writer;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.nio.ByteBuffer;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang.math.RandomUtils;
import org.joda.time.DateTime;
import org.joda.time.format.DateTimeFormatter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.amazonaws.services.kinesis.producer.Attempt;
import com.amazonaws.services.kinesis.producer.KinesisProducer;
import com.amazonaws.services.kinesis.producer.UserRecordFailedException;
import com.amazonaws.services.kinesis.producer.UserRecordResult;
import com.google.common.collect.Iterables;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import exercise.util.Constants;
import exercise.util.GeoHash;
import exercise.util.ProducerUtil;
import exercise.util.Utils;

public class TripDetailsWriter {
  private static final Logger log = LoggerFactory.getLogger(TripDetailsWriter.class);
  private static final ScheduledExecutorService EXECUTOR = Executors.newScheduledThreadPool(1);

  public static void main(String[] args) throws Exception {
    BufferedReader br = null;
    try {
      br = new BufferedReader(new FileReader(Constants.TRIP_DETAILS_DATA_SET_FILE_PATH));
    } catch (FileNotFoundException e) {
      System.out.println("DataSet Not Found");
      e.printStackTrace();
    }

    final KinesisProducer producer = ProducerUtil.getKinesisProducer();
    final AtomicLong completed = new AtomicLong(0);
    final FutureCallback<UserRecordResult> callback = new FutureCallback<UserRecordResult>() {
      @Override
      public void onFailure(Throwable t) {
        if (t instanceof UserRecordFailedException) {
          Attempt last =
              Iterables.getLast(((UserRecordFailedException) t).getResult().getAttempts());
          log.error(String.format("Record failed to put - %s : %s", last.getErrorCode(),
              last.getErrorMessage()));
        }
        log.error("Exception during put", t);
        System.exit(1);
      }

      @Override
      public void onSuccess(UserRecordResult result) {
        completed.getAndIncrement();
      }
    };

    // This gives us progress updates
    EXECUTOR.scheduleAtFixedRate(new Runnable() {
      @Override
      public void run() {
        long done = completed.get();
        log.info(String.format("%d puts have completed after 20 seconds", done));
      }
    }, Constants.TRIP_DETAIL_INITIAL_DELAY, Constants.TRIP_DETAIL_INTERVAL, TimeUnit.SECONDS);

    boolean condition = true;
    while (condition) {
      String line;
      try {
        if ((line = br.readLine()) != null) {
          String[] fields = line.split(",");
          DateTimeFormatter format =
              org.joda.time.format.DateTimeFormat.forPattern("yyyy-MM-dd HH:mm:ss");
          DateTime pickupTime = DateTime.parse(fields[0], format);
          DateTime dropoffTime = DateTime.parse(fields[1], format);
          Double pickupLongitude = Double.valueOf(fields[2]);
          Double pickupLatitude = Double.valueOf(fields[3]);
          Double dropoffLongitude = Double.valueOf(fields[4]);
          Double dropoffLatitude = Double.valueOf(fields[5]);
          // in Km
          Double tripDistance = Double.valueOf(fields[6]);
          // in Hours
          Double tripTime = Double.valueOf(dropoffTime.getMillis() - pickupTime.getMillis());
          tripTime = tripTime / (60 * 60 * 1000);
          // in Km/Hr
          Double tripSpeed = tripDistance / tripTime;
          System.out.println(tripSpeed);
          Set<String> routeGeohashes =
              getRoute(pickupLatitude, pickupLongitude, dropoffLatitude, dropoffLongitude);
          String tripDetails = String.valueOf(tripSpeed) + Constants.DELIMITER
              + routeGeohashes.stream().collect(Collectors.joining(","));
          ByteBuffer data = null;
          try {
            data = ByteBuffer.wrap(tripDetails.getBytes("UTF-8"));
          } catch (UnsupportedEncodingException e) {
            e.printStackTrace();
          }
          ListenableFuture<UserRecordResult> f =
              producer.addUserRecord(Constants.TRIP_DETAIL_STREAM_NAME,
                  Utils.randomExplicitHashKey(), Utils.randomExplicitHashKey(), data);
          Futures.addCallback(f, callback);
        } else {
          condition = false;
        }
      } catch (IOException e) {
        // TODO Auto-generated catch block
        e.printStackTrace();
      }
      Thread.sleep(RandomUtils.nextInt() % 5000);
    }
    producer.flushSync();
    log.info("Waiting for remaining puts to finish...");
    log.info("All records complete.");
    producer.destroy();
    try

    {
      br.close();
    } catch (IOException e) {
      e.printStackTrace();
    }
    log.info("Finished.");
    EXECUTOR.shutdown();
  }

  private static Set<String> getRoute(double y1, double x1, double y2, double x2) {
    Set<String> geoHashes = new HashSet<>();
    String pickupGeohash = GeoHash.getGeoHashString(y1, x1);
    String dropoffGeohash = GeoHash.getGeoHashString(y2, x2);
    geoHashes.add(pickupGeohash);
    double lat = y1, lng = x1;
    String currentGeohash = null;
    while (!StringUtils.equals(dropoffGeohash, currentGeohash)) {
      if (lat < y2 && y1 < y2) {
        lat += Constants.LATITUDE_ERROR;
      } else if (lat > y2 && y1 > y2) {
        lat -= Constants.LATITUDE_ERROR;
      }
      if (lng < x2 && x1 < x2) {
        lng += Constants.LONGITUDE_ERROR;
      } else if (lng > x2 && x1 > x2) {
        lng -= Constants.LONGITUDE_ERROR;
      }
      String newGeohash = GeoHash.getGeoHashString(lat, lng);
      if(StringUtils.equals(newGeohash, currentGeohash)) {
    	  break;
      }
      else { 
    	  currentGeohash = newGeohash;
      }
      geoHashes.add(currentGeohash);
    }
    log.info("Success:  Route Calculated. Number of GeoHashes : " + geoHashes.size());
    return geoHashes;
  }
}
