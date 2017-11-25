/*
 * Copyright 2015 Amazon.com, Inc. or its affiliates. All Rights Reserved.
 *
 * Licensed under the Amazon Software License (the "License"). You may not use this file except in
 * compliance with the License. A copy of the License is located at
 *
 * http://aws.amazon.com/asl/
 *
 * or in the "license" file accompanying this file. This file is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 */

package exercise.writer;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.nio.ByteBuffer;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
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
import exercise.processor.GeoHash;
import exercise.processor.Utils;
import exercise.util.Constants;
import exercise.util.ProducerUtil;

public class IncomingRequestWriter {
  private static final Logger log = LoggerFactory.getLogger(IncomingRequestWriter.class);
  private static final ScheduledExecutorService EXECUTOR = Executors.newScheduledThreadPool(1);

  public static void main(String[] args) throws Exception {
    String dataSetFilePath = "../Datasets/green_tripdata_2016-01.csv";
    BufferedReader br = null;
    try {
      br = new BufferedReader(new FileReader(dataSetFilePath));
    } catch (FileNotFoundException e) {
      System.out.println("DataSet Not Found");
      e.printStackTrace();
    }
    final BufferedReader finalBr = br;

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
        log.info(String.format("%d puts have completed", done));
      }
    }, 1, 1, TimeUnit.SECONDS);

    // Kick off the puts
    log.info(String.format("Starting puts... will run for %d seconds at %d records per second",
        Constants.SECONDS_TO_RUN, Constants.RECORDS_PER_SECOND));

    EXECUTOR.scheduleWithFixedDelay(new Runnable() {

      @Override
      public void run() {

        String line;
        try {
          while ((line = finalBr.readLine()) != null) {

            final String finalLine = line;

            try {
              new Thread(new Runnable() {
                @Override
                public void run() {

                  String[] fields = finalLine.split(",");

                  double longitude = Double.parseDouble(fields[5]);
                  double latitude = Double.parseDouble(fields[6]);
                  GeoHash g = new GeoHash(latitude, longitude);
                  g.sethashLength(6);
                  String geo = g.getGeoHashBase32();
                  // System.out.println(geo);

                  ByteBuffer data = null;
                  try {
                    data = ByteBuffer.wrap(geo.getBytes("UTF-8"));
                  } catch (UnsupportedEncodingException e) {
                    // TODO Auto-generated catch block
                    e.printStackTrace();
                  }
                  ListenableFuture<UserRecordResult> f =
                      producer.addUserRecord(Constants.STREAM_NAME, Utils.randomExplicitHashKey(),
                          Utils.randomExplicitHashKey(), data);
                  Futures.addCallback(f, callback);
                }
              }).start();

            } catch (Exception e) {
              log.error("Error running task", e);
              System.exit(1);
            }
          }
        } catch (IOException e) {
          // TODO Auto-generated catch block
          e.printStackTrace();
        }
      }
    }, 0, 1, TimeUnit.SECONDS);
    EXECUTOR.awaitTermination(Constants.SECONDS_TO_RUN + 1, TimeUnit.SECONDS);
    log.info("Waiting for remaining puts to finish...");
    producer.flushSync();
    log.info("All records complete.");
    producer.destroy();
    log.info("Finished.");
    EXECUTOR.shutdown();
  }

}
