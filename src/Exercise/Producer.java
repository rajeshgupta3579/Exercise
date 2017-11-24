/*
 * Copyright 2015 Amazon.com, Inc. or its affiliates. All Rights Reserved.
 *
 * Licensed under the Amazon Software License (the "License").
 * You may not use this file except in compliance with the License.
 * A copy of the License is located at
 *
 * http://aws.amazon.com/asl/
 *
 * or in the "license" file accompanying this file. This file is distributed
 * on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 * express or implied. See the License for the specific language governing
 * permissions and limitations under the License.
 */

package Exercise;

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

import com.amazonaws.auth.DefaultAWSCredentialsProviderChain;
import com.amazonaws.services.kinesis.producer.Attempt;
import com.amazonaws.services.kinesis.producer.KinesisProducerConfiguration;
import com.amazonaws.services.kinesis.producer.KinesisProducer;
import com.amazonaws.services.kinesis.producer.UserRecordFailedException;
import com.amazonaws.services.kinesis.producer.UserRecordResult;
import com.google.common.collect.Iterables;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;

/**
 * The Kinesis Producer Library (KPL) excels at handling large numbers of small
 * logical records by combining multiple logical records into a single Kinesis
 * record.
 * 
 * <p>
 * In this sample we'll be putting a monotonically increasing sequence number in
 * each logical record, and then padding the record to 128 bytes long. The
 * consumer will then check that all records are received correctly by verifying
 * that there are no gaps in the sequence numbers.
 * 
 * <p>
 * We will distribute the records evenly across all shards by using a random
 * explicit hash key.
 * 
 * <p>
 * To prevent the consumer from being confused by data from multiple runs of the
 * producer, each record also carries the time at which the producer started.
 * The consumer will reset its state whenever it detects a new, larger
 * timestamp. We will place the timestamp in the partition key. This does not
 * affect the random distribution of records across shards since we've set an
 * explicit hash key.
 * 
 * @see Consumer
 * @author chaodeng
 *
 */
/**
 * @author ruebe
 *
 */

public class Producer {
    private static final Logger log = LoggerFactory.getLogger(Producer.class);
    
    private static final ScheduledExecutorService EXECUTOR = Executors.newScheduledThreadPool(1);
    
    private static final int SECONDS_TO_RUN = 5;
    
    /**
     * Put this number of records per second.
     * 
     * Because multiple logical records are combined into each Kinesis record,
     * even a single shard can handle several thousand records per second, even
     * though there is a limit of 1000 Kinesis records per shard per second.
     * 
     * If a shard gets throttled, the KPL will continue to retry records until
     * either they succeed or reach a TTL set in the KPL's configuration, at
     * which point the KPL will return failures for those records.
     * 
     * @see {@link KinesisProducerConfiguration#setRecordTtl(long)}
     */
    private static final int RECORDS_PER_SECOND = 2000;
    
    /**
     * Change this to your stream name.
     */
    public static final String STREAM_NAME = "IncomingCabRequests";
    
    /**
     * Change this to the region you are using.
     */
    public static final String REGION = "eu-central-1";

    /**
     * Here'll walk through some of the config options and create an instance of
     * KinesisProducer, which will be used to put records.
     * 
     * @return KinesisProducer instance used to put records.
     */
    
    /**
     * @return
     */
    public static KinesisProducer getKinesisProducer() {
        // There are many configurable parameters in the KPL. See the javadocs
        // on each each set method for details.
        KinesisProducerConfiguration config = new KinesisProducerConfiguration();
        
        // You can also load config from file. A sample properties file is
        // included in the project folder.
        // KinesisProducerConfiguration config =
        //     KinesisProducerConfiguration.fromPropertiesFile("default_config.properties");
        
        // If you're running in EC2 and want to use the same Kinesis region as
        // the one your instance is in, you can simply leave out the region
        // configuration; the KPL will retrieve it from EC2 metadata.
        config.setRegion(REGION);
        
        //To avoid InterruptedException: sleep interrupted during KPL shutdown issue
        config.setCredentialsRefreshDelay(100);
        
        // You can pass credentials programmatically through the configuration,
        // similar to the AWS SDK. DefaultAWSCredentialsProviderChain is used
        // by default, so this configuration can be omitted if that is all
        // that is needed.
        config.setCredentialsProvider(new DefaultAWSCredentialsProviderChain());
        
        // The maxConnections parameter can be used to control the degree of
        // parallelism when making HTTP requests. We're going to use only 1 here
        // since our throughput is fairly low. Using a high number will cause a
        // bunch of broken pipe errors to show up in the logs. This is due to
        // idle connections being closed by the server. Setting this value too
        // large may also cause request timeouts if you do not have enough
        // bandwidth.
        config.setMaxConnections(1);
        
        // Set a more generous timeout in case we're on a slow connection.
        config.setRequestTimeout(60000);
        
        // RecordMaxBufferedTime controls how long records are allowed to wait
        // in the KPL's buffers before being sent. Larger values increase
        // aggregation and reduces the number of Kinesis records put, which can
        // be helpful if you're getting throttled because of the records per
        // second limit on a shard. The default value is set very low to
        // minimize propagation delay, so we'll increase it here to get more
        // aggregation.
        config.setRecordMaxBufferedTime(15000);

        // If you have built the native binary yourself, you can point the Java
        // wrapper to it with the NativeExecutable option. If you want to pass
        // environment variables to the executable, you can either use a wrapper
        // shell script, or set them for the Java process, which will then pass
        // them on to the child process.
        // config.setNativeExecutable("my_directory/kinesis_producer");
        
        // If you end up using the default configuration (a Configuration instance
        // without any calls to set*), you can just leave the config argument
        // out.
        //
        // Note that if you do pass a Configuration instance, mutating that
        // instance after initializing KinesisProducer has no effect. We do not
        // support dynamic re-configuration at the moment.
        
        //To avoid Record has reached expiration issue
        config.setRecordTtl(100000000);
        KinesisProducer producer = new KinesisProducer(config);
        
        return producer;
    }
    
    public static void main(String[] args) throws Exception {
    	
        String dataSetFilePath = "../Datasets/green_tripdata_2016-01.csv";
        BufferedReader br = null;

        try {
            br = new BufferedReader(new FileReader(dataSetFilePath));            
        }
        catch (FileNotFoundException e) {
        	System.out.println("DataSet Not Found");
            e.printStackTrace();
        }
        final BufferedReader finalBr = br;
        
        final KinesisProducer producer = getKinesisProducer();
        
        // The number of records that have finished (either successfully put, or failed)
        final AtomicLong completed = new AtomicLong(0);
        
        // KinesisProducer.addUserRecord is asynchronous. A callback can be used to receive the results.
        final FutureCallback<UserRecordResult> callback = new FutureCallback<UserRecordResult>() {
            @Override
            public void onFailure(Throwable t) {
                // We don't expect any failures during this sample. If it
                // happens, we will log the first one and exit.
                if (t instanceof UserRecordFailedException) {
                    Attempt last = Iterables.getLast(
                            ((UserRecordFailedException) t).getResult().getAttempts());
                    log.error(String.format(
                            "Record failed to put - %s : %s",
                            last.getErrorCode(), last.getErrorMessage()));
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
                log.info(String.format(
                        "%d puts have completed",
                        done  ));
            }
        }, 1, 1, TimeUnit.SECONDS);
        
        // Kick off the puts
        log.info(String.format(
                "Starting puts... will run for %d seconds at %d records per second",
                SECONDS_TO_RUN, RECORDS_PER_SECOND));
        
        EXECUTOR.scheduleWithFixedDelay(new Runnable() {

            @Override
            public void run() {
                
                String line;
				try {
					while ((line = finalBr.readLine())!= null) {
						
						final String finalLine = line;
						
					    try {
					    	new Thread(new Runnable() {
					    	    @Override
					    	    public void run() {
					    	    	
					                String[] fields = finalLine.split(",");
					                
					                double longitude = Double.parseDouble(fields[5]);
					                double latitude = Double.parseDouble(fields[6]);
					                GeoHash g = new GeoHash(latitude,longitude);
					                g.sethashLength(6);
					                String geo = g.getGeoHashBase32();
					                //System.out.println(geo);
					                
					                ByteBuffer data = null;
									try {
										data = ByteBuffer.wrap(geo.getBytes("UTF-8"));
									} catch (UnsupportedEncodingException e) {
										// TODO Auto-generated catch block
										e.printStackTrace();
									}
					                ListenableFuture<UserRecordResult> f =
					                        producer.addUserRecord(STREAM_NAME, Utils.randomExplicitHashKey(), Utils.randomExplicitHashKey(), data);
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
        }, 0, 1, TimeUnit.MILLISECONDS);
        
        // Wait for puts to finish. After this statement returns, we have
        // finished all calls to putRecord, but the records may still be
        // in-flight. We will additionally wait for all records to actually
        // finish later.
        EXECUTOR.awaitTermination(SECONDS_TO_RUN + 1, TimeUnit.SECONDS);
        
        // If you need to shutdown your application, call flushSync() first to
        // send any buffered records. This method will block until all records
        // have finished (either success or fail). There are also asynchronous
        // flush methods available.
        //
        // Records are also automatically flushed by the KPL after a while based
        // on the time limit set with Configuration.setRecordMaxBufferedTime()
        log.info("Waiting for remaining puts to finish...");
        producer.flushSync();
        log.info("All records complete.");
        

        /* try {
            finalBr.close();
        } catch (IOException e) {
            e.printStackTrace();
        }*/
        
        // This kills the child process and shuts down the threads managing it.
        producer.destroy();
        log.info("Finished.");
        EXECUTOR.shutdown();
    }

}
