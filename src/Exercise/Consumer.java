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

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.amazonaws.services.kinesis.clientlibrary.exceptions.InvalidStateException;
import com.amazonaws.services.kinesis.clientlibrary.exceptions.ShutdownException;
import com.amazonaws.services.kinesis.clientlibrary.exceptions.ThrottlingException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.amazonaws.auth.DefaultAWSCredentialsProviderChain;
import com.amazonaws.services.kinesis.clientlibrary.interfaces.IRecordProcessor;
import com.amazonaws.services.kinesis.clientlibrary.interfaces.IRecordProcessorCheckpointer;
import com.amazonaws.services.kinesis.clientlibrary.interfaces.IRecordProcessorFactory;
import com.amazonaws.services.kinesis.clientlibrary.lib.worker.InitialPositionInStream;
import com.amazonaws.services.kinesis.clientlibrary.lib.worker.KinesisClientLibConfiguration;
import com.amazonaws.services.kinesis.clientlibrary.lib.worker.Worker;
import com.amazonaws.services.kinesis.clientlibrary.lib.worker.ShutdownReason;
import com.amazonaws.services.kinesis.model.Record;

 

public class Consumer implements IRecordProcessorFactory {
    private static final Logger log = LoggerFactory.getLogger(Consumer.class);
    
    public static HashMap<String,Integer> hm; 
    

    /**
     * One instance of RecordProcessor is created for every shard in the stream.
     * All instances of RecordProcessor share state by capturing variables from
     * the enclosing SampleConsumer instance. This is a simple way to combine
     * the data from multiple shards.
     */
    
    private class RecordProcessor implements IRecordProcessor {
    	
        private String kinesisShardId;
    	
    	 // Reporting interval
        private static final long REPORTING_INTERVAL_MILLIS = 30000L; // 1 minute
        private long nextReportingTimeInMillis;
        
        // Checkpointing interval
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
                    String geohash = new String(b,"UTF-8");
                    Integer x = hm.get(geohash);
                    if(x == null) {
                    	Consumer.hm.put(geohash,1);
                    }
                    else {
                    	Consumer.hm.put(geohash,x+1); 
                    }  
                    
                } catch (Exception e) {
                    log.error("Error parsing record", e);
                    System.exit(1);
                }
                
            }         
            
            // If it is time to report stats as per the reporting interval, report stats
            if (System.currentTimeMillis() > nextReportingTimeInMillis) {
            	//Consumer.hm.clear();
            	System.out.println("--------------------------------------STATS----------------------------------------");
            	for (Map.Entry<String,Integer> entry : Consumer.hm.entrySet()) {
            		System.out.println(entry.getKey() + " : " + entry.getValue());
            	}
                nextReportingTimeInMillis = System.currentTimeMillis() + REPORTING_INTERVAL_MILLIS;
                System.out.println("------------------------------------FINISHED----------------------------------------");
            }

            // Checkpoint once every checkpoint interval
            if (System.currentTimeMillis() > nextCheckpointTimeInMillis) {
                checkpoint(checkpointer);
                nextCheckpointTimeInMillis = System.currentTimeMillis() + CHECKPOINT_INTERVAL_MILLIS;
            }

        }

        @Override
        public void shutdown(IRecordProcessorCheckpointer checkpointer, ShutdownReason reason) {
        	log.info("Shutting down record processor for shard: " + kinesisShardId + " , reason: " + reason);
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
            	log.error("Cannot save checkpoint to the DynamoDB table used by the Amazon Kinesis Client Library.", e);
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
                new KinesisClientLibConfiguration(
                        "IncomingRequestsConsumer",
                        Producer.STREAM_NAME,
                        new DefaultAWSCredentialsProviderChain(),
                        "IncomingRequestsConsumer")
                                .withRegionName(Producer.REGION)
                                .withInitialPositionInStream(InitialPositionInStream.TRIM_HORIZON);
        
        final Consumer consumer = new Consumer();
        
        Consumer.hm = new HashMap<String,Integer>();
        
        new Worker.Builder()
            .recordProcessorFactory(consumer)
            .config(config)
            .build()
            .run();
    }
}
