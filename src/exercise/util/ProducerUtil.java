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

package exercise.util;

import com.amazonaws.auth.DefaultAWSCredentialsProviderChain;
import com.amazonaws.services.kinesis.producer.KinesisProducer;
import com.amazonaws.services.kinesis.producer.KinesisProducerConfiguration;

public class ProducerUtil {
	public static KinesisProducer getKinesisProducer() {
        KinesisProducerConfiguration config = new KinesisProducerConfiguration();
        config.setRegion(Constants.REGION);
        config.setCredentialsRefreshDelay(100);
        config.setCredentialsProvider(new DefaultAWSCredentialsProviderChain());
        config.setMaxConnections(1);
        config.setRequestTimeout(60000);
        config.setRecordMaxBufferedTime(15000);
        config.setRecordTtl(100000000);
        KinesisProducer producer = new KinesisProducer(config);
        return producer;
    }
}

