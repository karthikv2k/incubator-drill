package org.apache.drill.exec.store.kafka;

import kafka.consumer.Consumer;
import kafka.consumer.ConsumerConfig;
import kafka.consumer.ConsumerIterator;
import kafka.consumer.KafkaStream;
import kafka.javaapi.consumer.ConsumerConnector;

import java.util.*;

/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
public class KafkaZookeeperConsumer {

    private final String zookeeper;
    private final String topic;
    private final String groupID;
    private final long recordsToConsume;
    private final long timeout;
    private final long relativeOffset;
    private final ConsumerConfig config;
    private ConsumerIterator<byte[],byte[]> it;
    private long recordsConsumed=0;
    private boolean exit = false;

    /**
     * Creates a zookeeper consumer that abstracts the kafka cluster topology from the consumer.
     * @param zookeeperQuorum This is the zookeeper that Kafka uses to manage its cluster.
     * @param topic Topic to consume. Consuming multiple topics are not supported.
     * @param groupID GroupID is a unique ID that groups a set of consumer instances and assigns partitions to the
     *                consumers while keeping the load balanced across the consumers.
     * @param relativeOffset If it is Long.MIN_VALUE then it starts reading from earliest offset and if it is a negative
     *                       number then it goes back that many from the latest offset.
     * @param recordsToConsume Number of recordsToConsume to consume.
     * @param timeout Number of time in milliseconds to consume. If it is zero then the consumer waits till number of
     *                recordsToConsume reaches @recordsToConsume
     */
    public KafkaZookeeperConsumer(String zookeeperQuorum, String topic, String groupID, long relativeOffset,
                                  long recordsToConsume, long timeout){
        this.zookeeper = zookeeperQuorum;
        this.topic = topic;
        this.groupID = groupID;
        this.recordsToConsume = recordsToConsume;
        this.timeout = timeout;
        this.relativeOffset = relativeOffset;
        config = createConsumerConfig();
    }

    public void setup(){
        ConsumerConnector consumer = Consumer.createJavaConsumerConnector(config);
        Map<String, Integer> topicCountMap = new HashMap<String, Integer>();
        topicCountMap.put(topic, new Integer(1));
        Map<String, List<KafkaStream<byte[], byte[]>>> consumerMap = consumer.createMessageStreams(topicCountMap);
        KafkaStream<byte[], byte[]> stream = consumerMap.get(topic).get(0);
        it = stream.iterator();

        if (timeout > 0) {
            Timer timer = new Timer();
            timer.schedule(new TimeoutTask(this), 0, timeout);
        }
    }

    private ConsumerConfig createConsumerConfig() {
        Properties props = new Properties();
        props.put("zookeeper.connect",zookeeper);
        props.put("autooffset.reset", "earliest");
        props.put("group.id", groupID);
        props.put("zookeeper.session.timeout.ms", "4000");
        props.put("zookeeper.sync.time.ms", "200000");
        props.put("auto.commit.interval.ms", "100000000");
        return new ConsumerConfig(props);
    }

    public byte[] next(){
        if (exit && it.hasNext() && recordsConsumed< recordsToConsume){
            byte[] msg =  it.next().message();
            recordsConsumed++;
            return msg;
        } else {
            return null;
        }
    }

    class TimeoutTask extends TimerTask {
        KafkaZookeeperConsumer consumer;

        TimeoutTask(KafkaZookeeperConsumer consumer){
            this.consumer = consumer;
        }

        @Override
        public void run() {
            consumer.exit = true;
        }
    }
}