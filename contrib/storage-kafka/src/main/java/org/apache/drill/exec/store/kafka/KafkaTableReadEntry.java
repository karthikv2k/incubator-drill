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
package org.apache.drill.exec.store.kafka;


import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

public class KafkaTableReadEntry {

    public final String topic;
    public final String groupID;
    public final long relativeOffset;
    public final long recordsToConsume;
    public final long timeout;

    @parquet.org.codehaus.jackson.annotate.JsonCreator
    public KafkaTableReadEntry(@JsonProperty("topic") String topic,
                                 @JsonProperty("groupID") String groupID,
                                 @JsonProperty("relativeOffset") long relativeOffset,
                                 @JsonProperty("recordsToConsume") long recordsToConsume,
                                 @JsonProperty("timeout") long timeout) {
        this.topic = topic;
        this.groupID = groupID;
        this.relativeOffset = relativeOffset;
        this.recordsToConsume = recordsToConsume;
        this.timeout = timeout;
    }
}
