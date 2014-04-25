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

import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;

import org.apache.drill.common.exceptions.ExecutionSetupException;
import org.apache.drill.common.expression.FieldReference;
import org.apache.drill.common.expression.SchemaPath;
import org.apache.drill.common.logical.StoragePluginConfig;
import org.apache.drill.exec.physical.OperatorCost;
import org.apache.drill.exec.physical.base.AbstractBase;
import org.apache.drill.exec.physical.base.PhysicalOperator;
import org.apache.drill.exec.physical.base.PhysicalVisitor;
import org.apache.drill.exec.physical.base.Size;
import org.apache.drill.exec.physical.base.SubScan;

import com.fasterxml.jackson.annotation.JacksonInject;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeName;
import com.google.common.base.Preconditions;
import com.google.common.collect.Iterators;
import org.apache.drill.exec.store.StoragePluginRegistry;

// Class containing information for reading a single HBase row group form HDFS
@JsonTypeName("kafka-row-group-scan")
public class KafkaSubScan extends AbstractBase implements SubScan {
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(KafkaSubScan.class);

  @JsonProperty
  public final StoragePluginConfig storage;
  @JsonIgnore
  private final KafkaStoragePlugin kafkaStoragePlugin;
  private final List<KafkaSubScanReadEntry> rowGroupReadEntries;

  @JsonCreator
  public KafkaSubScan(@JacksonInject StoragePluginRegistry registry, @JsonProperty("storage") StoragePluginConfig storage,
                      @JsonProperty("rowGroupReadEntries") LinkedList<KafkaSubScanReadEntry> rowGroupReadEntries)
          throws ExecutionSetupException {
    kafkaStoragePlugin = (KafkaStoragePlugin) registry.getPlugin(storage);
    this.rowGroupReadEntries = rowGroupReadEntries;
    this.storage = storage;
  }

  public KafkaSubScan(KafkaStoragePlugin plugin, KafkaStoragePluginConfig config,
                      List<KafkaSubScanReadEntry> regionInfoList) {
    kafkaStoragePlugin = plugin;
    storage = config;
    this.rowGroupReadEntries = regionInfoList;
  }

  public List<KafkaSubScanReadEntry> getRowGroupReadEntries() {
    return rowGroupReadEntries;
  }

  @JsonIgnore
  public StoragePluginConfig getStorageConfig() {
    return storage;
  }

  @Override
  public OperatorCost getCost() {
    return null;
  }

  @Override
  public Size getSize() {
    return null;
  }

  @Override
  public boolean isExecutable() {
    return false;
  }

  @JsonIgnore
  public KafkaStoragePlugin getStorageEngine(){
    return kafkaStoragePlugin;
  }

  @Override
  public <T, X, E extends Throwable> T accept(PhysicalVisitor<T, X, E> physicalVisitor, X value) throws E {
    return physicalVisitor.visitSubScan(this, value);
  }

  @Override
  public PhysicalOperator getNewWithChildren(List<PhysicalOperator> children) {
    Preconditions.checkArgument(children.isEmpty());
    return new KafkaSubScan(kafkaStoragePlugin, (KafkaStoragePluginConfig) storage, rowGroupReadEntries);
  }

  @Override
  public Iterator<PhysicalOperator> iterator() {
    return Iterators.emptyIterator();
  }

  public static class KafkaSubScanReadEntry {

    public final String topic;
      public final String groupID;
      public final long relativeOffset;
      public final long recordsToConsume;
      public final long timeout;

    @parquet.org.codehaus.jackson.annotate.JsonCreator
    public KafkaSubScanReadEntry(@JsonProperty("topic") String topic,
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

}
