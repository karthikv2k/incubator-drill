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

import java.io.IOException;
import java.util.*;
import java.util.concurrent.TimeUnit;

import com.google.common.collect.Iterators;
import com.google.common.collect.Lists;
import org.apache.drill.common.config.DrillConfig;
import org.apache.drill.common.exceptions.ExecutionSetupException;
import org.apache.drill.common.expression.SchemaPath;
import org.apache.drill.exec.physical.EndpointAffinity;
import org.apache.drill.exec.physical.OperatorCost;
import org.apache.drill.exec.physical.base.AbstractGroupScan;
import org.apache.drill.exec.physical.base.PhysicalOperator;
import org.apache.drill.exec.physical.base.Size;
import org.apache.drill.exec.proto.CoordinationProtos.DrillbitEndpoint;
import org.apache.drill.exec.store.StoragePluginRegistry;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.util.Bytes;
import parquet.org.codehaus.jackson.annotate.JsonCreator;

import com.fasterxml.jackson.annotation.JacksonInject;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeName;
import com.google.common.base.Preconditions;
import com.google.common.base.Stopwatch;
import com.google.common.collect.ArrayListMultimap;


@JsonTypeName("kafka-scan")
public class KafkaGroupScan extends AbstractGroupScan {
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(KafkaGroupScan.class);

  private ArrayListMultimap<Integer, KafkaSubScan.KafkaSubScanReadEntry> mappings;
  private Stopwatch watch = new Stopwatch();

  @JsonProperty("storage")
  public KafkaStoragePluginConfig getStorageConfig() {
    return this.storagePluginConfig;
  }

  private String topic;
  private KafkaStoragePlugin storagePlugin;
  private KafkaStoragePluginConfig storagePluginConfig;

  @JsonCreator
  public KafkaGroupScan(@JsonProperty("entries") List<KafkaTableReadEntry> entries,
                          @JsonProperty("storage") KafkaStoragePluginConfig storagePluginConfig,
                          @JacksonInject StoragePluginRegistry pluginRegistry
                           )throws IOException, ExecutionSetupException {
    Preconditions.checkArgument(entries.size() == 1);
    this.storagePlugin = (KafkaStoragePlugin) pluginRegistry.getPlugin(storagePluginConfig);
    this.storagePluginConfig = storagePluginConfig;
    this.topic = entries.get(0).topic;
  }

  public KafkaGroupScan(String topic, KafkaStoragePlugin storageEngine) throws IOException {
    this.storagePlugin = storageEngine;
    this.storagePluginConfig = storageEngine.getConfig();
    this.topic = topic;
  }

  @Override
  public List<EndpointAffinity> getOperatorAffinity() {
    return new LinkedList<EndpointAffinity>();
  }

  /**
   *
   * @param incomingEndpoints
   */
  @Override
  public void applyAssignments(List<DrillbitEndpoint> incomingEndpoints) {
  }

  @Override
  public KafkaSubScan getSpecificScan(int minorFragmentId) {
    return new KafkaSubScan(storagePlugin, storagePluginConfig, mappings.get(minorFragmentId));
  }


  @Override
  public int getMaxParallelizationWidth() {
    return 1;
  }

  @Override
  public OperatorCost getCost() {
    //TODO Figure out how to properly calculate cost
    return new OperatorCost(1,1,1,1);
  }

  @Override
  public Size getSize() {
    // TODO - this is wrong, need to populate correctly
    return new Size(10,10);
  }

  @Override
  @JsonIgnore
  public PhysicalOperator getNewWithChildren(List<PhysicalOperator> children) {
    Preconditions.checkArgument(children.isEmpty());
    //TODO return copy of self
    return this;
  }

}
