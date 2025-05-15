/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.hdds.scm.safemode;

import static org.apache.hadoop.hdds.HddsConfigKeys.HDDS_SCM_SAFEMODE_THRESHOLD_PCT;
import static org.apache.hadoop.hdds.HddsConfigKeys.HDDS_SCM_SAFEMODE_THRESHOLD_PCT_DEFAULT;

import com.google.common.annotations.VisibleForTesting;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;
import org.apache.hadoop.hdds.conf.ConfigurationSource;
import org.apache.hadoop.hdds.protocol.DatanodeID;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos.LifeCycleState;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos.ReplicationType;
import org.apache.hadoop.hdds.scm.container.ContainerID;
import org.apache.hadoop.hdds.scm.container.ContainerInfo;
import org.apache.hadoop.hdds.scm.container.ContainerManager;
import org.apache.hadoop.hdds.scm.container.ContainerNotFoundException;
import org.apache.hadoop.hdds.scm.events.SCMEvents;
import org.apache.hadoop.hdds.scm.server.SCMDatanodeProtocolServer.NodeRegistrationContainerReport;
import org.apache.hadoop.hdds.server.events.EventQueue;
import org.apache.hadoop.hdds.server.events.TypedEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Safe mode rule for EC containers.
 */
public class ECContainerSafeModeRule extends SafeModeExitRule<NodeRegistrationContainerReport> {

  private static final Logger LOG = LoggerFactory.getLogger(ECContainerSafeModeRule.class);
  private static final String NAME = "ECContainerSafeModeRule";

  private final ContainerManager containerManager;
  private final double safeModeCutoff;
  private final Map<ContainerID, Entry> ecContainers;
  private final AtomicLong ecContainerWithMinReplicas;
  private volatile int ecMaxContainer;

  public ECContainerSafeModeRule(EventQueue eventQueue,
      ConfigurationSource conf,
      ContainerManager containerManager,
      SCMSafeModeManager manager) {
    super(manager, NAME, eventQueue);
    this.safeModeCutoff = getSafeModeCutoff(conf);
    this.containerManager = containerManager;
    this.ecContainers = new ConcurrentHashMap<>();
    this.ecContainerWithMinReplicas = new AtomicLong(0);
    initializeRule();
  }

  private static double getSafeModeCutoff(ConfigurationSource conf) {
    final double cutoff = conf.getDouble(HDDS_SCM_SAFEMODE_THRESHOLD_PCT,
        HDDS_SCM_SAFEMODE_THRESHOLD_PCT_DEFAULT);
    if (cutoff < 0) {
      throw new IllegalArgumentException(HDDS_SCM_SAFEMODE_THRESHOLD_PCT
          + " = " + cutoff + " < 0");
    } else if (cutoff > 1) {
      throw new IllegalArgumentException(HDDS_SCM_SAFEMODE_THRESHOLD_PCT
          + " = " + cutoff + " > 1");
    }
    return cutoff;
  }

  @Override
  protected TypedEvent<NodeRegistrationContainerReport> getEventType() {
    return SCMEvents.CONTAINER_REGISTRATION_REPORT;
  }

  @Override
  protected synchronized boolean validate() {
    if (validateBasedOnReportProcessing()) {
      return getCurrentContainerThreshold() >= safeModeCutoff;
    }

    final List<ContainerInfo> containers = containerManager.getContainers(
        ReplicationType.EC);

    return containers.stream()
        .filter(this::isClosed)
        .noneMatch(this::isMissing);
  }

  /**
   * Checks if the container has at least the minimum required number of replicas.
   */
  private boolean isMissing(ContainerInfo info) {
    final int minReplica = getMinReplica(info);
    try {
      return containerManager.getContainerReplicas(info.containerID()).size() < minReplica;
    } catch (ContainerNotFoundException ex) {
      /*
       * This should never happen, in case this happens the container
       * somehow got removed from SCM.
       * Safemode rule doesn't have to log/fix this. We will just exclude this
       * from the rule validation.
       */
      return false;
    }
  }

  @VisibleForTesting
  public double getCurrentContainerThreshold() {
    final int max = ecMaxContainer;
    return max == 0 ? 1 : ecContainerWithMinReplicas.doubleValue() / max;
  }

  static int getMinReplica(ContainerInfo container) {
    return container.getReplicationConfig().getMinimumNodes();
  }

  @Override
  protected void process(NodeRegistrationContainerReport report) {
    final DatanodeID datanodeID = report.getDatanodeDetails().getID();
    report.getReport().getReportsList().forEach(c -> {
      final ContainerID containerID = ContainerID.valueOf(c.getContainerID());
      final Entry entry = ecContainers.get(containerID);
      if (entry != null) {
        entry.add(datanodeID);
        if (entry.hasMinReplica()) {
          getSafeModeMetrics().incCurrentContainersWithECDataReplicaReportedCount();
          ecContainerWithMinReplicas.incrementAndGet();
        }
      }
    });

    if (scmInSafeMode()) {
      SCMSafeModeManager.getLogger().info(
          "SCM in safe mode. {} % containers [EC] have at N reported replica",
          getCurrentContainerThreshold() * 100);
    }
  }

  private void initializeRule() {
    ecContainers.clear();
    containerManager.getContainers(ReplicationType.EC).stream()
        .filter(this::isClosed)
        .filter(c -> c.getNumberOfKeys() > 0)
        .forEach(info -> ecContainers.put(info.containerID(), new Entry(info)));
    ecMaxContainer = ecContainers.size();
    long ecCutOff = (long) Math.ceil(ecMaxContainer * safeModeCutoff);
    getSafeModeMetrics().setNumContainerWithECDataReplicaReportedThreshold(ecCutOff);

    LOG.info("Refreshed Containers with ec n replica threshold count {}.", ecCutOff);
  }

  private boolean isClosed(ContainerInfo container) {
    final LifeCycleState state = container.getState();
    return state == LifeCycleState.QUASI_CLOSED || state == LifeCycleState.CLOSED;
  }

  @Override
  public String getStatusText() {
    final double current = getCurrentContainerThreshold();
    String status = String.format(
        "%1.2f%% of [EC] Containers (%s / %s) with min reported replicas: %s safeModeCutoff (=%1.2f);",
        current * 100, ecContainerWithMinReplicas, ecMaxContainer,
        current >= safeModeCutoff ? ">=" : "<", safeModeCutoff);

    final List<ContainerID> sampleEcContainers = ecContainers.values().stream()
        .filter(entry -> !entry.hasMinReplica())
        .map(Entry::getContainer)
        .map(ContainerInfo::containerID)
        .limit(SAMPLE_CONTAINER_DISPLAY_LIMIT)
        .collect(Collectors.toList());

    String sample = "";
    if (!sampleEcContainers.isEmpty()) {
      sample = "\nSample EC Containers not having enough replicas: " + sampleEcContainers;
    }

    return status + sample;
  }

  @Override
  public synchronized void refresh(boolean forceRefresh) {
    if (forceRefresh || !validate()) {
      initializeRule();
    }
  }

  @Override
  protected void cleanup() {
    ecContainers.clear();
  }

  static class Entry {
    private final ContainerInfo container;
    private final Set<DatanodeID> datanodes = new HashSet<>();

    Entry(ContainerInfo container) {
      this.container = container;
    }

    ContainerInfo getContainer() {
      return container;
    }

    boolean hasMinReplica() {
      return getDatanodeCount() >= getMinReplica(getContainer());
    }

    int getDatanodeCount() {
      return datanodes.size();
    }

    void add(DatanodeID datanodeID) {
      datanodes.add(datanodeID);
    }
  }
}
