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

package org.apache.hadoop.ozone.client.rpc.read;

import org.apache.hadoop.hdds.client.BlockID;
import org.apache.hadoop.hdds.client.ECReplicationConfig;
import org.apache.hadoop.hdds.client.RatisReplicationConfig;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.conf.StorageUnit;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos.LifeCycleState;
import org.apache.hadoop.hdds.scm.OzoneClientConfig;
import org.apache.hadoop.hdds.scm.ScmConfigKeys;
import org.apache.hadoop.hdds.scm.container.ContainerID;
import org.apache.hadoop.hdds.scm.container.ContainerInfo;
import org.apache.hadoop.hdds.scm.container.ContainerManager;
import org.apache.hadoop.hdds.scm.pipeline.Pipeline;
import org.apache.hadoop.hdds.scm.pipeline.PipelineManager;
import org.apache.hadoop.hdds.scm.server.StorageContainerManager;
import org.apache.hadoop.hdds.scm.storage.StreamBlockInputStream;
import org.apache.hadoop.hdds.utils.db.CodecBuffer;
import org.apache.hadoop.ozone.ClientConfigForTesting;
import org.apache.hadoop.ozone.MiniOzoneCluster;
import org.apache.hadoop.ozone.client.OzoneBucket;
import org.apache.hadoop.ozone.client.OzoneClient;
import org.apache.hadoop.ozone.client.OzoneClientFactory;
import org.apache.hadoop.ozone.om.BucketForTesting;
import org.apache.hadoop.ozone.om.helpers.OmKeyInfo;
import org.apache.hadoop.ozone.om.helpers.OmKeyLocationInfo;
import org.apache.ozone.test.GenericTestUtils;
import org.apache.ratis.util.SizeInBytes;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.slf4j.LoggerFactory;
import org.slf4j.event.Level;

import java.io.OutputStream;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ThreadLocalRandom;

import static org.apache.hadoop.hdds.protocol.proto.HddsProtos.ReplicationFactor.THREE;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Tests {@link StreamBlockInputStream}.
 */
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public class TestStreamWriteV3 {

  {
    GenericTestUtils.setLogLevel(LoggerFactory.getLogger("com"), Level.ERROR);
    GenericTestUtils.setLogLevel(LoggerFactory.getLogger("org"), Level.ERROR);

    GenericTestUtils.setLogLevel(LoggerFactory.getLogger("BackgroundPipelineScrubber"), Level.ERROR);
    GenericTestUtils.setLogLevel(LoggerFactory.getLogger("ExpiredContainerReplicaOpScrubber"), Level.ERROR);
    GenericTestUtils.setLogLevel(LoggerFactory.getLogger("SCMHATransactionMonitor"), Level.ERROR);
    GenericTestUtils.setLogLevel(LoggerFactory.getLogger(CodecBuffer.class), Level.ERROR);
//    GenericTestUtils.setLogLevel(StreamBlockInputStream.class, Level.DEBUG);
  }

  static final int CHUNK_SIZE = 1 << 20;          // 1MB
  static final int FLUSH_SIZE = 2 * CHUNK_SIZE;       // 2MB
  static final int MAX_FLUSH_SIZE = 2 * FLUSH_SIZE;   // 4MB

  static final SizeInBytes KEY_SIZE = SizeInBytes.valueOf("1k");
  static final int BLOCK_SIZE = KEY_SIZE.getSizeInt();

  static final String V1_KEY = "v1Key";
  static final String V2_KEY = "v2Key";
  static final String V3_KEY = "v3Key";
  static final String EC_KEY = "ecKey";
  static final boolean CLOSE_PIPELINE = true;

  static final byte[] DATA = randomData(new byte[4096]);
  static final ECReplicationConfig EC_REP_CONF =  new ECReplicationConfig(3, 2);
  static final RatisReplicationConfig RATIS_REP_CONF = RatisReplicationConfig.getInstance(THREE);

  static byte[] randomData(byte[] array) {
    ThreadLocalRandom.current().nextBytes(array);
    return array;
  }

  private MiniOzoneCluster cluster;

  static MiniOzoneCluster newCluster() throws Exception {
    final OzoneConfiguration conf = new OzoneConfiguration();

    conf.setInt(ScmConfigKeys.OZONE_DATANODE_PIPELINE_LIMIT, 2);
    conf.setInt(ScmConfigKeys.OZONE_SCM_RATIS_PIPELINE_LIMIT, 2);
    conf.setQuietMode(true);

    ClientConfigForTesting.newBuilder(StorageUnit.BYTES)
        .setBlockSize(BLOCK_SIZE)
        .setChunkSize(CHUNK_SIZE)
        .setStreamBufferFlushSize(FLUSH_SIZE)
        .setStreamBufferMaxSize(MAX_FLUSH_SIZE)
        .applyTo(conf);

    return MiniOzoneCluster.newBuilder(conf)
        .setNumDatanodes(5)
        .build();
  }

  @BeforeAll
  void startup() throws Exception {
    System.out.println("cluster starting ...");
    cluster = newCluster();
    cluster.waitForClusterToBeReady();
    System.out.println("cluster ready");
  }

  @AfterAll
  void shutdown() {
//    IOUtils.closeQuietly(cluster);
  }

  @Test
  void testV1() throws Exception {
    runTestWriteKey(1);
  }

  @Test
  void testV2() throws Exception {
    runTestWriteKey(2);
  }

  @Test
  void testV3() throws Exception {
    runTestWriteKey(3);
  }

  @Test
  void testEC() throws Exception {
    runTestWriteKey(0);
  }

  void runTestWriteKey(int pipelineType) throws Exception {
    final OzoneConfiguration conf = cluster.getConf();
    // stream with raft
    final OzoneConfiguration steamWithRaftConf = new OzoneConfiguration(conf);
    // stream without raft
    final OzoneClientConfig clientConfig = conf.getObject(OzoneClientConfig.class);
    clientConfig.setDatastreamPutBlockWithoutRaftEnabled(true);
    final OzoneConfiguration steamWithoutRaftConf = new OzoneConfiguration(conf);
    steamWithoutRaftConf.setFromObject(clientConfig);

    assertFalse(  steamWithRaftConf.getObject(OzoneClientConfig.class).isDatastreamPutBlockWithoutRaftEnabled());
    assertTrue(steamWithoutRaftConf.getObject(OzoneClientConfig.class).isDatastreamPutBlockWithoutRaftEnabled());

    final StorageContainerManager scm = cluster.getStorageContainerManager();
    final ContainerManager containerManager = scm.getContainerManager();
    final PipelineManager pipelineManager = scm.getPipelineManager();

    try (OzoneClient steamWithoutRaftClient = OzoneClientFactory.getRpcClient(steamWithoutRaftConf);
         OzoneClient steamWithRaftClient = OzoneClientFactory.getRpcClient(steamWithRaftConf)) {
      final OzoneBucket bucketWithoutRaft = BucketForTesting.newBuilder(steamWithoutRaftClient).build().delegate();
      final OzoneBucket bucketWithRaft = BucketForTesting.newBuilder(steamWithRaftClient).build().delegate();

      final String key;
      if (pipelineType == 1) {
        key = V1_KEY;
        createRaftKey(bucketWithRaft, key, SizeInBytes.ONE_KB);
        printKey(steamWithoutRaftClient, bucketWithRaft, key, pipelineManager, containerManager);
      } else if (pipelineType == 2) {
        key = V2_KEY;
        createStreamKey(bucketWithRaft, key, SizeInBytes.ONE_KB);
        printKey(steamWithRaftClient, bucketWithRaft, key, pipelineManager, containerManager);
      } else if (pipelineType == 3) {
        key = V3_KEY;
        createStreamKey(bucketWithoutRaft, key, SizeInBytes.ONE_KB);
        printKey(steamWithoutRaftClient, bucketWithoutRaft, key, pipelineManager, containerManager);
      } else {
        key = EC_KEY;
        createEcKey(bucketWithRaft, key, SizeInBytes.ONE_KB);
        printKey(steamWithoutRaftClient, bucketWithRaft, key, pipelineManager, containerManager);
      }
      System.out.println("DONE " + key);
    }
  }


  static void printKey(OzoneClient client, OzoneBucket bucket, String key, PipelineManager pipelineManager, ContainerManager containerManager) throws Exception {
    Thread.sleep(1000);
    final OmKeyInfo info = client.getProxy().getKeyInfo(bucket.getVolumeName(), bucket.getName(), key, false);
    System.out.println(info);

    final List<OmKeyLocationInfo> locations = info.getLatestVersionLocations().getBlocksLatestVersionOnly();
    assertEquals(1, locations.size());
    final OmKeyLocationInfo location = locations.get(0);
    BlockID blockID = location.getBlockID();
    Pipeline pipeline = location.getPipeline();
    System.out.println("blockID: " + blockID + ", pipeline: " + pipeline.getId());

    if (CLOSE_PIPELINE) {
      pipelineManager.closePipeline(pipeline.getId());
      Thread.sleep(1000);
    }

    String previous = null;
    for(int i = 0; ; i++) {
      final ContainerInfo container = containerManager.getContainer(ContainerID.valueOf(blockID.getContainerID()));
      final String current = "container: " + container;
      if (!current.equals(previous)) {
        System.out.println(i + ": " + current);
      }
      previous = current;
      if (container.getState() == LifeCycleState.CLOSED) {
        break;
      }
      Thread.sleep(1000);
    }
    System.out.println("--------------------------------------------------------- ");
  }

  static void createRaftKey(OzoneBucket bucket, String keyName, SizeInBytes keySize) throws Exception {
    final long keySizeByte = keySize.getSize();
    try (OutputStream out = bucket.createKey(keyName, keySizeByte, RATIS_REP_CONF, Collections.emptyMap())) {
      writeData(keySizeByte, out);
    }
  }

  static void createStreamKey(OzoneBucket bucket, String keyName, SizeInBytes keySize) throws Exception {
    final long keySizeByte = keySize.getSize();
    try (OutputStream out = bucket.createStreamKey(keyName, keySizeByte, RATIS_REP_CONF, Collections.emptyMap())) {
      writeData(keySizeByte, out);
    }
  }


  static void createEcKey(OzoneBucket bucket, String keyName, SizeInBytes keySize) throws Exception {
    final long keySizeByte = keySize.getSize();
    try (OutputStream out = bucket.createKey(keyName, keySizeByte, EC_REP_CONF, Collections.emptyMap())) {
      writeData(keySizeByte, out);
    }
  }

  static void writeData(long keySizeByte, OutputStream out) throws Exception {
    long pos = 0;
    for (; pos < keySizeByte;) {
      final int writeSize = Math.toIntExact(Math.min(DATA.length, keySizeByte - pos));
      out.write(DATA, 0, writeSize);
      pos += writeSize;
    }
    assertEquals(keySizeByte, pos);
  }
}
