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

package org.apache.ozone.test;

import org.apache.hadoop.fs.CommonConfigurationKeysPublic;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdds.conf.DatanodeRatisServerConfig;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.ratis.conf.RatisClientConfig;
import org.apache.hadoop.hdds.scm.OzoneClientConfig;
import org.apache.hadoop.ozone.MiniOzoneCluster;
import org.apache.hadoop.ozone.MiniOzoneHAClusterImpl;
import org.apache.hadoop.ozone.client.ObjectStore;
import org.apache.hadoop.ozone.client.OzoneClient;
import org.apache.hadoop.ozone.om.OmMetadataManagerImpl;
import org.apache.hadoop.ozone.om.OzoneManager;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.event.Level;

import java.io.IOException;
import java.time.Duration;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

import static org.apache.hadoop.ozone.OzoneConfigKeys.OZONE_BLOCK_DELETING_SERVICE_INTERVAL;
import static org.apache.hadoop.ozone.OzoneConfigKeys.OZONE_OM_LEASE_SOFT_LIMIT;
import static org.apache.hadoop.ozone.OzoneConsts.OZONE_URI_SCHEME;
import static org.apache.hadoop.ozone.om.OMConfigKeys.OZONE_DIR_DELETING_SERVICE_INTERVAL;

/** Test Ozone with HA cluster. */
public class TestOzoneHAInconsistency {
  public static final Logger LOG = LoggerFactory.getLogger(TestOzoneHAInconsistency.class);

  {
    GenericTestUtils.setLogLevel(LoggerFactory.getLogger("org.apache.ratis"), Level.ERROR);
    GenericTestUtils.setLogLevel(LoggerFactory.getLogger("org.apache.hadoop.ipc"), Level.ERROR);
    GenericTestUtils.setLogLevel(LoggerFactory.getLogger("org.apache.hadoop.metrics2"), Level.ERROR);
    GenericTestUtils.setLogLevel(LoggerFactory.getLogger("org.apache.hadoop.hdds"), Level.ERROR);
    GenericTestUtils.setLogLevel(LoggerFactory.getLogger("SCMHATransactionMonitor"), Level.ERROR);
    GenericTestUtils.setLogLevel(LoggerFactory.getLogger("BackgroundPipelineScrubber"), Level.ERROR);
    GenericTestUtils.setLogLevel(LoggerFactory.getLogger("ExpiredContainerReplicaOpScrubber"), Level.ERROR);
    GenericTestUtils.setLogLevel(LoggerFactory.getLogger("org.apache.hadoop.ozone.container.common"), Level.ERROR);
  }

  static final Consumer<Object> DUMP = s -> System.out.format("DUMP %s%n", s);
  static final Consumer<Object> PRINT = s -> System.out.format("%s%n", s);

  static final String OM_SERVICE_ID = "om-service-1";

  static OzoneConfiguration newConf() {
    final OzoneConfiguration conf = new OzoneConfiguration();

    final DatanodeRatisServerConfig ratisServerConfig = conf.getObject(DatanodeRatisServerConfig.class);
    ratisServerConfig.setRequestTimeOut(Duration.ofSeconds(3));
    ratisServerConfig.setWatchTimeOut(Duration.ofSeconds(10));
    conf.setFromObject(ratisServerConfig);

    final RatisClientConfig.RaftConfig raftClientConfig = conf.getObject(RatisClientConfig.RaftConfig.class);
    raftClientConfig.setRpcRequestTimeout(Duration.ofSeconds(3));
    raftClientConfig.setRpcWatchRequestTimeout(Duration.ofSeconds(10));
    conf.setFromObject(raftClientConfig);

    final OzoneClientConfig clientConfig = conf.getObject(OzoneClientConfig.class);
    clientConfig.setStreamBufferFlushDelay(false);
    conf.setFromObject(clientConfig);

    conf.setTimeDuration(OZONE_OM_LEASE_SOFT_LIMIT, 0, TimeUnit.SECONDS);
    conf.setTimeDuration(OZONE_BLOCK_DELETING_SERVICE_INTERVAL, 100, TimeUnit.MILLISECONDS);
    conf.setTimeDuration(OZONE_DIR_DELETING_SERVICE_INTERVAL, 100, TimeUnit.MILLISECONDS);
    return conf;
  }

  static MiniOzoneHAClusterImpl newCluster(OzoneConfiguration conf) throws IOException {
    return MiniOzoneCluster.newHABuilder(conf)
        .setOMServiceId(OM_SERVICE_ID)
        .setNumOfOzoneManagers(3)
        .setSCMServiceId("scm-" + UUID.randomUUID())
        .setNumOfStorageContainerManagers(3)
        .build();
  }

  @Test
  public void testInconsistency() throws Exception {
    final OzoneConfiguration conf = newConf();
    try (MiniOzoneHAClusterImpl cluster = newCluster(conf)) {
      t0(cluster);

      TimeUnit.SECONDS.sleep(3);
    }
  }

  /**
   * create: /table/_tmp/task_id/attempt_id/file.txt
   * mkdirs: /table/_tmp/task_id/part1/part2
   */
  void t0(MiniOzoneHAClusterImpl cluster) throws Exception {
    final String attempt_id = "/table/_tmp/task_id/attempt_id";
    final String filename = "file.txt";
    final String part2 = "/table/_tmp/task_id/part1/part2";

    final OzoneConfiguration conf = new OzoneConfiguration(cluster.getConf());

    final String volumeName = "vol1";
    final String bucketName = "buck1";

    try(OzoneClient client = cluster.newClient()) {
      ObjectStore objectStore = client.getObjectStore();
      objectStore.createVolume(volumeName);
      objectStore.getVolume(volumeName).createBucket(bucketName);
    }

    final String rootPath = String.format("%s://%s.%s.%s/", OZONE_URI_SCHEME, bucketName, volumeName, OM_SERVICE_ID);
//    final String rootPath = String.format("%s://%s/", OZONE_OFS_URI_SCHEME, conf.get(OZONE_OM_ADDRESS_KEY));
    conf.set(CommonConfigurationKeysPublic.FS_DEFAULT_NAME_KEY, rootPath);
    try(FileSystem fs = FileSystem.get(conf)) {
      PRINT.accept("fs: " + fs.getUri());
      final Path part2path = new Path(part2);
      fs.mkdirs(part2path);

      final Path attempt_id_dir = new Path(attempt_id);
      fs.mkdirs(attempt_id_dir);

      final Path file_path = new Path(attempt_id_dir, filename);
      fs.create(file_path).close();

      PRINT.accept("file : " + fs.getFileStatus(file_path));
      PRINT.accept("part2: " + fs.getFileStatus(part2path));
    }

    dumpTables(cluster);
  }

  static void dumpTables(MiniOzoneHAClusterImpl cluster) throws Exception {
    for(OzoneManager om : cluster.getOzoneManagersList()) {
      DUMP.accept("");
      DUMP.accept(om.getOMNodeId());
      ((OmMetadataManagerImpl)om.getMetadataManager()).dumpFsoTables(DUMP);
    }
  }
}
