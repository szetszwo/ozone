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
import org.apache.hadoop.ozone.om.ratis.OzoneManagerRatisServer.RaftServerStatus;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.event.Level;

import java.io.IOException;
import java.time.Duration;
import java.util.EnumMap;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

import static org.apache.hadoop.ozone.OzoneConfigKeys.OZONE_ACL_AUTHORIZER_CLASS;
import static org.apache.hadoop.ozone.OzoneConfigKeys.OZONE_ACL_ENABLED;
import static org.apache.hadoop.ozone.OzoneConfigKeys.OZONE_BLOCK_DELETING_SERVICE_INTERVAL;
import static org.apache.hadoop.ozone.OzoneConfigKeys.OZONE_OM_LEASE_SOFT_LIMIT;
import static org.apache.hadoop.ozone.OzoneConsts.OZONE_URI_SCHEME;
import static org.apache.hadoop.ozone.om.OMConfigKeys.OZONE_DIR_DELETING_SERVICE_INTERVAL;

/** Test Ozone with HA cluster. */
public class TestOzoneHAInconsistency {
  public static final Logger LOG = LoggerFactory.getLogger(TestOzoneHAInconsistency.class);

  static final Consumer<Object> DUMP = s -> System.out.format("DUMP %s%n", s);
  static final Consumer<Object> ROLE = s -> System.out.format("ROLE %s%n", s);
  static final Consumer<Object> STEP = s -> System.out.format("STEP %s%n", s);
  static final Consumer<Object> PRINT = s -> System.out.format("PRINT %s%n", s);

  static final String OM_SERVICE_ID = "om-service-1";
  static final String VOLUME_NAME = "vol1";
  static final String BUCKET_NAME = "buck1";

  static void setLogLevel() {
    GenericTestUtils.setLogLevel(LoggerFactory.getLogger("org.apache.ratis"), Level.ERROR);
    GenericTestUtils.setLogLevel(LoggerFactory.getLogger("org.apache.hadoop.ipc"), Level.ERROR);
    GenericTestUtils.setLogLevel(LoggerFactory.getLogger("org.apache.hadoop.metrics2"), Level.ERROR);
    GenericTestUtils.setLogLevel(LoggerFactory.getLogger("org.apache.hadoop.hdds"), Level.ERROR);
    GenericTestUtils.setLogLevel(LoggerFactory.getLogger("SCMHATransactionMonitor"), Level.ERROR);
    GenericTestUtils.setLogLevel(LoggerFactory.getLogger("BackgroundPipelineScrubber"), Level.ERROR);
    GenericTestUtils.setLogLevel(LoggerFactory.getLogger("ExpiredContainerReplicaOpScrubber"), Level.ERROR);
    GenericTestUtils.setLogLevel(LoggerFactory.getLogger("org.apache.hadoop.ozone.container.common"), Level.ERROR);
  }

  static void dumpTables(MiniOzoneHAClusterImpl cluster) throws Exception {
    for(OzoneManager om : cluster.getOzoneManagersList()) {
      DUMP.accept("");
      DUMP.accept(om.getOMNodeId() + ": " + om.getOmRatisServer().getLeaderStatus());
      ((OmMetadataManagerImpl)om.getMetadataManager()).dumpFsoTables(DUMP);
    }
  }

  static EnumMap<RaftServerStatus, OzoneManager> getOMs(MiniOzoneHAClusterImpl cluster, Consumer<Object> out) {
    final EnumMap<RaftServerStatus, OzoneManager> map = new EnumMap<>(RaftServerStatus.class);
    for(OzoneManager om : cluster.getOzoneManagersList()) {
      final RaftServerStatus status = om.getOmRatisServer().getLeaderStatus();
      if (out != null) {
        out.accept(om.getOMNodeId() + ": " + status);
      }
      map.put(status, om);
    }
    return map;
  }

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

    conf.setBoolean(OZONE_ACL_ENABLED, true);
    conf.set(OZONE_ACL_AUTHORIZER_CLASS, OmAuthorizerForTesting.class.getName());

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

  final String tmp = "/table/_tmp";
  final Path tmp_path = new Path(tmp);

  final String attempt_id = tmp + "/task_id/attempt_id";
  final Path attempt_id_path = new Path(attempt_id);

  final String filename = "file.txt";
  final Path file_path = new Path(attempt_id_path, filename);

  final String part1 = tmp + "/task_id/part1";
  final Path part1_path = new Path(part1);

  final String part2 = part1 + "/part2";
  final Path part2_path = new Path(part2);
  final Path part2_file_path = new Path(part2_path, filename);

  final String table_part1 = "/table/part1";
  final Path table_part1_path = new Path(table_part1);

  @Test
  public void testInconsistency() throws Exception {
    setLogLevel();
    try (MiniOzoneHAClusterImpl cluster = newCluster(newConf())) {
      // setup volume and bucket
      try(OzoneClient client = cluster.newClient()) {
        final ObjectStore store = client.getObjectStore();
        store.createVolume(VOLUME_NAME);
        store.getVolume(VOLUME_NAME).createBucket(BUCKET_NAME);
      }

      final EnumMap<RaftServerStatus, OzoneManager> ozoneManagers = getOMs(cluster, ROLE);
      
      // run tests
      final OzoneConfiguration conf = getConf(cluster);
      try(FileSystem fs = FileSystem.get(conf)) {

        // t0: create /table/_tmp/task_id/attempt_id/file.txt
        // t1: mkdirs /table/_tmp/task_id/part1/part2
        t0_1(fs);
        dumpTables(cluster);

        /////////////////////////////////////////////////
        // Set disallow for one of the non-leader OMs.
        final OzoneManager nonLeader = ozoneManagers.get(RaftServerStatus.NOT_LEADER);
        final OmAuthorizerForTesting nonLeaderAuthorizer = OmAuthorizerForTesting.getInstance(nonLeader.getOMNodeId());
        nonLeaderAuthorizer.setAllowed(false);
        PRINT.accept("Disallow non-leader authorizer for " + nonLeader.getOMNodeId());
        /////////////////////////////////////////////////

        // t2: rename table/_tmp/task_id/attempt_id/file.txt table/_tmp/task_id/part1/part2
        rename("t2", file_path, part2_file_path, fs);
        dumpTables(cluster);

        // t3: rename src: table/_tmp/task_id/part1  table/part1
        rename("t3", part1_path, table_part1_path, fs);
        dumpTables(cluster);

        // t4: delete table/_tmp
        delete("t4", tmp_path, fs);
        dumpTables(cluster);
      }

      TimeUnit.SECONDS.sleep(3);
    }
  }

  static OzoneConfiguration getConf(MiniOzoneHAClusterImpl cluster) {
    final OzoneConfiguration conf = new OzoneConfiguration(cluster.getConf());
    final String rootPath = String.format("%s://%s.%s.%s/", OZONE_URI_SCHEME, BUCKET_NAME, VOLUME_NAME, OM_SERVICE_ID);
//    final String rootPath = String.format("%s://%s/", OZONE_OFS_URI_SCHEME, conf.get(OZONE_OM_ADDRESS_KEY));
    conf.set(CommonConfigurationKeysPublic.FS_DEFAULT_NAME_KEY, rootPath);
    return conf;
  }

  /**
   * Initialize the fs.
   * t0: create /table/_tmp/task_id/attempt_id/file.txt
   * t1: mkdirs /table/_tmp/task_id/part1/part2
   */
  void t0_1(FileSystem fs) throws Exception {
    PRINT.accept("fs: " + fs.getUri());

    // t0
    STEP.accept("t0");
    fs.mkdirs(attempt_id_path);
    fs.create(file_path).close();
    PRINT.accept("file : " + fs.getFileStatus(file_path));

    // t1
    STEP.accept("t1");
    fs.mkdirs(part2_path);
    PRINT.accept("part2: " + fs.getFileStatus(part2_path));
  }

  static void rename(String step, Path src, Path dst, FileSystem fs) throws Exception {
    STEP.accept(String.format("%s: rename %s %s", step, src, dst));
    fs.rename(src, dst);
    PRINT.accept(String.format("%s: %s", dst, fs.getFileStatus(dst)));
  }

  static void delete(String step, Path path, FileSystem fs) throws Exception {
    STEP.accept(String.format("%s: delete %s", step, path));
    fs.delete(path, true);
  }
}
