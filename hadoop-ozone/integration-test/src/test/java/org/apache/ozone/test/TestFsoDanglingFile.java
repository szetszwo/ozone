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
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.ozone.MiniOzoneCluster;
import org.apache.hadoop.ozone.MiniOzoneHAClusterImpl;
import org.apache.hadoop.ozone.client.ObjectStore;
import org.apache.hadoop.ozone.client.OzoneClient;
import org.apache.hadoop.ozone.om.OmMetadataManagerImpl;
import org.apache.hadoop.ozone.om.OzoneManager;
import org.apache.hadoop.ozone.om.ratis.OzoneManagerRatisServer.RaftServerStatus;
import org.apache.ratis.protocol.ClientId;
import org.apache.ratis.protocol.RaftClientReply;
import org.apache.ratis.protocol.TransferLeadershipRequest;
import org.apache.ratis.server.RaftServer;
import org.apache.ratis.util.JavaUtils;
import org.apache.ratis.util.TimeDuration;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.event.Level;

import java.io.IOException;
import java.util.EnumMap;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

import static org.apache.hadoop.ozone.OzoneConfigKeys.OZONE_ACL_AUTHORIZER_CLASS;
import static org.apache.hadoop.ozone.OzoneConfigKeys.OZONE_ACL_ENABLED;
import static org.apache.hadoop.ozone.OzoneConfigKeys.OZONE_BLOCK_DELETING_SERVICE_INTERVAL;
import static org.apache.hadoop.ozone.OzoneConsts.OZONE_URI_SCHEME;
import static org.apache.hadoop.ozone.om.OMConfigKeys.OZONE_DIR_DELETING_SERVICE_INTERVAL;
import static org.apache.hadoop.ozone.om.ratis.OzoneManagerRatisServer.RaftServerStatus.LEADER_AND_READY;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** Test a dangling file problem in FSO. */
public class TestFsoDanglingFile {
  public static final Logger LOG = LoggerFactory.getLogger(TestFsoDanglingFile.class);

  static final Consumer<Object> DUMP = s -> {
    if (s == null) {
      System.out.println();
    } else {
      System.out.format("DUMP %s%n", s);
    }
  };
  static final Consumer<Object> ROLE = s -> System.out.format("ROLE %s%n", s);
  static final Consumer<Object> STEP = s -> {
    System.out.println("---------------------------------------------------------------------------------------------");
    LOG.info("STEP {}", s);
  };
  static final Consumer<Object> PRINT = s -> System.out.format("PRINT %s%n", s);

  static final String OM_SERVICE_ID = "om-service-1";
  static final String VOLUME_NAME = "vol1";
  static final String BUCKET_NAME = "buck1";

  static void setLogLevel() {
    GenericTestUtils.setLogLevel(LoggerFactory.getLogger("org.apache.ratis"), Level.ERROR);
    GenericTestUtils.setLogLevel(LoggerFactory.getLogger("org.apache.hadoop.ipc"), Level.ERROR);
    GenericTestUtils.setLogLevel(LoggerFactory.getLogger("org.apache.hadoop.metrics2"), Level.ERROR);
    GenericTestUtils.setLogLevel(LoggerFactory.getLogger("org.apache.hadoop.hdds"), Level.ERROR);
    GenericTestUtils.setLogLevel(LoggerFactory.getLogger("org.apache.hadoop.ozone.container.common"), Level.ERROR);
  }

  static void dumpTables(MiniOzoneHAClusterImpl cluster) throws Exception {
    dumpTables(cluster, false);
  }

  static void dumpTables(MiniOzoneHAClusterImpl cluster, boolean assertion) throws Exception {
    String previousDump = null;
    for(OzoneManager om : cluster.getOzoneManagersList()) {
      DUMP.accept(null);
      DUMP.accept(om.getOMNodeId() + ": " + om.getOmRatisServer().getLeaderStatus());

      final StringBuilder b = new StringBuilder();
      ((OmMetadataManagerImpl)om.getMetadataManager()).dumpFsoTables(obj -> {
        b.append(obj).append("\n");
        DUMP.accept(obj);
      });

      if (assertion) {
        final String currentDump = b.toString();
        if (previousDump != null) {
          assertEquals(previousDump, currentDump);
        }
        previousDump = currentDump;
      }
    }
  }

  static EnumMap<RaftServerStatus, OzoneManager> getOMs(MiniOzoneHAClusterImpl cluster) {
    final EnumMap<RaftServerStatus, OzoneManager> map = new EnumMap<>(RaftServerStatus.class);
    for(OzoneManager om : cluster.getOzoneManagersList()) {
      final RaftServerStatus status = om.getOmRatisServer().getLeaderStatus();
      ROLE.accept(om.getOMNodeId() + ": " + status);
      map.put(status, om);
    }
    return map;
  }

  static OzoneConfiguration newConf() {
    final OzoneConfiguration conf = new OzoneConfiguration();

    conf.setTimeDuration(OZONE_BLOCK_DELETING_SERVICE_INTERVAL, 100, TimeUnit.MILLISECONDS);
    conf.setTimeDuration(OZONE_DIR_DELETING_SERVICE_INTERVAL, 100, TimeUnit.MILLISECONDS);

    conf.setBoolean(OZONE_ACL_ENABLED, true);
    conf.set(OZONE_ACL_AUTHORIZER_CLASS, OmAuthorizerForTesting.class.getName());

    return conf;
  }

  static MiniOzoneHAClusterImpl newCluster(OzoneConfiguration conf) throws IOException {
    return (MiniOzoneHAClusterImpl) MiniOzoneCluster.newHABuilder(conf)
        .setOMServiceId(OM_SERVICE_ID)
        .setNumOfOzoneManagers(3)
        .setSCMServiceId("scm-service")
        .setNumOfStorageContainerManagers(1)
        .setNumDatanodes(3)
        .build();
  }

  final String tmp = "/table/_tmp";
  final Path tmp_path = new Path(tmp);

  final String task_id = tmp + "/task_id";

  final String attempt_id = task_id + "/attempt_id";
  final Path attempt_id_path = new Path(attempt_id);

  final String filename = "file.txt";
  final Path file_path = new Path(attempt_id_path, filename);

  final String part1 = task_id + "/part1";
  final Path part1_path = new Path(part1);

  final String part2_name = "part2";
  final Path part2_path = new Path(part1, part2_name);

  final String table_part1 = "/table/part1";
  final Path table_part1_path = new Path(table_part1);
  final Path new_part2_path = new Path(table_part1, part2_name);

  static final String T0 = "t0: create /table/_tmp/task_id/attempt_id/file.txt";
  static final String T1 = "t1: mkdirs /table/_tmp/task_id/part1/part2";
  static final String T2 = "t2: rename /table/_tmp/task_id/attempt_id/file.txt /table/_tmp/task_id/part1/part2";
  static final String T3 = "t3: rename /table/_tmp/task_id/part1 /table/part1";
  static final String T4 = "t4: delete /table/_tmp";
  static final String T5 = "t5: wait for purging /table/_tmp";
  // T5.5: change leader
  static final String T6 = "t6: wait for purging /table/_tmp/task_id";
  static final String T7 = "t6: wait for purging /table/part1/part2";

  @Test
  public void testInconsistency() throws Exception {
    setLogLevel();
    try (MiniOzoneHAClusterImpl cluster = newCluster(newConf())) {
      cluster.waitForClusterToBeReady();

      // setup volume and bucket
      try(OzoneClient client = cluster.newClient()) {
        final ObjectStore store = client.getObjectStore();
        store.createVolume(VOLUME_NAME);
        store.getVolume(VOLUME_NAME).createBucket(BUCKET_NAME);
      }

      final EnumMap<RaftServerStatus, OzoneManager> ozoneManagers = getOMs(cluster);

      // run tests
      final OzoneConfiguration conf = getConf(cluster);
      try(FileSystem fs = FileSystem.get(conf)) {
        PRINT.accept("fs: " + fs.getUri());

        // t0: create /table/_tmp/task_id/attempt_id/file.txt
        t0(fs);
        // t1: mkdirs /table/_tmp/task_id/part1/part2
        t1(fs);
        dumpTables(cluster);

        /////////////////////////////////////////////////
        // Disallow access for one of the non-leader OMs.
        final OzoneManager nonLeader = ozoneManagers.get(RaftServerStatus.NOT_LEADER);
        final OmAuthorizerForTesting nonLeaderAuthorizer = OmAuthorizerForTesting.getInstance(nonLeader.getOMNodeId());
        nonLeaderAuthorizer.setAllowed(false);
        PRINT.accept("Disallow non-leader authorizer for " + nonLeader.getOMNodeId());
        /////////////////////////////////////////////////

        // t2: rename table/_tmp/task_id/attempt_id/file.txt table/_tmp/task_id/part1/part2
        rename(T2, file_path, part2_path, fs);
        dumpTables(cluster);

        // t3: rename table/_tmp/task_id/part1  table/part1
        rename(T3, part1_path, table_part1_path, fs);
        dumpTables(cluster);

        // t4: delete table/_tmp
        delete(T4, tmp_path, fs);
        dumpTables(cluster);

        // t5: wait for purging _tmp;
        waitForPurge(T5, tmp_path);
        dumpTables(cluster);

        /////////////////////////////////////////////////
        // Change leader to the OM with disallowed access
        final OzoneManager leader = ozoneManagers.get(LEADER_AND_READY);
        STEP.accept("Change leader from " + leader.getOMNodeId() + " to " + nonLeader.getOMNodeId());
        changeLeader(leader, nonLeader);
        JavaUtils.attempt(() -> assertSame(LEADER_AND_READY, nonLeader.getOmRatisServer().getLeaderStatus()),
            20, TimeDuration.ONE_SECOND,  nonLeader.getOMNodeId() + " becoming " + LEADER_AND_READY, LOG);
        /////////////////////////////////////////////////

        // t6: wait for purging /table/_tmp/task_id
        waitForPurge(T6, task_id);
        dumpTables(cluster);

        // t7: wait for purging /table/part1/part2
        waitForPurge(T7, new_part2_path);
        dumpTables(cluster);
      }

      // DONE: sleep and see if there are any changes
      STEP.accept("DONE");
      sleep(5);
      dumpTables(cluster, true);

      GenericTestUtils.setLogLevel(LoggerFactory.getLogger("org"), Level.ERROR);
    }
  }

  static void changeLeader(OzoneManager leader, OzoneManager nonLeader) throws Exception {
    final RaftServer.Division leaderServer = leader.getOmRatisServer().getServerDivision();
    final RaftServer.Division nonLeaderServer = nonLeader.getOmRatisServer().getServerDivision();
    final TransferLeadershipRequest request = new TransferLeadershipRequest(ClientId.emptyClientId(),
        leaderServer.getId(), leaderServer.getMemberId().getGroupId(), 0, nonLeaderServer.getId(), 10_000);
    final RaftClientReply reply = leaderServer.getRaftServer().transferLeadership(request);
    assertTrue(reply.isSuccess());

    JavaUtils.attemptUntilTrue(() -> nonLeaderServer.getInfo().isLeader(),
        20, TimeDuration.ONE_SECOND, nonLeaderServer.getId() + " becoming leader", LOG);
  }

  static void sleep(int seconds) throws InterruptedException {
    PRINT.accept("Sleep " + seconds + "s");
    TimeUnit.SECONDS.sleep(seconds);
  }

  static OzoneConfiguration getConf(MiniOzoneHAClusterImpl cluster) {
    final OzoneConfiguration conf = new OzoneConfiguration(cluster.getConf());
    final String rootPath = String.format("%s://%s.%s.%s/", OZONE_URI_SCHEME, BUCKET_NAME, VOLUME_NAME, OM_SERVICE_ID);
//    final String rootPath = String.format("%s://%s/", OZONE_OFS_URI_SCHEME, conf.get(OZONE_OM_ADDRESS_KEY));
    conf.set(CommonConfigurationKeysPublic.FS_DEFAULT_NAME_KEY, rootPath);
    return conf;
  }

  static void assertEndsWith(String step, Object expected) {
    assertEndsWith(step, expected.toString());
  }

  static void assertEndsWith(String step, String expected) {
    assertTrue(step.endsWith(expected), "\"" + step + "\" does not end with \"" + expected + "\"");
  }

  void t0(FileSystem fs) throws Exception {
    assertEndsWith(T0, file_path);
    STEP.accept(T0);
    fs.mkdirs(attempt_id_path);
    fs.create(file_path).close();
    PRINT.accept("file : " + fs.getFileStatus(file_path));
  }

  void t1(FileSystem fs) throws Exception {
    assertEndsWith(T1, part2_path);
    STEP.accept(T1);
    fs.mkdirs(part2_path);
    PRINT.accept("part2: " + fs.getFileStatus(part2_path));
  }

  static void rename(String step, Path src, Path dst, FileSystem fs) throws Exception {
    assertEndsWith(step, src + " " + dst);
    STEP.accept(String.format("%s: rename %s %s", step, src, dst));
    fs.rename(src, dst);
    PRINT.accept(String.format("%s: %s", dst, fs.getFileStatus(dst)));
  }

  static void delete(String step, Path path, FileSystem fs) throws Exception {
    assertEndsWith(step, path);
    STEP.accept(String.format("%s: delete %s", step, path));
    fs.delete(path, true);
  }

  static void waitForPurge(String step, Object path) throws Exception {
    STEP.accept(step);
    assertEndsWith(step, path);
    sleep(3);
  }
}
