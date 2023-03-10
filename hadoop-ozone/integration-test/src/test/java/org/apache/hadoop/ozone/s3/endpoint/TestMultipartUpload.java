/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 *
 */

package org.apache.hadoop.ozone.s3.endpoint;

import org.apache.hadoop.hdds.HddsConfigKeys;
import org.apache.hadoop.hdds.client.ReplicationConfig;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.conf.StorageUnit;
import org.apache.hadoop.hdds.scm.ScmConfig;
import org.apache.hadoop.hdds.scm.ScmConfigKeys;
import org.apache.hadoop.ozone.MiniOzoneCluster;
import org.apache.hadoop.ozone.MiniOzoneClusterImpl;
import org.apache.hadoop.ozone.OzoneConfigKeys;
import org.apache.hadoop.ozone.client.ObjectStore;
import org.apache.hadoop.ozone.client.OzoneBucket;
import org.apache.hadoop.ozone.client.OzoneClient;
import org.apache.hadoop.ozone.client.OzoneKeyDetails;
import org.apache.hadoop.ozone.client.io.OzoneInputStream;
import org.apache.hadoop.ozone.client.io.OzoneOutputStream;
import org.apache.hadoop.ozone.container.common.statemachine.DatanodeConfiguration;
import org.apache.hadoop.ozone.om.OMConfigKeys;
import org.apache.hadoop.ozone.om.helpers.BucketLayout;
import org.apache.hadoop.ozone.om.helpers.OmMultipartCommitUploadPartInfo;
import org.apache.hadoop.ozone.om.helpers.OmMultipartInfo;
import org.apache.hadoop.ozone.om.helpers.OmMultipartUploadCompleteInfo;
import org.apache.ozone.test.GenericTestUtils;
import org.apache.ratis.util.FileUtils;
import org.apache.ratis.util.JavaUtils;
import org.apache.ratis.util.Preconditions;
import org.apache.ratis.util.SizeInBytes;
import org.apache.ratis.util.StringUtils;
import org.apache.ratis.util.Timestamp;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.hdds.client.ReplicationFactor;
import org.apache.hadoop.hdds.client.ReplicationType;
import org.slf4j.event.Level;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.function.IntPredicate;

import static org.apache.hadoop.hdds.HddsConfigKeys.HDDS_COMMAND_STATUS_REPORT_INTERVAL;
import static org.apache.hadoop.hdds.HddsConfigKeys.HDDS_CONTAINER_REPORT_INTERVAL;
import static org.apache.hadoop.hdds.HddsConfigKeys.HDDS_HEARTBEAT_INTERVAL;
import static org.apache.hadoop.hdds.HddsConfigKeys.HDDS_NODE_REPORT_INTERVAL;
import static org.apache.hadoop.hdds.HddsConfigKeys.HDDS_PIPELINE_REPORT_INTERVAL;
import static org.apache.hadoop.hdds.scm.ScmConfigKeys.HDDS_SCM_WATCHER_TIMEOUT;
import static org.apache.hadoop.hdds.scm.ScmConfigKeys.OZONE_SCM_STALENODE_INTERVAL;

/**
 * Class to test Multipart upload end to end.
 */
public class TestMultipartUpload {
  private static final Logger LOG =
      LoggerFactory.getLogger(TestMultipartUpload.class);
  static Duration ONE_SECOND = Duration.ofSeconds(1);

  static class TimerLog {
    private final Timestamp ctime = Timestamp.currentTime();
    private final Map<String, Timestamp> startTimes = new ConcurrentHashMap<>();

    AutoCloseable start(String name) {
      LOG.info("{}ms) {} start", ctime.elapsedTimeMs(), name);
      final Timestamp previous = startTimes.put(name, Timestamp.currentTime());
      Preconditions.assertNull(previous, "previous");
      return () -> end(name);
    }

    void end(String name) {
      LOG.info("{}ms) {} end, elapsed {}ms", ctime.elapsedTimeMs(), name,
          startTimes.remove(name).elapsedTimeMs());
    }
  }

  static final TimerLog TIMER_LOG = new TimerLog();

  static final int PART_SIZE = SizeInBytes.valueOf("8KB").getSizeInt();
  static final String BUCKET_NAME = "s3bucket";

  private static final OzoneConfiguration CONF = new OzoneConfiguration();
  private static final ObjectEndpoint REST = new ObjectEndpoint();

  private static MiniOzoneClusterImpl cluster;
  private static OzoneClient client;
  private static OzoneBucket bucket;

  @BeforeClass
  public static void initBeforeClass() throws Exception {
    try (AutoCloseable auto = TIMER_LOG.start("init")) {
      init();
    }
  }

  static void init() throws Exception {
    final File testDir = new File(GenericTestUtils.getTempPath(""));
    FileUtils.deleteFully(testDir);
    GenericTestUtils.setLogLevel(FileUtils.LOG, Level.TRACE);
    FileUtils.createDirectories(testDir);
    du(testDir);

    final int chunkSize = 16 << 10;
    final int flushSize = 2 * chunkSize;
    final int maxFlushSize = 2 * flushSize;
    final int blockSize = 2 * maxFlushSize;

    final BucketLayout layout = BucketLayout.FILE_SYSTEM_OPTIMIZED;
    CONF.set(OMConfigKeys.OZONE_DEFAULT_BUCKET_LAYOUT, layout.name());
    CONF.setBoolean(OMConfigKeys.OZONE_OM_RATIS_ENABLE_KEY, false);

    CONF.setTimeDuration(OMConfigKeys.OZONE_DIR_DELETING_SERVICE_INTERVAL,
        5, TimeUnit.SECONDS);
    CONF.setTimeDuration(OzoneConfigKeys.OZONE_BLOCK_DELETING_SERVICE_INTERVAL,
        5, TimeUnit.SECONDS);

    final ScmConfig scmConf = CONF.getObject(ScmConfig.class);
    scmConf.setBlockDeletionInterval(ONE_SECOND);
    CONF.setFromObject(scmConf);
    CONF.setStorageSize(ScmConfigKeys.OZONE_SCM_CONTAINER_SIZE,
        1, StorageUnit.MB);

    final DatanodeConfiguration datanodeConf = CONF.getObject(DatanodeConfiguration.class);
    datanodeConf.setBlockDeletionInterval(ONE_SECOND);
//    datanodeConf.setRecoveringContainerScrubInterval(ONE_SECOND);
    CONF.setFromObject(datanodeConf);

    final int reportIntervalMs = 1000;
    CONF.setTimeDuration(HDDS_HEARTBEAT_INTERVAL,
        reportIntervalMs, TimeUnit.MILLISECONDS);
    CONF.setTimeDuration(HDDS_CONTAINER_REPORT_INTERVAL,
        reportIntervalMs, TimeUnit.MILLISECONDS);
    CONF.setTimeDuration(HDDS_COMMAND_STATUS_REPORT_INTERVAL,
        reportIntervalMs, TimeUnit.MILLISECONDS);
    CONF.setTimeDuration(HDDS_NODE_REPORT_INTERVAL,
        reportIntervalMs, TimeUnit.MILLISECONDS);
    CONF.setTimeDuration(HDDS_PIPELINE_REPORT_INTERVAL,
        reportIntervalMs, TimeUnit.MILLISECONDS);

    CONF.setTimeDuration(HDDS_SCM_WATCHER_TIMEOUT,
        reportIntervalMs, TimeUnit.MILLISECONDS);
    CONF.setTimeDuration(OZONE_SCM_STALENODE_INTERVAL,
        3, TimeUnit.SECONDS);

    cluster = (MiniOzoneClusterImpl) MiniOzoneCluster.newBuilder(CONF)
        .setNumDatanodes(5)
        .setTotalPipelineNumLimit(1)
        .setBlockSize(blockSize)
        .setChunkSize(chunkSize)
        .setStreamBufferFlushSize(flushSize)
        .setStreamBufferMaxSize(maxFlushSize)
        .setStreamBufferSizeUnit(org.apache.hadoop.conf.StorageUnit.BYTES)
        .build();
    cluster.waitForClusterToBeReady();
    cluster.getOzoneManager()
        .setMinMultipartUploadPartSize(PART_SIZE);

    client = cluster.getClient();
    // create s3 bucket
    final ObjectStore objectStore = client.getObjectStore();
    objectStore.createS3Bucket(BUCKET_NAME);
    bucket = objectStore.getS3Bucket(BUCKET_NAME);
    Assert.assertNotNull(bucket);

    REST.setOzoneConfiguration(CONF);
    REST.setClient(client);
  }

  @AfterClass
  public static void teardownAfterClass() throws Exception {
    if (client != null) {
      client.close();
    }
    if (cluster != null) {
      cluster.stop();
    }
  }

  static final int N = 10;

  @Test
  public void testMultipartParallel() {
    final int n = 2;
    final ExecutorService executor = Executors.newFixedThreadPool(2);
    final List<CompletableFuture<Void>> futures = new ArrayList<>();
    try {
      for(int i = 0; i < n; i++) {
        final int id = i;
        final CompletableFuture<Void> f = CompletableFuture.runAsync(
            () -> runTestMultipart(N, null, id), executor);
        futures.add(f);
      }

      JavaUtils.allOf(futures).join();
      runTestMultipart(1, i -> false, n);
    } finally {
      executor.shutdown();
    }
  }

  @Test
  public void testMultipartNoDelete() {
    runTestMultipart(N, i -> false);
  }

  @Test
  public void testMultipartDeleteAll() {
    runTestMultipart(N, i -> true);
  }

  @Test
  public void testMultipartDeleteAllExceptLast() {
    runTestMultipart(N, i -> i < N - 1);
  }

  @Test
  public void testMultipartDeleteLastKey() {
    runTestMultipart(N, i -> i == N - 1);
  }

  @Test
  public void testMultipartDeleteEven() {
    runTestMultipart(N, i -> i % 2 == 0);
  }

  @Test
  public void testMultipartDeleteOdd() {
    runTestMultipart(N, i -> i % 2 == 1);
  }

  static void runTestMultipart(int n, IntPredicate decideToDelete) {
    runTestMultipart(n, decideToDelete, 0);
  }

  /**
   * Repeating upload the same key name n times
   * using multipart upload with 1000 parts and 8KB each.
   *
   * @param n the number of uploads
   * @param decideToDelete decide if delete the i-th uploaded key.
   * @param testId For running in parallel
   */
  static void runTestMultipart(int n, IntPredicate decideToDelete, int testId) {
    final String keyName = "testKey";
    final int numParts = 1_000;

    ByteGenerator generator = null;
    boolean deleted = false;
    for (int i = 0; i < n; i++) {
      final String name = testId + keyName + i;
      generator = ByteGenerator.get(name);
      try (AutoCloseable auto = TIMER_LOG.start(name)) {
        uploadKey(keyName, numParts, generator);
        if (decideToDelete != null) {
          checkKey(keyName, numParts, generator);

          deleted = decideToDelete.test(i);
          LOG.info("{}) delete? {}", i, deleted);
          if (deleted) {
            bucket.deleteKey(keyName);
          }
          Assert.assertEquals(!deleted, bucket.listKeys(keyName).hasNext());
        }
      } catch (Exception e) {
        throw new IllegalStateException("Failed to runTestMultipart " + name, e);
      }
      cluster.printContainerInfo(false);
    }

    if (decideToDelete != null) {
      try {
        checkKey(keyName, numParts, generator, deleted);
      } catch (Exception e) {
        throw new IllegalStateException("Failed to checkKey", e);
      }
    }
  }

  static void checkKey(String keyName, int numParts, ByteGenerator generator, boolean deleted) throws Exception {
    final File clusterDir = cluster.getDir();
    int previousBlockFileCount = -1;
    int age = 0;
    for(int i = 0; ; i++) {
      // run du to see the usage information
      du(clusterDir);

      // find .block files on disks
      final List<File> files = find(clusterDir);
      cluster.verifyBlockFiles(files);

      final int blockFileCount = files.size();
      if (blockFileCount == previousBlockFileCount) {
        age++;
        if (age >= 10) {
          Assert.fail("blockFileCount remains unchanged: " + blockFileCount);
        }
      } else {
        previousBlockFileCount = blockFileCount;
      }
      LOG.info("{}) found {} block file(s) in {}", i, blockFileCount, clusterDir);
      if (deleted && blockFileCount == 0) {
        return;
      }
      final int expectedCount = 3*numParts;
      if (!deleted) {
        if (blockFileCount == expectedCount) {
          checkKey(keyName, numParts, generator);
          return;
        }
        if (blockFileCount < expectedCount) {
          Assert.fail("blockFileCount = " + blockFileCount
              + " < expectedCount = " + expectedCount);
        }
      }

      // sleep and retry
      Thread.sleep(5000);
      if (!deleted) {
        checkKey(keyName, numParts, generator);
      }
      cluster.printContainerInfo(true);
    }
  }

  public static void main(String[] args) throws Exception {
    final File dir = new File(".");
    LOG.info("dir {}", dir);
    exec(dir, "pwd");
    find(dir);
  }

  static Consumer<String> limitedPrint(int limit) {
    final AtomicInteger count = new AtomicInteger();
    return s -> {
      final int previous = count.getAndIncrement();
      if (previous < limit) {
        System.out.println(s);
      } else if (previous == limit) {
        System.out.println("...");
      }
    };
  }

  static Consumer<String> getFile(List<File> files) {
    return s -> files.add(new File(s));
  }


  static List<File> find(File dir) throws Exception {
    final List<File> files = new ArrayList<>();
    final int count = exec(dir, "find . -name *.block", limitedPrint(10), getFile(files));
    Assert.assertEquals(count, files.size());
    return Collections.unmodifiableList(files);
  }

  static int du(File dir) throws Exception {
    return exec(dir, "du -h -d 2");
  }

  static int exec(File dir, String cmd) throws Exception {
    return exec(dir, cmd, System.out::println);
  }

  static int exec(File dir, String cmd, Consumer<String>... consumers) throws Exception {
    LOG.info("exec '{}' at {}", cmd, dir);

    final Process p = Runtime.getRuntime().exec(cmd.split(" "), null, dir);
    int count = 0;
    try(BufferedReader in = new BufferedReader(new InputStreamReader(p.getInputStream()))) {
      for(String line; (line = in.readLine()) != null;) {
        count++;

        for(Consumer<String> c : consumers) {
          c.accept(line);
        }
      }
    }
    final int exit = p.waitFor();
    Preconditions.assertSame(0, exit, cmd);
    return count;
  }

  static void checkKey(String keyName, int numParts, ByteGenerator generator) throws Exception {
    LOG.info("checkKey: {} with {} parts", keyName, numParts);
    final OzoneKeyDetails details = bucket.getKey(keyName);
    LOG.debug("details = {}", details);
    final long size = details.getDataSize();
    Assert.assertEquals(numParts * PART_SIZE, size);

    final byte[] buffer = new byte[4 << 10];
    try (OzoneInputStream in = bucket.readKey(keyName)) {
      for (int part = 1; part <= numParts; part++) {
        final BiConsumer<byte[], Integer> verifier = generator.verifier(part);

        for (int offset = 0; offset < PART_SIZE; ) {
          final int toRead = Math.min(PART_SIZE - offset, buffer.length);
          final int read = in.read(buffer, 0, toRead);
          if (LOG.isDebugEnabled()) {
            LOG.debug("{}-{}.{}: read {}", keyName, part, offset,
                bytes2HexShortString(buffer));
          }
          try {
            verifier.accept(buffer, read);
          } catch (AssertionError e) {
            throw new IllegalStateException("Failed to verify " + keyName
                + ": part=" + part
                + ", offset=" + offset
                + ", read=" + read, e);
          }
          offset += read;
        }
      }
    }
  }


  static void uploadKey(String keyName, int numParts, ByteGenerator g) throws Exception {
    LOG.info("uploadKey: {} with {} parts", keyName, numParts);
    final OmMultipartInfo multipartInfo = bucket.initiateMultipartUpload(keyName,
        ReplicationConfig.fromTypeAndFactor(ReplicationType.RATIS, ReplicationFactor.THREE));

    LOG.info("multipartInfo = {}", multipartInfo);
    Assert.assertNotNull(multipartInfo);
    Assert.assertEquals(keyName, multipartInfo.getKeyName());

    final String uploadID = multipartInfo.getUploadID();
    final Map<Integer, String> parts;
    final ExecutorService executor = Executors.newFixedThreadPool(32);
    try {
      parts = uploadKey(keyName, numParts, g, uploadID, executor);
    } finally {
      executor.shutdown();
    }

    completeUpload(keyName, parts, uploadID);
  }

  static Map<Integer, String> uploadKey(String keyName, int numParts, ByteGenerator g,
      String uploadID, ExecutorService executor) {
    final List<CompletableFuture<OmMultipartCommitUploadPartInfo>> futures = new ArrayList<>();
    for(int i = 0; i < numParts; i++) {
      final int part = i + 1;
      final CompletableFuture<OmMultipartCommitUploadPartInfo> f
          = CompletableFuture.supplyAsync(
              () -> uploadPart(keyName, part, uploadID, g.get(part)), executor);
      futures.add(f);
    }

    final Map<Integer, String> parts = new TreeMap<>();
    for(int i = 0; i < numParts; i++) {
      final int part = i + 1;
      final String partName = futures.get(i).join().getPartName();
      parts.put(part, partName);
    }
    return parts;
  }

  static String bytes2HexShortString(byte[] bytes) {
    int size = bytes.length;
    if (size == 0) {
      return "<EMPTY>";
    } else if (size <= 10) {
      return StringUtils.bytes2HexString(bytes).toUpperCase();
    }
    return StringUtils.bytes2HexString(bytes, 0, 10).toUpperCase()
        + "...(size=" + size + ")";
  }

  static OmMultipartCommitUploadPartInfo uploadPart(String keyName,
      int part, String uploadID, Consumer<byte[]> random) {
    if (part % 100 == 0) {
      LOG.info("uploadPart {} for {}", part, uploadID);
    } else {
      LOG.debug("uploadPart {} for {}", part, uploadID);
    }

    final int size = PART_SIZE;
    try {
      final OzoneOutputStream out = bucket.createMultipartKey(
          keyName, size, part, uploadID);
      final byte[] buffer = new byte[4 << 10];
      for (int offset = 0; offset < size; ) {
        final int toWrite = Math.min(size - offset, buffer.length);
        random.accept(buffer);
        if (LOG.isDebugEnabled()) {
          LOG.debug("{}-{}.{}: write {}", keyName, part, offset,
              bytes2HexShortString(buffer));
        }
        out.write(buffer, 0, toWrite);
        offset += toWrite;
      }
      out.close();

      final OmMultipartCommitUploadPartInfo commitInfo
          = out.getCommitUploadPartInfo();
      LOG.debug("{}-{} commitInfo = {}", keyName, part, commitInfo);
      Assert.assertNotNull(commitInfo);
      Assert.assertNotNull(commitInfo.getPartName());
      return commitInfo;
    } catch (IOException e) {
      throw new IllegalStateException(
          "Failed to upload part " + part + " for " + uploadID, e);
    }
  }

  static OmMultipartUploadCompleteInfo completeUpload(String keyName,
      Map<Integer, String> partsMap, String uploadID) throws Exception {
    final OmMultipartUploadCompleteInfo completeInfo = bucket.completeMultipartUpload(
        keyName, uploadID, partsMap);
    LOG.info("completeInfo = {}", completeInfo);
    Assert.assertNotNull(completeInfo);
    Assert.assertEquals(completeInfo.getVolume(), bucket.getVolumeName());
    Assert.assertEquals(completeInfo.getBucket(), bucket.getName());
    Assert.assertEquals(completeInfo.getKey(), keyName);
    Assert.assertNotNull(completeInfo.getHash());
    return completeInfo;
  }
}
