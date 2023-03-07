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

import org.apache.hadoop.conf.StorageUnit;
import org.apache.hadoop.hdds.client.ReplicationConfig;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.ozone.MiniOzoneCluster;
import org.apache.hadoop.ozone.client.ObjectStore;
import org.apache.hadoop.ozone.client.OzoneBucket;
import org.apache.hadoop.ozone.client.OzoneClient;
import org.apache.hadoop.ozone.client.OzoneKeyDetails;
import org.apache.hadoop.ozone.client.io.OzoneInputStream;
import org.apache.hadoop.ozone.client.io.OzoneOutputStream;
import org.apache.hadoop.ozone.om.OMConfigKeys;
import org.apache.hadoop.ozone.om.helpers.BucketLayout;
import org.apache.hadoop.ozone.om.helpers.OmMultipartCommitUploadPartInfo;
import org.apache.hadoop.ozone.om.helpers.OmMultipartInfo;
import org.apache.hadoop.ozone.om.helpers.OmMultipartUploadCompleteInfo;
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

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.function.BiConsumer;
import java.util.function.Consumer;

import static org.apache.hadoop.ozone.OzoneConfigKeys.OZONE_BLOCK_DELETING_SERVICE_INTERVAL;
import static org.apache.hadoop.ozone.om.OMConfigKeys.OZONE_DEFAULT_BUCKET_LAYOUT;
import static org.apache.hadoop.ozone.om.OMConfigKeys.OZONE_OM_RATIS_ENABLE_KEY;

/**
 * Class to test Multipart upload end to end.
 */
public class TestMultipartUpload {
  private static final Logger LOG =
      LoggerFactory.getLogger(TestMultipartUpload.class);

  static class TimerLog {
    private final Timestamp ctime = Timestamp.currentTime();
    private final Map<String, Timestamp> startTimes = new ConcurrentHashMap<>();

    void start(String name) {
      LOG.info("{}ms) {} start", ctime.elapsedTimeMs(), name);
      final Timestamp previous = startTimes.put(name, Timestamp.currentTime());
      Preconditions.assertNull(previous, "previous");
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

  private static MiniOzoneCluster cluster;
  private static OzoneClient client;
  private static OzoneBucket bucket;

  @BeforeClass
  public static void init() throws Exception {
    TIMER_LOG.start("init");
    final int chunkSize = 16 << 10;
    final int flushSize = 2 * chunkSize;
    final int maxFlushSize = 2 * flushSize;
    final int blockSize = 2 * maxFlushSize;
    final BucketLayout layout = BucketLayout.FILE_SYSTEM_OPTIMIZED;

    CONF.setBoolean(OZONE_OM_RATIS_ENABLE_KEY, false);
    CONF.set(OZONE_DEFAULT_BUCKET_LAYOUT, layout.name());
    CONF.setTimeDuration(OMConfigKeys.OZONE_DIR_DELETING_SERVICE_INTERVAL,
        500, TimeUnit.MILLISECONDS);
    CONF.setTimeDuration(OZONE_BLOCK_DELETING_SERVICE_INTERVAL,
        100, TimeUnit.MILLISECONDS);

    cluster = MiniOzoneCluster.newBuilder(CONF)
        .setNumDatanodes(5)
        .setTotalPipelineNumLimit(10)
        .setBlockSize(blockSize)
        .setChunkSize(chunkSize)
        .setStreamBufferFlushSize(flushSize)
        .setStreamBufferMaxSize(maxFlushSize)
        .setStreamBufferSizeUnit(StorageUnit.BYTES)
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

    TIMER_LOG.end("init");
  }

  @AfterClass
  public static void teardown() throws Exception {
    if (client != null) {
      client.close();
    }
    if (cluster != null) {
      cluster.shutdown();
    }
  }

  @Test
  public void testMultipart() throws Exception {
    TIMER_LOG.start("testMultipart");
    final String keyName = "testKey";
    final int numParts = 1_000;
    uploadKey(keyName, numParts);
    checkKey(keyName, numParts);

    TIMER_LOG.end("testMultipart");
  }

  static void checkKey(String keyName, int numParts) throws Exception {
    LOG.info("checkKey: {} with {} parts", keyName, numParts);
    final OzoneKeyDetails details = bucket.getKey(keyName);
    LOG.info("details = {}", details);
    final long size = details.getDataSize();
    Assert.assertEquals(numParts * PART_SIZE, size);

    final ByteGenerator generator = ByteGenerator.get(keyName);
    final byte[] buffer = new byte[4 << 10];
    try (OzoneInputStream in = bucket.readKey(keyName)) {
      for (int part = 1; part <= numParts; part++) {
        final BiConsumer<byte[], Integer> verifier = generator.verifier(part);

        for (int offset = 0; offset < PART_SIZE; ) {
          final int toRead = Math.min(PART_SIZE - offset, buffer.length);
          final int read = in.read(buffer, 0, toRead);
          LOG.info("{}-{}.{}: read {}", keyName, part, offset,
              bytes2HexShortString(buffer));
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

  static void uploadKey(String keyName, int numParts) throws Exception {
    LOG.info("uploadKey: {} with {} parts", keyName, numParts);
    final OmMultipartInfo multipartInfo = bucket.initiateMultipartUpload(keyName,
        ReplicationConfig.fromTypeAndFactor(ReplicationType.RATIS, ReplicationFactor.THREE));

    LOG.info("multipartInfo = {}", multipartInfo);
    Assert.assertNotNull(multipartInfo);
    Assert.assertEquals(keyName, multipartInfo.getKeyName());

    final String uploadID = multipartInfo.getUploadID();
    final ExecutorService executor = Executors.newFixedThreadPool(32);
    final List<CompletableFuture<OmMultipartCommitUploadPartInfo>> futures = new ArrayList<>();
    for(int i = 0; i < numParts; i++) {
      final int part = i + 1;
      final CompletableFuture<OmMultipartCommitUploadPartInfo> f
          = CompletableFuture.supplyAsync(
              () -> uploadPart(keyName, part, uploadID), executor);
      futures.add(f);
    }

    final Map<Integer, String> parts = new TreeMap<>();
    for(int i = 0; i < numParts; i++) {
      final int part = i + 1;
      final String partName = futures.get(i).join().getPartName();
      parts.put(part, partName);
    }

    completeUpload(keyName, parts, uploadID);
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
      int part, String uploadID) {
    LOG.info("uploadPart {} for {}", part, uploadID);
    final Consumer<byte[]> random = ByteGenerator.get(keyName).get(part);
    final int size = PART_SIZE;
    try {
      final OzoneOutputStream out = bucket.createMultipartKey(
          keyName, size, part, uploadID);
      final byte[] buffer = new byte[4 << 10];
      for (int offset = 0; offset < size; ) {
        final int toWrite = Math.min(size - offset, buffer.length);
        random.accept(buffer);
        LOG.info("{}-{}.{}: write {}", keyName, part, offset,
            bytes2HexShortString(buffer));
        out.write(buffer, 0, toWrite);
        offset += toWrite;
      }
      out.close();

      final OmMultipartCommitUploadPartInfo commitInfo
          = out.getCommitUploadPartInfo();
      LOG.info("{}-{} commitInfo = {}", keyName, part, commitInfo);
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
