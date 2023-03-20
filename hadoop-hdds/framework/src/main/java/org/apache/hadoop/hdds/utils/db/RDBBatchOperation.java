/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */
package org.apache.hadoop.hdds.utils.db;

import org.apache.hadoop.hdds.utils.db.RocksDatabase.ColumnFamily;
import org.apache.hadoop.hdds.utils.db.managed.ManagedWriteBatch;
import org.apache.hadoop.hdds.utils.db.managed.ManagedWriteOptions;
import org.apache.ratis.util.StringUtils;
import org.apache.ratis.util.TraditionalBinaryPrefix;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Supplier;

/**
 * Batch operation implementation for rocks db.
 */
public class RDBBatchOperation implements BatchOperation {
  static final Logger LOG = LoggerFactory.getLogger(RDBBatchOperation.class);

  static void debug(Supplier<String> message) {
    if (LOG.isDebugEnabled()) {
      LOG.debug("\n{}", message.get());
    }
  }

  static String byteSize2String(int length) {
    return TraditionalBinaryPrefix.long2String(length, "B", 3);
  }

  static String countSize2String(int count, int size) {
    return count + " (" + byteSize2String(size) + ")";
  }

  /**
   * To implement {@link #equals(Object)} and {@link #hashCode()}
   * based on the contents of {@link #bytes}.
   * <p>
   * Note that it is incorrect to use {@link #bytes#equals(Object)}
   * and {@link #bytes#hashCode()} here since
   * they do not use the contents of {@link #bytes} in the computations.
   * These methods simply inherit from {@link Object).
   */
  static final class ByteArray {
    private final byte[] bytes;

    ByteArray(byte[] bytes) {
      this.bytes = bytes;
    }

    @Override
    public boolean equals(Object obj) {
      if (this == obj) {
        return true;
      } else if (obj == null || getClass() != obj.getClass()) {
        return false;
      }
      final ByteArray that = (ByteArray) obj;
      return Arrays.equals(this.bytes, that.bytes);
    }

    @Override
    public int hashCode() {
      return Arrays.hashCode(bytes);
    }
  }

  class PutOpCache {
    class FamilyCache {
      private final ColumnFamily family;
      /** A (key -> value) map. */
      private final Map<ByteArray, byte[]> putOps = new HashMap<>();

      private int batchSize;
      private int discardedSize;
      private int discardedCount;
      private int putCount;
      private int delCount;

      FamilyCache(ColumnFamily family) {
        this.family = family;
      }

      /** Batch put the entire family cache. */
      void batchPut(ManagedWriteBatch writeBatch) throws IOException {
        for (Map.Entry<ByteArray, byte[]> op : putOps.entrySet()) {
          family.batchPut(writeBatch, op.getKey().bytes, op.getValue());
        }
        putOps.clear();
      }

      void put(byte[] key, byte[] value) {
        batchSize += key.length + value.length;
        putCount++;
        final byte[] previousValue = putOps.put(new ByteArray(key), value);

        if (previousValue != null) {
          discardedSize += key.length + previousValue.length;
          discardedCount++;
          debug(() -> String.format("Overwriting a previous put(value: %s)",
              byteSize2String(previousValue.length)));
        }

        debug(() -> String.format(
            "%s: %s put(key: %s, value: %s), newPut=%s, %s; key=%s",
            name, family.getName(), byteSize2String(key.length),
            byteSize2String(value.length),
            putCount, getBatchSizeDiscardedString(),
            StringUtils.bytes2HexString(key).toUpperCase()));
      }

      void remove(byte[] key) {
        batchSize += key.length;
        delCount++;
        final byte[] removed = putOps.remove(new ByteArray(key));
        if (removed != null) {
          discardedSize += key.length + removed.length;
          discardedCount++;
          debug(() -> String.format("Removed a previous put(value: %s)",
              byteSize2String(removed.length)));
        }

        debug(() -> String.format(
            "%s: %s delete(key: %s), newDel=%s, %s; key=%s",
            name, family.getName(), byteSize2String(key.length),
            delCount, getBatchSizeDiscardedString(),
            StringUtils.bytes2HexString(key).toUpperCase()));
      }

      String getBatchSizeDiscardedString() {
        return String.format("batchSize=%s, discarded: %s",
            byteSize2String(batchSize),
            countSize2String(discardedCount, discardedSize));
      }
    }

    /** A (family name -> {@link FamilyCache}) map. */
    private final Map<String, FamilyCache> map = new HashMap<>();

    void put(ColumnFamily family, byte[] key, byte[] value) {
      map.computeIfAbsent(family.getName(), k -> new FamilyCache(family))
          .put(key, value);
    }

    void remove(ColumnFamily family, byte[] key) {
      final FamilyCache familyCache = map.get(family.getName());
      if (familyCache != null) {
        familyCache.remove(key);
      }
    }

    /** Batch put the entire cache. */
    void batchPut(ManagedWriteBatch writeBatch) throws IOException {
      for (Map.Entry<String, FamilyCache> e : map.entrySet()) {
        e.getValue().batchPut(writeBatch);
      }
      map.clear();
    }

    String getCommitString() {
      int putCount = 0;
      int delCount = 0;
      int opSize = 0;
      int discardedCount = 0;
      int discardedSize = 0;

      for (FamilyCache f : map.values()) {
        putCount += f.putCount;
        delCount += f.delCount;
        opSize += f.batchSize;
        discardedCount += f.discardedCount;
        discardedSize += f.discardedSize;
      }

      final int opCount = putCount + delCount;
      return String.format(
          "#put=%s, #del=%s, batchSize: %s, discarded: %s, committed: %s",
          putCount, delCount,
          countSize2String(opCount, opSize),
          countSize2String(discardedCount, discardedSize),
          countSize2String(opCount - discardedCount, opSize - discardedSize));
    }
  }

  private static final AtomicInteger BATCH_COUNT = new AtomicInteger();

  private final String name = "Batch-" + BATCH_COUNT.getAndIncrement();
  private final ManagedWriteBatch writeBatch;
  private final PutOpCache putOpCache = new PutOpCache();

  public RDBBatchOperation() {
    writeBatch = new ManagedWriteBatch();
  }

  public RDBBatchOperation(ManagedWriteBatch writeBatch) {
    this.writeBatch = writeBatch;
  }

  @Override
  public String toString() {
    return name;
  }

  public void commit(RocksDatabase db) throws IOException {
    debug(() -> String.format("%s: commit %s",
        name, putOpCache.getCommitString()));
    putOpCache.batchPut(writeBatch);
    db.batchWrite(writeBatch);
  }

  public void commit(RocksDatabase db, ManagedWriteOptions writeOptions)
      throws IOException {
    debug(() -> String.format("%s: commit-with-writeOptions %s",
        name, putOpCache.getCommitString()));
    putOpCache.batchPut(writeBatch);
    db.batchWrite(writeBatch, writeOptions);
  }

  @Override
  public void close() {
    debug(() -> String.format("%s: close", name));
    writeBatch.close();
  }

  public void delete(ColumnFamily family, byte[] key) throws IOException {
    putOpCache.remove(family, key);
    family.batchDelete(writeBatch, key);
  }

  public void put(ColumnFamily family, byte[] key, byte[] value)
      throws IOException {
    putOpCache.put(family, key, value);
  }
}
