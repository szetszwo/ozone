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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Supplier;

/**
 * Batch operation implementation for rocks db.
 */
public class RDBBatchOperation implements BatchOperation {
//  private static final Logger LOG
//      = LoggerFactory.getLogger(RDBBatchOperation.class);

  private static final AtomicInteger OBJECT_COUNT = new AtomicInteger();

  private final String name = "Batch-" + OBJECT_COUNT.getAndIncrement();
  private final ManagedWriteBatch writeBatch;
  private final AtomicInteger batchSize = new AtomicInteger();
  private final AtomicInteger putCount = new AtomicInteger();
  private final AtomicInteger delCount = new AtomicInteger();

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

  String toString(int byteSize) {
    if (byteSize < 1024) {
      return byteSize + " B";
    }
    final double k = byteSize / 1024.0;
    if (k < 1024) {
      return String.format("%.3f KB", k);
    }
    final double m = k / 1024.0;
    if (m < 1024) {
      return String.format("%.3f MB", m);
    }
    final double g = m / 1024.0;
    return String.format("%.3f GB", g);
  }

  String getBatchSizeString() {
    final int size = batchSize.get();
    if (size < 1024) {
      return "batchSize: " + size + " B";
    } else {
      return "batchSize: " + size + " B (= " + toString(size) + ")";
    }
  }

  public void commit(RocksDatabase db) throws IOException {
    println(() -> String.format("%s: commit (#put=%s, #del=%s): %s",
        name, putCount, delCount, getBatchSizeString()));
    db.batchWrite(writeBatch);
  }

  public void commit(RocksDatabase db, ManagedWriteOptions writeOptions)
      throws IOException {
    println(() -> String.format("%s: commit (#put=%s, #del=%s): %s",
        name, putCount, delCount, getBatchSizeString()));
    db.batchWrite(writeBatch, writeOptions);
  }

  @Override
  public void close() {
    println(() -> String.format("%s: close (#put=%s, #del=%s): %s",
        name, putCount, delCount, getBatchSizeString()));
    writeBatch.close();
  }

  public void delete(ColumnFamily family, byte[] key) throws IOException {
    final int newBatchSize = batchSize.addAndGet(key.length);
    final int newDel = delCount.incrementAndGet();
    println(() -> String.format("%s: %s delete(key: %s B), newDel=%s, newBatchSize=%s%n  del key=%s",
        name, family.getName(), key.length, newDel,
        toString(newBatchSize), StringUtils.bytes2HexString(key).toUpperCase()));
    family.batchDelete(writeBatch, key);
  }

  public void put(ColumnFamily family, byte[] key, byte[] value)
      throws IOException {
    final int newBatchSize = batchSize.addAndGet(key.length + value.length);
    final int newPut = putCount.incrementAndGet();
    println(() -> String.format("%s: %s put(key: %s B, value: %s), newPut=%s, newBatchSize=%s%n  put key=%s",
        name, family.getName(), key.length, toString(value.length), newPut,
        toString(newBatchSize), StringUtils.bytes2HexString(key).toUpperCase()));
    family.batchPut(writeBatch, key, value);
  }

  void println(Supplier<String> message) {
    System.out.println(message.get());
  }
}
