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

import java.io.IOException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Supplier;

import static org.apache.hadoop.hdds.PrintUtils.byteSize2String;
import static org.apache.hadoop.hdds.PrintUtils.print;
import static org.apache.hadoop.hdds.PrintUtils.println;

/**
 * Batch operation implementation for rocks db.
 */
public class RDBBatchOperation implements BatchOperation {
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

  String getBatchSizeString() {
    final int size = batchSize.get();
    if (size < 1024) {
      return "batchSize: " + size + " B";
    } else {
      return "batchSize: " + size + " B (= " + byteSize2String(size) + ")";
    }
  }


  public void commit(RocksDatabase db) throws IOException {
    println(() -> String.format("%s: commit (#put=%s, #del=%s): %s",
        name, putCount, delCount, getBatchSizeString()));
    db.batchWrite(writeBatch);
  }

  public void commit(RocksDatabase db, ManagedWriteOptions writeOptions)
      throws IOException {
    println(() -> String.format("%s: commit-with-writeOptions(#put=%s, #del=%s): %s",
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
        byteSize2String(newBatchSize), StringUtils.bytes2HexString(key).toUpperCase()));
    family.batchDelete(writeBatch, key);
  }

  public void put(ColumnFamily family, byte[] key, byte[] value)
      throws IOException {
    final int newBatchSize = batchSize.addAndGet(key.length + value.length);
    final int newPut = putCount.incrementAndGet();

    final Supplier<String> message = () -> String.format(
        "%s: %s put(key: %s B, value: %s), newPut=%s, newBatchSize=%s%n  put key=%s",
        name, family.getName(), key.length, byteSize2String(value.length), newPut,
        byteSize2String(newBatchSize), StringUtils.bytes2HexString(key).toUpperCase());
    print(value.length, message);

    family.batchPut(writeBatch, key, value);
  }
}
