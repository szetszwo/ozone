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

package org.apache.hadoop.ozone.om.lock;

import com.google.common.annotations.VisibleForTesting;
import java.util.Collection;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.hadoop.util.Time;
import org.apache.ratis.util.function.UncheckedAutoCloseableSupplier;

/**
 * Interface for OM Metadata locks.
 */
public interface IOzoneManagerLock {

  OMLockDetails acquireReadLock(Resource resource,
                                String... resources);

  OMLockDetails acquireReadLocks(Resource resource, Collection<String[]> resources);

  OMLockDetails acquireWriteLock(Resource resource,
                                 String... resources);

  OMLockDetails acquireWriteLocks(Resource resource,
                                 Collection<String[]> resources);

  OMLockDetails acquireResourceWriteLock(Resource resource);

  boolean acquireMultiUserLock(String firstUser, String secondUser);

  void releaseMultiUserLock(String firstUser, String secondUser);

  OMLockDetails releaseWriteLock(Resource resource,
                        String... resources);

  OMLockDetails releaseWriteLocks(Resource resource,
                                 Collection<String[]> resources);

  OMLockDetails releaseResourceWriteLock(Resource resource);

  OMLockDetails releaseReadLock(Resource resource,
                                String... resources);

  OMLockDetails releaseReadLocks(Resource resource,
                                Collection<String[]> resources);

  @VisibleForTesting
  int getReadHoldCount(Resource resource,
      String... resources);

  @VisibleForTesting
  int getWriteHoldCount(Resource resource,
      String... resources);

  @VisibleForTesting
  boolean isWriteLockedByCurrentThread(Resource resource,
      String... resources);

  void cleanup();

  OMLockMetrics getOMLockMetrics();

  default UncheckedAutoCloseableSupplier<OMLockDetails> acquireResourceLock(Resource resource) {

    OMLockDetails lockDetails = acquireResourceWriteLock(resource);
    if (!lockDetails.isLockAcquired()) {
      throw new RuntimeException("Failed to acquire lock for resource " + resource.getName());
    }
    AtomicBoolean closed = new AtomicBoolean(false);
    return new UncheckedAutoCloseableSupplier<OMLockDetails>() {
      @Override
      public void close() {
        if (closed.compareAndSet(false, true)) {
          releaseResourceWriteLock(resource);
        }
      }

      @Override
      public OMLockDetails get() {
        return lockDetails;
      }
    };
  }

  default UncheckedAutoCloseableSupplier<OMLockDetails> acquireLock(Resource resource, String key, boolean isReadLock) {
    OMLockDetails lockDetails = isReadLock ? acquireReadLock(resource, key) : acquireWriteLock(resource, key);
    if (!lockDetails.isLockAcquired()) {
      throw new RuntimeException("Failed to acquire lock for resource " + resource.getName() + " with key " + key);
    }
    AtomicBoolean closed = new AtomicBoolean(false);
    return new UncheckedAutoCloseableSupplier<OMLockDetails>() {
      @Override
      public void close() {
        if (closed.compareAndSet(false, true)) {
          if (isReadLock) {
            releaseReadLock(resource, key);
          } else {
            releaseWriteLock(resource, key);
          }
        }
      }

      @Override
      public OMLockDetails get() {
        return lockDetails;
      }
    };
  }

  /**
   * Defines a resource interface used to represent entities that can be
   * associated with locks in the Ozone Manager Lock mechanism. A resource
   * implementation provides a name and an associated {@link ResourceManager}
   * to manage its locking behavior.
   */
  interface Resource {

    String getName();

    ResourceManager getResourceManager();
  }

  /**
   * The ResourceManager class provides functionality for managing
   * information about resource read and write lock usage. It tracks the time of
   * read and write locks acquired and held by individual threads, enabling
   * more granular lock usage metrics.
   */
  class ResourceManager {
    private final ThreadLocal<Long> readNanos = new ThreadLocal<>();
    private final ThreadLocal<Long> writeNanos = new ThreadLocal<>();

    void startRead() {
      readNanos.set(Time.monotonicNowNanos());
    }

    void startWrite() {
      writeNanos.set(Time.monotonicNowNanos());
    }

    long getReadNanos() {
      final long start = Objects.requireNonNull(readNanos.get(), "readNanos");
      return Time.monotonicNowNanos() - start;
    }

    long getWriteNanos() {
      final long start = Objects.requireNonNull(writeNanos.get(), "writeNanos");
      return Time.monotonicNowNanos() - start;
    }
  }
}
