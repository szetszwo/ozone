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
package org.apache.hadoop.ozone.om.lock.granular;

import com.google.common.util.concurrent.Striped;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.locks.ReadWriteLock;
import org.apache.hadoop.hdds.utils.SimpleStriped;
import org.apache.hadoop.ozone.om.lock.granular.OmComponentLock.Component;
import org.apache.hadoop.ozone.om.lock.granular.OmComponentLock.Type;
import org.apache.ratis.util.UncheckedAutoCloseable;

/**
 * Manage locking of volume, bucket and keys.
 */
public class OmLockManager {
  private final Striped<ReadWriteLock> volumeLocks =  SimpleStriped.readWriteLock(1 << 10, true);
  private final Striped<ReadWriteLock> bucketLocks = SimpleStriped.readWriteLock(1 << 12, true);
  private final Striped<ReadWriteLock> keyLocks = SimpleStriped.readWriteLock(1 << 16, true);

  private OmComponentLock newVolumeLock(Type type, String name) {
    return new OmComponentLock(name, Component.VOLUME, type, volumeLocks.get(name));
  }

  private OmComponentLock newBucketLock(Type type, String name) {
    return new OmComponentLock(name, Component.BUCKET, type, bucketLocks.get(name));
  }

  private OmComponentLock newKeyLock(Type type, String name) {
    return new OmComponentLock(name, Component.KEY, type, keyLocks.get(name));
  }

  private List<OmComponentLock> newKeyLocks(Type type, List<String> names) {
    final List<OmComponentLock> list = new ArrayList<>();
    final Iterator<String> i = names.iterator();
    for(ReadWriteLock lock : keyLocks.bulkGet(names)) {
      list.add(new OmComponentLock(i.next(), Component.KEY, type, lock));
    }
    list.sort(Comparator.naturalOrder());
    return Collections.unmodifiableList(list);
  }

  private OmOperationLock newVolumeReadBucketWriteLock(String volume, String bucket) {
    return OmOperationLock.newInstance(
        newVolumeLock(Type.READ, volume),
        newBucketLock(Type.WRITE, bucket));
  }

  private OmOperationLock newBucketReadKeyWriteLock(String bucket, String key) {
    return OmOperationLock.newInstance(
        newBucketLock(Type.READ, bucket),
        newKeyLock(Type.WRITE, key));
  }

  private OmOperationLock newBucketReadKeyWriteLock(String bucket, List<String> keys) {
    return OmOperationLock.newInstance(
        newBucketLock(Type.READ, bucket),
        newKeyLocks(Type.WRITE, keys));
  }

  /**
   * Acquire the OM operation lock for the given bucket and key.
   * <p>
   * try (OmOperationLock ignored = lockManager.acquireObsLock("buck1", "key1")) {
   *   // op code
   * }
   */
  public UncheckedAutoCloseable acquireBucketReadKeyWriteLock(String bucket, String key) {
    return newBucketReadKeyWriteLock(bucket, key)
        .acquire();
  }

  public UncheckedAutoCloseable acquireBucketReadKeyWriteLock(String bucket, List<String> keys) {
    return newBucketReadKeyWriteLock(bucket, keys)
        .acquire();
  }
}
