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

package com.google.common.util.concurrent;

import jakarta.annotation.Nonnull;

import java.util.concurrent.locks.ReentrantReadWriteLock;

public class StripedLock implements Comparable<StripedLock> {
  private final ReentrantReadWriteLock lock;
  private final int index;

  StripedLock(ReentrantReadWriteLock lock, int index) {
    this.lock = lock;
    this.index = index;
  }

  public ReentrantReadWriteLock getLock() {
    return lock;
  }

  public int getIndex() {
    return index;
  }

  @Override
  public int compareTo(@Nonnull StripedLock that) {
    return Integer.compare(this.index, that.index);
  }
}
