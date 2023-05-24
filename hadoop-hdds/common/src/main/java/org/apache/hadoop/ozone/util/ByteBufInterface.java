/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.ozone.util;

import org.apache.ratis.thirdparty.io.netty.buffer.ByteBuf;
import org.apache.ratis.util.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.ref.WeakReference;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * For supporting leak detection for {@link ByteBuf}
 */
public interface ByteBufInterface {
  Logger LOG = LoggerFactory.getLogger(ByteBufInterface.class);

  class Util {
    private static final AtomicInteger LEAK_COUNT = new AtomicInteger();
  }

  /** Force gc to check leakage. */
  static void gc() throws InterruptedException {
    // use WeakReference to detect gc
    Object obj = new Object();
    final WeakReference<Object> weakRef = new WeakReference<>(obj);
    obj = null;

    // loop until gc has completed.
    for (int i = 0; weakRef.get() != null; i++) {
      LOG.info("gc attempt {}", i);
      System.gc();
      Thread.sleep(100);
    }
    LOG.info("gc completed successfully");
    assertNoLeaks();
  }

  /** Assert the number of leak detected is zero. */
  static void assertNoLeaks() {
    final long leak = Util.LEAK_COUNT.get();
    if (leak > 0) {
      throw new AssertionError("Found " + leak + " leaked objects, check logs");
    }
  }

  ByteBuf getByteBuf();

  default void detectLeak(boolean isReleased) {
    // leak detection
    final ByteBuf buf = getByteBuf();
    final int capacity = buf.capacity();
    if (!isReleased && capacity > 0) {
      final int refCnt = buf.refCnt();
      if (refCnt > 0) {
        final int leak = Util.LEAK_COUNT.incrementAndGet();
        LOG.warn("LEAK {}: {}, refCnt={}, capacity={}",
            leak, this, refCnt, capacity);
        buf.release(refCnt);
      }
    }
  }

  default void assertRefCnt(int expected) {
    Preconditions.assertSame(expected, getByteBuf().refCnt(), "refCnt");
  }
}

