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

import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * For accessing package private methods such as the Striped.custom(int, Supplier) method.
 *
 * @see <a href="https://github.com/google/guava/issues/2514">
 *   https://github.com/google/guava/issues/2514</a>
 */
public final class SimpleStriped {

  private SimpleStriped() {
  }

  /**
   * The same as Striped.readWriteLock(stripes) except that this method has a fair parameter.
   *
   * @param fair for passing to {@link ReentrantReadWriteLock#ReentrantReadWriteLock(boolean)}.
   */
  public static Striped<ReadWriteLock> readWriteLock(int stripes, boolean fair) {
    return Striped.custom(stripes, () -> new ReentrantReadWriteLock(fair));
  }

}
