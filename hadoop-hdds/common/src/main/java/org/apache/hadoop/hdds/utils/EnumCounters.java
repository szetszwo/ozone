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

package org.apache.hadoop.hdds.utils;

import java.util.Arrays;
import java.util.Objects;

/**
 * Counters for an enum type.
 * <p>
 * For example, suppose there is an enum type
 * <pre>
 * enum Fruit { APPLE, ORANGE, GRAPE }
 * </pre>
 * An {@link EnumCounters} object can be created for counting the numbers of
 * APPLE, ORANGE and GRAPE.
 * <p>
 * This class is NOT thread-safe.
 *
 * @param <E> the enum type
 */
public class EnumCounters<E extends Enum<E>> {
  /** The class of the enum. */
  private final Class<E> enumClass;
  /** An array of longs corresponding to the enum. */
  private final long[] counters;

  /**
   * Construct counters for the given enum constants.
   * All counts are initialized to zeros.
   *
   * @param enumClass the enum class of the counters.
   */
  public EnumCounters(final Class<E> enumClass) {
    final E[] enumConstants = enumClass.getEnumConstants();
    Objects.requireNonNull(enumConstants, () -> enumClass + " is not an enum");
    this.enumClass = enumClass;
    this.counters = new long[enumConstants.length];
  }

  /** @return new counters with the same enum class and all counts initialized to zero. */
  public EnumCounters<E> newEnumCounters() {
    return new EnumCounters<>(enumClass);
  }

  public E[] getEnumConstants() {
    return enumClass.getEnumConstants();
  }

  public int numCounters() {
    return getEnumConstants().length;
  }

  /** @return the count of the given element. */
  public final long get(final E element) {
    return counters[element.ordinal()];
  }

  /** @return the count of the given element as an int. */
  public final int getInt(final E e) {
    return Math.toIntExact(counters[e.ordinal()]);
  }

  /** Set counter of the given element to the given value. */
  public void set(final E element, final long value) {
    counters[element.ordinal()] = value;
  }

  /** Set all the counters to the given counters. */
  public EnumCounters<E> set(final EnumCounters<E> that) {
    System.arraycopy(that.counters, 0, this.counters, 0, counters.length);
    return this;
  }

  /** Set all the counters to the given count. */
  public void setAll(long count) {
    for (int i = 0; i < counters.length; i++) {
      this.counters[i] = count;
    }
  }

  /** Reset all counters to zero. */
  public void reset() {
    setAll(0L);
  }

  /** Increment the counter of the given element by 1. */
  public void increment(final E element) {
    counters[element.ordinal()]++;
  }

  /** Add the given value to the counter of the given element. */
  public void add(final E element, final long value) {
    counters[element.ordinal()] += value;
  }

  /** Add the given counters to this object. */
  public EnumCounters<E> add(final EnumCounters<E> that) {
    for (int i = 0; i < counters.length; i++) {
      this.counters[i] += that.counters[i];
    }
    return this;
  }

  /** @return the sum of all counts. */
  public long sum() {
    long sum = 0;
    for (long counter : counters) {
      sum += counter;
    }
    return sum;
  }

  /** @return a copy of this object. */
  public EnumCounters<E> copy() {
    return newEnumCounters().set(this);
  }

  @Override
  public boolean equals(Object obj) {
    if (obj == this) {
      return true;
    } else if (!(obj instanceof EnumCounters)) {
      return false;
    }
    final EnumCounters<?> that = (EnumCounters<?>) obj;
    return this.enumClass == that.enumClass
        && Arrays.equals(this.counters, that.counters);
  }

  @Override
  public int hashCode() {
    return Arrays.hashCode(counters);
  }

  @Override
  public String toString() {
    final E[] enumConstants = getEnumConstants();
    if (enumConstants.length == 0) {
      return "<empty_counters>";
    }

    final StringBuilder b = new StringBuilder();
    for (int i = 0; i < counters.length; i++) {
      final String name = enumConstants[i].name();
      b.append(name).append("=").append(counters[i]).append(", ");
    }
    return b.substring(0, b.length() - 2);
  }
}
