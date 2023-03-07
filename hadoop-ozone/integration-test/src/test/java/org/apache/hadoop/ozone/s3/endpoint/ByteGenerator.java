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

import org.junit.Assert;

import java.util.Map;
import java.util.Objects;
import java.util.Random;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.BiConsumer;
import java.util.function.Consumer;

class ByteGenerator {
  static final Map<String, ByteGenerator> CACHE = new ConcurrentHashMap<>();

  static ByteGenerator get(String keyName) {
    return CACHE.computeIfAbsent(keyName, ByteGenerator::new);
  }

  private final String keyName;

  private ByteGenerator(String keyName) {
    this.keyName = keyName;
  }

  Consumer<byte[]> get(int part) {
    final Random random = new Random(Objects.hash(keyName, part));
    return random::nextBytes;
  }

  BiConsumer<byte[], Integer> verifier(int part) {
    final Consumer<byte[]> random = get(part);
    return (computed, length) -> verify(computed, length, random);
  }

  static void verify(byte[] computed, int length, Consumer<byte[]> random) {
    final byte[] expected = new byte[length];
    random.accept(expected);
    for (int i = 0; i < expected.length; i++) {
      Assert.assertEquals("i=" + i, expected[i], computed[i]);
    }
  }
}
