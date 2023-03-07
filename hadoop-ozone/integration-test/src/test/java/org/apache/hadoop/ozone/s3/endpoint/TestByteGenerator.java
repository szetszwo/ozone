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

import org.junit.Test;

import java.util.function.BiConsumer;
import java.util.function.Consumer;

/**
 * Class to test Multipart upload end to end.
 */
public class TestByteGenerator {
  @Test
  public void test() throws Exception {
    final ByteGenerator abc = ByteGenerator.get("abc");

    final byte[][] buffers = {new byte[1024], new byte[512], new byte[256]};
    for(int part = 0; part < 10; part++) {
      final Consumer<byte[]> generator = abc.get(part);
      final BiConsumer<byte[], Integer> verifier = abc.verifier(part);

      for(byte[] buffer : buffers) {
        generator.accept(buffer);
        verifier.accept(buffer, buffer.length);
      }
    }
  }
}
