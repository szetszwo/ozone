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
package org.apache.hadoop.ozone.client.io;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.concurrent.ThreadLocalRandom;

/**
 * Test {@link ByteArrayStreamOutput}.
 */
@Timeout(30)
public class TestByteArrayStreamOutput {
  static final Logger LOG = LoggerFactory.getLogger(
      TestByteArrayStreamOutput.class);

  static class ByteArrayStreamOutputForTesting extends ByteArrayStreamOutput {
    private final ByteArrayOutputStream out
        = new ByteArrayOutputStream(1 << 20);

    byte[] getArray() {
      return out.toByteArray();
    }

    @Override
    public void write(@Nonnull byte[] b, int off, int len) throws IOException {
      out.write(b, off, len);
    }
  }

  @Test
  public void testByteBuffer() throws Exception {
    runTestByteBuffer(0);
    for(int n = 4 ; n <= 1 << 20; n <<= 2) {
      runTestByteBuffer(n);
    }
  }

  /** Test {@link ByteArrayStreamOutput#write(ByteBuffer, int, int)}. */
  static void runTestByteBuffer(int dataSize) throws Exception {
    LOG.info("runTestByteBuffer: {}", dataSize);
    final byte[] data = new byte[dataSize];
    ThreadLocalRandom.current().nextBytes(data);

    final byte[] output;
    try(ByteArrayStreamOutputForTesting out = new ByteArrayStreamOutputForTesting()) {
      out.write(ByteBuffer.wrap(data));
      output = out.getArray();
    }
    Assertions.assertArrayEquals(data, output);
  }
}