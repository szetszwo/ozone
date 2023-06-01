/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.ozone.recon.spi.impl;

import org.apache.hadoop.hdds.utils.db.Codec;
import org.apache.hadoop.hdds.utils.db.CodecBuffer;
import org.apache.hadoop.hdds.utils.db.StringCodec;
import org.apache.hadoop.ozone.recon.api.types.ContainerKeyPrefix;
import org.apache.ratis.util.Preconditions;

import javax.annotation.Nonnull;
import java.nio.charset.StandardCharsets;
import java.util.Objects;
import java.util.function.IntFunction;

/**
 * Codec to serialize/deserialize {@link ContainerKeyPrefix}.
 */
public final class ContainerKeyPrefixCodec
    implements Codec<ContainerKeyPrefix> {

  private static final String KEY_DELIMITER = "_";
  private static final byte[] KEY_DELIMITER_BYTES
      = KEY_DELIMITER.getBytes(StandardCharsets.UTF_8);


  static boolean isNonEmpty(String s) {
    return s != null && !s.isEmpty();
  }

  static int getSerializedSizeUpperBound(ContainerKeyPrefix object) {
    int upperBound = Long.BYTES; // containerId
    final String keyPrefix = object.getKeyPrefix();
    if (isNonEmpty(keyPrefix)) {
      upperBound += KEY_DELIMITER_BYTES.length
          + StringCodec.get().getSerializedSizeUpperBound(keyPrefix);
    }
    if (object.getKeyVersion() != -1) {
      upperBound += KEY_DELIMITER_BYTES.length + Long.BYTES;
    }
    return upperBound;
  }

  private static final Codec<ContainerKeyPrefix> INSTANCE =
      new ContainerKeyPrefixCodec();

  public static Codec<ContainerKeyPrefix> get() {
    return INSTANCE;
  }

  private ContainerKeyPrefixCodec() {
    // singleton
  }

  @Override
  public boolean supportCodecBuffer() {
    return true;
  }

  @Override
  public CodecBuffer toCodecBuffer(@Nonnull ContainerKeyPrefix object,
      IntFunction<CodecBuffer> allocator) {
    final CodecBuffer buffer = allocator.apply(
        getSerializedSizeUpperBound(object));
    buffer.putLong(object.getContainerId());
    final String keyPrefix = object.getKeyPrefix();
    if (isNonEmpty(keyPrefix)) {
      buffer.put(KEY_DELIMITER_BYTES).putUtf8(keyPrefix);
    }
    final long keyVersion = object.getKeyVersion();
    if (keyVersion != -1) {
      buffer.put(KEY_DELIMITER_BYTES).putLong(keyVersion);
    }
    return buffer;
  }

  static CodecBuffer skipKeyDelimiter(CodecBuffer buffer) {
    final String d = buffer.getUtf8(KEY_DELIMITER_BYTES.length);
    Preconditions.assertTrue(Objects.equals(d, KEY_DELIMITER),
        () -> "Unexpected key delimiter: \"" + d
            + "\", KEY_DELIMITER = \"" + KEY_DELIMITER + "\"");
    return buffer;
  }

  @Override
  public ContainerKeyPrefix fromCodecBuffer(@Nonnull CodecBuffer buffer) {
    final long containerId = buffer.getLong();
    final int keyPrefixLength = buffer.readableBytes()
        - Long.BYTES - KEY_DELIMITER_BYTES.length;
    final String keyPrefix = skipKeyDelimiter(buffer).getUtf8(keyPrefixLength);
    final long keyVersion = skipKeyDelimiter(buffer).getLong();
    return ContainerKeyPrefix.get(containerId, keyPrefix, keyVersion);
  }

  @Override
  public byte[] toPersistedFormat(ContainerKeyPrefix object) {
    try (CodecBuffer buffer = toCodecBuffer(object, CodecBuffer::allocateHeap)) {
      return buffer.getArray();
    }
  }

  @Override
  public ContainerKeyPrefix fromPersistedFormat(byte[] rawData) {
    return fromCodecBuffer(CodecBuffer.wrap(rawData));
  }

  @Override
  public ContainerKeyPrefix copyObject(ContainerKeyPrefix object) {
    return object;
  }
}
