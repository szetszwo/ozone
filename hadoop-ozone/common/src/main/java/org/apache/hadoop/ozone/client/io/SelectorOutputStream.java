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

import javassist.bytecode.ByteArray;
import org.apache.hadoop.fs.Syncable;
import org.apache.hadoop.hdds.scm.storage.ByteBufferStreamOutput;
import org.apache.hadoop.ozone.util.ByteBufInterface;
import org.apache.ratis.thirdparty.io.netty.buffer.ByteBuf;
import org.apache.ratis.thirdparty.io.netty.buffer.ByteBufAllocator;
import org.apache.ratis.thirdparty.io.netty.buffer.PooledByteBufAllocator;
import org.apache.ratis.util.Preconditions;
import org.apache.ratis.util.function.CheckedFunction;

import javax.annotation.Nonnull;
import java.io.IOException;
import java.io.OutputStream;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * An {@link OutputStream} first write data to a buffer up to the capacity.
 * Then, select {@link Underlying} by the number of bytes written.
 * When {@link #flush()}, {@link #hflush()}, {@link #hsync()}
 * or {@link #close()} is invoked,
 * it will force flushing the buffer and {@link OutputStream} selection.
 * <p>
 * This class, like many implementation of {@link OutputStream},
 * is not threadsafe.
 *
 * @param <OUT> The underlying {@link OutputStream} type.
 */
public class SelectorOutputStream<OUT extends OutputStream>
    extends OutputStream implements Syncable {
  private static final ByteBufAllocator POOL
      = PooledByteBufAllocator.DEFAULT;

  /** A buffer to store data prior {@link OutputStream} selection. */
  static final class Buffer implements ByteBufInterface {
    private final int capacity;
    private final ByteBuf buf = POOL.directBuffer();

    private Buffer(int capacity) {
      this.capacity = capacity;
    }

    @Override
    public ByteBuf getByteBuf() {
      return buf;
    }

    @Override
    protected void finalize() throws Throwable {
      detectLeak(buf.refCnt() == 0);
      super.finalize();
    }

    private void assertRemaining(int outstandingBytes) {
      assertRefCnt(1);

      final int remaining = capacity - buf.writerIndex();
      if (remaining < 0) {
        throw new IllegalStateException("remaining = " + remaining + " <= 0");
      }
      if (remaining < outstandingBytes) {
        throw new IllegalArgumentException("Buffer overflow: remaining = "
            + remaining + " < outstandingBytes = " + outstandingBytes);
      }
    }

    void write(int b) {
      assertRemaining(1);
      buf.writeByte(b);
    }

    void write(byte[] src, int srcOffset, int length) {
      Objects.requireNonNull(src, "src == null");
      assertRemaining(length);
      buf.writeBytes(src, srcOffset, length);
    }

    <OUT extends OutputStream> OUT trySelect(
        int outstandingBytes, boolean force,
        CheckedFunction<Integer, OUT, IOException> selector)
        throws IOException {
      assertRemaining(0);
      final int readable = buf.readableBytes();
      final int required = readable + outstandingBytes;
      if (force || required > capacity) {
        final OUT out = selector.apply(required);
        if (out instanceof ByteBufferStreamOutput
            && buf.nioBufferCount() > 0) {
          ((ByteBufferStreamOutput) out).write(
              buf.nioBuffer().asReadOnlyBuffer());
        } else {
          buf.readBytes(out, readable);
        }
        buf.release();
        return out;
      }
      return null;
    }
  }

  /** To select the underlying {@link OutputStream}. */
  private final class Underlying {
    /** Select an {@link OutputStream} by the number of bytes. */
    private final CheckedFunction<Integer, OUT, IOException> selector;
    private OUT out;

    private Underlying(CheckedFunction<Integer, OUT, IOException> selector) {
      this.selector = selector;
    }

    OUT select(int outstandingBytes, boolean force) throws IOException {
      if (out == null) {
        out = buffer.trySelect(outstandingBytes, force, selector);
      }
      return out;
    }
  }

  private final Buffer buffer;
  private final Underlying underlying;

  /**
   * Construct a {@link SelectorOutputStream} which first writes to a buffer.
   * Once the buffer has become full, select an {@link OutputStream}.
   *
   * @param selectionThreshold The buffer capacity.
   * @param selector Use bytes-written to select an {@link OutputStream}.
   */
  public SelectorOutputStream(int selectionThreshold,
      CheckedFunction<Integer, OUT, IOException> selector) {
    this.buffer = new Buffer(selectionThreshold);
    this.underlying = new Underlying(selector);
  }

  public OUT getUnderlying() {
    return underlying.out;
  }

  @Override
  public void write(int b) throws IOException {
    final OUT out = underlying.select(1, false);
    if (out != null) {
      out.write(b);
    } else {
      buffer.write(b);
    }
  }

  @Override
  public void write(@Nonnull byte[] array, int off, int len)
      throws IOException {
    final OUT selected = underlying.select(len, false);
    if (selected != null) {
      selected.write(array, off, len);
    } else {
      buffer.write(array, off, len);
    }
  }

  private OUT select() throws IOException {
    return underlying.select(0, true);
  }

  @Override
  public void flush() throws IOException {
    select().flush();
  }

  @Override
  public void hflush() throws IOException {
    final OUT out = select();
    if (out instanceof Syncable) {
      ((Syncable)out).hflush();
    } else {
      throw new IllegalStateException(
          "Failed to hflush: The underlying OutputStream ("
              + out.getClass() + ") is not Syncable.");
    }
  }

  @Override
  public void hsync() throws IOException {
    final OUT out = select();
    if (out instanceof Syncable) {
      ((Syncable)out).hsync();
    } else {
      throw new IllegalStateException(
          "Failed to hsync: The underlying OutputStream ("
              + out.getClass() + ") is not Syncable.");
    }
  }

  @Override
  public void close() throws IOException {
    select().close();
  }
}
