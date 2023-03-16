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
package org.apache.hadoop.hdds;

import java.io.PrintStream;
import java.util.function.Supplier;

public interface PrintUtils {
  int ONE_MB = 1 << 20;

  static String byteSize2String(int byteSize) {
    if (byteSize < 1024) {
      return byteSize + " B";
    }
    final double k = byteSize / 1024.0;
    if (k < 1024) {
      return String.format("%.3f KB", k);
    }
    final double m = k / 1024.0;
    if (m < 1024) {
      return String.format("%.3f MB", m);
    }
    final double g = m / 1024.0;
    return String.format("%.3f GB", g);
  }


  static PrintStream getPrintStream() {
    return System.out;
  }

  static void println(Supplier<String> message) {
    getPrintStream().println(message.get());
  }

  static void printTrace(Supplier<String> message) {
    new Throwable(message.get()).printStackTrace(getPrintStream());
  }

  static void print(int size, Supplier<String> message) {
    if (size < ONE_MB) {
      println(message);
    } else {
      printTrace(message);
    }
  }
}