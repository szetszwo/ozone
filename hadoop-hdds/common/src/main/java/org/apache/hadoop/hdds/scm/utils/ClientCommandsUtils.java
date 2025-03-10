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

package org.apache.hadoop.hdds.scm.utils;

import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos;

/**
 * These methods should be merged with other similar utility classes.
 */
public final class ClientCommandsUtils {

  /** Utility classes should not be constructed. **/
  private ClientCommandsUtils() {

  }

  public static ContainerProtos.ReadChunkVersion getReadChunkVersion(
      ContainerProtos.ReadChunkRequestProto readChunkRequest) {
    if (readChunkRequest.hasReadChunkVersion()) {
      return readChunkRequest.getReadChunkVersion();
    } else {
      return ContainerProtos.ReadChunkVersion.V0;
    }
  }

  public static ContainerProtos.ReadChunkVersion getReadChunkVersion(
      ContainerProtos.GetSmallFileRequestProto getSmallFileRequest) {
    if (getSmallFileRequest.hasReadChunkVersion()) {
      return getSmallFileRequest.getReadChunkVersion();
    } else {
      return ContainerProtos.ReadChunkVersion.V0;
    }
  }
}
