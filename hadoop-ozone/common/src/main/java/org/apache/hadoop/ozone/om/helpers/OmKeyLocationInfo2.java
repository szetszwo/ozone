/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership.  The ASF
 * licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
package org.apache.hadoop.ozone.om.helpers;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import org.apache.hadoop.hdds.client.BlockID;
import org.apache.hadoop.hdds.scm.pipeline.Pipeline;
import org.apache.hadoop.hdds.scm.pipeline.UnknownPipelineStateException;
import org.apache.hadoop.hdds.scm.storage.BlockLocationInfo;
import org.apache.hadoop.hdds.security.token.OzoneBlockTokenIdentifier;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.KeyLocation;
import org.apache.hadoop.ozone.protocolPB.OzonePBHelper;
import org.apache.hadoop.security.token.Token;

/**
 * One key can be too huge to fit in one container. In which case it gets split
 * into a number of subkeys. This class represents one such subkey instance.
 */
@JsonIgnoreProperties
public final class OmKeyLocationInfo2 {
  private BlockID blockID;
  private long length;
  private long offset;
  private long createVersion;
  private int partNumber;

  public OmKeyLocationInfo2() {

  }

  public void setBlockID(BlockID blockID) {
    this.blockID = blockID;
  }

  public void setCreateVersion(long createVersion) {
    this.createVersion = createVersion;
  }

  public void setLength(long length) {
    this.length = length;
  }

  public void setOffset(long offset) {
    this.offset = offset;
  }

  public void setPartNumber(int partNumber) {
    this.partNumber = partNumber;
  }


}
