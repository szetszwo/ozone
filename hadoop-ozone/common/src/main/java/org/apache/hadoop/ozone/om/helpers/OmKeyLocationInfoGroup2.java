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
import com.google.common.base.Objects;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.KeyLocationList;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * A list of key locations. This class represents one single version of the
 * blocks of a key.
 */
@JsonIgnoreProperties
public class OmKeyLocationInfoGroup2 {
  private int version;
  private Map<Long, List<OmKeyLocationInfo2>> locationVersionMap;
  private boolean isMultipartKey;

  public OmKeyLocationInfoGroup2() {
  }

  public void setIsMultipartKey(boolean multipartKey) {
    isMultipartKey = multipartKey;
  }

  public void setVersion(int version) {
    this.version = version;
  }

  public void setLocationVersionMap(Map<Long, List<OmKeyLocationInfo2>> locationVersionMap) {
    this.locationVersionMap = locationVersionMap;
  }

  /**
   * Use this expensive method only when absolutely needed!
   * It creates a new list so it is not an O(1) operation.
   * Use getLocationLists() instead.
   * @return a list of OmKeyLocationInfo2
   */
  public List<OmKeyLocationInfo2> getLocationList() {
    return locationVersionMap.values().stream().flatMap(List::stream)
        .collect(Collectors.toList());
  }

  public Collection<List<OmKeyLocationInfo2>> getLocationLists() {
    return locationVersionMap.values();
  }

  public long getLocationListCount() {
    return locationVersionMap.values().stream().mapToLong(List::size).sum();
  }

  @Deprecated
  public List<OmKeyLocationInfo2> getLocationList(Long versionToFetch) {
    return new ArrayList<>(locationVersionMap.get(versionToFetch));
  }


  void removeBlocks(long versionToRemove) {
    locationVersionMap.remove(versionToRemove);
  }

  void addAll(long versionToAdd, List<OmKeyLocationInfo2> locationInfoList) {
    locationVersionMap.putIfAbsent(versionToAdd, new ArrayList<>());
    List<OmKeyLocationInfo2> list = locationVersionMap.get(versionToAdd);
    list.addAll(locationInfoList);
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder();
    for (List<OmKeyLocationInfo2> kliList : locationVersionMap.values()) {
      for (OmKeyLocationInfo2 kli: kliList) {
      }
    }
    return sb.toString();
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    OmKeyLocationInfoGroup2 that = (OmKeyLocationInfoGroup2) o;
    return
        Objects.equal(locationVersionMap, that.locationVersionMap);
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(locationVersionMap);
  }
}
