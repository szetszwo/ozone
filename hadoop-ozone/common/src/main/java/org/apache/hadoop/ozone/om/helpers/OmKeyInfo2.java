/**
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
package org.apache.hadoop.ozone.om.helpers;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.fs.FileChecksum;
import org.apache.hadoop.fs.FileEncryptionInfo;
import org.apache.hadoop.hdds.client.ReplicationConfig;
import org.apache.hadoop.hdds.server.JsonUtils;
import org.apache.hadoop.ozone.OzoneAcl;
import org.apache.hadoop.ozone.OzoneConsts;
import org.apache.hadoop.util.Time;
import org.apache.ratis.util.Preconditions;
import org.apache.ratis.util.TraditionalBinaryPrefix;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.util.ArrayList;
import java.util.BitSet;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.TreeMap;

/**
 * Args for key block. The block instance for the key requested in putKey.
 * This is returned from OM to client, and client use class to talk to
 * datanode. Also, this is the metadata written to om.db on server side.
 */
@JsonIgnoreProperties({ "encInfo", "replicationConfig" })
public final class OmKeyInfo2 extends WithParentObjectId {
  private static final Logger LOG = LoggerFactory.getLogger(OmKeyInfo2.class);
  private String volumeName;
  private String bucketName;
  // name of key client specified
  private String keyName;
  private long dataSize;
  private List<OmKeyLocationInfoGroup2> keyLocationVersions;
  private long creationTime;
  private long modificationTime;
  private ReplicationConf replicationConfig;
  private FileEncryptionInfo2 encInfo;
  private FileChecksum fileChecksum;
  /**
   * Support OFS use-case to identify if the key is a file or a directory.
   */
  private boolean isFile;

  /**
   * Represents leaf node name. This also will be used when the keyName is
   * created on a FileSystemOptimized(FSO) bucket. For example, the user given
   * keyName is "a/b/key1" then the fileName stores "key1".
   */
  private String fileName;

  /**
   * ACL Information.
   */
  private List<OzoneAcl> acls;

  public OmKeyInfo2() {
  }

  @SuppressWarnings("parameternumber")
  OmKeyInfo2(String volumeName, String bucketName, String keyName,
      List<OmKeyLocationInfoGroup2> versions, long dataSize,
      long creationTime, long modificationTime,
      ReplicationConfig replicationConfig,
      Map<String, String> metadata,
      FileEncryptionInfo encInfo, List<OzoneAcl> acls,
      long objectID, long updateID, FileChecksum fileChecksum) {
    this.volumeName = volumeName;
    this.bucketName = bucketName;
    this.keyName = keyName;
    this.dataSize = dataSize;
    this.keyLocationVersions = versions;
    this.creationTime = creationTime;
    this.modificationTime = modificationTime;
    this.metadata = metadata;
    this.acls = acls;
    this.objectID = objectID;
    this.updateID = updateID;
    this.fileChecksum = fileChecksum;
  }

  @SuppressWarnings("parameternumber")
  OmKeyInfo2(String volumeName, String bucketName, String keyName,
            String fileName, List<OmKeyLocationInfoGroup2> versions,
            long dataSize, long creationTime, long modificationTime,
            ReplicationConfig replicationConfig,
            Map<String, String> metadata,
            FileEncryptionInfo encInfo, List<OzoneAcl> acls,
            long parentObjectID, long objectID, long updateID,
            FileChecksum fileChecksum, boolean isFile) {
    this(volumeName, bucketName, keyName, versions, dataSize,
            creationTime, modificationTime, replicationConfig, metadata,
            encInfo, acls, objectID, updateID, fileChecksum);
    this.fileName = fileName;
    this.parentObjectID = parentObjectID;
    this.isFile = isFile;
  }

  public String getVolumeName() {
    return volumeName;
  }

  public String getBucketName() {
    return bucketName;
  }

  public ReplicationConf getReplicationConfig() {
    return replicationConfig;
  }

  public String getKeyName() {
    return keyName;
  }

  public void setKeyName(String keyName) {
    this.keyName = keyName;
  }

  public long getDataSize() {
    return dataSize;
  }

  public void setDataSize(long size) {
    this.dataSize = size;
  }

  public void setFileName(String fileName) {
    this.fileName = fileName;
  }

  public String getFileName() {
    return fileName;
  }

  public long getParentObjectID() {
    return parentObjectID;
  }


  public synchronized OmKeyLocationInfoGroup2 getLatestVersionLocations() {
    return keyLocationVersions.size() == 0 ? null :
        keyLocationVersions.get(keyLocationVersions.size() - 1);
  }

  public List<OmKeyLocationInfoGroup2> getKeyLocationVersions() {
    return keyLocationVersions;
  }

  public void setKeyLocationVersions(
      List<OmKeyLocationInfoGroup2> keyLocationVersions) {
    this.keyLocationVersions = keyLocationVersions;
  }

  public void updateModifcationTime() {
    this.modificationTime = Time.monotonicNow();
  }

  public void setFile(boolean file) {
    isFile = file;
  }

  public boolean isFile() {
    return isFile;
  }

  public long getCreationTime() {
    return creationTime;
  }

  public long getModificationTime() {
    return modificationTime;
  }

  public void setModificationTime(long modificationTime) {
    this.modificationTime = modificationTime;
  }


  public List<OzoneAcl> getAcls() {
    return acls;
  }

  public boolean addAcl(OzoneAcl acl) {
    return OzoneAclUtil.addAcl(acls, acl);
  }

  public boolean removeAcl(OzoneAcl acl) {
    return OzoneAclUtil.removeAcl(acls, acl);
  }

  public boolean setAcls(List<OzoneAcl> newAcls) {
    return OzoneAclUtil.setAcl(acls, newAcls);
  }

  public void setParentObjectID(long parentObjectID) {
    this.parentObjectID = parentObjectID;
  }

  public void setReplicationConfig(ReplicationConf repConfig) {
    this.replicationConfig = repConfig;
  }

  public FileChecksum getFileChecksum() {
    return fileChecksum;
  }

  /**
   * Builder of OmKeyInfo.
   */
  public static class Builder {
    private String volumeName;
    private String bucketName;
    private String keyName;
    private long dataSize;
    private List<OmKeyLocationInfoGroup2> OmKeyLocationInfoGroup2s =
        new ArrayList<>();
    private long creationTime;
    private long modificationTime;
    private ReplicationConfig replicationConfig;
    private Map<String, String> metadata;
    private FileEncryptionInfo encInfo;
    private List<OzoneAcl> acls;
    private long objectID;
    private long updateID;
    // not persisted to DB. FileName will be the last element in path keyName.
    private String fileName;
    private long parentObjectID;
    private FileChecksum fileChecksum;

    private boolean isFile;

    public Builder() {
      this.metadata = new HashMap<>();
      OmKeyLocationInfoGroup2s = new ArrayList<>();
      acls = new ArrayList<>();
    }

    public Builder setVolumeName(String volume) {
      this.volumeName = volume;
      return this;
    }

    public Builder setBucketName(String bucket) {
      this.bucketName = bucket;
      return this;
    }

    public Builder setKeyName(String key) {
      this.keyName = key;
      return this;
    }

    public Builder setOmKeyLocationInfos(
        List<OmKeyLocationInfoGroup2> omKeyLocationInfoList) {
      if (omKeyLocationInfoList != null) {
        this.OmKeyLocationInfoGroup2s.addAll(omKeyLocationInfoList);
      }
      return this;
    }

    public Builder addOmKeyLocationInfoGroup2(OmKeyLocationInfoGroup2
        OmKeyLocationInfoGroup2) {
      if (OmKeyLocationInfoGroup2 != null) {
        this.OmKeyLocationInfoGroup2s.add(OmKeyLocationInfoGroup2);
      }
      return this;
    }

    public Builder setDataSize(long size) {
      this.dataSize = size;
      return this;
    }

    public Builder setCreationTime(long crTime) {
      this.creationTime = crTime;
      return this;
    }

    public Builder setModificationTime(long mTime) {
      this.modificationTime = mTime;
      return this;
    }

    public Builder setReplicationConfig(ReplicationConfig replConfig) {
      this.replicationConfig = replConfig;
      return this;
    }

    public Builder addMetadata(String key, String value) {
      metadata.put(key, value);
      return this;
    }

    public Builder addAllMetadata(Map<String, String> newMetadata) {
      metadata.putAll(newMetadata);
      return this;
    }

    public Builder setFileEncryptionInfo(FileEncryptionInfo feInfo) {
      this.encInfo = feInfo;
      return this;
    }

    public Builder setAcls(List<OzoneAcl> listOfAcls) {
      if (listOfAcls != null) {
        this.acls.addAll(listOfAcls);
      }
      return this;
    }

    public Builder addAcl(OzoneAcl ozoneAcl) {
      if (ozoneAcl != null) {
        this.acls.add(ozoneAcl);
      }
      return this;
    }

    public Builder setObjectID(long obId) {
      this.objectID = obId;
      return this;
    }

    public Builder setUpdateID(long id) {
      this.updateID = id;
      return this;
    }

    public Builder setFileName(String keyFileName) {
      this.fileName = keyFileName;
      return this;
    }

    public Builder setParentObjectID(long parentID) {
      this.parentObjectID = parentID;
      return this;
    }

    public Builder setFileChecksum(FileChecksum checksum) {
      this.fileChecksum = checksum;
      return this;
    }

    public Builder setFile(boolean isAFile) {
      this.isFile = isAFile;
      return this;
    }

    public OmKeyInfo2 build() {
      return new OmKeyInfo2(
              volumeName, bucketName, keyName, fileName,
              OmKeyLocationInfoGroup2s, dataSize, creationTime,
              modificationTime, replicationConfig, metadata, encInfo, acls,
              parentObjectID, objectID, updateID, fileChecksum, isFile);
    }
  }


  @Override
  public String getObjectInfo() {
    return "OMKeyInfo{" +
        "volume='" + volumeName + '\'' +
        ", bucket='" + bucketName + '\'' +
        ", key='" + keyName + '\'' +
        ", dataSize='" + dataSize + '\'' +
        ", creationTime='" + creationTime + '\'' +
        ", objectID='" + objectID + '\'' +
        ", parentID='" + parentObjectID + '\'' +
        ", replication='" + replicationConfig + '\'' +
        ", fileChecksum='" + fileChecksum +
        '}';
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    OmKeyInfo2 omKeyInfo = (OmKeyInfo2) o;
    return dataSize == omKeyInfo.dataSize &&
        creationTime == omKeyInfo.creationTime &&
        modificationTime == omKeyInfo.modificationTime &&
        volumeName.equals(omKeyInfo.volumeName) &&
        bucketName.equals(omKeyInfo.bucketName) &&
        keyName.equals(omKeyInfo.keyName) &&
        Objects
            .equals(keyLocationVersions, omKeyInfo.keyLocationVersions) &&
        replicationConfig.equals(omKeyInfo.replicationConfig) &&
        Objects.equals(metadata, omKeyInfo.metadata) &&
        Objects.equals(acls, omKeyInfo.acls) &&
        objectID == omKeyInfo.objectID &&
        updateID == omKeyInfo.updateID &&
        parentObjectID == omKeyInfo.parentObjectID;
  }

  @Override
  public int hashCode() {
    return Objects.hash(volumeName, bucketName, keyName, parentObjectID);
  }

  /**
   * Return a new copy of the object.
   */
  public OmKeyInfo2 copyObject() {
    OmKeyInfo2.Builder builder = new OmKeyInfo2.Builder()
        .setVolumeName(volumeName)
        .setBucketName(bucketName)
        .setKeyName(keyName)
        .setCreationTime(creationTime)
        .setModificationTime(modificationTime)
        .setDataSize(dataSize)
        .setObjectID(objectID)
        .setUpdateID(updateID)
        .setParentObjectID(parentObjectID)
        .setFileName(fileName)
        .setFile(isFile);

    acls.forEach(acl -> builder.addAcl(new OzoneAcl(acl.getType(),
            acl.getName(), (BitSet) acl.getAclBitSet().clone(),
        acl.getAclScope())));

    if (metadata != null) {
      metadata.forEach((k, v) -> builder.addMetadata(k, v));
    }

    if (fileChecksum != null) {
      builder.setFileChecksum(fileChecksum);
    }

    return builder.build();
  }

  /**
   * Method to clear the fileEncryptionInfo.
   * This method is called when a KeyDelete operation is performed.
   * This ensures that when TDE is enabled and GDPR is enforced on a bucket,
   * the encryption info is deleted from Key Metadata before the key is moved
   * to deletedTable in OM Metadata.
   */
  public void clearFileEncryptionInfo() {
    this.encInfo = null;
  }

  /**
   * Set the file encryption info.
   * @param fileEncryptionInfo
   */
  public void setFileEncryptionInfo(FileEncryptionInfo2 fileEncryptionInfo) {
    this.encInfo = fileEncryptionInfo;
  }

  public String getPath() {
    if (StringUtils.isBlank(getFileName())) {
      return getKeyName();
    }
    return getParentObjectID() + OzoneConsts.OM_KEY_PREFIX + getFileName();
  }


  @Override
  public String toString() {
    final String root = rootObjectId != null? ", root:" + rootObjectId: "";
    return volumeName + "/" + bucketName + "/" + getPath() + ":" + getObjectID()
        + " (size=" + getDataSize() + ")" + root;
  }

  private Long rootObjectId = null;

  long findRoot(Map<Long, OmDirectoryInfo> map) {
    if (rootObjectId == null) {
      rootObjectId = OmDirectoryInfo.findRoot(getParentObjectID(), map);
    }
    return rootObjectId;
  }

  /**
   * Run this by
   *   java OmKeyInfo2 directoryTable.json filetable.json
   */
  public static void main(String[] args) throws Exception {
    final Map<Long, OmDirectoryInfo> dirMap = OmDirectoryInfo.parse(args[0]);
    final Map<Long, OmKeyInfo2> fileMap = OmKeyInfo2.parse(args[1]);
    final Map<Long, Root> roots = new TreeMap<>();

    for(OmDirectoryInfo dir : dirMap.values()) {
      final long rid = dir.findRoot(dirMap);
      roots.computeIfAbsent(rid, Root::new).add(dir);
    }

    long size = 0L;
    for(OmKeyInfo2 f : fileMap.values()) {
      final long rid = f.findRoot(dirMap);
      roots.computeIfAbsent(rid, Root::new).add(f);
      size += f.getDataSize();
    }
    System.out.println("size = " + size + " = " + TraditionalBinaryPrefix.long2String(size, "B", 3));
    System.out.println("#roots = " + roots.size());
    for(Root r : roots.values()) {
      r.print();
    }
  }

  static class Root {
    private final long id;
    private final List<OmKeyInfo2> files = new ArrayList<>();
    private final List<OmDirectoryInfo> dirs = new ArrayList<>();

    Root(long id) {
      this.id = id;
    }

    void print() {
      long size = 0L;
      System.out.println(this);
      for(OmDirectoryInfo d : dirs) {
        System.out.println(d);
      }
      for(OmKeyInfo2 f : files) {
        size += f.getDataSize();
        System.out.println(f);
      }
      System.out.println(this + ", size = " + size + " = "
          + TraditionalBinaryPrefix.long2String(size, "B", 3));
    }

    void add(OmKeyInfo2 file) {
      files.add(file);
    }

    void add(OmDirectoryInfo dir) {
      dirs.add(dir);
    }

    @Override
    public String toString() {
      return "Root " + id + ": #dirs=" + dirs.size() + ", #files=" + files.size();
    }
  }

  public static Map<Long, OmKeyInfo2> parse(String filename) throws Exception {
    final File f = new File(filename);
    System.out.println("parsing " + f.getAbsolutePath());
    final List<OmKeyInfo2> a = JsonUtils.readFromFile(f, OmKeyInfo2.class);
    System.out.println("list size: " + a.size());
    for(int i = 0; i < 10; i++) {
      final OmKeyInfo2 key = a.get(i);
      System.out.println(i + ": " + key);
    }

    final Map<Long, OmKeyInfo2> map = new TreeMap<>();
    for(OmKeyInfo2 dir : a) {
      final OmKeyInfo2 previous = map.put(dir.getObjectID(), dir);
      Preconditions.assertNull(previous, () -> "previous=" + previous + ", dir=" + dir);
    }
    return map;
  }
}
