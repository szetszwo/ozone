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

package org.apache.ozone.test;

import org.apache.hadoop.ozone.om.KeyManager;
import org.apache.hadoop.ozone.om.OzoneManager;
import org.apache.hadoop.ozone.om.PrefixManager;
import org.apache.hadoop.ozone.security.acl.IOzoneManagerAuthorizer;
import org.apache.hadoop.ozone.security.acl.IOzoneObj;
import org.apache.hadoop.ozone.security.acl.OzoneObj.ResourceType;
import org.apache.hadoop.ozone.security.acl.OzoneObjInfo;
import org.apache.hadoop.ozone.security.acl.RequestContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.EnumMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Test for Ozone Manager ACLs.
 */
public class OmAuthorizerForTesting implements IOzoneManagerAuthorizer {
  static final Logger LOG = LoggerFactory.getLogger(OmAuthorizerForTesting.class);

  static final Map<String, OmAuthorizerForTesting> INSTANCES = new ConcurrentHashMap<>();

  private final EnumMap<ResourceType, Boolean> isAllowed = new EnumMap<>(ResourceType.class);
  private String id;

  public static OmAuthorizerForTesting getInstance(String omNodeId) {
    return INSTANCES.get(omNodeId);
  }

  public OmAuthorizerForTesting() {
    setAllowed(true);
  }

  public void setAllowed(boolean allowed) {
    for(ResourceType type : ResourceType.values()) {
      setAllowed(type, allowed);
    }
  }

  public void setAllowed(ResourceType resourceType, boolean allowed) {
    isAllowed.put(resourceType, allowed);
  }

  public boolean isAllowed(ResourceType type) {
    final Boolean allowed = isAllowed.get(type);
    return allowed != null && allowed;
  }

  @Override
  public boolean checkAccess(IOzoneObj obj, RequestContext context) {
    if (!(obj instanceof OzoneObjInfo)) {
      throw new UnsupportedOperationException("Not instanceof OzoneObjInfo: " + obj.getClass());
    }
    final boolean allowed = isAllowed(((OzoneObjInfo) obj).getResourceType());
    LOG.info("{}: ACCESS check for {} {}, allowed? {}", id, obj, context.getAclRights(), allowed);
    return allowed;
  }

  @Override
  public void setOzoneManager(OzoneManager om) {
    id = om.getOMNodeId();
    final OmAuthorizerForTesting previous = INSTANCES.get(id);
    if (previous != null) {
      for(ResourceType type : ResourceType.values()) {
        setAllowed(type, previous.isAllowed(type));
      }
    }
    INSTANCES.put(id, this);
  }

  @Override
  public void setKeyManager(KeyManager km) {
  }

  @Override
  public void setPrefixManager(PrefixManager pm) {
  }
}
