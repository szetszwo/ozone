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
package org.apache.hadoop.ozone.client;

import org.apache.ratis.grpc.GrpcTlsConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.net.URL;
import java.util.Optional;

public interface SecurityTestUtils {
  Logger LOG = LoggerFactory.getLogger(SecurityTestUtils.class);

  ClassLoader CLASS_LOADER = SecurityTestUtils.class.getClassLoader();

  static File getResource(String name) {
    final File file = Optional.ofNullable(CLASS_LOADER.getResource(name))
        .map(URL::getFile)
        .map(File::new)
        .orElse(null);
    LOG.info("Getting resource {}: {}", name, file);
    return file;
  }

  static GrpcTlsConfig newServerGrpcTlsConfig(boolean mutualAuthn) {
    return new GrpcTlsConfig(
        getResource("ssl/server.pem"),
        getResource("ssl/server.crt"),
        getResource("ssl/client.crt"),
        mutualAuthn);
  }

  static GrpcTlsConfig newClientGrpcTlsConfig(boolean mutualAuthn) {
    return new GrpcTlsConfig(
        getResource("ssl/client.pem"),
        getResource("ssl/client.crt"),
        getResource("ssl/ca.crt"),
        mutualAuthn);
  }
}