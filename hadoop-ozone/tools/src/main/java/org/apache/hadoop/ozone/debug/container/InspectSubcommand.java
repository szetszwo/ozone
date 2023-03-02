/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package org.apache.hadoop.ozone.debug.container;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonObject;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.ozone.OzoneConsts;
import org.apache.hadoop.ozone.container.common.impl.ContainerData;
import org.apache.hadoop.ozone.container.common.interfaces.Container;
import org.apache.hadoop.ozone.container.keyvalue.KeyValueContainerData;
import org.apache.hadoop.ozone.container.keyvalue.KeyValueContainerMetadataInspector;
import org.apache.hadoop.ozone.container.keyvalue.helpers.BlockUtils;
import org.apache.hadoop.ozone.container.metadata.DatanodeStore;
import picocli.CommandLine;
import picocli.CommandLine.Command;

import java.util.Iterator;
import java.util.concurrent.Callable;

/**
 * {@code ozone debug container inspect},
 * a command to run {@link KeyValueContainerMetadataInspector}.
 */
@Command(
    name = "inspect",
    description = "Check the deletion information for all the containers.")
public class InspectSubcommand implements Callable<Void> {

  @CommandLine.ParentCommand
  private ContainerCommands parent;

  @Override
  public Void call() throws Exception {
    final OzoneConfiguration conf = parent.getOzoneConf();
    parent.loadContainersFromVolumes();

    final Gson gson = new GsonBuilder()
        .setPrettyPrinting()
        .serializeNulls()
        .create();

    for(Container<?> container : parent.getController().getContainerSet()) {
      final ContainerData data = container.getContainerData();
      if (!(data instanceof KeyValueContainerData)) {
        continue;
      }
      final KeyValueContainerData kvData = (KeyValueContainerData)data;
      try(DatanodeStore store = BlockUtils.getUncachedDatanodeStore(
          kvData, conf, true)) {
        final JsonObject json = KeyValueContainerMetadataInspector
            .inspectContainer(kvData, store);
        System.out.println(gson.toJson(json));
      }
    }

    return null;
  }
}
