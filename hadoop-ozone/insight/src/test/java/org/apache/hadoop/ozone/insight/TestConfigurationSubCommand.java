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

package org.apache.hadoop.ozone.insight;

import static org.assertj.core.api.Assertions.assertThat;

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.io.UnsupportedEncodingException;
import java.nio.charset.StandardCharsets;
import org.apache.hadoop.hdds.conf.Config;
import org.apache.hadoop.hdds.conf.ConfigGroup;
import org.apache.hadoop.hdds.conf.ConfigTag;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Test insight report which prints out configs.
 */
public class TestConfigurationSubCommand {

  private static final PrintStream OLD_OUT = System.out;

  private final ByteArrayOutputStream out = new ByteArrayOutputStream();

  @BeforeEach
  public void setup() throws Exception {
    System.setOut(new PrintStream(out, false, StandardCharsets.UTF_8.name()));
  }

  @AfterEach
  public void reset() {
    System.setOut(OLD_OUT);
  }

  @Test
  public void testPrintConfig() throws UnsupportedEncodingException {
    OzoneConfiguration conf = new OzoneConfiguration();
    conf.set("ozone.scm.client.address", "omclient");
    ConfigurationSubCommand subCommand = new ConfigurationSubCommand();

    subCommand.printConfig(CustomConfig.class, conf);

    final String output = out.toString(StandardCharsets.UTF_8.name());
    assertThat(output).contains(">>> ozone.scm.client.address");
    assertThat(output).contains("default: localhost");
    assertThat(output).contains("current: omclient");
    assertThat(output).contains(">>> ozone.scm.client.secure");
    assertThat(output).contains("default: true");
    assertThat(output).contains("current: true");
  }

  /**
   * Example configuration parent.
   */
  public static class ParentConfig {
    @Config(key = "secure", defaultValue = "true", description = "Make "
        + "everything secure.", tags = ConfigTag.MANAGEMENT)
    private boolean secure = true;

    public boolean isSecure() {
      return secure;
    }
  }

  /**
   * Example configuration.
   */
  @ConfigGroup(prefix = "ozone.scm.client")
  public static class CustomConfig extends ParentConfig {

    @Config(key = "address", defaultValue = "localhost", description = "Client "
        + "address (To test string injection).", tags = ConfigTag.MANAGEMENT)
    private String clientAddress;

    public String getClientAddress() {
      return clientAddress;
    }

    public void setClientAddress(String clientAddress) {
      this.clientAddress = clientAddress;
    }
  }
}
