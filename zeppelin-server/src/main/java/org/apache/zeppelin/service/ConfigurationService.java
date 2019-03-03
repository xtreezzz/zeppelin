/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */


package org.apache.zeppelin.service;

import org.apache.zeppelin.configuration.ZeppelinConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.util.Map;

@Component
public class ConfigurationService {
  private static final Logger LOGGER = LoggerFactory.getLogger(ConfigurationService.class);

  private final ZeppelinConfiguration zConf;

  @Autowired
  public ConfigurationService(final ZeppelinConfiguration zConf) {
    this.zConf = zConf;
  }

  public Map<String, String> getAllProperties() {
    return zConf.dumpConfigurations(key ->
            !key.contains("password")
                    && !key.equals(ZeppelinConfiguration.ConfVars.ZEPPELIN_NOTEBOOK_AZURE_CONNECTION_STRING.getVarName()));
  }

  public Map<String, String> getPropertiesWithPrefix(final String prefix) {
    return zConf.dumpConfigurations(key ->
            !key.contains("password")
                    && !key.equals(ZeppelinConfiguration.ConfVars.ZEPPELIN_NOTEBOOK_AZURE_CONNECTION_STRING.getVarName())
                    && key.startsWith(prefix));
  }
}
