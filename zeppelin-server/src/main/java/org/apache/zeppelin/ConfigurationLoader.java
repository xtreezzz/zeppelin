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

package org.apache.zeppelin;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;
import ru.tinkoff.zeppelin.engine.Configuration;

import java.util.Arrays;
import java.util.Set;
import java.util.stream.Collectors;

@Component
public class ConfigurationLoader {

  public ConfigurationLoader(@Value("${zeppelin.admin_users}") final String admin_users,
                             @Value("${zeppelin.admin_group}") final String admin_group,
                             @Value("${zeppelin.thrift.address}") final String thriftAddress,
                             @Value("${zeppelin.thrift.port}") final int thriftPort,
                             @Value("${zeppelin.instance.markerPrefix}") final String instanceMarkerPrefix,
                             @Value("${zeppelin.metaserver.address}") final String metaserverLocation,
                             @Value("${zeppelin.home_node}") final String homeNodeId) {

    Configuration.create(
            parseString(admin_users, ","),
            parseString(admin_group, ","),
            thriftAddress,
            thriftPort,
            instanceMarkerPrefix,
            metaserverLocation,
            homeNodeId
    );
  }

  private Set<String> parseString(final String param, final String delimeter) {
    return Arrays.stream(param.split(delimeter)).map(String::trim).collect(Collectors.toSet());
  }
}
