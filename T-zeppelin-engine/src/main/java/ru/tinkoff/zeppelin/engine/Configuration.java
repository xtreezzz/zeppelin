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

package ru.tinkoff.zeppelin.engine;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import javax.annotation.PostConstruct;
import java.util.Arrays;
import java.util.Set;
import java.util.stream.Collectors;

@Component("configuration")
public class Configuration {

  private final Set<String> adminUsers;
  private final Set<String> adminGroups;

  private final String thriftAddress;
  private final int thriftPort;

  private final String instanceMarkerPrefix;
  private final String metaserverLocation;
  private final String homeNodeId;

  private static Configuration instance;

  private Configuration(@Value("${zeppelin.admin_users}") final String admin_users,
                        @Value("${zeppelin.admin_group}") final String admin_group,
                        @Value("${zeppelin.thrift.address}") final String thriftAddress,
                        @Value("${zeppelin.thrift.port}") final int thriftPort,
                        @Value("${zeppelin.instance.markerPrefix}") final String instanceMarkerPrefix,
                        @Value("${zeppelin.metaserver.address}") final String metaserverLocation,
                        @Value("${zeppelin.home_node}") final String homeNodeId) {
    this.adminUsers = parseString(admin_users, ",");
    this.adminGroups = parseString(admin_group, ",");
    this.thriftAddress = thriftAddress;
    this.thriftPort = thriftPort;
    this.instanceMarkerPrefix = instanceMarkerPrefix;
    this.metaserverLocation = metaserverLocation;
    this.homeNodeId = homeNodeId;
    instance = this;
  }

  @PostConstruct
  private void init() {
    instance = this;
  }


  public static Set<String> getAdminUsers() {
    return instance.adminUsers;
  }

  public static Set<String> getAdminGroups() {
    return instance.adminGroups;
  }

  public static String getThriftAddress() {
    return instance.thriftAddress;
  }

  public static int getThriftPort() {
    return instance.thriftPort;
  }

  public static String getInstanceMarkerPrefix() {
    return instance.instanceMarkerPrefix;
  }

  public static String getMetaserverLocation() {
    return instance.metaserverLocation;
  }

  public static String getHomeNodeId() {
    return instance.homeNodeId;
  }

  private Set<String> parseString(final String param, final String delimeter) {
    return Arrays.stream(param.split(delimeter)).map(String::trim).collect(Collectors.toSet());
  }
}
