package ru.tinkoff.zeppelin.engine;

import java.util.Set;

public class Configuration {

  private final Set<String> adminUsers;
  private final Set<String> adminGroups;

  private final String thriftAddress;
  private final int thriftPort;

  private final String instanceMarkerPrefix;
  private final String metaserverLocation;
  private final String homeNodeId;

  private static Configuration instance;

  private Configuration(final Set<String> adminUsers,
                        final Set<String> adminGroups,
                        final String thriftAddress,
                        final int thriftPort,
                        final String instanceMarkerPrefix,
                        final String metaserverLocation,
                        final String homeNodeId) {
    this.adminGroups = adminGroups;
    this.adminUsers = adminUsers;

    this.thriftAddress = thriftAddress;
    this.thriftPort = thriftPort;

    this.instanceMarkerPrefix = instanceMarkerPrefix;
    this.metaserverLocation = metaserverLocation;
    this.homeNodeId = homeNodeId;
    instance = this;
  }

  public synchronized static void create(final Set<String> adminUsers,
                                         final Set<String> adminGroups,
                                         final String thriftAddress,
                                         final int thriftPort,
                                         final String instanceMarkerPrefix,
                                         final String metaserverLocation,
                                         final String homeNodeId) {
    if (instance != null) {
      return;
    }

    new Configuration(adminUsers,
            adminGroups,
            thriftAddress,
            thriftPort,
            instanceMarkerPrefix,
            metaserverLocation,
            homeNodeId);
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
}
