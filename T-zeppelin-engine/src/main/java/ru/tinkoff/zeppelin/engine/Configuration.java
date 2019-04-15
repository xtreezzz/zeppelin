package ru.tinkoff.zeppelin.engine;

import java.util.Set;

public class Configuration {

  private final Set<String> adminUsers;
  private final Set<String> adminGroups;

  private final String thriftAddress;
  private final int thriftPort;

  private final String instanceMarkerPrefix;

  private static Configuration instance;

  private Configuration(final Set<String> adminUsers,
                        final Set<String> adminGroups,
                        final String thriftAddress,
                        final int thriftPort,
                        final String instanceMarkerPrefix) {
    this.adminGroups = adminGroups;
    this.adminUsers = adminUsers;

    this.thriftAddress = thriftAddress;
    this.thriftPort = thriftPort;

    this.instanceMarkerPrefix = instanceMarkerPrefix;
    instance = this;
  }

  public synchronized static void create(final Set<String> adminUsers,
                                         final Set<String> adminGroups,
                                         final String thriftAddress,
                                         final int thriftPort,
                                         final String instanceMarkerPrefix) {
    if (instance != null) {
      return;
    }

    new Configuration(adminUsers,
            adminGroups,
            thriftAddress,
            thriftPort,
            instanceMarkerPrefix);
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
}
