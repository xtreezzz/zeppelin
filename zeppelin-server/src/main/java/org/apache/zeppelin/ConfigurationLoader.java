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
                             @Value("${zeppelin.instance.markerPrefix}") final String instanceMarkerPrefix) {

    Configuration.create(
            parseString(admin_users, ","),
            parseString(admin_group, ","),
            thriftAddress,
            thriftPort,
            instanceMarkerPrefix
    );
  }

  private Set<String> parseString(final String param, final String delimeter) {
    return Arrays.stream(param.split(delimeter)).map(String::trim).collect(Collectors.toSet());
  }
}
