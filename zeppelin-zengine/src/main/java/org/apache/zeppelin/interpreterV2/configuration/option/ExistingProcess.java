package org.apache.zeppelin.interpreterV2.configuration.option;

import java.io.Serializable;
import java.util.StringJoiner;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.builder.ToStringBuilder;

/**
 *  Option 'Connect to existing process' on interpreter configuration page.
 */
public class ExistingProcess implements Serializable {
  private String host;
  private int port;
  private boolean isEnabled;

  public ExistingProcess() {
    this.host = StringUtils.EMPTY;
    this.port = -1;
    this.isEnabled = false;
  }

  public ExistingProcess(String host, int port, boolean isEnabled) {
    this.host = host;
    this.port = port;
    this.isEnabled = isEnabled;
  }

  public String getHost() {
    return host;
  }

  public void setHost(String host) {
    this.host = host;
  }

  public int getPort() {
    return port;
  }

  public void setPort(int port) {
    this.port = port;
  }

  public boolean isEnabled() {
    return isEnabled;
  }

  public void setEnabled(boolean enabled) {
    isEnabled = enabled;
  }

  @Override
  public String toString() {
    return new StringJoiner(", ", "{", "}")
        .add("host: '" + host + "'")
        .add("port: " + port)
        .add("isEnabled: " + isEnabled)
        .toString();
  }
}
