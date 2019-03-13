package org.apache.zeppelin.interpreterV2.configuration.option;

import org.apache.commons.lang3.StringUtils;

/**
 *  Option 'Connect to existing process' on interpreter configuration page.
 */
public class ExistingProcess {
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
}
