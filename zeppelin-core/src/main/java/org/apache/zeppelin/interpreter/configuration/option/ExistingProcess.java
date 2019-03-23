package org.apache.zeppelin.interpreter.configuration.option;

import com.google.common.base.Preconditions;
import com.google.gson.Gson;
import java.io.Serializable;
import java.util.StringJoiner;
import javax.annotation.Nonnull;
import org.apache.commons.lang3.StringUtils;

/**
 * Option 'Connect to existing process' on interpreter configuration page.
 */
public class ExistingProcess implements Serializable {
  @Nonnull
  private String host;
  private int port;
  private boolean isEnabled;

  public ExistingProcess() {
    this.host = StringUtils.EMPTY;
    this.port = -1;
    this.isEnabled = false;
  }

  public ExistingProcess(@Nonnull final String host, final int port, final boolean isEnabled) {
    Preconditions.checkArgument(isValidPort(port), "Wrong port: %s", String.valueOf(port));
    Preconditions.checkArgument(isValidHost(host), "Wrong host: %s", host);

    this.host = host;
    this.port = port;
    this.isEnabled = isEnabled;
  }

  @Nonnull
  public String getHost() {
    return host;
  }

  public void setHost(@Nonnull final String host) {
    Preconditions.checkArgument(isValidHost(host), "Wrong host: %s", host);
    this.host = host;
  }

  public int getPort() {
    return port;
  }

  public void setPort(final int port) {
    Preconditions.checkArgument(isValidPort(port), "Wrong port: %s", String.valueOf(port));
    this.port = port;
  }

  public boolean isEnabled() {
    return isEnabled;
  }

  public void setEnabled(final boolean enabled) {
    isEnabled = enabled;
  }

  /**
   * Port number validation:
   *    -1: default value when process is not enabled.
   *    [1; 2^16): available ports.
   *
   * @param port, port number.
   * @return {@code true} if port is valid, {@code false} otherwise.
   */
  private static boolean isValidPort(final int port) {
    return port == -1 || port > 0 && port < Math.pow(2, 16);
  }

  /**
   * Host address validation:
   *    "": default value when process is not enabled
   *    host: valid host otherwise
   *
   * @param host
   * @return
   */
  private static boolean isValidHost(@Nonnull final String host) {
    //TODO(egorklimov): regexp for host
    return true;
  }

  public static ExistingProcess fromJson(@Nonnull final String message) {
    Preconditions.checkNotNull(message);

    final ExistingProcess remote = new Gson().fromJson(message, ExistingProcess.class);

    Preconditions.checkState(isValidHost(remote.host));
    Preconditions.checkState(isValidPort(remote.port));
    return remote;
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
