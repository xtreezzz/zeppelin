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
package ru.tinkoff.zeppelin.core.configuration.interpreter.option;

import com.google.common.base.Preconditions;
import com.google.gson.Gson;
import org.apache.commons.lang3.StringUtils;

import javax.annotation.Nonnull;
import java.io.Serializable;
import java.util.StringJoiner;

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
