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

import static org.apache.commons.lang3.StringUtils.isNotBlank;

import com.google.gson.Gson;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import org.sonatype.aether.repository.Authentication;
import org.sonatype.aether.repository.Proxy;

public class Repository {

  private boolean snapshot;
  @Nonnull
  private String id;
  @Nonnull
  private String url;
  @Nonnull
  private String username;
  @Nonnull
  private String password;
  @Nullable
  private String proxyProtocol;
  @Nullable
  private String proxyHost;
  @Nullable
  private Integer proxyPort;
  @Nullable
  private String proxyLogin;
  @Nullable
  private String proxyPassword;

  public Repository(final boolean snapshot, @Nonnull final String id, @Nonnull final String url,
      @Nonnull final String username, @Nonnull final String password,
      @Nullable final String proxyProtocol, @Nullable final String proxyHost,
      @Nullable final Integer proxyPort, @Nullable final String proxyLogin,
      final String proxyPassword) {
    this.snapshot = snapshot;
    this.id = id;
    this.url = url;
    this.username = username;
    this.password = password;
    this.proxyProtocol = proxyProtocol;
    this.proxyHost = proxyHost;
    this.proxyPort = proxyPort;
    this.proxyLogin = proxyLogin;
    this.proxyPassword = proxyPassword;
  }

  @Nonnull
  public Authentication getAuthentication() {
    return new Authentication(this.username, this.password);
  }

  @Nullable
  public Proxy getProxy() {
    if (isNotBlank(proxyHost) && proxyPort != null) {
      if (isNotBlank(proxyLogin)) {
        return new Proxy(proxyProtocol, proxyHost, proxyPort,
            new Authentication(proxyLogin, proxyPassword));
      } else {
        return new Proxy(proxyProtocol, proxyHost, proxyPort, null);
      }
    }
    return null;
  }

  public boolean isSnapshot() {
    return snapshot;
  }

  public void setSnapshot(final boolean snapshot) {
    this.snapshot = snapshot;
  }

  @Nonnull
  public String getId() {
    return id;
  }

  public void setId(final String id) {
    this.id = id;
  }

  @Nonnull
  public String getUrl() {
    return url;
  }

  public void setUrl(final String url) {
    this.url = url;
  }

  @Nonnull
  public String getUsername() {
    return username;
  }

  public void setUsername(@Nonnull final String username) {
    this.username = username;
  }

  @Nonnull
  public String getPassword() {
    return password;
  }

  public void setPassword(@Nonnull final String password) {
    this.password = password;
  }

  @Nullable
  public String getProxyProtocol() {
    return proxyProtocol;
  }

  public void setProxyProtocol(@Nullable final String proxyProtocol) {
    this.proxyProtocol = proxyProtocol;
  }

  @Nullable
  public String getProxyHost() {
    return proxyHost;
  }

  public void setProxyHost(@Nullable final String proxyHost) {
    this.proxyHost = proxyHost;
  }
  @Nullable
  public Integer getProxyPort() {
    return proxyPort;
  }

  public void setProxyPort(@Nullable final Integer proxyPort) {
    this.proxyPort = proxyPort;
  }

  @Nullable
  public String getProxyLogin() {
    return proxyLogin;
  }

  public void setProxyLogin(@Nullable final String proxyLogin) {
    this.proxyLogin = proxyLogin;
  }

  @Nullable
  public String getProxyPassword() {
    return proxyPassword;
  }

  public void setProxyPassword(@Nullable final String proxyPassword) {
    this.proxyPassword = proxyPassword;
  }

  public String toJson() {
    return new Gson().toJson(this);
  }

  public static Repository fromJson(@Nonnull final String json) {
    return new Gson().fromJson(json, Repository.class);
  }
}
