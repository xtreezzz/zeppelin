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
package org.apache.zeppelin.server;

import org.apache.zeppelin.configuration.ZeppelinConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.servlet.*;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.net.InetAddress;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.UnknownHostException;

/**
 * Cors filter.
 */
public class CorsFilter implements Filter {

  private static final Logger LOGGER = LoggerFactory.getLogger(CorsFilter.class);

  @Override
  public void doFilter(final ServletRequest request, final ServletResponse response, final FilterChain filterChain)
      throws IOException, ServletException {
    final String sourceHost = ((HttpServletRequest) request).getHeader("Origin");
    String origin = "";

    try {
      if (isValidOrigin(sourceHost, ZeppelinConfiguration.create())) {
        origin = sourceHost;
      }
    } catch (final URISyntaxException e) {
      LOGGER.error("Exception in WebDriverManager while getWebDriver ", e);
    }

    if (((HttpServletRequest) request).getMethod().equals("OPTIONS")) {
      final HttpServletResponse resp = ((HttpServletResponse) response);
      addCorsHeaders(resp, origin);
      return;
    }

    if (response instanceof HttpServletResponse) {
      final HttpServletResponse alteredResponse = ((HttpServletResponse) response);
      addCorsHeaders(alteredResponse, origin);
    }
    filterChain.doFilter(request, response);
  }

  private void addCorsHeaders(final HttpServletResponse response, final String origin) {
    response.setHeader("Access-Control-Allow-Origin", origin);
    response.setHeader("Access-Control-Allow-Credentials", "true");
    response.setHeader("Access-Control-Allow-Headers", "authorization,Content-Type");
    response.setHeader("Access-Control-Allow-Methods", "POST, GET, OPTIONS, PUT, HEAD, DELETE");

    final ZeppelinConfiguration zeppelinConfiguration = ZeppelinConfiguration.create();
    response.setHeader("X-FRAME-OPTIONS", zeppelinConfiguration.getXFrameOptions());
    if (zeppelinConfiguration.useSsl()) {
      response.setHeader("Strict-Transport-Security", zeppelinConfiguration.getStrictTransport());
    }
    response.setHeader("X-XSS-Protection", zeppelinConfiguration.getXxssProtection());
  }

  @Override
  public void destroy() {}

  @Override
  public void init(final FilterConfig filterConfig) {}

  private static Boolean isValidOrigin(final String sourceHost, final ZeppelinConfiguration conf)
          throws UnknownHostException, URISyntaxException {

    String sourceUriHost = "";

    if (sourceHost != null && !sourceHost.isEmpty()) {
      sourceUriHost = new URI(sourceHost).getHost();
      sourceUriHost = (sourceUriHost == null) ? "" : sourceUriHost.toLowerCase();
    }

    sourceUriHost = sourceUriHost.toLowerCase();
    final String currentHost = InetAddress.getLocalHost().getHostName().toLowerCase();

    return conf.getAllowedOrigins().contains("*")
            || currentHost.equals(sourceUriHost)
            || "localhost".equals(sourceUriHost)
            || conf.getAllowedOrigins().contains(sourceHost);
  }
}
