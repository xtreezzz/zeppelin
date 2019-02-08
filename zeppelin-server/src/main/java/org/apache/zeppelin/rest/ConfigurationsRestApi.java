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
package org.apache.zeppelin.rest;

import org.apache.zeppelin.annotation.ZeppelinApi;
import org.apache.zeppelin.server.JsonResponse;
import org.apache.zeppelin.service.ConfigurationService;
import org.apache.zeppelin.service.SecurityService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.Map;

/**
 * Configurations Rest API Endpoint.
 */
@RestController
@RequestMapping("/api/configurations")
public class ConfigurationsRestApi extends AbstractRestApi {

  private final ConfigurationService configurationService;

  @Autowired
  public ConfigurationsRestApi(
          @Qualifier("NoSecurityService") final SecurityService securityService,
          final ConfigurationService configurationService) {
    super(securityService);
    this.configurationService = configurationService;
  }

  @ZeppelinApi
  @GetMapping(value = "/all", produces = "application/json")
  public ResponseEntity getAll() {
    try {
      final Map<String, String> properties = configurationService.getAllProperties();
      return new JsonResponse(HttpStatus.OK, "", properties).build();
    } catch (final Exception e) {
      return new JsonResponse(HttpStatus.INTERNAL_SERVER_ERROR, "Fail to get configuration", e).build();
    }
  }

  @ZeppelinApi
  @GetMapping(value = "/prefix/{prefix}", produces = "application/json")
  public ResponseEntity getByPrefix(@PathVariable("prefix") final String prefix) {
    try {
      final Map<String, String> properties = configurationService.getPropertiesWithPrefix(prefix);
      return new JsonResponse(HttpStatus.OK, "", properties).build();
    } catch (final Exception e) {
      return new JsonResponse(HttpStatus.INTERNAL_SERVER_ERROR, "Fail to get configuration", e).build();
    }
  }
}
