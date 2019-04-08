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

import com.google.common.collect.Lists;
import org.apache.commons.lang.StringUtils;
import org.apache.zeppelin.rest.message.LoggerRequest;
import org.apache.zeppelin.rest.message.SchedulerConfigRequest;
import org.apache.zeppelin.server.JsonResponse;
import org.apache.zeppelin.service.AdminService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.List;

/**
 * This rest apis support some of feature related admin. e.g. changin log level.
 */
@RestController
@RequestMapping("/api/admin")
public class AdminRestApi {
  private static final Logger logger = LoggerFactory.getLogger(AdminRestApi.class);

  private final AdminService adminService;

  @Autowired
  public AdminRestApi(final AdminService adminService) {
    this.adminService = adminService;
  }

  /**
   * It gets current loggers' name and level.
   *
   * @param name FQCN
   * @return List of current loggers' name and level with json format. It returns all of loggers'
   * name and level without name. With name, it returns only specific logger's name and level.
   */
  @GetMapping(produces = "application/json")
  public List<org.apache.log4j.Logger> getLoggerSetting(@RequestParam("name") final String name) {
    logger.debug("name: {}", name);
    return null == name || name.isEmpty()
            ? adminService.getLoggers()
            : Lists.newArrayList(adminService.getLogger(name));
  }

  /**
   * It change logger's level.
   *
   * @param loggerRequest logger's name and level with json format
   * @return The changed logger's name and level.
   */
  @PostMapping(produces = "application/json")
  public List<org.apache.log4j.Logger> setLoggerLevel(final LoggerRequest loggerRequest) {
    if (null == loggerRequest
            || StringUtils.isEmpty(loggerRequest.getName())
            || StringUtils.isEmpty(loggerRequest.getLevel())) {
      logger.trace("loggerRequest: {}", loggerRequest);
      throw new IllegalArgumentException("Wrong request body");
    }
    logger.debug("loggerRequest: {}", loggerRequest);

    adminService.setLoggerLevel(loggerRequest);

    return Lists.newArrayList(adminService.getLogger(loggerRequest.getName()));
  }


  /**
   * Change quartz thread pool size REST API.
   *
   * @param message - JSON with poolSize value.
   * @return JSON with status.OK
   */
  @PostMapping(value = "cron/pool/{id}", produces = "application/json")
  public ResponseEntity changeScheduler(@PathVariable("id") final String schedulerId, final String message) {
    logger.info("Change cron pool size with msg={}", message);
    final SchedulerConfigRequest request = SchedulerConfigRequest.fromJson(message);
    if (request.getPoolSize() != null) {
      adminService.setSchedulerThreadPoolSize(schedulerId, request.getPoolSize());
    }
    return new JsonResponse(HttpStatus.OK).build();
  }

  /**
   * Get scheduler settings REST API.
   *
   * @return JSON with status.OK
   */
  @GetMapping(value = "cron/pool", produces = "application/json")
  public ResponseEntity getQuartzSchedulerPoolInfo() {
    final List<SchedulerConfigRequest> settings = adminService.getSchedulersInfoList();
    if (settings == null) {
      return new JsonResponse(HttpStatus.NOT_FOUND).build();
    } else {
      return new JsonResponse(HttpStatus.OK, "", settings).build();
    }
  }
}
