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

import org.apache.commons.lang.exception.ExceptionUtils;
import org.apache.zeppelin.annotation.ZeppelinApi;
import org.apache.zeppelin.interpreterV2.configuration.InterpreterPropertyType;
import org.apache.zeppelin.interpreterV2.configuration.InterpreterSettingRepository;
import org.apache.zeppelin.interpreterV2.configuration.InterpreterSettingV2;
import org.apache.zeppelin.rest.message.NewInterpreterSettingRequest;
import org.apache.zeppelin.server.JsonResponse;
import org.apache.zeppelin.service.InterpreterService;
import org.apache.zeppelin.service.SecurityService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

/**
 * Interpreter Rest API.
 */
@RestController
@RequestMapping("/api/interpreter")
public class InterpreterRestApi {

  private static final Logger logger = LoggerFactory.getLogger(InterpreterRestApi.class);

  private final SecurityService securityService;
  private final InterpreterService interpreterService;
  private final InterpreterSettingRepository interpreterSettingRepository;

  @Autowired
  public InterpreterRestApi(
          @Qualifier("NoSecurityService") final SecurityService securityService,
          final InterpreterService interpreterService,
          final InterpreterSettingRepository interpreterSettingRepository) {
    this.securityService = securityService;
    this.interpreterService = interpreterService;
    this.interpreterSettingRepository = interpreterSettingRepository;
  }

  /**
   * List all interpreter settings.
   */
  @ZeppelinApi
  @GetMapping(value = "/setting", produces = "application/json")
  public ResponseEntity listSettings() {
    return new JsonResponse(HttpStatus.OK, "", interpreterSettingRepository.get()).build();
  }

  /**
   * Get a setting.
   */
  @ZeppelinApi
  @GetMapping(value = "/setting/{settingId}", produces = "application/json")
  public ResponseEntity getSetting(@PathVariable("settingId") final String settingId) {
    try {
      final InterpreterSettingV2 setting = interpreterSettingRepository.get(settingId);
      if (setting == null) {
        return new JsonResponse(HttpStatus.NOT_FOUND).build();
      } else {
        return new JsonResponse(HttpStatus.OK, "", setting).build();
      }
    } catch (final NullPointerException e) {
      logger.error("Exception in InterpreterRestApi while creating ", e);
      return new JsonResponse(HttpStatus.INTERNAL_SERVER_ERROR, e.getMessage(),
          ExceptionUtils.getStackTrace(e)).build();
    }
  }

  /**
   * Add new interpreter setting.
   *
   * @param message NewInterpreterSettingRequest
   */
  @ZeppelinApi
  @PostMapping(value = "/setting", produces = "application/json")
  public ResponseEntity newSettings(final String message) {
    try {
      final NewInterpreterSettingRequest request =
          NewInterpreterSettingRequest.fromJson(message);
      if (request == null) {
        return new JsonResponse(HttpStatus.BAD_REQUEST).build();
      }

      final InterpreterSettingV2 interpreterSetting = interpreterSettingRepository
          .createNewSetting(request.getName(), request.getGroup(), request.getDependencies(),
              request.getOption(), request.getProperties());
      logger.info("new setting created with {}", interpreterSetting.getId());
      return new JsonResponse(HttpStatus.OK, "", interpreterSetting).build();
    } catch (final IOException e) {
      logger.error("Exception in InterpreterRestApi while creating ", e);
      return new JsonResponse(HttpStatus.NOT_FOUND, e.getMessage(), ExceptionUtils.getStackTrace(e))
          .build();
    }
  }

  @ZeppelinApi
  @PutMapping(value = "/setting/{settingId}", produces = "application/json")
  public ResponseEntity updateSetting(final String message, @PathVariable("settingId") final String settingId) {
    logger.info("Update interpreterSetting {}", settingId);
    //
    //    try {
    //      final UpdateInterpreterSettingRequest request =
    //          UpdateInterpreterSettingRequest.fromJson(message);
    //      interpreterSettingRepository
    //          .setPropertyAndRestart(settingId, request.getOption(), request.getProperties(),
    //              request.getDependencies());
    //    } catch (final InterpreterException e) {
    //      logger.error("Exception in InterpreterRestApi while updateSetting ", e);
    //      return new JsonResponse(HttpStatus.NOT_FOUND, e.getMessage(), ExceptionUtils.getStackTrace(e))
    //          .build();
    //    } catch (final IOException e) {
    //      logger.error("Exception in InterpreterRestApi while updateSetting ", e);
    //      return new JsonResponse(HttpStatus.INTERNAL_SERVER_ERROR, e.getMessage(),
    //          ExceptionUtils.getStackTrace(e)).build();
    //    }
    //    final InterpreterSetting setting = interpreterSettingManager.get(settingId);
    //    if (setting == null) {
    //      return new JsonResponse(HttpStatus.NOT_FOUND, "", settingId).build();
    //    }
    return new JsonResponse(HttpStatus.NOT_IMPLEMENTED, "").build();
  }

  /**
   * Remove interpreter setting.
   */
  @ZeppelinApi
  @DeleteMapping(value = "/setting/{settingId}", produces = "application/json")
  public ResponseEntity removeSetting(@PathVariable("settingId") final String settingId) throws IOException {
    //    logger.info("Remove interpreterSetting {}", settingId);
    //    interpreterSettingManager.remove(settingId);
    //    return new JsonResponse(HttpStatus.OK).build();
    return new JsonResponse(HttpStatus.NOT_IMPLEMENTED, "").build();
  }

  /**
   * Restart interpreter setting.
   */
  @ZeppelinApi
  @PutMapping(value = "/setting/restart/{settingId}", produces = "application/json")
  public ResponseEntity restartSetting(final String message, @PathVariable("settingId") final String settingId) {
    logger.info("Restart interpreterSetting {}, msg={}", settingId, message);

    //    final InterpreterSetting setting = interpreterSettingManager.get(settingId);
    //    try {
    //      final RestartInterpreterRequest request = RestartInterpreterRequest.fromJson(message);
    //
    //      final String noteId = request == null ? null : request.getNoteId();
    //      if (null == noteId) {
    //        interpreterSettingManager.close(settingId);
    //      } else {
    //        interpreterSettingManager.restart(settingId, noteId, securityService.getPrincipal());
    //      }
    //
    //    } catch (final InterpreterException e) {
    //      logger.error("Exception in InterpreterRestApi while restartSetting ", e);
    //      return new JsonResponse(HttpStatus.NOT_FOUND, e.getMessage(), ExceptionUtils.getStackTrace(e))
    //          .build();
    //    }
    //    if (setting == null) {
    //      return new JsonResponse(HttpStatus.NOT_FOUND, "", settingId).build();
    //    }
    //    return new JsonResponse(HttpStatus.OK, "", setting).build();
    return new JsonResponse(HttpStatus.NOT_IMPLEMENTED, "").build();
  }

  /**
   * List all available interpreters by group.
   */
  @GetMapping(produces = "application/json")
  public ResponseEntity listInterpreter() {
    final Map<String, InterpreterSettingV2> m = interpreterSettingRepository.getInterpreterSettingTemplates();
    return new JsonResponse(HttpStatus.OK, "", m).build();
  }

  /**
   * List of dependency resolving repositories.
   */
  @ZeppelinApi
  @GetMapping(value = "/repository", produces = "application/json")
  public ResponseEntity listRepositories() {
    //    final List<RemoteRepository> interpreterRepositories = interpreterSettingRepository.getRepositories();
    //    return new JsonResponse(HttpStatus.OK, "", interpreterRepositories).build();
    return new JsonResponse(HttpStatus.NOT_IMPLEMENTED, "").build();
  }

  /**
   * Add new repository.
   *
   * @param message Repository
   */
  @ZeppelinApi
  @PostMapping(value = "/repository", produces = "application/json")
  public ResponseEntity addRepository(final String message) {
    //    try {
    //      final Repository request = Repository.fromJson(message);
    //      interpreterSettingManager.addRepository(request.getId(), request.getUrl(),
    //          request.isSnapshot(), request.getAuthentication(), request.getProxy());
    //      logger.info("New repository {} added", request.getId());
    //    } catch (final Exception e) {
    //      logger.error("Exception in InterpreterRestApi while adding repository ", e);
    //      return new JsonResponse(HttpStatus.INTERNAL_SERVER_ERROR, e.getMessage(),
    //          ExceptionUtils.getStackTrace(e)).build();
    //    }
    //    return new JsonResponse(HttpStatus.OK).build();
    return new JsonResponse(HttpStatus.NOT_IMPLEMENTED, "").build();
  }

  /**
   * Delete repository.
   *
   * @param repoId ID of repository
   */
  @ZeppelinApi
  @DeleteMapping(value = "/repository/{repoId}", produces = "application/json")
  public ResponseEntity removeRepository(@PathVariable("repoId") final String repoId) {
    logger.info("Remove repository {}", repoId);
    //    try {
    //      interpreterSettingManager.removeRepository(repoId);
    //    } catch (final Exception e) {
    //      logger.error("Exception in InterpreterRestApi while removing repository ", e);
    //      return new JsonResponse(HttpStatus.INTERNAL_SERVER_ERROR, e.getMessage(),
    //          ExceptionUtils.getStackTrace(e)).build();
    //    }
    //    return new JsonResponse(HttpStatus.OK).build();
    return new JsonResponse(HttpStatus.NOT_IMPLEMENTED, "").build();
  }

  /**
   * Get available types for property
   */
  @GetMapping(value = "/property/types", produces = "application/json")
  public ResponseEntity listInterpreterPropertyTypes() {
    return new JsonResponse(HttpStatus.OK, InterpreterPropertyType.getTypes()).build();
  }

  /** Install interpreter */
  //@POST
  //@Path("install")
 /* @ZeppelinApi
  @PostMapping(value = "/install", produces = "application/json")
  public ResponseEntity install(@NotNull String message) {
    logger.info("Install interpreter: {}", message);
    InterpreterInstallationRequest request = InterpreterInstallationRequest.fromJson(message);

    try {
      interpreterService.install(
          request,
          new SimpleServiceCallback<String>() {
            @Override
            public void onStart(String message, ServiceContext context) {
              Message m = new Message(OP.INTERPRETER_INSTALL_STARTED);
              Map<String, Object> data = Maps.newHashMap();
              data.put("result", "Starting");
              data.put("message", message);
              m.data = data;
              notebookServer.broadcast(m);
            }

            @Override
            public void onSuccess(String message, ServiceContext context) {
              Message m = new Message(OP.INTERPRETER_INSTALL_RESULT);
              Map<String, Object> data = Maps.newHashMap();
              data.put("result", "Succeed");
              data.put("message", message);
              m.data = data;
              notebookServer.broadcast(m);
            }

            @Override
            public void onFailure(Exception ex, ServiceContext context) {
              Message m = new Message(OP.INTERPRETER_INSTALL_RESULT);
              Map<String, Object> data = Maps.newHashMap();
              data.put("result", "Failed");
              data.put("message", ex.getMessage());
              m.data = data;
              notebookServer.broadcast(m);
            }
          });
    } catch (Throwable t) {
      return new JsonResponse(HttpStatus.INTERNAL_SERVER_ERROR, t.getMessage()).build();
    }

    return new JsonResponse<>(Status.OK).build();
  }
  */

  /**
   * Get all running interpreters.
   */
  @ZeppelinApi
  @GetMapping(value = "/running", produces = "application/json")
  public ResponseEntity listRunningInterpreters() {
    //    return new JsonResponse(HttpStatus.OK, "", interpreterSettingManager.getRunningInterpretersInfo()).build();
    return new JsonResponse(HttpStatus.NOT_IMPLEMENTED, "").build();
  }

  /**
   * Get info about the running paragraphs grouped by their interpreters.
   *
   * @return JSON with status.OK
   */
  @ZeppelinApi
  @GetMapping(value = "/running/jobs", produces = "application/json")
  public ResponseEntity getRunning() {
    final Map<String, Object> response = new HashMap<>();
    response.put("lastResponseUnixTime", System.currentTimeMillis());
    response.put("runningInterpreters", interpreterService.getRunningInterpretersParagraphInfo());

    return new JsonResponse(HttpStatus.OK, "", response).build();
  }
}
