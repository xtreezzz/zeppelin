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

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.commons.lang.exception.ExceptionUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.zeppelin.Repository;
import org.apache.zeppelin.annotation.ZeppelinApi;
import org.apache.zeppelin.interpreterV2.configuration.BaseInterpreterConfig;
import org.apache.zeppelin.interpreterV2.configuration.InterpreterOption;
import org.apache.zeppelin.interpreterV2.configuration.InterpreterProperty;
import org.apache.zeppelin.interpreterV2.configuration.InterpreterSource;
import org.apache.zeppelin.interpreterV2.configuration.option.ExistingProcess;
import org.apache.zeppelin.interpreterV2.configuration.option.Permissions;
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
import org.springframework.web.bind.annotation.DeleteMapping;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.PutMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

/**
 * Interpreter Rest API.
 */
@RestController
@RequestMapping("/api/interpreter")
public class InterpreterRestApi {

  private static final Logger logger = LoggerFactory.getLogger(InterpreterRestApi.class);

  private final SecurityService securityService;
  private final InterpreterService interpreterService;

  private final InterpreterOption markdownOption;
  private final List<InterpreterSource> interpreterSources;
  private final List<Repository> repositories;

  @Autowired
  public InterpreterRestApi(
          @Qualifier("NoSecurityService") final SecurityService securityService,
          final InterpreterService interpreterService) {
    Map<String, InterpreterProperty> interpreterPropertyMap = new HashMap<>();
    interpreterPropertyMap.put("markdown.parser.type",
        new InterpreterProperty(
            "MARKDOWN_PARSER_TYPE",
            "markdown.parser.type",
            "pegdown",
            "Markdown Parser Type. Available values: pegdown, markdown4j. Default = pegdown",
            "string"
        )
    );

    markdownOption = new InterpreterOption(
        "Best Markdown Interpreter", "md", "%md",
        "shared", "shared", new BaseInterpreterConfig(
            "md", "md", "org.apache.zeppelin.markdown.Markdown", interpreterPropertyMap),
        new ExistingProcess(), new Permissions(), StringUtils.EMPTY, 1);

    interpreterSources = new ArrayList<>();
    interpreterSources.add(new InterpreterSource("md", "org.apache.zeppelin:zeppelin-markdown:0.9.0-SNAPSHOT"));

    repositories = new ArrayList<>();
    repositories.add(new Repository("hardcoded"));

    this.securityService = securityService;
    this.interpreterService = interpreterService;
  }

  /**
   * List all interpreter settings.
   */
  @ZeppelinApi
  @GetMapping(value = "/setting", produces = "application/json")
  public ResponseEntity listSettings() {
    List<InterpreterOption> settings = new ArrayList<>();
    settings.add(markdownOption);
    return new JsonResponse<>(HttpStatus.OK, "", settings).build();
  }

  /**
   * Get a setting.
   */
  @ZeppelinApi
  @GetMapping(value = "/setting/{settingId}", produces = "application/json")
  public ResponseEntity getSetting(@PathVariable("settingId") final String settingId) {
    try {
      final InterpreterOption setting = markdownOption;
      if (setting == null) {
        return new JsonResponse<>(HttpStatus.NOT_FOUND).build();
      } else {
        return new JsonResponse<>(HttpStatus.OK, "", setting).build();
      }
    } catch (final NullPointerException e) {
      logger.error("Exception in InterpreterRestApi while creating ", e);
      return new JsonResponse<>(HttpStatus.INTERNAL_SERVER_ERROR, e.getMessage(),
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
    final NewInterpreterSettingRequest request =
        NewInterpreterSettingRequest.fromJson(message);
    if (request == null) {
      return new JsonResponse(HttpStatus.BAD_REQUEST).build();
    }

    markdownOption.setCustomInterpreterName(request.getName());
    markdownOption.setInterpreterName(request.getGroup());
    return new JsonResponse(HttpStatus.OK, "", markdownOption).build();
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
    return new JsonResponse<>(HttpStatus.NOT_IMPLEMENTED, "").build();
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
    return new JsonResponse<>(HttpStatus.NOT_IMPLEMENTED, "").build();
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
    return new JsonResponse<>(HttpStatus.NOT_IMPLEMENTED, "").build();
  }

  /**
   * List all available interpreters by group.
   */
  @GetMapping(produces = "application/json")
  public ResponseEntity listInterpreter() {
    final Map<String, InterpreterOption> m =  new HashMap<>();
    m.put("md", markdownOption);
    return new JsonResponse<>(HttpStatus.OK, "", m).build();
  }

  /**
   * List of dependency resolving repositories.
   */
  @ZeppelinApi
  @GetMapping(value = "/repository", produces = "application/json")
  public ResponseEntity listRepositories() {
    return new JsonResponse<>(HttpStatus.OK, "", repositories).build();
  }

  /**
   * Add new repository.
   *
   * @param message Repository
   */
  @ZeppelinApi
  @PostMapping(value = "/repository", produces = "application/json")
  public ResponseEntity addRepository(@RequestBody final String message) {
    final Repository request = Repository.fromJson(message);
    repositories.add(
        new Repository(request.getId())
            .url(request.getUrl())
            .credentials(
                request.getAuthentication().getUsername(),
                request.getAuthentication().getPassword())
    );
    logger.info("New repository {} added", request.getId());
    return new JsonResponse(HttpStatus.OK).build();
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
    repositories.removeIf(repo -> repo.getId().equalsIgnoreCase(repoId));
    return new JsonResponse(HttpStatus.OK).build();
  }

  /**
   * List of all sources.
   */
  @ZeppelinApi
  @GetMapping(value = "/source", produces = "application/json")
  public ResponseEntity listSources() {
    return new JsonResponse<>(HttpStatus.OK, "", interpreterSources).build();
  }

  /**
   * Add new source.
   *
   * @param message InterpreterSource
   */
  @ZeppelinApi
  @PostMapping(value = "/source", produces = "application/json")
  public ResponseEntity addSource(@RequestBody final String message) {
    final InterpreterSource request = InterpreterSource.fromJson(message);
    interpreterSources.add(request);
    logger.info("New source {} added", request.getArtifact());
    return new JsonResponse(HttpStatus.OK).build();
  }

  /**
   * Delete source.
   *
   * @param artifact of interpreter
   */
  @ZeppelinApi
  @DeleteMapping(value = "/source/{artifact}", produces = "application/json")
  public ResponseEntity removeSource(@PathVariable("artifact") final String artifact) {
    logger.info("Remove source {}", artifact);
    interpreterSources.removeIf(src -> src.getArtifact().equalsIgnoreCase(artifact));
    return new JsonResponse(HttpStatus.OK).build();
  }

  /**
   * Get available types for property
   */
  @GetMapping(value = "/property/types", produces = "application/json")
  public ResponseEntity listInterpreterPropertyTypes() {
    return new JsonResponse<>(HttpStatus.OK, InterpreterProperty.getTypes()).build();
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
    return new JsonResponse<>(HttpStatus.NOT_IMPLEMENTED, "").build();
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

    return new JsonResponse<>(HttpStatus.OK, "", response).build();
  }
}
