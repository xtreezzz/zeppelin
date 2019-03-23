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

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.commons.lang.exception.ExceptionUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.zeppelin.Repository;
import org.apache.zeppelin.Repository.ProxyProtocol;
import org.apache.zeppelin.annotation.ZeppelinApi;
import org.apache.zeppelin.interpreter.configuration.BaseInterpreterConfig;
import org.apache.zeppelin.interpreter.configuration.InterpreterArtifactSource;
import org.apache.zeppelin.interpreter.configuration.InterpreterOption;
import org.apache.zeppelin.interpreter.configuration.InterpreterOption.ProcessType;
import org.apache.zeppelin.interpreter.configuration.InterpreterProperty;
import org.apache.zeppelin.interpreter.configuration.option.ExistingProcess;
import org.apache.zeppelin.interpreter.configuration.option.Permissions;
import org.apache.zeppelin.server.JsonResponse;
import org.apache.zeppelin.service.InterpreterService;
import org.apache.zeppelin.service.SecurityService;
import org.apache.zeppelin.storage.InterpreterOptionRepository;
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


//TODO(egorklimov):
//  1. Добавить метод get all interpreter configs для получения базовых настроек интерпретаторов (из файлов).
//  2. install Source, uninstall source

/**
 * Interpreter Rest API.
 */
@RestController
@RequestMapping("/api/interpreter")
public class InterpreterRestApi {

  private static final Logger logger = LoggerFactory.getLogger(InterpreterRestApi.class);

  private final SecurityService securityService;
  private final InterpreterService interpreterService;
  private final InterpreterOptionRepository interpreterOptionRepository;

  @Autowired
  public InterpreterRestApi(
      @Qualifier("NoSecurityService") final SecurityService securityService,
      final InterpreterService interpreterService,
      final InterpreterOptionRepository interpreterOptionRepository) {
    this.interpreterOptionRepository = interpreterOptionRepository;
    this.securityService = securityService;
    this.interpreterService = interpreterService;
    try {
      tempRepositoryInit();
    } catch (final Exception e) {
      logger.error("Failed to init interpreter settings", e);
    }
  }

  /**
   * Initialize interpreter settings
   */
  private void tempRepositoryInit() {
    interpreterOptionRepository.saveSource(new InterpreterArtifactSource("md", "org.apache.zeppelin:zeppelin-markdown:0.9.0-SNAPSHOT"));

    final Map<String, InterpreterProperty> interpreterPropertyMap = new HashMap<>();
    interpreterPropertyMap.put("markdown.parser.type",
        new InterpreterProperty(
            "MARKDOWN_PARSER_TYPE",
            "markdown.parser.type",
            "pegdown",
            "Markdown Parser Type. Available values: pegdown, markdown4j. Default = pegdown",
            "string"
        )
    );
    interpreterOptionRepository.saveOption(new InterpreterOption(
        "Best Markdown Interpreter", "md", "%md",
        ProcessType.SHARED, ProcessType.SHARED, new BaseInterpreterConfig(
        "md", "md", "org.apache.zeppelin.markdown.Markdown", interpreterPropertyMap, new HashMap<>()),
        new ExistingProcess(), new Permissions(), StringUtils.EMPTY, 1, false));

    interpreterOptionRepository.saveRepository(
        new Repository(true, "central", "http://repo1.maven.org/maven2/",
            "username", "password", ProxyProtocol.HTTP, "127.0.0.1",
            8000, "proxyLogin", "proxyPass"));

  }

  /**
   * List all interpreter settings.
   */
  @ZeppelinApi
  @GetMapping(value = "/setting", produces = "application/json")
  public ResponseEntity listSettings() {
    try {
      return new JsonResponse<>(HttpStatus.OK, "", interpreterOptionRepository.getAllOptions()).build();
    } catch (final Exception e) {
      logger.error("Fail to get all interpreter setting", e);
      return new JsonResponse<>(HttpStatus.INTERNAL_SERVER_ERROR, e.getMessage(),
          ExceptionUtils.getStackTrace(e)).build();
    }
  }

  /**
   * Get a setting.
   */
  @ZeppelinApi
  @GetMapping(value = "/setting/{shebang}", produces = "application/json")
  public ResponseEntity getSetting(@PathVariable("shebang") final String shebang) {
    try {
      final InterpreterOption setting = interpreterOptionRepository.getOption(shebang);
      if (setting == null) {
        return new JsonResponse<>(HttpStatus.NOT_FOUND).build();
      } else {
        return new JsonResponse<>(HttpStatus.OK, "", setting).build();
      }
    } catch (final Exception e) {
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
//    final NewInterpreterSettingRequest request =
//        NewInterpreterSettingRequest.fromJson(message);
//    if (request == null) {
//      return new JsonResponse(HttpStatus.BAD_REQUEST).build();
//    }
//
//    markdownOption.setCustomInterpreterName(request.getName());
//    markdownOption.setInterpreterName(request.getGroup());
    return new JsonResponse(HttpStatus.NOT_IMPLEMENTED, "").build();
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
  @DeleteMapping(value = "/setting/{shebang}", produces = "application/json")
  public ResponseEntity removeSetting(@PathVariable("shebang") final String shebang) {
    try {
      logger.info("Remove interpreterSetting {}", shebang);
      interpreterOptionRepository.removeOption(shebang);
      return new JsonResponse(HttpStatus.OK).build();
    } catch (final Exception e) {
      logger.error("Exception in InterpreterRestApi while removing interpreter option ", e);
      return new JsonResponse<>(HttpStatus.INTERNAL_SERVER_ERROR, e.getMessage(),
          ExceptionUtils.getStackTrace(e)).build();
    }
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
    try {
      final Map<String, InterpreterOption> m =  new HashMap<>();
      final List<InterpreterOption> interpreters = interpreterOptionRepository.getAllOptions();
      for (final InterpreterOption option : interpreters) {
        m.put(option.getConfig().getGroup(), option);
      }
      return new JsonResponse<>(HttpStatus.OK, "", m).build();
    } catch (final Exception e) {
      logger.error("Exception in InterpreterRestApi while loading all options ", e);
      return new JsonResponse<>(HttpStatus.INTERNAL_SERVER_ERROR, e.getMessage(),
          ExceptionUtils.getStackTrace(e)).build();
    }
  }

  /**
   * List of dependency resolving repositories.
   */
  @ZeppelinApi
  @GetMapping(value = "/repository", produces = "application/json")
  public ResponseEntity listRepositories() {
    try {
      return new JsonResponse<>(HttpStatus.OK, "", interpreterOptionRepository.getAllRepositories()).build();
    } catch (final Exception e) {
      logger.error("Exception in InterpreterRestApi while loading all repositories ", e);
      return new JsonResponse<>(HttpStatus.INTERNAL_SERVER_ERROR, e.getMessage(),
          ExceptionUtils.getStackTrace(e)).build();
    }
  }

  /**
   * Add new repository.
   *
   * @param message Repository
   */
  @ZeppelinApi
  @PostMapping(value = "/repository", produces = "application/json")
  public ResponseEntity addRepository(@RequestBody final String message) {
    try {
      final Repository request = Repository.fromJson(message);
      interpreterOptionRepository.saveRepository(request);
      logger.info("New repository {} added", request.getId());
      return new JsonResponse(HttpStatus.OK).build();
    } catch (final Exception e) {
      logger.error("Exception in InterpreterRestApi while creating repository ", e);
      return new JsonResponse<>(HttpStatus.INTERNAL_SERVER_ERROR, e.getMessage(),
          ExceptionUtils.getStackTrace(e)).build();
    }
  }

  /**
   * Delete repository.
   *
   * @param repoId ID of repository
   */
  @ZeppelinApi
  @DeleteMapping(value = "/repository/{repoId}", produces = "application/json")
  public ResponseEntity removeRepository(@PathVariable("repoId") final String repoId) {
    try {
      logger.info("Remove repository {}", repoId);
      interpreterOptionRepository.removeRepository(repoId);
      return new JsonResponse(HttpStatus.OK).build();
    } catch (final Exception e) {
      logger.error("Exception in InterpreterRestApi while deleting repository ", e);
      return new JsonResponse<>(HttpStatus.INTERNAL_SERVER_ERROR, e.getMessage(),
          ExceptionUtils.getStackTrace(e)).build();
    }
  }

  /**
   * List of all sources.
   */
  @ZeppelinApi
  @GetMapping(value = "/source", produces = "application/json")
  public ResponseEntity listSources() {
    try {
    return new JsonResponse<>(HttpStatus.OK, "", interpreterOptionRepository.getAllSources()).build();
    } catch (final Exception e) {
      logger.error("Exception in InterpreterRestApi while loading all sources ", e);
      return new JsonResponse<>(HttpStatus.INTERNAL_SERVER_ERROR, e.getMessage(),
          ExceptionUtils.getStackTrace(e)).build();
    }
  }

  /**
   * Add new source.
   *
   * @param message InterpreterArtifactSource
   */
  @ZeppelinApi
  @PostMapping(value = "/source", produces = "application/json")
  public ResponseEntity addSource(@RequestBody final String message) {
    try {
      final InterpreterArtifactSource request = InterpreterArtifactSource.fromJson(message);
      interpreterOptionRepository.saveSource(request);
      logger.info("New source {} added", request.getArtifact());
      return new JsonResponse(HttpStatus.OK).build();
    } catch (final Exception e) {
      logger.error("Exception in InterpreterRestApi while creating new source ", e);
      return new JsonResponse<>(HttpStatus.INTERNAL_SERVER_ERROR, e.getMessage(),
          ExceptionUtils.getStackTrace(e)).build();
    }
}

  /**
   * Delete source.
   *
   * @param interpreterName of interpreter
   */
  @ZeppelinApi
  @DeleteMapping(value = "/source/{name}", produces = "application/json")
  public ResponseEntity removeSource(@PathVariable("name") final String interpreterName) {
    try {
      logger.info("Remove source {}", interpreterName);
      interpreterOptionRepository.removeSource(interpreterName);
      return new JsonResponse(HttpStatus.OK).build();
    } catch (final Exception e) {
      logger.error("Exception in InterpreterRestApi while deleting source ", e);
      return new JsonResponse<>(HttpStatus.INTERNAL_SERVER_ERROR, e.getMessage(),
          ExceptionUtils.getStackTrace(e)).build();
    }
  }

  /**
   * Get available types for property
   */
  @GetMapping(value = "/property/types", produces = "application/json")
  public ResponseEntity listInterpreterPropertyTypes() {
    return new JsonResponse<>(HttpStatus.OK, InterpreterProperty.getTypes()).build();
  }
}
