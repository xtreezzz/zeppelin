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

package org.apache.zeppelin.rest.moduleSettings;

import com.google.gson.Gson;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import org.apache.commons.lang.exception.ExceptionUtils;
import org.apache.zeppelin.Repository;
import org.apache.zeppelin.rest.JsonResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.web.bind.annotation.DeleteMapping;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.PutMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;
import ru.tinkoff.zeppelin.core.configuration.interpreter.ModuleInnerConfiguration;
import ru.tinkoff.zeppelin.core.configuration.interpreter.ModuleSource;
import ru.tinkoff.zeppelin.core.configuration.interpreter.ModuleSource.Status;
import ru.tinkoff.zeppelin.core.configuration.interpreter.ModuleConfiguration;
import ru.tinkoff.zeppelin.core.configuration.interpreter.InterpreterProperty;
import ru.tinkoff.zeppelin.engine.ModuleSettingService;
import ru.tinkoff.zeppelin.engine.server.ModuleInstaller;


/**
 * Interpreter Rest API.
 */
@RestController
@RequestMapping("/api/interpreter")
public class InterpreterRestApi {

  private static final Logger logger = LoggerFactory.getLogger(InterpreterRestApi.class);

  private final ModuleSettingService moduleSettingService;

  @Autowired
  public InterpreterRestApi(final ModuleSettingService moduleSettingService) {
    this.moduleSettingService = moduleSettingService;
  }

  /**
   * List of dependency resolving repositories.
   */
  @GetMapping(value = "/repository", produces = "application/json")
  public ResponseEntity listRepositories() {
    try {
      return new JsonResponse<>(HttpStatus.OK, "", moduleSettingService.getAllRepositories()).build();
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
  @Transactional(propagation = Propagation.REQUIRES_NEW)
  @PostMapping(value = "/repository", produces = "application/json")
  public ResponseEntity addRepository(@RequestBody final String message) {
    try {
      final Repository request = Repository.fromJson(message);
      moduleSettingService.persistRepository(request);
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
  @Transactional(propagation = Propagation.REQUIRES_NEW)
  @DeleteMapping(value = "/repository/{repoId}", produces = "application/json")
  public ResponseEntity removeRepository(@PathVariable("repoId") final String repoId) {
    try {
      logger.info("Remove repository {}", repoId);
      moduleSettingService.deleteRepository(repoId);
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
  @GetMapping(value = "/source", produces = "application/json")
  public ResponseEntity listSources() {
    try {
      return new JsonResponse<>(HttpStatus.OK, "", moduleSettingService.getAllSources()).build();
    } catch (final Exception e) {
      logger.error("Exception in InterpreterRestApi while loading all sources ", e);
      return new JsonResponse<>(HttpStatus.INTERNAL_SERVER_ERROR, e.getMessage(),
              ExceptionUtils.getStackTrace(e)).build();
    }
  }

  /**
   * Install source.
   */
  @Transactional(propagation = Propagation.REQUIRES_NEW)
  @PostMapping(value = "/source/install/{interpreterName}", produces = "application/json")
  public ResponseEntity installSource(@PathVariable("interpreterName") final String interpreterName) {
    try {
      final List<ModuleSource> sources = moduleSettingService.getAllSources();
      final ModuleSource source = sources.stream()
              .filter(s -> s.getName().equals(interpreterName))
              .findFirst()
              .orElse(null);

      Objects.requireNonNull(source);
      moduleSettingService.installSource(source, true, false);
      logger.info("Source {} installed", interpreterName);
      return new JsonResponse(HttpStatus.OK).build();
    } catch (final Exception e) {
      logger.error("Exception in InterpreterRestApi while installing new source ", e);
      return new JsonResponse<>(HttpStatus.INTERNAL_SERVER_ERROR, e.getMessage(),
              ExceptionUtils.getStackTrace(e)).build();
    }
  }

  /**
   * Uninstall source.
   */
  @Transactional(propagation = Propagation.REQUIRES_NEW)
  @PostMapping(value = "/source/uninstall/{interpreterName}", produces = "application/json")
  public ResponseEntity uninstallSource(@PathVariable("interpreterName") final String interpreterName) {
    try {
      final List<ModuleSource> sources = moduleSettingService.getAllSources();
      final ModuleSource source = sources.stream()
              .filter(s -> s.getName().equals(interpreterName))
              .findFirst()
              .orElse(null);

      Objects.requireNonNull(source);
      moduleSettingService.installSource(source, true, false);

      moduleSettingService.uninstallSource(source, false);
      logger.info("Source {} uninstalled", interpreterName);
      return new JsonResponse(HttpStatus.OK).build();
    } catch (final Exception e) {
      logger.error("Exception in InterpreterRestApi while uninstalling new source ", e);
      return new JsonResponse<>(HttpStatus.INTERNAL_SERVER_ERROR, e.getMessage(),
              ExceptionUtils.getStackTrace(e)).build();
    }
  }

  /**
   * Reinstall source.
   */
  @Transactional(propagation = Propagation.REQUIRES_NEW)
  @PostMapping(value = "/source/reinstall/{interpreterName}", produces = "application/json")
  public ResponseEntity reinstallSource(@PathVariable("interpreterName") final String interpreterName) {
    try {
      final List<ModuleSource> sources = moduleSettingService.getAllSources();
      final ModuleSource source = sources.stream()
              .filter(s -> s.getName().equals(interpreterName))
              .findFirst()
              .orElse(null);

      Objects.requireNonNull(source);

      moduleSettingService.uninstallSource(source, false);
      moduleSettingService.installSource(source, true, false);
      logger.info("Source {} reinstalled", interpreterName);
      return new JsonResponse(HttpStatus.OK).build();
    } catch (final Exception e) {
      logger.error("Exception in InterpreterRestApi while uninstalling new source ", e);
      return new JsonResponse<>(HttpStatus.INTERNAL_SERVER_ERROR, e.getMessage(),
              ExceptionUtils.getStackTrace(e)).build();
    }
  }

  /**
   * Add new source.
   *
   * @param message ModuleSource
   */
  @Transactional(propagation = Propagation.REQUIRES_NEW)
  @PostMapping(value = "/source", produces = "application/json")
  public ResponseEntity addSource(@RequestBody final String message) {
    try {
      final ModuleSource source = new Gson().fromJson(message, ModuleSource.class);

      //source.setType(ModuleSource.Type.INTERPRETER);
      moduleSettingService.installSource(source, false, true);

      logger.info("New source {} added", source.getName());
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
  @Transactional(propagation = Propagation.REQUIRES_NEW)
  @DeleteMapping(value = "/source/{name}", produces = "application/json")
  public ResponseEntity removeSource(@PathVariable("name") final String interpreterName) {
    try {
      logger.info("Remove source {}", interpreterName);
      final List<ModuleSource> sources = moduleSettingService.getAllSources();
      final ModuleSource source = sources.stream()
              .filter(s -> s.getName().equals(interpreterName))
              .findFirst()
              .orElse(null);

      Objects.requireNonNull(source);

      moduleSettingService.uninstallSource(source, true);
      return new JsonResponse(HttpStatus.OK).build();
    } catch (final Exception e) {
      logger.error("Exception in InterpreterRestApi while deleting source ", e);
      return new JsonResponse<>(HttpStatus.INTERNAL_SERVER_ERROR, e.getMessage(),
              ExceptionUtils.getStackTrace(e)).build();
    }
  }


  /**
   * List all interpreter settings.
   */
  @GetMapping(value = "/setting", produces = "application/json")
  public ResponseEntity listSettings() {
    try {
      return new JsonResponse<>(HttpStatus.OK, "", moduleSettingService.getAllOptions()).build();
    } catch (final Exception e) {
      logger.error("Fail to get all interpreter setting", e);
      return new JsonResponse<>(HttpStatus.INTERNAL_SERVER_ERROR, e.getMessage(),
              ExceptionUtils.getStackTrace(e)).build();
    }
  }

  /**
   * Get a setting.
   */
  @GetMapping(value = "/setting/{shebang}", produces = "application/json")
  public ResponseEntity getSetting(@PathVariable("shebang") final String shebang) {
    try {
      final ModuleConfiguration setting = moduleSettingService.getOption(shebang);
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
  @Transactional(propagation = Propagation.REQUIRES_NEW)
  @PostMapping(value = "/setting", produces = "application/json")
  public ResponseEntity newSettings(@RequestBody final String message) {
    logger.info("Trying to add option via msg: {}", message);
    if (message == null) {
      return new JsonResponse(HttpStatus.BAD_REQUEST).build();
    }
    try {
      final ModuleConfiguration option = new Gson().fromJson(message, ModuleConfiguration.class);
      logger.info("Trying to add option: {}", option);
      moduleSettingService.persistModuleConfiguration(option);
      logger.info("New option {} added", option.getShebang());
      return new JsonResponse(HttpStatus.OK).build();
    } catch (final Exception e) {
      logger.error("Exception in InterpreterRestApi while creating option ", e);
      return new JsonResponse<>(HttpStatus.INTERNAL_SERVER_ERROR, e.getMessage(),
              ExceptionUtils.getStackTrace(e)).build();
    }
  }

  @Transactional(propagation = Propagation.REQUIRES_NEW)
  @PutMapping(value = "/setting/{shebang}", produces = "application/json")
  public ResponseEntity updateSetting(@RequestBody final String message, @PathVariable("shebang") final String shebang) {
    logger.info("Update interpreterSetting {}", shebang);
    if (message == null) {
      return new JsonResponse(HttpStatus.BAD_REQUEST).build();
    }
    try {
//      final InterpreterOption option = new Gson().fromJson(message, InterpreterOption.class);
//      interpreterSettingService.updateOption(option);
//      // restart with new settings
//      interpreterSettingService.restartInterpreter("%" + shebang);
//      logger.info("Option {} updated", option.getShebang());
      return new JsonResponse(HttpStatus.OK).build();
    } catch (final Exception e) {
      logger.error("Exception in InterpreterRestApi while creating option ", e);
      return new JsonResponse<>(HttpStatus.INTERNAL_SERVER_ERROR, e.getMessage(),
              ExceptionUtils.getStackTrace(e)).build();
    }
  }

  @Transactional(propagation = Propagation.REQUIRES_NEW)
  @PutMapping(value = "/source/{interpreterName}", produces = "application/json")
  public ResponseEntity updateSource(@RequestBody final String message,
                                     @PathVariable("interpreterName") final String interpreterName) {
    logger.info("Update source {}", interpreterName);
    if (message == null) {
      return new JsonResponse(HttpStatus.BAD_REQUEST).build();
    }
    try {
      final ModuleSource src = new Gson().fromJson(message, ModuleSource.class);

      final List<ModuleSource> sources = moduleSettingService.getAllSources();
      final ModuleSource source = sources.stream()
              .filter(s -> s.getName().equals(interpreterName))
              .findFirst()
              .orElse(null);

      Objects.requireNonNull(source);

      moduleSettingService.uninstallSource(source, false);
      moduleSettingService.updateSource(src);
      moduleSettingService.installSource(source, true, false);

      logger.info("source - {}", src);
      moduleSettingService.updateSource(src);
      logger.info("Source {} updated", src.getName());
      return new JsonResponse(HttpStatus.OK).build();
    } catch (final Exception e) {
      logger.error("Exception in InterpreterRestApi while creating option ", e);
      return new JsonResponse<>(HttpStatus.INTERNAL_SERVER_ERROR, e.getMessage(),
              ExceptionUtils.getStackTrace(e)).build();
    }
  }

  /**
   * Remove interpreter setting.
   */
  @Transactional(propagation = Propagation.REQUIRES_NEW)
  @DeleteMapping(value = "/setting/{shebang}", produces = "application/json")
  public ResponseEntity removeSetting(@PathVariable("shebang") final String shebang) {
    try {
      logger.info("Remove interpreterSetting {}", "%" + shebang);
      moduleSettingService.removeOption("%" + shebang);
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
  @Transactional(propagation = Propagation.REQUIRES_NEW)
  @PutMapping(value = "/setting/restart/{settingId}", produces = "application/json")
  public ResponseEntity restartSetting(@PathVariable("settingId") final String shebang) {
    logger.info("Restart interpreterSetting {}", shebang);

    moduleSettingService.restartInterpreter("%" + shebang);
    return new JsonResponse<>(HttpStatus.OK, "").build();
  }

  /**
   * List all available interpreters by group.
   */
  @GetMapping(produces = "application/json")
  public ResponseEntity listInterpreter() {
    try {
      final Map<String, ModuleInnerConfiguration> m = new HashMap<>();
      final List<ModuleSource> sources = moduleSettingService.getAllSources();
      for (final ModuleSource source: sources) {
        if (source.getStatus().equals(Status.INSTALLED)) {
          final List<ModuleInnerConfiguration> configList =
              ModuleInstaller.getDefaultConfig(source.getName());
          if (!configList.isEmpty()) {
            m.put(source.getName(), configList.get(0));
          }
        }
      }
      return new JsonResponse<>(HttpStatus.OK, "", m).build();
    } catch (final Exception e) {
      logger.error("Exception in InterpreterRestApi while loading all options ", e);
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
