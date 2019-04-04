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

import com.google.gson.Gson;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.zeppelin.NoteService;
import org.apache.zeppelin.helium.HeliumBundleFactory;
import org.apache.zeppelin.helium.V2.HeliumEnabledRegistries;
import org.apache.zeppelin.helium.V2.HeliumRegistry;
import org.apache.zeppelin.notebook.Note;
import org.apache.zeppelin.notebook.Paragraph;
import org.apache.zeppelin.server.JsonResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.io.File;

/**
 * Helium Rest Api.
 */
@RestController
@RequestMapping("/api/helium")
public class HeliumRestApi {
  Logger logger = LoggerFactory.getLogger(HeliumRestApi.class);

  //private final Helium helium;
  private final Gson gson = new Gson();
  private final NoteService noteService;


  @Autowired
  public HeliumRestApi(//final Helium helium,
      NoteService noteService) {
    //this.helium = helium;
    this.noteService = noteService;
  }

  /**
   * Get all packages info.
   */
  @GetMapping(value = "/package", produces = "application/json")
  public ResponseEntity getAllPackageInfo() {
    try {
      //return new JsonResponse(HttpStatus.OK, "", helium.getAllPackageInfo()).build();
      return null;
    } catch (final RuntimeException e) {
      logger.error(e.getMessage(), e);
      return new JsonResponse(HttpStatus.INTERNAL_SERVER_ERROR, e.getMessage()).build();
    }
  }

  /**
   * Get all enabled packages info.
   */
  @GetMapping(value = "/enabledPackage", produces = "application/json")
  public ResponseEntity getAllEnabledPackageInfo() {
    try {
      return new JsonResponse(HttpStatus.OK, "", HeliumEnabledRegistries.getRegistries()).build();
    } catch (final RuntimeException e) {
      logger.error(e.getMessage(), e);
      return new JsonResponse(HttpStatus.INTERNAL_SERVER_ERROR, e.getMessage()).build();
    }
  }

  /**
   * Get single package info.
   */
  @GetMapping(value = "/package/{packageName}", produces = "application/json")
  public ResponseEntity getSinglePackageInfo(@PathVariable("packageName") final String packageName) {
    if (StringUtils.isEmpty(packageName)) {
      return new JsonResponse(HttpStatus.BAD_REQUEST,
              "Can't get package info for empty name").build();
    }

    try {
      //return new JsonResponse(HttpStatus.OK, "", helium.getSinglePackageInfo(packageName)).build();
      return null;
    } catch (final RuntimeException e) {
      logger.error(e.getMessage(), e);
      return new JsonResponse(HttpStatus.INTERNAL_SERVER_ERROR, e.getMessage()).build();
    }
  }

  @GetMapping(value = "/suggest/{noteId}/{paragraphId}", produces = "application/json")
  public ResponseEntity suggest(@PathVariable("noteId") final String noteId,
                          @PathVariable("paragraphId") final String paragraphId) {
    final Note note = noteService.getNote(noteId);
    if (note == null) {
      return new JsonResponse(HttpStatus.NOT_FOUND, "Note " + noteId + " not found").build();
    }

    final Paragraph paragraph = null;//note.getParagraph(paragraphId);
    if (paragraph == null) {
      return new JsonResponse(HttpStatus.NOT_FOUND, "Paragraph " + paragraphId + " not found")
              .build();
    }
    try {
      //return new JsonResponse(HttpStatus.OK, "", helium.suggestApp(paragraph)).build();
      return null;
    } catch (final RuntimeException e) {
      logger.error(e.getMessage(), e);
      return new JsonResponse(HttpStatus.INTERNAL_SERVER_ERROR, e.getMessage()).build();
    }

  }

  @PostMapping(value = "/load/{noteId}/{paragraphId}", produces = "application/json")
  public ResponseEntity load(@PathVariable("noteId") final String noteId,
                             @PathVariable("paragraphId") final String paragraphId, final String heliumPackage) {
    final Note note = noteService.getNote(noteId);
    if (note == null) {
      return new JsonResponse(HttpStatus.NOT_FOUND, "Note " + noteId + " not found").build();
    }

    final Paragraph paragraph = null;//note.getParagraph(paragraphId);
    if (paragraph == null) {
      return new JsonResponse(HttpStatus.NOT_FOUND, "Paragraph " + paragraphId + " not found")
              .build();
    }
    //final HeliumPackage pkg = HeliumPackage.fromJson(heliumPackage);
    try {
      //return new JsonResponse(HttpStatus.OK, "",
      //        helium.getApplicationFactory().loadAndRun(pkg, paragraph)).build();
      return null;
    } catch (final RuntimeException e) {
      logger.error(e.getMessage(), e);
      return new JsonResponse(HttpStatus.INTERNAL_SERVER_ERROR, e.getMessage()).build();
    }
  }

  @GetMapping(value = "/bundle/load/{packageName}", produces = "text/javascript")
  public ResponseEntity bundleLoad(@RequestParam(name = "refresh", required= false, defaultValue = "false") final String refresh,
                             @PathVariable("packageName") final String packageName) {
    if (StringUtils.isEmpty(packageName)) {
      return new JsonResponse(
              HttpStatus.BAD_REQUEST,
              "Can't get bundle due to empty package name").build();
    }


    try {
      final File bundle;
      final boolean rebuild = (refresh != null && refresh.equals("true"));
      final HeliumBundleFactory bundleFactory = new HeliumBundleFactory();

      final HeliumRegistry.HeliumPackage heliumPackage = HeliumEnabledRegistries.getRegistries()
              .stream()
              .filter(registry -> registry.getPkg().getName().equals(packageName))
              .map(registry -> registry.getPkg())
              .findFirst()
              .orElse(null);

      if(heliumPackage == null) {
        return new JsonResponse(
                HttpStatus.BAD_REQUEST,
                "Can't get bundle due to empty package name").build();
      }

      bundle = bundleFactory.buildPackage(heliumPackage, rebuild, true);

      if (bundle == null) {
        return new JsonResponse(HttpStatus.OK).build();
      } else {
        final String stringified = FileUtils.readFileToString(bundle);
        return new JsonResponse(HttpStatus.OK, stringified).build();
      }

    } catch (final Exception e) {
      logger.error(e.getMessage(), e);
      // returning error will prevent zeppelin front-end render any notebook.
      // visualization load fail doesn't need to block notebook rendering work.
      // so it's better return ok instead of any error.
      return new JsonResponse(HttpStatus.OK, "ERROR: " + e.getMessage()).build();
    }
  }

  @PostMapping(value = "/enable/{packageName}", produces = "application/json")
  public ResponseEntity enablePackage(@PathVariable("packageName") final String packageName, final String artifact) {
    /*
    try {
      if (helium.enable(packageName, artifact)) {
        return new JsonResponse(HttpStatus.OK).build();
      } else {
        return new JsonResponse(HttpStatus.NOT_FOUND).build();
      }
    } catch (final IOException e) {
      logger.error(e.getMessage(), e);
      return new JsonResponse(HttpStatus.INTERNAL_SERVER_ERROR, e.getMessage()).build();
    }
    */
    return null;
  }

  @PostMapping(value = "/disable/{packageName}", produces = "application/json")
  public ResponseEntity disablePackage(@PathVariable("packageName") final String packageName) {
    /*
    try {
      if (helium.disable(packageName)) {
        return new JsonResponse(HttpStatus.OK).build();
      } else {
        return new JsonResponse(HttpStatus.NOT_FOUND).build();
      }
    } catch (final IOException e) {
      logger.error(e.getMessage(), e);
      return new JsonResponse(HttpStatus.INTERNAL_SERVER_ERROR, e.getMessage()).build();
    }
    */
    return null;
  }

  @GetMapping(value = "/spell/config/{packageName}", produces = "application/json")
  public ResponseEntity getSpellConfigUsingMagic(@PathVariable("packageName") final String packageName) {
    if (StringUtils.isEmpty(packageName)) {
      return new JsonResponse(HttpStatus.BAD_REQUEST, "packageName is empty").build();
    }

    /*
    try {
      final Map<String, Map<String, Object>> config =
              helium.getSpellConfig(packageName);

      if (config == null) {
        return new JsonResponse(HttpStatus.BAD_REQUEST,
                "Failed to find enabled package for " + packageName).build();
      }

      return new JsonResponse(HttpStatus.OK, config).build();
    } catch (final RuntimeException e) {
      logger.error(e.getMessage(), e);
      return new JsonResponse(HttpStatus.INTERNAL_SERVER_ERROR, e.getMessage()).build();
    }
    */
    return null;
  }

  @GetMapping(value = "/config", produces = "application/json")
  public ResponseEntity getAllPackageConfigs() {
    /*
    try {
      final Map<String, Map<String, Object>> config = helium.getAllPackageConfig();
      return new JsonResponse(HttpStatus.OK, config).build();
    } catch (final RuntimeException e) {
      logger.error(e.getMessage(), e);
      return new JsonResponse(HttpStatus.INTERNAL_SERVER_ERROR, e.getMessage()).build();
    }
    */
    return null;
  }

  @GetMapping(value = "/config/{packageName}/{artifact}", produces = "application/json")
  public ResponseEntity getPackageConfig(@PathVariable("packageName") final String packageName,
                                   @PathVariable("artifact") final String artifact) {
    if (StringUtils.isEmpty(packageName) || StringUtils.isEmpty(artifact)) {
      return new JsonResponse(HttpStatus.BAD_REQUEST,
              "package name or artifact is empty"
      ).build();
    }

    /*
    try {
      final Map<String, Map<String, Object>> config =
              helium.getPackageConfig(packageName, artifact);

      if (config == null) {
        return new JsonResponse(HttpStatus.BAD_REQUEST,
                "Failed to find package for " + artifact).build();
      }

      return new JsonResponse(HttpStatus.OK, config).build();
    } catch (final RuntimeException e) {
      logger.error(e.getMessage(), e);
      return new JsonResponse(HttpStatus.INTERNAL_SERVER_ERROR, e.getMessage()).build();
    }
    */
    return null;
  }

  @PostMapping(value = "/config/{packageName}/{artifact}", produces = "application/json")
  public ResponseEntity updatePackageConfig(@PathVariable("packageName") final String packageName,
                                            @PathVariable("artifact") final String artifact, final String rawConfig) {
    if (StringUtils.isEmpty(packageName) || StringUtils.isEmpty(artifact)) {
      return new JsonResponse(HttpStatus.BAD_REQUEST,
              "package name or artifact is empty"
      ).build();
    }

    /*
    try {
      final Map<String, Object> packageConfig
              = gson.fromJson(rawConfig, new TypeToken<Map<String, Object>>() {}.getType());
      helium.updatePackageConfig(artifact, packageConfig);
      return new JsonResponse(HttpStatus.OK, packageConfig).build();
    } catch (final JsonParseException e) {
      logger.error(e.getMessage(), e);
      return new JsonResponse(HttpStatus.BAD_REQUEST, e.getMessage()).build();
    } catch (final IOException | RuntimeException e) {
      return new JsonResponse(HttpStatus.INTERNAL_SERVER_ERROR, e.getMessage()).build();
    }
    */
    return null;
  }

  @GetMapping(value = "/order/visualization", produces = "application/json")
  public ResponseEntity getVisualizationPackageOrder() {
    /*
    try {
      final List<String> order = helium.getVisualizationPackageOrder();
      return new JsonResponse(HttpStatus.OK, order).build();
    } catch (final RuntimeException e) {
      logger.error(e.getMessage(), e);
      return new JsonResponse(HttpStatus.INTERNAL_SERVER_ERROR, e.getMessage()).build();
    }
    */
    return null;
  }

  @PostMapping(value = "/order/visualization", produces = "application/json")
  public ResponseEntity setVisualizationPackageOrder(final String orderedPackageNameList) {
    /*
    final List<String> orderedList = gson.fromJson(
            orderedPackageNameList, new TypeToken<List<String>>() {
            }.getType());
    try {
      helium.setVisualizationPackageOrder(orderedList);
      return new JsonResponse(HttpStatus.OK).build();
    } catch (final IOException e) {
      logger.error(e.getMessage(), e);
      return new JsonResponse(HttpStatus.INTERNAL_SERVER_ERROR, e.getMessage()).build();
    }
    */
    return null;
  }
}
