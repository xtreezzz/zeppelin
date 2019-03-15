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
import org.apache.commons.lang3.StringUtils;
import org.apache.zeppelin.notebook.Note;
import org.apache.zeppelin.notebook.Paragraph;
import org.apache.zeppelin.storage.DatabaseNoteRepository;
import org.apache.zeppelin.server.JsonResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

/**
 * Helium Rest Api.
 */
@RestController
@RequestMapping("/api/helium")
public class HeliumRestApi {
  Logger logger = LoggerFactory.getLogger(HeliumRestApi.class);

  //private final Helium helium;
  private final Gson gson = new Gson();
  private final DatabaseNoteRepository noteRepository;

  @Autowired
  public HeliumRestApi(//final Helium helium,
      DatabaseNoteRepository noteRepository) {
    //this.helium = helium;
    this.noteRepository = noteRepository;
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
      //return new JsonResponse(HttpStatus.OK, "", helium.getAllEnabledPackages()).build();
      return null;
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
    final Note note = noteRepository.getNote(noteId);
    if (note == null) {
      return new JsonResponse(HttpStatus.NOT_FOUND, "Note " + noteId + " not found").build();
    }

    final Paragraph paragraph = note.getParagraph(paragraphId);
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
    final Note note = noteRepository.getNote(noteId);
    if (note == null) {
      return new JsonResponse(HttpStatus.NOT_FOUND, "Note " + noteId + " not found").build();
    }

    final Paragraph paragraph = note.getParagraph(paragraphId);
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
  public ResponseEntity bundleLoad(@RequestParam("refresh") final String refresh,
                             @PathVariable("packageName") final String packageName) {
    if (StringUtils.isEmpty(packageName)) {
      return new JsonResponse(
              HttpStatus.BAD_REQUEST,
              "Can't get bundle due to empty package name").build();
    }

    /*HeliumPackageSearchResult psr = null;
    final List<HeliumPackageSearchResult> enabledPackages = helium.getAllEnabledPackages();
    for (final HeliumPackageSearchResult e : enabledPackages) {
      if (e.getPkg().getName().equals(packageName)) {
        psr = e;
        break;
      }
    }

    if (psr == null) {
      // return empty to specify
      return new JsonResponse(HttpStatus.OK).build();
    }

    try {
      final File bundle;
      final boolean rebuild = (refresh != null && refresh.equals("true"));
      bundle = helium.getBundle(psr.getPkg(), rebuild);

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
    */
    return null;
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
