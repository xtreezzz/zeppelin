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

package org.apache.zeppelin.service;

import com.google.common.collect.Lists;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.stream.Collectors;

import jline.internal.Preconditions;
import org.apache.commons.io.FileUtils;
import org.apache.zeppelin.conf.ZeppelinConfiguration;
import org.apache.zeppelin.dep.DependencyResolver;
import org.apache.zeppelin.interpreter.InterpreterNotFoundException;
import org.apache.zeppelin.interpreter.InterpreterSettingManager;
import org.apache.zeppelin.interpreter.ManagedInterpreterGroup;
import org.apache.zeppelin.notebook.Note;
import org.apache.zeppelin.notebook.Notebook;
import org.apache.zeppelin.notebook.Paragraph;
import org.apache.zeppelin.rest.message.InterpreterInstallationRequest;
import org.eclipse.jetty.util.annotation.ManagedOperation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.sonatype.aether.RepositoryException;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

/**
 * This class handles all of business logic for {@link org.apache.zeppelin.rest.InterpreterRestApi}
 */
@Component
public class InterpreterService {

  private static final String ZEPPELIN_ARTIFACT_PREFIX = "zeppelin-";
  private static final Logger LOG = LoggerFactory.getLogger(InterpreterService.class);
  private static final ExecutorService executorService =
      Executors.newSingleThreadExecutor(
          new ThreadFactoryBuilder()
              .setNameFormat(InterpreterService.class.getSimpleName() + "-")
              .build());

  private final ZeppelinConfiguration conf;
  private final InterpreterSettingManager interpreterSettingManager;
  private final Notebook notebook;

  @Autowired
  public InterpreterService(final ZeppelinConfiguration conf,
                            final InterpreterSettingManager interpreterSettingManager,
                            final Notebook notebook) {
    this.conf = conf;
    this.interpreterSettingManager = interpreterSettingManager;
    this.notebook = notebook;
  }

  public void installInterpreter(
      final InterpreterInstallationRequest request, final ServiceCallback serviceCallback)
      throws Exception {
    Preconditions.checkNotNull(request);
    final String interpreterName = request.getName();
    Preconditions.checkNotNull(interpreterName);
    Preconditions.checkNotNull(request.getArtifact());

    final String interpreterBaseDir = conf.getInterpreterDir();
    final String localRepoPath = conf.getInterpreterLocalRepoPath();

    final DependencyResolver dependencyResolver = new DependencyResolver(localRepoPath);

    final String proxyUrl = conf.getZeppelinProxyUrl();
    if (null != proxyUrl) {
      final String proxyUser = conf.getZeppelinProxyUser();
      final String proxyPassword = conf.getZeppelinProxyPassword();
      try {
        dependencyResolver.setProxy(new URL(proxyUrl), proxyUser, proxyPassword);
      } catch (final MalformedURLException e) {
        // TODO(jl): Not sure if it's good to raise an exception
        throw new Exception("Url is not valid format", e);
      }
    }

    // TODO(jl): Make a rule between an interpreter name and an installation directory
    final List<String> possibleInterpreterDirectories = Lists.newArrayList();
    possibleInterpreterDirectories.add(interpreterName);
    if (interpreterName.startsWith(ZEPPELIN_ARTIFACT_PREFIX)) {
      possibleInterpreterDirectories.add(interpreterName.replace(ZEPPELIN_ARTIFACT_PREFIX, ""));
    } else {
      possibleInterpreterDirectories.add(ZEPPELIN_ARTIFACT_PREFIX + interpreterName);
    }

    for (final String pn : possibleInterpreterDirectories) {
      final Path testInterpreterDir = Paths.get(interpreterBaseDir, pn);
      if (Files.exists(testInterpreterDir)) {
        throw new Exception("Interpreter " + interpreterName + " already exists with " + pn);
      }
    }

    final Path interpreterDir = Paths.get(interpreterBaseDir, interpreterName);

    try {
      Files.createDirectories(interpreterDir);
    } catch (final Exception e) {
      throw new Exception("Cannot create " + interpreterDir.toString());
    }

    // It might take time to finish it
    executorService.execute(
        new Runnable() {
          @Override
          public void run() {
            downloadInterpreter(request, dependencyResolver, interpreterDir, serviceCallback);
          }
        });
  }

  void downloadInterpreter(
          final InterpreterInstallationRequest request,
          final DependencyResolver dependencyResolver,
          final Path interpreterDir,
          final ServiceCallback<String> serviceCallback) {
    try {
      LOG.info("Start to download a dependency: {}", request.getName());
      if (null != serviceCallback) {
        serviceCallback.onStart("Starting to download " + request.getName() + " interpreter", null);
      }

      dependencyResolver.load(request.getArtifact(), interpreterDir.toFile());
      interpreterSettingManager.refreshInterpreterTemplates();
      LOG.info(
          "Finish downloading a dependency {} into {}",
          request.getName(),
          interpreterDir.toString());
      if (null != serviceCallback) {
        serviceCallback.onSuccess(request.getName() + " downloaded", null);
      }
    } catch (final RepositoryException | IOException e) {
      LOG.error("Error while downloading dependencies", e);
      try {
        FileUtils.deleteDirectory(interpreterDir.toFile());
      } catch (final IOException e1) {
        LOG.error(
            "Error while removing directory. You should handle it manually: {}",
            interpreterDir.toString(),
            e1);
      }
      if (null != serviceCallback) {
        try {
          serviceCallback.onFailure(
              new Exception("Error while downloading " + request.getName() + " as " +
                  e.getMessage()), null);
        } catch (final IOException e1) {
          LOG.error("ServiceCallback failure", e1);
        }
      }
    }
  }

  /**
   * Extract info about running interpreters with additional paragraph info.
   */
  public List<Map<String, String>> getRunningInterpretersParagraphInfo() {
    return notebook.getAllNotes().stream()
            .map(Note::getParagraphs)
            .flatMap(Collection::stream)
            .filter(Paragraph::isRunning)
            .map(this::extractParagraphInfo)
            .collect(Collectors.toList());
  }

  /**
   * Extract paragraph's interpreter info with paragraph data.
   */
  private Map<String, String> extractParagraphInfo(final Paragraph paragraph) {
    try {
      final ManagedInterpreterGroup process = (ManagedInterpreterGroup) paragraph
              .getBindedInterpreter().getInterpreterGroup();
      // add all info about binded interpreter
      final Map<String, String> info = notebook.getInterpreterSettingManager().extractProcessInfo(process);
      // add paragraph info
      info.put("interpreterText", paragraph.getIntpText());
      info.put("noteName", paragraph.getNote().getName());
      info.put("noteId", paragraph.getNote().getId());
      info.put("id", paragraph.getId());
      info.put("user", paragraph.getUser());
      return info;
    } catch (InterpreterNotFoundException e) {
      LOG.error("Failed to get binded interpreter for paragraph {}", paragraph, e);
      return new HashMap<>();
    }
  }
}
