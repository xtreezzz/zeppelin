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

package org.apache.zeppelin.repo;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.apache.commons.lang.StringUtils;
import org.apache.zeppelin.conf.ZeppelinConfiguration;
import org.apache.zeppelin.repo.api.NotebookRepo;
import org.apache.zeppelin.repo.api.NotebookRepoWithVersionControl;
import org.reflections.Reflections;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import javax.annotation.PostConstruct;
import java.io.File;
import java.io.IOException;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.*;

@Component
public class ZeppelinRepository {

  private static final Logger LOG = LoggerFactory.getLogger(ZeppelinRepository.class);

  private static final String defaultStorage = "org.apache.zeppelin.notebook.repo.GitNotebookRepo";

  private final ZeppelinConfiguration conf;
  private NotebookRepo repo;


  @Autowired
  public ZeppelinRepository(final ZeppelinConfiguration conf) {
    this.conf = conf;
  }

  @PostConstruct
  public void initRepository() {
    LOG.info("Start loading repository implementations");
    final File repoFolder = new File("repositories/");

    final File[] directories = repoFolder.listFiles(File::isDirectory);
    if (directories == null || directories.length == 0) {
      throw new IllegalStateException("Can't load repository implementation from " + repoFolder.getAbsolutePath());
    }

    LOG.info("Found repositories folders: {}", Arrays.asList(directories));

    // load any jars in folder into separate classloader
    final List<ClassLoader> repoClassLoaders = Lists.newArrayList();
    for (final File dir : directories) {
      try {
        final List<URL> urls = Lists.newArrayList();
        for (final File file : dir.listFiles()) {
          //TODO(KOT) Critical remove all zeppelin jars!!!!!
          LOG.debug("Add file " + file.getAbsolutePath() + " to classpath of plugin: " + dir.getName());
          urls.add(file.toURI().toURL());
        }

        if (urls.isEmpty()) {
          LOG.warn("Can not load plugin; folder " + dir.getName() + " is empty.");
          continue;
        }
        repoClassLoaders.add(new URLClassLoader(urls.toArray(new URL[0])));
      } catch (final Exception e) {
        LOG.error("Error while load files from {}", dir);
      }
    }

    final Map<ClassLoader, Set<Class<? extends NotebookRepo>>> repositoryLoadResults = Maps.newHashMap();
    for (final ClassLoader classLoader : repoClassLoaders) {
      final Reflections reflections = new Reflections(classLoader);
      final Set<Class<? extends NotebookRepo>> repoClasses = reflections.getSubTypesOf(NotebookRepo.class);

      LOG.info("Found repositories implementations: {}", repoClasses);
      repositoryLoadResults.put(classLoader, repoClasses);
    }

    LOG.info("Found repositories implementations: {}", repositoryLoadResults);

    final String storageClassName = StringUtils.isNotEmpty(conf.getNotebookStorageClass())
            ? conf.getNotebookStorageClass().trim()
            : defaultStorage;

    LOG.info("Search repository implementation: {}", storageClassName);

    ClassLoader resultClassLoader = null;
    for (final Map.Entry<ClassLoader, Set<Class<? extends NotebookRepo>>> result : repositoryLoadResults.entrySet()) {

      if (result.getValue().stream().anyMatch(aClass -> aClass.getName().equals(storageClassName))) {
        resultClassLoader = result.getKey();
        break;
      }
    }

    if (Objects.isNull(resultClassLoader)) {
      throw new IllegalStateException("Notebook repository not found!");
    }

    try {
      repo = (NotebookRepo) resultClassLoader.loadClass(storageClassName).newInstance();
      repo.init(conf);
    } catch (final IOException | InstantiationException | IllegalAccessException | ClassNotFoundException e) {
      LOG.error("Fail to instantiate notebookrepo from class:" + storageClassName, e);
      throw new IllegalStateException("Fail to instantiate notebookrepo from class:" + storageClassName, e);
    }
  }


  public boolean isRevisionSupported() {
    return repo instanceof NotebookRepoWithVersionControl;
  }

  public NotebookRepo get() {
    return repo;
  }

  public NotebookRepoWithVersionControl getAsVCS() {
    return (NotebookRepoWithVersionControl) repo;
  }
}
