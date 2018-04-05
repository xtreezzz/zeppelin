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

package org.apache.zeppelin.plugin;

import org.apache.zeppelin.conf.ZeppelinConfiguration;
import org.apache.zeppelin.dep.Dependency;
import org.apache.zeppelin.dep.DependencyResolver;
import org.apache.zeppelin.metadata.MetadataGenerator;
import org.apache.zeppelin.metadata.MetadataGeneratorSetting;
import org.apache.zeppelin.notebook.repo.NotebookRepo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.sonatype.aether.RepositoryException;

import java.io.File;
import java.io.IOException;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.*;

/**
 * Class for loading Plugins
 */
public class PluginManager {
  private static final Logger LOGGER = LoggerFactory.getLogger(PluginManager.class);

  private static PluginManager instance;

  private ZeppelinConfiguration zConf = ZeppelinConfiguration.create();
  private String pluginsDir = zConf.getPluginsDir();
  private String metadataLocalRepoDir = zConf.getMetadataLocalRepoDir();

  public static synchronized PluginManager get() {
    if (instance == null) {
      instance = new PluginManager();
    }
    return instance;
  }

  public NotebookRepo loadNotebookRepo(String notebookRepoClassName) throws IOException {
    LOGGER.info("Loading NotebookRepo Plugin: " + notebookRepoClassName);
    // load plugin from classpath directly when it is test.
    // otherwise load it from plugin folder
    String isTest = System.getenv("IS_ZEPPELIN_TEST");
    if (isTest != null && isTest.equals("true")) {
      try {
        NotebookRepo notebookRepo = (NotebookRepo)
            (Class.forName(notebookRepoClassName).newInstance());
        return notebookRepo;
      } catch (InstantiationException | IllegalAccessException | ClassNotFoundException e) {
        LOGGER.warn("Fail to instantiate notebookrepo from classpath directly:"
            + notebookRepoClassName, e);
      }
    }

    String simpleClassName = notebookRepoClassName.substring(notebookRepoClassName.lastIndexOf(".")
        + 1);
    File pluginFolder = new File(pluginsDir + "/NotebookRepo/" + simpleClassName);
    if (!pluginFolder.exists() || pluginFolder.isFile()) {
      LOGGER.warn("pluginFolder " + pluginFolder.getAbsolutePath() +
          " doesn't exist or is not a directory");
      return null;
    }
    List<URL> urls = new ArrayList<>();
    for (File file : pluginFolder.listFiles()) {
      LOGGER.debug("Add file " + file.getAbsolutePath() + " to classpath of plugin "
          + notebookRepoClassName);
      urls.add(file.toURI().toURL());
    }
    if (urls.isEmpty()) {
      LOGGER.warn("Can not load plugin " + notebookRepoClassName +
          ", because the plugin folder " + pluginFolder + " is empty.");
      return null;
    }
    URLClassLoader classLoader = new URLClassLoader(urls.toArray(new URL[0]));
    Iterator<NotebookRepo> iter = ServiceLoader.load(NotebookRepo.class, classLoader).iterator();
    NotebookRepo notebookRepo = iter.next();
    if (notebookRepo == null) {
      LOGGER.warn("Unable to load NotebookRepo Plugin: " + notebookRepoClassName);
    }
    return notebookRepo;
  }

  public Map<String, MetadataGenerator> loadMetadataGenerators(
      List<MetadataGeneratorSetting> metadataGeneratorsSettingList,
      DependencyResolver dependencyResolver) throws IOException {
    Map<String, MetadataGenerator> metadataGenerators = new HashMap<>();

    for (MetadataGeneratorSetting metadataGeneratorSetting : metadataGeneratorsSettingList) {
      try {
        List<URL> urls = new ArrayList<>();
        List<Dependency> deps = metadataGeneratorSetting.getDependencies();
        if (deps != null) {
          for (Dependency dep : deps) {
            File destDir = new File(metadataLocalRepoDir);
            if (dep.getExclusions() != null) {
              dependencyResolver.load(dep.getGroupArtifactVersion(), dep.getExclusions(),
                  new File(destDir, metadataGeneratorSetting.getId()));
            } else {
              dependencyResolver.load(dep.getGroupArtifactVersion(), new File(destDir,
                  metadataGeneratorSetting.getId()));
            }
          }

          File libs = new File(metadataLocalRepoDir, metadataGeneratorSetting.getId());
          if (libs.exists() && libs.isDirectory()) {
            for (File file : libs.listFiles()) {
              urls.add(file.toURI().toURL());
            }
          }
        }

        File pluginFolder = new File(pluginsDir + "/MetadataGenerator/"
            + metadataGeneratorSetting.getClassName());
        if (!pluginFolder.exists() || pluginFolder.isFile()) {
          LOGGER.warn("pluginFolder " + pluginFolder.getAbsolutePath()
              + " doesn't exist or is not a directory");
          return null;
        }


        for (File file : pluginFolder.listFiles()) {
          LOGGER.debug("Add file " + file.getAbsolutePath()
              + " to classpath of plugin " + metadataGeneratorSetting.getClassName());
          urls.add(file.toURI().toURL());
        }
        if (urls.isEmpty()) {
          LOGGER.warn("Can not load plugin " + metadataGeneratorSetting.getClassName()
              + ", because the plugin folder " + pluginFolder + " is empty.");
          return null;
        }

        // dependencies
        metadataGeneratorSetting.getDependencies();

        URLClassLoader classLoader = new URLClassLoader(urls.toArray(new URL[0]));
        Iterator<MetadataGenerator> iter = ServiceLoader.load(MetadataGenerator.class,
            classLoader).iterator();
        MetadataGenerator metadataGenerator = iter.next();
        if (metadataGenerator == null) {
          LOGGER.warn("Unable to load MetadataGenerator Plugin: "
              + metadataGeneratorSetting.getClassName());
        }
        metadataGenerator.init(zConf, metadataGeneratorSetting);
        metadataGenerators.put(metadataGeneratorSetting.getId(), metadataGenerator);
      } catch (RepositoryException e) {
        LOGGER.error("plugin dependencies exception", e);
      }
    }
    return metadataGenerators;
  }

}
