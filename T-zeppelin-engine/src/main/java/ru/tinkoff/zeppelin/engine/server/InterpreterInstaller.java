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
package ru.tinkoff.zeppelin.engine.server;

import com.google.common.collect.Lists;
import com.google.gson.Gson;
import java.io.File;
import java.io.IOException;
import java.lang.reflect.Type;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;
import org.apache.zeppelin.DependencyResolver;
import org.apache.zeppelin.Repository;
import org.apache.zeppelin.storage.ZLog;
import org.apache.zeppelin.storage.ZLog.ET;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import ru.tinkoff.zeppelin.core.configuration.interpreter.BaseInterpreterConfig;


/**
 * Class for installing interpreters
 *
 * @author Andrey Koshkin
 * @version 1.0
 * @since 1.0
 */
public class InterpreterInstaller {

  private static final Logger LOG = LoggerFactory.getLogger(InterpreterInstaller.class);
  private static final String DESTINATION_FOLDER = "interpreters/";

  private InterpreterInstaller() {
    // not called.
  }

  public static boolean isInstalled(final String name) {
    final File folderToStore = new File(DESTINATION_FOLDER + name + "/");
    return folderToStore.exists() && Objects.requireNonNull(folderToStore.list()).length > 0;
  }

  public static String install(final String name, final String artifact, final List<Repository> repositories) {
    ZLog.log(ET.INTERPRETER_INSTALL,
        String.format("Attempt for install interpreter with artifact: %s", artifact),
        String.format("Installation called for interpreter with name: %s, artifact: %s, using repos: %s", name, artifact, repositories.toString()),
        "Unknown");
    if (isInstalled(name)) {
      final String path = getDirectory(name);
      ZLog.log(ET.INTERPRETER_ALREADY_INSTALLED,
          String.format("Interpreter with artifact: %s is already installed", artifact),
          String.format("Interpreter sources with name: %s and artifact: %s, found in %s",
              name, artifact, path), "Unknown");
      return path;
    }

    final File folderToStore = new File(DESTINATION_FOLDER + name + "/");
    try {
      final DependencyResolver dependencyResolver = new DependencyResolver(repositories);
      dependencyResolver.load(artifact, folderToStore);
      ZLog.log(ET.INTERPRETER_SUCCESSFULLY_INSTALLED,
          String.format("Interpreter \"%s\" installed successfully", name),
          String.format("Interpreter \"%s\" installed by artifact %s to %s", name, artifact, folderToStore.getAbsolutePath()),
      "Unknown");
      return folderToStore.getAbsolutePath();
    } catch (final Exception e) {
      LOG.error("Error while install interpreter", e);
      uninstallInterpreter(name);
      ZLog.log(ET.INTERPRETER_INSTALLATION_FAILED,
          String.format("Exception thrown during interpreter installation, name: %s, destination folder %s would be deleted",
              name, folderToStore.getAbsolutePath()),
          String.format("Error occured during installation interpreter[name=%s;artifact=%s,destination folder=%s], error: %s",
              name, artifact, folderToStore.getAbsolutePath(), e.getMessage()),
          "Unknown");
      return "";
    }
  }

  public static void uninstallInterpreter(final String name) {
    final File folderToStore = new File(DESTINATION_FOLDER + name + "/");
    try {
      FileUtils.deleteDirectory(folderToStore);
      ZLog.log(ET.INTERPRETER_SUCCESSFULLY_UNINSTALLED,
          String.format("Interpreter[name=%s] sources deleted", name),
          String.format("Folder %s deleted, interpreter[name=%s] uninstalled",
              folderToStore.getAbsolutePath(), name), "Unknown");
    } catch (final Exception e) {
      LOG.error("Error while remove interpreter", e);
      ZLog.log(ET.INTERPRETER_DELETION_FAILED,
          String.format("Failed to uninstall interpreter[name=%s]", name),
          String.format("Error occured during folder deletion[path=%s], error: %s", folderToStore.getAbsolutePath(),
              e.getMessage()), "Unknown");
    }
  }

  public static List<BaseInterpreterConfig> getDefaultConfig(final String name) {
    final File folderToStore = new File(DESTINATION_FOLDER + name + "/");
    ZLog.log(ET.INTERPRETER_CONFIGURATION_REQUESTED,
        String.format("Requested for interpreter[name:%s] configuration", name),
        String.format("Requested for \"interpreter-setting.json\" in %s", folderToStore.getAbsolutePath()),
        "Unknown");

    URLClassLoader classLoader = null;
    try {
      final List<URL> urls = Lists.newArrayList();
      for (final File file : folderToStore.listFiles()) {
        final URL url = file.toURI().toURL();

        urls.add(new URL("jar:" + url.toString() + "!/"));
      }

      classLoader = URLClassLoader.newInstance(urls.toArray(new URL[0]));
      final String config = IOUtils.toString(classLoader.getResourceAsStream("interpreter-setting.json"), "UTF-8");
      final List<BaseInterpreterConfig> result = Arrays.asList(new Gson().fromJson(config, (Type) BaseInterpreterConfig[].class));
      ZLog.log(ET.INTERPRETER_CONFIGURAION_FOUND,
          String.format("Configuration for interpreter %s successfully found", name),
          String.format("\"interpreter-setting.json\" for interpreter[%s] in %s successfully parsed",
              name, folderToStore.getAbsolutePath()), "Unknown");
      return result;
    } catch (final Exception e) {
      ZLog.log(ET.INTERPRETER_CONFIGURATION_PROCESSING_FAILED,
          String.format("Failed to get configuration for interpreter[name=%s]", name),
          String.format("Error occurred during processing interpreter[name=%s, path=%s] configuration, error: %s",
              name, folderToStore.getAbsolutePath(), e.getMessage()), "Unknown");
      throw new IllegalArgumentException("Wrong config format", e);
    } finally {
      if (classLoader != null) {
        try {
          classLoader.close();
        } catch (final IOException e) {
          LOG.error("Failed to process config", e);
        }
      }
    }
  }

  public static String getDirectory(final String name) {
      final File folderToStore = new File(DESTINATION_FOLDER + name + "/");
      return folderToStore.getAbsolutePath();
  }
}
