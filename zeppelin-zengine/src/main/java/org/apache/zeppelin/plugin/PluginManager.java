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

import com.google.common.annotations.VisibleForTesting;
import org.apache.zeppelin.configuration.ZeppelinConfiguration;
import org.apache.zeppelin.interpreter.launcher.InterpreterLauncher;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Class for loading Plugins
 */
public class PluginManager {
  private static final Logger LOGGER = LoggerFactory.getLogger(PluginManager.class);

  private static PluginManager instance;

  private ZeppelinConfiguration zConf = ZeppelinConfiguration.create();
  private String pluginsDir = zConf.getPluginsDir();

  private Map<String, InterpreterLauncher> cachedLaunchers = new HashMap<>();

  public static synchronized PluginManager get() {
    if (instance == null) {
      instance = new PluginManager();
    }
    return instance;
  }



  public synchronized InterpreterLauncher loadInterpreterLauncher(String launcherPlugin)
      throws IOException {

    if (cachedLaunchers.containsKey(launcherPlugin)) {
      return cachedLaunchers.get(launcherPlugin);
    }
    LOGGER.info("Loading Interpreter Launcher Plugin: " + launcherPlugin);
    ClassLoader pluginClassLoader = getPluginClassLoader(pluginsDir, "Launcher", launcherPlugin);
    if(pluginClassLoader == null) {
      // try to use default classloader
      // TODO: test this
      pluginClassLoader = ClassLoader.getSystemClassLoader();
    }
    String pluginClass = "org.apache.zeppelin.interpreter.launcher." + launcherPlugin;
    InterpreterLauncher launcher = null;
    try {
      launcher = (InterpreterLauncher) (Class.forName(pluginClass, true, pluginClassLoader))
          .getConstructor(ZeppelinConfiguration.class)
          .newInstance(zConf);
    } catch (InstantiationException | IllegalAccessException | ClassNotFoundException
        | NoSuchMethodException | InvocationTargetException e) {
      LOGGER.warn("Fail to instantiate Launcher from plugin classpath:" + launcherPlugin, e);
    }

    if (launcher == null) {
      throw new IOException("Fail to load plugin: " + launcherPlugin);
    }
    cachedLaunchers.put(launcherPlugin, launcher);
    return launcher;
  }

  private URLClassLoader getPluginClassLoader(String pluginsDir,
                                              String pluginType,
                                              String pluginName) throws IOException {

    File pluginFolder = new File(pluginsDir + "/" + pluginType + "/" + pluginName);
    if (!pluginFolder.exists() || pluginFolder.isFile()) {
      LOGGER.warn("PluginFolder " + pluginFolder.getAbsolutePath() +
          " doesn't exist or is not a directory");
      return null;
    }
    List<URL> urls = new ArrayList<>();
    for (File file : pluginFolder.listFiles()) {
      LOGGER.debug("Add file " + file.getAbsolutePath() + " to classpath of plugin: "
          + pluginName);
      urls.add(file.toURI().toURL());
    }
    if (urls.isEmpty()) {
      LOGGER.warn("Can not load plugin " + pluginName +
          ", because the plugin folder " + pluginFolder + " is empty.");
      return null;
    }
    return new URLClassLoader(urls.toArray(new URL[0]));
  }

  @VisibleForTesting
  public static void reset() {
    instance = null;
  }
}
