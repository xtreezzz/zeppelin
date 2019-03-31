package org.apache.zeppelin.interpreterV2.server;

import com.google.common.collect.Lists;
import com.google.gson.Gson;
import java.io.File;
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
import org.apache.zeppelin.interpreter.configuration.BaseInterpreterConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class InterpreterInstaller {

  private static final Logger LOG = LoggerFactory.getLogger(InterpreterInstaller.class);
  private static final String destinationFolder = "interpreters/";


  public static boolean isInstalled(final String name, final String artifact) {
    final File folderToStore = new File(destinationFolder + name + "/");
    return folderToStore.exists() && Objects.requireNonNull(folderToStore.list()).length > 0;
  }

  public String install(final String name, final String artifact, final List<Repository> repositories) {
    if (isInstalled(name, artifact)) {
      return getDirectory(name, artifact);
    }

    final File folderToStore = new File(destinationFolder + name + "/");
    try {
      final DependencyResolver dependencyResolver = new DependencyResolver(repositories);
      dependencyResolver.load(artifact, folderToStore);
      return folderToStore.getAbsolutePath();
    } catch (final Exception e) {
      LOG.error("Error while install interpreter", e);
      uninstallInterpreter(name, artifact);
      return "";
    }
  }

  public static void uninstallInterpreter(final String name, final String artifact) {
    final File folderToStore = new File(destinationFolder + name + "/");
    try {
      FileUtils.deleteDirectory(folderToStore);
    } catch (final Exception e) {
      LOG.error("Error while remove interpreter", e);
    }
  }

  public static List<BaseInterpreterConfig> getDefaultConfig(final String name, final String artifact) {
    final File folderToStore = new File(destinationFolder + name + "/");

    try {
      final List<URL> urls = Lists.newArrayList();
      for (final File file : folderToStore.listFiles()) {
        final URL url = file.toURI().toURL();

        urls.add(new URL("jar:" + url.toString() + "!/"));
      }

      final URLClassLoader classLoader = URLClassLoader.newInstance(urls.toArray(new URL[0]));
      final String config = IOUtils.toString(classLoader.getResourceAsStream("interpreter-setting.json"), "UTF-8");
      return Arrays.asList(new Gson().fromJson(config, (Type) BaseInterpreterConfig[].class));
    } catch (final Exception e) {
      throw new IllegalArgumentException("Wrong config format", e);
    }
  }

  public static String getDirectory(final String name, final String artifact) {
      final File folderToStore = new File(destinationFolder + name + "/");
      return folderToStore.getAbsolutePath();
  }
}
