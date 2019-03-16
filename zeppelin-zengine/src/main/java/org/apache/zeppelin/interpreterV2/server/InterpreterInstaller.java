package org.apache.zeppelin.interpreterV2.server;

import com.google.common.collect.Lists;
import com.google.gson.Gson;
import java.io.File;
import java.lang.reflect.Type;
import java.net.URL;
import java.net.URLClassLoader;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import org.apache.commons.io.IOUtils;
import org.apache.zeppelin.DependencyResolver;
import org.apache.zeppelin.interpreter.configuration.BaseInterpreterConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class InterpreterInstaller {

  private static final Logger LOG = LoggerFactory.getLogger(InterpreterInstaller.class);

  private final String[] exclusions = new String[]{"org.apache.zeppelin:zeppelin-zengine",
          "org.apache.zeppelin:zeppelin-interpreter",
          "org.apache.zeppelin:zeppelin-server"};

  private final DependencyResolver dependencyResolver;

  public InterpreterInstaller() {
    this.dependencyResolver = new DependencyResolver("");
  }

  public boolean isInstalled(final String interpreterGroup, final String artifact) {
    final File folderToStore = new File("interpreters/" + interpreterGroup + "/");
    return folderToStore.exists() && Objects.requireNonNull(folderToStore.list()).length > 0;
  }

  public void install(final String interpreterGroup, final String artifact) {
    if (isInstalled(interpreterGroup, artifact)) {
      return;
    }

    final File folderToStore = new File("interpreters/" + interpreterGroup + "/");
    try {
      dependencyResolver.load(artifact, new ArrayList<>(), folderToStore);
    } catch (final Exception e) {
      LOG.error("Error while install interpreter", e);
      uninstallInterpreter(interpreterGroup, artifact);
    }
  }

  public void uninstallInterpreter(final String interpreterGroup, final String artifact) {
    final File folderToStore = new File("interpreters/" + interpreterGroup + "/");
    try {
      Files.delete(folderToStore.toPath());
    } catch (final Exception e) {
      LOG.error("Error while remove interpreter", e);
    }
  }

  public List<BaseInterpreterConfig> getDafaultConfig(final String interpreterGroup, final String artifact) {
    final File folderToStore = new File("interpreters/" + interpreterGroup + "/");

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

  public String getDirectory(final String interpreterGroup, final String artifact) {
      final File folderToStore = new File("interpreters/" + interpreterGroup + "/");
      return folderToStore.getAbsolutePath();
  }
}
