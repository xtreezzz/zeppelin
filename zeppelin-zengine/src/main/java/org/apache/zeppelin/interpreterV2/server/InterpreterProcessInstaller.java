package org.apache.zeppelin.interpreterV2.server;

import org.apache.zeppelin.DependencyResolver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.Objects;

public class InterpreterProcessInstaller {

  private static final Logger LOG = LoggerFactory.getLogger(InterpreterProcessInstaller.class);

  private final DependencyResolver dependencyResolver;

  public InterpreterProcessInstaller() {
    this.dependencyResolver = new DependencyResolver("");
  }

  public boolean isInterpreterInstalled(final String interpreterGroup, final String artifact) {
    final File folderToStore = new File("interpreters/" + interpreterGroup + "/");
    return folderToStore.exists() && Objects.requireNonNull(folderToStore.list()).length > 0;
  }

  public void installInterpreter(final String interpreterGroup, final String artifact) {
    if (isInterpreterInstalled(interpreterGroup, artifact)) {
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

}
