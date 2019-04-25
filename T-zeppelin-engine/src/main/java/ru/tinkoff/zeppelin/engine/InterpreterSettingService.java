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

package ru.tinkoff.zeppelin.engine;

import java.util.List;
import java.util.stream.Collectors;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import javax.annotation.PostConstruct;

import org.apache.commons.lang3.StringUtils;
import org.apache.zeppelin.Repository;
import org.apache.zeppelin.storage.InterpreterOptionDAO;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import ru.tinkoff.zeppelin.core.configuration.interpreter.InterpreterArtifactSource;
import ru.tinkoff.zeppelin.core.configuration.interpreter.InterpreterOption;
import ru.tinkoff.zeppelin.engine.server.AbstractRemoteProcess;
import ru.tinkoff.zeppelin.engine.server.ModuleInstaller;
import ru.tinkoff.zeppelin.engine.server.RemoteProcessType;

/**
 * Repository for:
 * {@link InterpreterOption},
 * {@link InterpreterArtifactSource},
 * {@link Repository}
 */
public class InterpreterSettingService {

  @Nonnull
  private final InterpreterOptionDAO storage;

  public InterpreterSettingService(@Nonnull final NamedParameterJdbcTemplate jdbcTemplate) {
    this.storage = new InterpreterOptionDAO(jdbcTemplate);
  }

  @PostConstruct
  private void init() {
    getAllSources().stream()
            .filter(InterpreterArtifactSource::isReinstallOnStart)
            .forEach(src -> {
              uninstallSource(src, false);
              installSource(src, true, false);
            });
  }

  public synchronized void installSource(@Nonnull final InterpreterArtifactSource source,
                                         final boolean installSources,
                                         final boolean checkNames) {

    // 1 check that name does not exist
    final boolean hasSameName = getAllSources().stream()
            .anyMatch(s -> s.getInterpreterName().equals(source.getInterpreterName()));

    if (hasSameName && checkNames) {
      throw new RuntimeException("Wrong configuration. Found duplicated name.");
    }

    //1 check for ability to install
    final String installationDir;
    try {
      ModuleInstaller.uninstallInterpreter(source.getInterpreterName());
      installationDir = ModuleInstaller.install(source.getInterpreterName(), source.getArtifact(), getAllRepositories());
      if (StringUtils.isEmpty(installationDir)) {
        throw new RuntimeException();
      }

    } catch (final Exception e) {
      ModuleInstaller.uninstallInterpreter(source.getInterpreterName());
      throw new RuntimeException("Error while install sources: " + e.getMessage());
    }

    //2 check configuration file
    try {
      ModuleInstaller.getDefaultConfig(source.getInterpreterName());
    } catch (final Exception e) {
      ModuleInstaller.uninstallInterpreter(source.getInterpreterName());
      throw new RuntimeException("Wrong zeppelin module configuration: " + e.getMessage());
    }

    if (!installSources) {
      ModuleInstaller.uninstallInterpreter(source.getInterpreterName());
    }

    if (!StringUtils.isAllBlank(installationDir) && installSources) {
      source.setPath(installationDir);
      source.setStatus(InterpreterArtifactSource.Status.INSTALLED);
    } else {
      source.setPath(null);
      source.setStatus(InterpreterArtifactSource.Status.NOT_INSTALLED);
    }
    if (hasSameName) {
      storage.updateSource(source);
    } else {
      storage.saveInterpreterSource(source);
    }
  }


  public synchronized void uninstallSource(@Nonnull final InterpreterArtifactSource source, final boolean delete) {

    final List<InterpreterOption> interpreterOptions = storage.getAllInterpreterOptions()
            .stream()
            .filter(o -> o.getConfig().getGroup().equals(source.getInterpreterName()))
            .collect(Collectors.toList());

    // 1 disable all interpreters
    for (final InterpreterOption option : interpreterOptions) {
      option.setEnabled(false);
      storage.updateInterpreterOption(option);
    }

    // 2 stop all interpreters
    for (final InterpreterOption option : interpreterOptions) {
      final String shebang = option.getShebang();
      final AbstractRemoteProcess process = AbstractRemoteProcess.get(shebang, RemoteProcessType.INTERPRETER);
      if (process != null) {
        process.forceKill();
      }
    }

    // 3 uninstall interpreter
    ModuleInstaller.uninstallInterpreter(source.getInterpreterName());

    // 4 set correct status
    source.setStatus(InterpreterArtifactSource.Status.NOT_INSTALLED);
    storage.updateSource(source);

    // 4 early exit if not delete drom db
    if (!delete) {
      return;
    }

    // 5 delete all configurations
    for (final InterpreterOption option : interpreterOptions) {
      storage.removeInterpreterOption(option.getShebang());
    }

    // 5 delete sources
    removeSource(source.getInterpreterName());
  }

  public void restartInterpreter(final String shebang) {
    final AbstractRemoteProcess process = AbstractRemoteProcess.get(shebang, RemoteProcessType.INTERPRETER);
    if (process != null) {
      process.forceKill();
    }
  }

  @Nonnull
  public List<InterpreterOption> getAllOptions() {
    return storage.getAllInterpreterOptions();
  }

  @Nullable
  public InterpreterOption getOption(@Nonnull final String shebang) {
    return storage.getInterpreterOption(shebang);
  }

  @Nonnull
  public InterpreterOption saveOption(@Nonnull final InterpreterOption interpreterOption) {
    storage.saveInterpreterOption(interpreterOption);
    return interpreterOption;
  }

  @Nonnull
  public InterpreterOption updateOption(@Nonnull final InterpreterOption interpreterOption) {
    storage.updateInterpreterOption(interpreterOption);
    return interpreterOption;
  }

  public boolean removeOption(@Nonnull final String shebang) {
    return storage.removeInterpreterOption(shebang);
  }

  @Nonnull
  public List<InterpreterArtifactSource> getAllSources() {
    return storage.getAllSources();
  }

  @Nullable
  public InterpreterArtifactSource getSource(@Nonnull final String name) {
    return storage.getSource(name);
  }

  @Nonnull
  public InterpreterArtifactSource updateSource(
          @Nonnull final InterpreterArtifactSource interpreterArtifactSource) {
    storage.updateSource(interpreterArtifactSource);
    return interpreterArtifactSource;
  }

  public boolean removeSource(@Nonnull final String name) {
    return storage.removeInterpreterSource(name);
  }

  @Nonnull
  public List<Repository> getAllRepositories() {
    return storage.getAllRepositories();
  }

  @Nullable
  public Repository getRepository(@Nonnull final String id) {
    return storage.getRepository(id);
  }

  @Nonnull
  public Repository saveRepository(@Nonnull final Repository repository) {
    storage.saveRepository(repository);
    return repository;
  }

  public boolean removeRepository(@Nonnull final String id) {
    return storage.removeRepository(id);
  }
}
