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
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import javax.annotation.PostConstruct;
import org.apache.commons.lang3.StringUtils;
import org.apache.zeppelin.Repository;
import org.apache.zeppelin.storage.InterpreterOptionDAO;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import ru.tinkoff.zeppelin.core.configuration.interpreter.InterpreterArtifactSource;
import ru.tinkoff.zeppelin.core.configuration.interpreter.InterpreterArtifactSource.Status;
import ru.tinkoff.zeppelin.core.configuration.interpreter.InterpreterOption;
import ru.tinkoff.zeppelin.engine.server.InterpreterInstaller;

/**
 * Repository for:
 *    {@link InterpreterOption},
 *    {@link InterpreterArtifactSource},
 *    {@link Repository}
 *
 * @see org.apache.zeppelin.rest.InterpreterRestApi
 */
public class InterpreterSettingService {

  @Nonnull
  private final InterpreterOptionDAO storage;

  public InterpreterSettingService(@Nonnull final NamedParameterJdbcTemplate jdbcTemplate) {
    this.storage = new InterpreterOptionDAO(jdbcTemplate);
  }

  @PostConstruct
  private void reinstall() {
    getAllSources().stream()
        .filter(InterpreterArtifactSource::isReinstallOnStart)
        .forEach(this::reInstallSource);
  }

  public void uninstallSource(@Nonnull final InterpreterArtifactSource source) {
    disableRelatedInterpreters(source.getInterpreterName());

    InterpreterInstaller.uninstallInterpreter(source.getInterpreterName());
    if (!InterpreterInstaller.isInstalled(source.getInterpreterName())) {
      source.setStatus(Status.NOT_INSTALLED);
      source.setPath(null);
    }
    updateSource(source);
  }

  public void installSource(@Nonnull final InterpreterArtifactSource source) {
    final String installationDir = InterpreterInstaller.install(source.getInterpreterName(), source.getArtifact(), getAllRepositories());
    if (!StringUtils.isAllBlank(installationDir)) {
      source.setPath(installationDir);
      source.setStatus(InterpreterArtifactSource.Status.INSTALLED);
    } else {
      source.setPath(null);
      source.setStatus(InterpreterArtifactSource.Status.NOT_INSTALLED);
    }
    updateSource(source);
  }

  private void disableRelatedInterpreters(@Nonnull final String interpreterName) {
    getAllOptions()
        .stream()
        .filter(o -> o.isEnabled() && o.getConfig().getGroup().equals(interpreterName))
        .forEach(o -> {
          o.setEnabled(false);
          //FIXME: stop process?
          updateOption(o);
        });
  }

  public void reInstallSource(@Nonnull final InterpreterArtifactSource source) {
    uninstallSource(source);
    installSource(source);
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
  public InterpreterArtifactSource saveSource(
      @Nonnull final InterpreterArtifactSource interpreterArtifactSource) {
    storage.saveInterpreterSource(interpreterArtifactSource);
    return interpreterArtifactSource;
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
