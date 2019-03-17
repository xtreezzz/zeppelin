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

package org.apache.zeppelin.storage;

import java.util.List;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import org.apache.zeppelin.Repository;
import org.apache.zeppelin.interpreter.configuration.InterpreterArtifactSource;
import org.apache.zeppelin.interpreter.configuration.InterpreterOption;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;

/**
 * Repository for:
 *    {@link InterpreterOption},
 *    {@link InterpreterArtifactSource},
 *    {@link Repository}
 *
 * @see org.apache.zeppelin.rest.InterpreterRestApi
 */
public class InterpreterOptionRepository {

  @Nonnull
  private final InterpreterOptionDAO storage;

  public InterpreterOptionRepository(@Nonnull final NamedParameterJdbcTemplate jdbcTemplate) {
    this.storage = new InterpreterOptionDAO(jdbcTemplate);
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
