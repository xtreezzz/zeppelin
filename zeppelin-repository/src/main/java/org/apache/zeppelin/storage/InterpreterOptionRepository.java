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
import org.apache.zeppelin.Repository;
import org.apache.zeppelin.interpreter.configuration.InterpreterOption;
import org.apache.zeppelin.interpreter.configuration.InterpreterSource;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;


public class InterpreterOptionRepository {

  private final InterpreterOptionDAO storage;

  public InterpreterOptionRepository(final NamedParameterJdbcTemplate jdbcTemplate) {
    this.storage = new InterpreterOptionDAO(jdbcTemplate);
  }

  public InterpreterOption getOption(final String shebang) {
    return storage.getInterpreterOption(shebang);
  }

  public List<InterpreterOption> getAllOptions() {
    return storage.getAllInterpreterOptions();
  }

  public Repository getRepository(final String id) {
    return null;
  }

  public List<Repository> getAllRepositories() {
    return null;
  }

  public InterpreterSource getSource(final String artifact) {
    return storage.getSource(artifact);
  }

  public List<InterpreterSource> getAllSources() {
    return storage.getAllSources();
  }

  public Repository saveRepostitory(final Repository repository) {
    return repository;
  }

  public InterpreterOption saveOption(final InterpreterOption interpreterOption) {
    storage.saveInterpreterOption(interpreterOption);
    return interpreterOption;
  }

  public InterpreterSource saveSource(final InterpreterSource interpreterSource) {
    storage.saveInterpreterSource(interpreterSource);
    return interpreterSource;
  }

  public boolean removeRepository(final String id) {
    return false;
  }

  public boolean removeOption(final String shebang) {
    return storage.removeInterpreterOption(shebang);
  }

  public boolean removeSource(final String artifact) {
    return storage.removeInterpreterSource(artifact);
  }
}
