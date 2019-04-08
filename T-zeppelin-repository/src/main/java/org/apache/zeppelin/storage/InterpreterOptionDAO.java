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

import com.google.common.base.Preconditions;
import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;
import org.apache.zeppelin.Repository;
import org.postgresql.util.PGobject;
import org.springframework.dao.EmptyResultDataAccessException;
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import org.springframework.jdbc.support.GeneratedKeyHolder;
import org.springframework.jdbc.support.KeyHolder;
import ru.tinkoff.zeppelin.core.configuration.interpreter.BaseInterpreterConfig;
import ru.tinkoff.zeppelin.core.configuration.interpreter.InterpreterArtifactSource;
import ru.tinkoff.zeppelin.core.configuration.interpreter.InterpreterOption;
import ru.tinkoff.zeppelin.core.configuration.interpreter.InterpreterProperty;
import ru.tinkoff.zeppelin.core.configuration.interpreter.option.ExistingProcess;
import ru.tinkoff.zeppelin.core.configuration.interpreter.option.Permissions;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.lang.reflect.Type;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Data Access Object for:
 *    {@link InterpreterOption},
 *    {@link InterpreterArtifactSource},
 *    {@link Repository}.
 *
 * Access to {@link org.apache.zeppelin.interpreter.configuration.BaseInterpreterConfig} is hidden.
 */
class InterpreterOptionDAO {

  @Nonnull
  private final NamedParameterJdbcTemplate jdbcTemplate;
  @Nonnull
  private final KeyHolder keyHolder;

  private static final Gson gson = new Gson();

  private static final String GET_ALL_OPTIONS = "SELECT * FROM INTERPRETER_OPTION";

  private static final String GET_OPTION = "SELECT * FROM INTERPRETER_OPTION WHERE "
      + "shebang = :shebang";

  private static final String INSERT_CONFIG = "INSERT INTO BASE_INTERPRETER_CONFIG(name, "
      + "\"group\", class_name, properties, editor) VALUES (:name, :group, :class_name, :properties, "
      + ":editor)";

  // get BaseConfig by shebang
  private static final String GET_CONFIG = "SELECT * FROM BASE_INTERPRETER_CONFIG "
      + "JOIN INTERPRETER_OPTION ON INTERPRETER_OPTION.config_id = BASE_INTERPRETER_CONFIG.id WHERE "
      + "INTERPRETER_OPTION.shebang = :shebang";

  // update BaseConfig by shebang
  private static final String UPDATE_CONFIG = "UPDATE BASE_INTERPRETER_CONFIG SET name = :name, "
      + "\"group\" = :group, class_name = :class_name, properties = :properties, "
      + "editor = :editor FROM INTERPRETER_OPTION WHERE INTERPRETER_OPTION.config_id = "
      + "BASE_INTERPRETER_CONFIG.id AND INTERPRETER_OPTION.shebang = :shebang";

  private static final String INSERT_OPTION = "INSERT INTO INTERPRETER_OPTION(shebang, "
      + "custom_interpreter_name, interpreter_name, per_note, per_user, jvm_options, "
      + "concurrent_tasks, config_id, remote_process, permissions, is_enabled) VALUES (:shebang, "
      + ":custom_interpreter_name, :interpreter_name, :per_note, :per_user, "
      + ":jvm_options, :concurrent_tasks, :config_id, :remote_process, :permissions, :is_enabled)";

  private static final String UPDATE_OPTION = "UPDATE INTERPRETER_OPTION SET "
      + "custom_interpreter_name = :custom_interpreter_name, interpreter_name = :interpreter_name, "
      + "per_note = :per_note, per_user = :per_user, jvm_options = :jvm_options, "
      + "concurrent_tasks = :concurrent_tasks, config_id = :config_id, remote_process = :remote_process, "
      + "permissions = :permissions, is_enabled = :is_enabled WHERE shebang = :shebang";

  // option will be deleted because of cascade rule on FK
  private static final String DELETE_OPTION = "DELETE FROM BASE_INTERPRETER_CONFIG USING "
      + "INTERPRETER_OPTION WHERE BASE_INTERPRETER_CONFIG.id = INTERPRETER_OPTION.config_id AND "
      + "INTERPRETER_OPTION.shebang = :shebang";

  private static final String GET_ALL_SOURCES = "SELECT * FROM INTERPRETER_ARTIFACT_SOURCE";

  private static final String GET_SOURCE = "SELECT * FROM INTERPRETER_ARTIFACT_SOURCE WHERE "
      + "interpreter_name = :interpreter_name";

  private static final String INSERT_SOURCE = "INSERT INTO INTERPRETER_ARTIFACT_SOURCE(interpreter_name, "
      + "artifact, status, \"path\") VALUES (:interpreter_name, :artifact, :status, :path)";

  private static final String GET_ALL_REPOSITORIES = "SELECT * FROM repository";

  private static final String GET_REPOSITORY = "SELECT * FROM repository WHERE repository_id = "
      + ":repository_id";

  private static final String INSERT_REPOSITORY = "INSERT INTO repository(repository_id, snapshot, "
      + "url, username, password, proxy_protocol, proxy_host, proxy_port, proxy_login, "
      + "proxy_password) VALUES (:repository_id, :snapshot, :url, :username, :password, "
      + ":proxy_protocol, :proxy_host, :proxy_port, :proxy_login, :proxy_password)";

  private static final String DELETE_SOURCE = "DELETE FROM INTERPRETER_ARTIFACT_SOURCE WHERE "
      + "interpreter_name = :interpreter_name";

  private static final String DELETE_REPOSITORY = "DELETE FROM Repository WHERE "
      + "repository_id = :repository_id";


  public InterpreterOptionDAO(@Nonnull final NamedParameterJdbcTemplate jdbcTemplate) {
    Preconditions.checkNotNull(jdbcTemplate);
    this.jdbcTemplate = jdbcTemplate;
    this.keyHolder = new GeneratedKeyHolder();
  }

  @Nonnull
  List<InterpreterOption> getAllInterpreterOptions() {
    final List<InterpreterOption> result = jdbcTemplate
        .query(GET_ALL_OPTIONS, (resultSet, i) -> convertResultSetToInterpreterOption(resultSet));
    Preconditions.checkNotNull(result);
    return result;
  }

  @Nullable
  InterpreterOption getInterpreterOption(@Nonnull final String shebang) {
    Preconditions.checkNotNull(shebang);
    try {
      final InterpreterOption option = jdbcTemplate.queryForObject(
          GET_OPTION,
          new MapSqlParameterSource("shebang", shebang),
          (resultSet, i) -> convertResultSetToInterpreterOption(resultSet)
      );

      if (option == null) {
        throw new RuntimeException("Fail to find interpreter   option: " + shebang);
      }
      return option;
    } catch (final EmptyResultDataAccessException ignore) {
      return null;
    }
  }

  /**
   * Add interpreter option record. This method also adds config record.
   *
   * @see InterpreterOptionDAO#convertInterpreterOptionToParameters
   */
  void saveInterpreterOption(@Nonnull final InterpreterOption option) {
    Preconditions.checkNotNull(option);
    final int affectedRows =
        jdbcTemplate.update(INSERT_OPTION, convertInterpreterOptionToParameters(option));

    if (affectedRows == 0) {
      // delete config
      removeInterpreterOption(option.getShebang());
      throw new RuntimeException("Fail to save option " + option.getShebang());
    }
  }

  void updateInterpreterOption(@Nonnull final InterpreterOption option) {
    //TODO(egorklimov): if config update completed, but option update
    // failed data would be inconsistent, transaction?
    Preconditions.checkNotNull(option);
    jdbcTemplate.update(UPDATE_CONFIG, convertInterpreterConfigToParameters(option.getConfig())
        .addValue("shebang", option.getShebang()));
    final int affectedRows =
        jdbcTemplate.update(UPDATE_OPTION, convertInterpreterOptionToParameters(option));

    if (affectedRows == 0) {
      throw new RuntimeException("Fail to update option " + option.getShebang());
    }
  }

  boolean removeInterpreterOption(@Nonnull final String shebang) {
    Preconditions.checkNotNull(shebang);
    return jdbcTemplate.update(DELETE_OPTION, new MapSqlParameterSource("shebang", shebang)) != 0;
  }


  /**
   * Add interpreter config record.
   *
   * @param config - config, obtained from {@link InterpreterOption}
   * @return key - id of added record
   */
  private long saveInterpreterConfig(@Nonnull final BaseInterpreterConfig config) {
    Preconditions.checkNotNull(config);
    final int affectedRows =
        jdbcTemplate.update(INSERT_CONFIG,
            convertInterpreterConfigToParameters(config), keyHolder,
            new String[]{"id"});

    if (affectedRows == 0 || keyHolder.getKey() == null) {
      throw new RuntimeException("Fail to save config " + config);
    }
    //TODO(egorklimov): checkState on key value
    return keyHolder.getKey().longValue();
  }

  /**
   * Get id of existing interpreter config record.
   *
   * @param shebang - shebang of InterpreterOption which contains sought-for config.
   * @return id of a record, if it exists, null otherwise
   */
  private long findInterpreterConfigIdx(@Nonnull final String shebang) {
    Preconditions.checkNotNull(shebang);
    //TODO(egorklimov): checkState on key value
    return jdbcTemplate.queryForObject(
        GET_CONFIG,
        new MapSqlParameterSource("shebang", shebang),
        ((resultSet, i) -> Long.parseLong(resultSet.getString("id"))));
  }

  @Nullable
  private BaseInterpreterConfig getInterpreterConfig(@Nonnull final String shebang) {
    Preconditions.checkNotNull(shebang);
    return jdbcTemplate.queryForObject(
        GET_CONFIG,
        new MapSqlParameterSource("shebang", shebang),
        ((resultSet, i) -> convertResultSetToInterpreterConfig(resultSet)));
  }

  /**
   * Get id of existing record or save it.
   *
   * @param option - parent option where config is nested.
   * @return id of found or created record.
   */
  private long getOrSaveInterpreterConfig(@Nonnull final InterpreterOption option) {
    Preconditions.checkNotNull(option);
    try {
      return findInterpreterConfigIdx(option.getShebang());
    } catch (final EmptyResultDataAccessException | NullPointerException ignore) {
      return saveInterpreterConfig(option.getConfig());
    }
  }

  @Nonnull
  List<InterpreterArtifactSource> getAllSources() {
    final List<InterpreterArtifactSource> result = jdbcTemplate
        .query(GET_ALL_SOURCES, (resultSet, i) -> convertResultSetToInterpreterSource(resultSet));
    Preconditions.checkNotNull(result);
    return result;
  }

  @Nullable
  InterpreterArtifactSource getSource(@Nonnull final String name) {
    Preconditions.checkNotNull(name);
    try {
      final InterpreterArtifactSource source = jdbcTemplate.queryForObject(
          GET_SOURCE,
          new MapSqlParameterSource("interpreter_name", name),
          (resultSet, i) -> convertResultSetToInterpreterSource(resultSet)
      );

      if (source == null) {
        throw new RuntimeException("Fail to find interpreter source: " + name);
      }

      return source;
    } catch (final EmptyResultDataAccessException ignore) {
      return null;
    }
  }

  void saveInterpreterSource(@Nonnull final InterpreterArtifactSource source) {
    Preconditions.checkNotNull(source);
    final int affectedRows =
        jdbcTemplate.update(INSERT_SOURCE, convertInterpreterSourceToParameters(source));

    if (affectedRows == 0) {
      throw new RuntimeException("Fail to save source " + source.getInterpreterName());
    }
  }

  boolean removeInterpreterSource(@Nonnull final String interpreterName) {
    Preconditions.checkNotNull(interpreterName);
    return jdbcTemplate.update(DELETE_SOURCE,
        new MapSqlParameterSource("interpreter_name", interpreterName)) != 0;
  }

  @Nonnull
  List<Repository> getAllRepositories() {
    final List<Repository> result = jdbcTemplate.query(
        GET_ALL_REPOSITORIES, (resultSet, i) -> convertResultSetToRepository(resultSet));
    Preconditions.checkNotNull(result);
    return result;
  }

  @Nullable
  Repository getRepository(@Nonnull final String id) {
    Preconditions.checkNotNull(id);
    try {
      final Repository repository = jdbcTemplate.queryForObject(
          GET_REPOSITORY,
          new MapSqlParameterSource("repository_id", id),
          (resultSet, i) -> convertResultSetToRepository(resultSet)
      );

      if (repository == null) {
        throw new RuntimeException("Fail to find repository: " + id);
      }

      return repository;
    } catch (final EmptyResultDataAccessException ignore) {
      return null;
    }
  }

  void saveRepository(@Nonnull final Repository repository) {
    Preconditions.checkNotNull(repository);
    final int affectedRows =
        jdbcTemplate.update(INSERT_REPOSITORY, convertRepositoryToParameters(repository));

    if (affectedRows == 0) {
      throw new RuntimeException("Fail to save repository " + repository.getId());
    }
  }

  boolean removeRepository(@Nonnull final String id) {
    Preconditions.checkNotNull(id);
    return jdbcTemplate.update(
        DELETE_REPOSITORY, new MapSqlParameterSource("repository_id", id)) != 0;
  }

  //---------------------- CONVERTERS FROM RESULT SETS TO OBJECTS ---------------------
  @Nonnull
  private BaseInterpreterConfig convertResultSetToInterpreterConfig(@Nonnull final ResultSet resultSet)
      throws SQLException {
    Preconditions.checkNotNull(resultSet);

    final String name = resultSet.getString("name");
    final String group = resultSet.getString("group");
    final String className = resultSet.getString("class_name");

    final Type type = new TypeToken<Map<String, InterpreterProperty>>(){}.getType();
    final Map<String, InterpreterProperty> properties =
        gson.fromJson(resultSet.getString("properties"), type);

    return new BaseInterpreterConfig(name, group, className, properties, new HashMap<>());
  }

  @Nonnull
  private InterpreterOption convertResultSetToInterpreterOption(@Nonnull final ResultSet resultSet)
      throws SQLException {
    Preconditions.checkNotNull(resultSet);

    final String shebang = resultSet.getString("shebang");
    final String name = resultSet.getString("interpreter_name");
    final String customInterpreterName = resultSet.getString("custom_interpreter_name");
    final InterpreterOption.ProcessType perNote = InterpreterOption.ProcessType.valueOf(resultSet.getString("per_note"));
    final InterpreterOption.ProcessType perUser = InterpreterOption.ProcessType.valueOf(resultSet.getString("per_user"));
    final int concurrentTasks = Integer.parseInt(resultSet.getString("concurrent_tasks"));
    final String jvmOptions = resultSet.getString("jvm_options");
    final boolean isEnabled = resultSet.getBoolean("is_enabled");

    final BaseInterpreterConfig config = getInterpreterConfig(shebang);
    final ExistingProcess existingProcess =
        ExistingProcess.fromJson(resultSet.getString("remote_process"));
    final Permissions permissions = gson.fromJson(
        resultSet.getString("permissions"), Permissions.class);

    Preconditions.checkState(config != null, "Fail to get config");
    return new InterpreterOption(
        customInterpreterName, name, shebang, perNote,
        perUser, config, existingProcess,
        permissions, jvmOptions, concurrentTasks, isEnabled);
  }

  @Nonnull
  private InterpreterArtifactSource convertResultSetToInterpreterSource(
      @Nonnull final ResultSet resultSet)
      throws SQLException {
    Preconditions.checkNotNull(resultSet);
    return new InterpreterArtifactSource(
        resultSet.getString("interpreter_name"),
        resultSet.getString("artifact"),
        resultSet.getString("path"),
        InterpreterArtifactSource.Status.valueOf(resultSet.getString("status"))
    );
  }

  @Nonnull
  private Repository convertResultSetToRepository(@Nonnull final ResultSet resultSet) throws SQLException {
    Preconditions.checkNotNull(resultSet);
    return new Repository(resultSet.getBoolean("snapshot"), resultSet.getString("repository_id"),
        resultSet.getString("url"), resultSet.getString("username"),
        resultSet.getString("password"), Repository.ProxyProtocol.valueOf(resultSet.getString("proxy_protocol")),
        resultSet.getString("proxy_host"), resultSet.getInt("proxy_port"),
        resultSet.getString("proxy_login"), resultSet.getString("proxy_password"));
  }

  //---------------------- CONVERTERS FROM OBJECTS TO PARAMETERS ----------------------
  @Nonnull
  private MapSqlParameterSource convertInterpreterConfigToParameters(
      @Nonnull final BaseInterpreterConfig config) {
    Preconditions.checkNotNull(config);
    final MapSqlParameterSource parameters = new MapSqlParameterSource();
    parameters
        .addValue("name", config.getName())
        .addValue("group", config.getGroup())
        .addValue("class_name", config.getClassName())
        .addValue("properties", generatePGjson(config.getProperties()))
        .addValue("editor", generatePGjson(config.getEditor()));
    Preconditions.checkNotNull(parameters);
    return parameters;
  }

  /**
   * Converts InterpreterOption to query parameters and creates config record.
   *
   * @param option option to convert
   * @return query parameters
   * @see InterpreterOptionDAO#saveInterpreterConfig(BaseInterpreterConfig)
   */
  @Nonnull
  private MapSqlParameterSource convertInterpreterOptionToParameters(
      @Nonnull final InterpreterOption option) {
    Preconditions.checkNotNull(option);
    final MapSqlParameterSource parameters = new MapSqlParameterSource();
    try {
      parameters
          .addValue("shebang", option.getShebang())
          .addValue("custom_interpreter_name", option.getCustomInterpreterName())
          .addValue("interpreter_name", option.getInterpreterName())
          .addValue("per_note", option.getPerNote().name())
          .addValue("per_user", option.getPerUser().name())
          .addValue("jvm_options", option.getJvmOptions())
          .addValue("concurrent_tasks", option.getConcurrentTasks())
          .addValue("config_id", getOrSaveInterpreterConfig(option))
          .addValue("remote_process", generatePGjson(option.getRemoteProcess()))
          .addValue("permissions", generatePGjson(option.getPermissions()))
          .addValue("is_enabled", option.isEnabled());
    } catch (final Exception e) {
      throw new RuntimeException("Fail to convert option", e);
    }
    Preconditions.checkNotNull(parameters);
    return parameters;
  }

  @Nonnull
  private MapSqlParameterSource convertInterpreterSourceToParameters(
      @Nonnull final InterpreterArtifactSource source) {
    Preconditions.checkNotNull(source);
    final MapSqlParameterSource parameters = new MapSqlParameterSource();
    try {
      parameters
          .addValue("artifact", source.getArtifact())
          .addValue("interpreter_name", source.getInterpreterName())
          .addValue("status", source.getStatus().name())
          .addValue("path", source.getPath());
    } catch (final Exception e) {
      throw new RuntimeException("Fail to convert source", e);
    }
    Preconditions.checkNotNull(parameters);
    return parameters;
  }

  @Nonnull
  private MapSqlParameterSource convertRepositoryToParameters(
      @Nonnull final Repository source) {
    Preconditions.checkNotNull(source);
    final MapSqlParameterSource parameters = new MapSqlParameterSource();
    try {
      parameters
          .addValue("repository_id", source.getId())
          .addValue("snapshot", source.isSnapshot())
          .addValue("url", source.getUrl())
          .addValue("username", source.getUsername())
          .addValue("password", source.getPassword())
          .addValue("proxy_protocol",
              source.getProxyProtocol() != null ? source.getProxyProtocol().name() : null)
          .addValue("proxy_host", source.getProxyHost())
          .addValue("proxy_port", source.getProxyPort())
          .addValue("proxy_login", source.getProxyLogin())
          .addValue("proxy_password", source.getProxyPassword());
    } catch (final Exception e) {
      throw new RuntimeException("Fail to convert repository", e);
    }
    Preconditions.checkNotNull(parameters);
    return parameters;
  }

  //----------------------------- UTIL -----------------------------
  @Nonnull
  private PGobject generatePGjson(@Nonnull final Object value) {
    Preconditions.checkNotNull(value);
    try {
      final PGobject pgObject = new PGobject();
      pgObject.setType("jsonb");
      pgObject.setValue(gson.toJson(value));
      Preconditions.checkNotNull(pgObject);
      return pgObject;
    } catch (final SQLException e) {
      throw new RuntimeException("Can't generate postgres json", e);
    }
  }
}
