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

import com.google.gson.Gson;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.zeppelin.interpreter.configuration.BaseInterpreterConfig;
import org.apache.zeppelin.interpreter.configuration.InterpreterOption;
import org.apache.zeppelin.interpreter.configuration.InterpreterProperty;
import org.apache.zeppelin.interpreter.configuration.InterpreterSource;
import org.apache.zeppelin.interpreter.configuration.option.ExistingProcess;
import org.apache.zeppelin.interpreter.configuration.option.Permissions;
import org.postgresql.util.PGobject;
import org.springframework.dao.EmptyResultDataAccessException;
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import org.springframework.jdbc.support.GeneratedKeyHolder;
import org.springframework.jdbc.support.KeyHolder;

public class
InterpreterOptionDAO {

  private final NamedParameterJdbcTemplate jdbcTemplate;
  private final KeyHolder keyHolder = new GeneratedKeyHolder();

  private static final Gson gson = new Gson();

  private static final String GET_ALL_OPTIONS = "SELECT * FROM InterpreterOption";

  private static final String GET_OPTION = "SELECT * FROM InterpreterOption WHERE "
      + "shebang = :shebang";

  private static final String INSERT_CONFIG = "INSERT INTO BaseInterpreterConfig(name, "
      + "\"group\", class_name, properties, editor) VALUES (:name, :group, :class_name, :properties, "
      + ":editor)";

  // get BaseConfig by shebang
  private static final String GET_CONFIG = "SELECT * FROM BaseInterpreterConfig "
      + "JOIN InterpreterOption ON InterpreterOption.config_id = BaseInterpreterConfig.id WHERE "
      + "InterpreterOption.shebang = :shebang";

  // update BaseConfig by shebang
  private static final String UPDATE_CONFIG = "UPDATE BaseInterpreterConfig SET name = :name, "
      + "\"group\" = :group, class_name = :class_name, properties = :properties, "
      + "editor = :editor JOIN InterpreterOption ON InterpreterOption.config_id = "
      + "BaseInterpreterConfig.id WHERE BaseInterpreterConfig.id = "
      + "InterpreterOption.config_id AND InterpreterOption.shebang = :shebang";

  private static final String INSERT_OPTION = "INSERT INTO InterpreterOption(shebang, "
      + "custom_interpreter_name, interpreter_name, per_note, per_user, jvm_options, "
      + "concurrent_tasks, config_id, remote_process, permissions) VALUES (:shebang, "
      + ":custom_interpreter_name, :interpreter_name, :per_note, :per_user, "
      + ":jvm_options, :concurrent_tasks, :config_id, :remote_process, :permissions)";

  private static final String UPDATE_OPTION = "UPDATE InterpreterOption SET "
      + "custom_interpreter_name = :custom_interpreter_name, interpreter_name=:interpreter_name, "
      + "per_note=:per_note, per_user:=per_user, jvm_options=:jvm_options, "
      + "concurrent_tasks=:concurrent_tasks, config_id:=config_id, remote_process:=remote_process, "
      + "permissions:=permissions WHERE shebang=:shebang";

  // option will be deleated because of cascade rule on FK
  private static final String DELETE_OPTION = "DELETE FROM BaseInterpreterConfig JOIN "
      + "InterpreterOption ON InterpreterOption.config_id = BaseInterpreterConfig.id "
      + "WHERE BaseInterpreterConfig.id = InterpreterOption.config_id AND "
      + "InterpreterOption.shebang = :shebang";

  private static final String GET_ALL_SOURCES = "SELECT * FROM InterpreterSource";

  private static final String GET_SOURCE = "SELECT * FROM InterpreterSource WHERE artifact = "
      + ":artifact";

  private static final String INSERT_SOURCE = "INSERT INTO InterpreterSource(interpreter_name, "
      + "artifact) VALUES (:interpreter_name, :artifact)";

  private static final String UPDATE_SOURCE = "UPDATE InterpreterSource SET interpreter_name = "
      + ":interpreter_name WHERE artifact = :artifact";

  private static final String GET_ALL_REPOSITORIES = "SELECT * FROM repository";

  private static final String GET_REPOSITORY = "SELECT * FROM repository WHERE repository_id = "
      + ":repository_id";

  private static final String INSERT_REPOSITORY = "INSERT INTO repository(repository_id, snapshot, "
      + "url, username, password, proxy_protocol, proxy_host, proxy_port, proxy_login, "
      + "proxy_password) VALUES (:repository_id, :snapshot, :url, :username, :password, "
      + ":proxy_protocol, :proxy_host, :proxy_port, :proxy_login, :proxy_password)";

  private static final String UPDATE_REPOSITORY = "UPDATE repository SET snapshot = :snapshot, "
      + "url = :url, username = :username, password = :password, proxy_protocol = :proxy_protocol,"
      + " proxy_host = :proxy_host, proxy_port = :proxy_port, proxy_login = :proxy_login, "
      + "proxy_password = :proxy_password WHERE repository_id = :repository_id";

  private static final String DELETE_SOURCE = "DELETE FROM InterpreterSource WHERE "
      + "artifact = :artifact";

  public InterpreterOptionDAO(final NamedParameterJdbcTemplate jdbcTemplate) {
    this.jdbcTemplate = jdbcTemplate;
  }

  /**
   * Add interpreter option record. This method also adds config record.
   *
   * @param option
   * @see InterpreterOptionDAO#saveInterpreterConfig(BaseInterpreterConfig)
   */
  void saveInterpreterOption(final InterpreterOption option) {
    int affectedRows =
        jdbcTemplate.update(INSERT_OPTION, convertInterpreterOptionToParameters(option));

    if (affectedRows == 0) {
      throw new RuntimeException("Fail to save option " + option.getShebang());
    }
  }

  InterpreterOption getInterpreterOption(final String shebang) {
    try {
      InterpreterOption option = jdbcTemplate.queryForObject(
          GET_OPTION,
          new MapSqlParameterSource("shebang", shebang),
          (resultSet, i) -> convertResultSetToInterpreterOption(resultSet)
      );

      if (option == null) {
        throw new RuntimeException("Fail to find interpreter option: " + shebang);
      }

      return option;
    } catch (EmptyResultDataAccessException ignore) {
      return null;
    }
  }

  List<InterpreterOption> getAllInterpreterOptions() {
    return jdbcTemplate
        .query(GET_ALL_OPTIONS, (resultSet, i) -> convertResultSetToInterpreterOption(resultSet));
  }

  boolean removeInterpreterOption(final String shebang) {
    return jdbcTemplate.update(DELETE_OPTION, new MapSqlParameterSource("shebang", shebang)) != 0;
  }

  void updateInterpreterOption(final InterpreterOption option) {
    int affectedRows =
        jdbcTemplate.update(INSERT_OPTION, convertInterpreterOptionToParameters(option));

    if (affectedRows == 0) {
      throw new RuntimeException("Fail to save option " + option.getShebang());
    }
  }

  /**
   * Add interpreter config record.
   *
   * @param config - config, obtained from {@link InterpreterOption}
   * @return key - id of added record
   */
  private long saveInterpreterConfig(final BaseInterpreterConfig config) {
    int affectedRows =
        jdbcTemplate.update(INSERT_CONFIG, convertInterpreterConfigToParameters(config), keyHolder,
            new String[] { "id" });

    if (affectedRows == 0 || keyHolder.getKey() == null) {
      throw new RuntimeException("Fail to save config " + config);
    }

    return keyHolder.getKey().longValue();
  }

  private long findInterpreterConfigIdx(final String shebang)
      throws EmptyResultDataAccessException, NullPointerException {
    return jdbcTemplate.queryForObject(
        GET_CONFIG,
        new MapSqlParameterSource("shebang", shebang),
        ((resultSet, i) -> Long.parseLong(resultSet.getString("id"))));
  }

  private BaseInterpreterConfig getInterpreterConfig(final String shebang)
      throws EmptyResultDataAccessException, NullPointerException {
    return jdbcTemplate.queryForObject(
        GET_CONFIG,
        new MapSqlParameterSource("shebang", shebang),
        ((resultSet, i) -> convertResultSetToInterpreterConfig(resultSet)));
  }

  private long getOrSaveInterpreterConfig(final InterpreterOption option) {
    try {
      return findInterpreterConfigIdx(option.getShebang());
    } catch (EmptyResultDataAccessException | NullPointerException ignore) {
      return saveInterpreterConfig(option.getConfig());
    }
  }

  List<InterpreterSource> getAllSources() {
    return jdbcTemplate
        .query(GET_ALL_SOURCES, (resultSet, i) -> convertResultSetToInterpreterSource(resultSet));
  }

  InterpreterSource getSource(final String artifact) {
    try {
      InterpreterSource source = jdbcTemplate.queryForObject(
          GET_SOURCE,
          new MapSqlParameterSource("artifact", artifact),
          (resultSet, i) -> convertResultSetToInterpreterSource(resultSet)
      );

      if (source == null) {
        throw new RuntimeException("Fail to find interpreter source: " + artifact);
      }

      return source;
    } catch (EmptyResultDataAccessException ignore) {
      return null;
    }
  }

  void saveInterpreterSource(final InterpreterSource source) {
    int affectedRows =
        jdbcTemplate.update(INSERT_SOURCE, convertInterpreterSourceToParameters(source));

    if (affectedRows == 0) {
      throw new RuntimeException("Fail to save source " + source.getArtifact());
    }
  }

  boolean removeInterpreterSource(final String artifact) {
    return jdbcTemplate.update(DELETE_SOURCE, new MapSqlParameterSource("artifact", artifact)) != 0;
  }

  //---------------------- CONVERTERS FROM RESULT SETS TO OBJECTS ---------------------
  private BaseInterpreterConfig convertResultSetToInterpreterConfig(final ResultSet resultSet)
      throws SQLException {
    String name = resultSet.getString("name");
    String group = resultSet.getString("group");
    String className = resultSet.getString("class_name");

    Map<String, InterpreterProperty> properties = new HashMap<>();
    properties = gson.fromJson(resultSet.getString("properties"), properties.getClass());

    return new BaseInterpreterConfig(name, group, className, properties);
  }

  private InterpreterOption convertResultSetToInterpreterOption(final ResultSet resultSet)
      throws SQLException {
    String shebang = resultSet.getString("shebang");
    String name = resultSet.getString("interpreter_name");
    String customInterpreterName = resultSet.getString("custom_interpreter_name");
    String perNote = resultSet.getString("per_note");
    String perUser = resultSet.getString("per_user");
    BaseInterpreterConfig config = getInterpreterConfig(shebang);
    ExistingProcess existingProcess = gson.fromJson(
        resultSet.getString("remote_process"), ExistingProcess.class);
    Permissions permissions = gson.fromJson(
        resultSet.getString("permissions"), Permissions.class);
    int concurrentTasks = Integer.parseInt(resultSet.getString("concurrent_tasks"));
    String jvmOptions = resultSet.getString("jvm_options");

    return new InterpreterOption(
        customInterpreterName, name, shebang, perNote, perUser, config, existingProcess,
        permissions, jvmOptions, concurrentTasks);
  }

  private InterpreterSource convertResultSetToInterpreterSource(final ResultSet resultSet)
    throws SQLException {
    return new InterpreterSource(
        resultSet.getString("interpreter_name"),
        resultSet.getString("artifact")
    );
  }
  //---------------------- CONVERTERS FROM OBJECTS TO PARAMETERS ----------------------
  private MapSqlParameterSource convertInterpreterConfigToParameters(final BaseInterpreterConfig config) {
    MapSqlParameterSource parameters = new MapSqlParameterSource();
    parameters
        .addValue("name", config.getName())
        .addValue("group", config.getGroup())
        .addValue("class_name", config.getClassName())
        .addValue("properties", generatePGjson(config.getProperties()))
        .addValue("editor", generatePGjson(config.getEditor()));

    return parameters;
  }

  /**
   * Converts InterpreterOption to query parameters and creates config record.
   *
   * @see InterpreterOptionDAO#saveInterpreterConfig(BaseInterpreterConfig)
   * @param option option to convert
   * @return query parameters
   */
  private MapSqlParameterSource convertInterpreterOptionToParameters(final InterpreterOption option) {
    MapSqlParameterSource parameters = new MapSqlParameterSource();
    try {
      parameters
          .addValue("shebang", option.getShebang())
          .addValue("custom_interpreter_name", option.getCustomInterpreterName())
          .addValue("interpreter_name", option.getInterpreterName())
          .addValue("per_note", option.getPerNote())
          .addValue("per_user", option.getPerUser())
          .addValue("jvm_options", option.getJvmOptions())
          .addValue("concurrent_tasks", option.getConcurrentTasks())
          .addValue("config_id", getOrSaveInterpreterConfig(option))
          .addValue("remote_process", generatePGjson(option.getRemoteProcess()))
          .addValue("permissions", generatePGjson(option.getPermissions()));
    } catch (Exception e) {
      throw new RuntimeException("Fail to convert option", e);
    }
    return parameters;
  }

  private MapSqlParameterSource convertInterpreterSourceToParameters(final InterpreterSource source) {
    MapSqlParameterSource parameters = new MapSqlParameterSource();
    try {
      parameters
          .addValue("artifact", source.getArtifact())
          .addValue("interpreter_name", source.getInterpreterName());
    } catch (Exception e) {
      throw new RuntimeException("Fail to convert option", e);
    }
    return parameters;
  }

  //----------------------------- UTIL -----------------------------
  private PGobject generatePGjson(final Object value) {
    try {
      PGobject pgObject = new PGobject();
      pgObject.setType("jsonb");
      pgObject.setValue(gson.toJson(value));
      return pgObject;
    } catch (SQLException e) {
      throw new RuntimeException("Can't generate postgres json", e);
    }
  }
}
