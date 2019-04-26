package org.apache.zeppelin.storage;

import com.google.common.base.Preconditions;
import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import org.springframework.jdbc.core.namedparam.SqlParameterSource;
import org.springframework.jdbc.support.GeneratedKeyHolder;
import org.springframework.jdbc.support.KeyHolder;
import org.springframework.stereotype.Component;
import ru.tinkoff.zeppelin.core.configuration.interpreter.ModuleInnerConfiguration;
import ru.tinkoff.zeppelin.core.configuration.interpreter.InterpreterProperty;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;
import java.util.Map;


@Component
public class ModuleInnerConfigurationDAO {

  public static final String GET_ALL = "" +
          "SELECT ID,\n" +
          "       CLASS_NAME,\n" +
          "       PROPERTIES,\n" +
          "       EDITOR\n" +
          "FROM MODULE_INNER_CONFIGURATION;";

  public static final String GET_BY_ID = "" +
          "SELECT ID,\n" +
          "       CLASS_NAME,\n" +
          "       PROPERTIES,\n" +
          "       EDITOR\n" +
          "FROM MODULE_INNER_CONFIGURATION\n" +
          "WHERE ID = :ID;";


  private static final String PERSIST = "" +
          "INSERT INTO MODULE_INNER_CONFIGURATION (CLASS_NAME,\n" +
          "                                        PROPERTIES,\n" +
          "                                        EDITOR)\n" +
          "VALUES (:CLASS_NAME,\n" +
          "        :PROPERTIES,\n" +
          "        :EDITOR);";

  private static final String UPDATE = "" +
          "UPDATE MODULE_INNER_CONFIGURATION\n" +
          "SET CLASS_NAME = :CLASS_NAME,\n" +
          "    PROPERTIES = :PROPERTIES,\n" +
          "    EDITOR     = :EDITOR\n" +
          "WHERE ID = :ID;";

  private static final String DELETE = "" +
          "DELETE\n" +
          "FROM MODULE_INNER_CONFIGURATION\n" +
          "WHERE ID = :ID;";

  private final NamedParameterJdbcTemplate namedParameterJdbcTemplate;


  public ModuleInnerConfigurationDAO(final NamedParameterJdbcTemplate namedParameterJdbcTemplate) {
    this.namedParameterJdbcTemplate = namedParameterJdbcTemplate;
  }

  private static ModuleInnerConfiguration mapRow(final ResultSet resultSet, final int i) throws SQLException {
    Preconditions.checkNotNull(resultSet);

    final String className = resultSet.getString("class_name");

    final Map<String, InterpreterProperty> properties =
            new Gson().fromJson(
                    resultSet.getString("properties"),
                    new TypeToken<Map<String, InterpreterProperty>>() {
                    }.getType()
            );
    final Map<String, Object> editor =
            new Gson().fromJson(
                    resultSet.getString("editor"),
                    new TypeToken<Map<String, Object>>() {
                    }.getType()
            );
    return new ModuleInnerConfiguration(className, properties, editor);
  }

  public List<ModuleInnerConfiguration> getAll() {

    final SqlParameterSource parameters = new MapSqlParameterSource();

    return namedParameterJdbcTemplate.query(
            GET_BY_ID,
            parameters,
            ModuleInnerConfigurationDAO::mapRow
    );
  }

  public ModuleInnerConfiguration getById(final long id) {

    final SqlParameterSource parameters = new MapSqlParameterSource()
            .addValue("ID", id);

    return namedParameterJdbcTemplate.query(
            GET_BY_ID,
            parameters,
            ModuleInnerConfigurationDAO::mapRow)
            .stream()
            .findFirst()
            .orElse(null);
  }

  public ModuleInnerConfiguration persist(final ModuleInnerConfiguration config) {
    final KeyHolder holder = new GeneratedKeyHolder();

    final MapSqlParameterSource parameters = new MapSqlParameterSource()
            .addValue("CLASS_NAME", config.getClassName())
            .addValue("PROPERTIES", Utils.generatePGjson(config.getProperties()))
            .addValue("EDITOR", Utils.generatePGjson(config.getEditor()));
    namedParameterJdbcTemplate.update(PERSIST, parameters, holder);

    config.setId((Long) holder.getKeys().get("id"));
    return config;
  }


  private ModuleInnerConfiguration update(final ModuleInnerConfiguration config) {

    final MapSqlParameterSource parameters = new MapSqlParameterSource()
            .addValue("ID", config.getId())
            .addValue("CLASS_NAME", config.getClassName())
            .addValue("PROPERTIES", Utils.generatePGjson(config.getProperties()))
            .addValue("EDITOR", Utils.generatePGjson(config.getEditor()));
    namedParameterJdbcTemplate.update(UPDATE, parameters);

    return config;
  }

  public void delete(final long id) {

    final SqlParameterSource parameters = new MapSqlParameterSource()
            .addValue("ID", id);

    namedParameterJdbcTemplate.update(DELETE, parameters);
  }
}
