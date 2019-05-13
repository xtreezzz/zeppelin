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
package ru.tinkoff.zeppelin.storage;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.LocalDateTime;
import java.util.List;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import org.springframework.jdbc.core.namedparam.SqlParameterSource;
import org.springframework.stereotype.Component;
import ru.tinkoff.zeppelin.SystemEvent;
import ru.tinkoff.zeppelin.storage.SystemEventType.ET;

/**
 * Data Access Object for {@link SystemEvent}
 */
@Component
public class SystemEventDAO {

  private static final String GET_ALL_TYPES = "" +
      "SELECT ID,\n" +
      "       NAME\n" +
      "FROM SYSTEM_EVENT_TYPE;";

  private static final String GET_ALL_EVENTS = "" +
      "SELECT ID,\n" +
      "       USERNAME,\n" +
      "       EVENT_TYPE,\n" +
      "       MESSAGE,\n" +
      "       DESCRIPTION,\n"+
      "       ACTION_TIME\n" +
      "FROM SYSTEM_EVENT\n" +
      "ORDER BY ACTION_TIME DESC\n" +
      "LIMIT 1000;";

  private static final String PERSIST =
      "INSERT INTO SYSTEM_EVENT (USERNAME,\n "
          + "                    EVENT_TYPE,\n "
          + "                    MESSAGE,\n "
          + "                    DESCRIPTION,\n "
          + "                    ACTION_TIME)\n "
          + "VALUES (:USERNAME,\n"
          + "        :EVENT_TYPE,\n"
          + "        :MESSAGE,\n"
          + "        :DESCRIPTION,\n"
          + "        :ACTION_TIME);";

  private static final String GET_EVENT_TYPE_BY_NAME = "" +
      "SELECT ID,\n" +
      "       NAME\n" +
      "FROM SYSTEM_EVENT_TYPE\n" +
      "WHERE NAME = :NAME";

  private static final String GET_EVENT_TYPE_BY_ID = "" +
      "SELECT ID,\n" +
      "       NAME\n" +
      "FROM SYSTEM_EVENT_TYPE\n" +
      "WHERE ID = :ID";

  @Nonnull
  private final NamedParameterJdbcTemplate jdbcTemplate;

  @Autowired
  public SystemEventDAO(@Nonnull final NamedParameterJdbcTemplate jdbcTemplate) {
    this.jdbcTemplate = jdbcTemplate;
  }

  public List<SystemEventType> getAllTypes() {
    final SqlParameterSource parameters = new MapSqlParameterSource();

    return jdbcTemplate.query(
        GET_ALL_TYPES,
        parameters,
        SystemEventDAO::mapTypeRow
    );
  }

  @Nullable
  public SystemEventType getTypeByName(@Nonnull final String name) {

    final SqlParameterSource parameters = new MapSqlParameterSource()
        .addValue("NAME", name);

    return jdbcTemplate.query(
        GET_EVENT_TYPE_BY_NAME,
        parameters,
        SystemEventDAO::mapTypeRow)
        .stream()
        .findFirst()
        .orElse(null);
  }

  @Nullable
  public SystemEventType getTypeById(final long id) {

    final SqlParameterSource parameters = new MapSqlParameterSource()
        .addValue("ID", id);

    return jdbcTemplate.query(
        GET_EVENT_TYPE_BY_ID,
        parameters,
        SystemEventDAO::mapTypeRow)
        .stream()
        .findFirst()
        .orElse(null);
  }

  public List<SystemEvent> getAllEvents() {
    final SqlParameterSource parameters = new MapSqlParameterSource();

    return jdbcTemplate.query(
        GET_ALL_EVENTS,
        parameters,
        SystemEventDAO::mapRow
    );
  }

  SystemEvent persist(@Nonnull final SystemEventDTO event) {
    final SystemEventType type = getTypeByName(event.getEventType());
    if (type == null) {
      throw new RuntimeException(String.format("Type %s is not defined", event.getEventType()));
    }

    final MapSqlParameterSource parameters = new MapSqlParameterSource()
        .addValue("USERNAME", event.getUsername())
        .addValue("EVENT_TYPE", type.getDatabaseId())
        .addValue("MESSAGE", event.getMessage())
        .addValue("DESCRIPTION", event.getDescription())
        .addValue("ACTION_TIME", event.getActionTime());

    jdbcTemplate.update(PERSIST, parameters);
    return new SystemEvent(type.getDatabaseId(), event.getUsername(), event.getMessage(), event.getDescription(), event.getActionTime());
  }

  private static SystemEvent mapRow(final ResultSet resultSet, final int i) throws SQLException {

    return new SystemEvent(
        resultSet.getLong("EVENT_TYPE"),
        resultSet.getString("USERNAME"),
        resultSet.getString("MESSAGE"),
        resultSet.getString("DESCRIPTION"),
        resultSet.getObject("ACTION_TIME", LocalDateTime.class)
    );
  }

  private static SystemEventType mapTypeRow(final ResultSet resultSet, final int i) throws SQLException {

    return new SystemEventType(
        ET.valueOf(resultSet.getString("NAME")),
        resultSet.getLong("ID")
    );
  }
}
