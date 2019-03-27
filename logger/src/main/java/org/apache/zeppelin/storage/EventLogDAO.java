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
import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.LocalDateTime;
import javax.annotation.Nonnull;
import org.apache.zeppelin.Logger.EventType;
import org.apache.zeppelin.SystemEvent;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import org.springframework.jdbc.support.GeneratedKeyHolder;
import org.springframework.jdbc.support.KeyHolder;
import org.springframework.stereotype.Component;

/**
 * Data Access Object for {@link SystemEvent}
 */
@Component
class EventLogDAO {

  @Nonnull
  private final NamedParameterJdbcTemplate jdbcTemplate;

  @Nonnull
  private final KeyHolder keyHolder;

  //====================== GET =====================
  private static final String GET_ALL_EVENTS =
      "SELECT ID,  FROM SYSTEM_EVENT\n "
          + "ORDER BY ACTION_TIME DESC;";

  private static final String GET_LAST_EVENT =
      "SELECT * FROM SYSTEM_EVENT\n "
          + "ORDER BY ACTION_TIME DESC\n "
          + "LIMIT 1;";

  //----------------- USER SPECIFIC ----------------
  private static final String GET_LAST_USER_EVENT =
      "SELECT * FROM SYSTEM_EVENT\n "
          + "WHERE USERNAME = :USERNAME\n "
          + "ORDER BY ACTION_TIME DESC\n "
          + "LIMIT 1";

  private static final String GET_ALL_USER_EVENTS =
      "SELECT * FROM SYSTEM_EVENT\n "
          + "WHERE USERNAME = :USERNAME\n "
          + "ORDER BY ACTION_TIME DESC;";

  private static final String GET_N_USER_EVENTS =
      "SELECT * FROM SYSTEM_EVENT\n "
          + "WHERE USERNAME = :USERNAME\n "
          + "ORDER BY ACTION_TIME DESC\n "
          + "LIMIT :N";

  //=================== INSERT ====================
  private static final String LOG =
      "INSERT INTO SYSTEM_EVENT (USERNAME,\n "
          + "                   EVENT_TYPE,\n "
          + "                   MESSAGE,\n "
          + "                   DESCRIPTION,\n "
          + "                   ACTION_TIME)\n "
          + "VALUES (:USERNAME,\n"
          + "        :EVENT_TYPE,\n"
          + "        :MESSAGE,\n"
          + "        :DESCRIPTION,\n"
          + "        :ACTION_TIME);";

  private static final String ADD_EVENT_TYPE_OR_RETURN_ID =
      "INSERT INTO SYSTEM_EVENT_TYPE (NAME)\n"
          + "          SELECT :NAME\n"
          + "            WHERE NOT EXISTS (\n"
          + "                   SELECT ID, NAME\n"
          + "                   FROM SYSTEM_EVENT_TYPE\n"
          + "                   WHERE NAME = :NAME\n"
          + "                   );\n";

  @Autowired
  public EventLogDAO(@Nonnull final NamedParameterJdbcTemplate jdbcTemplate) {
    Preconditions.checkNotNull(jdbcTemplate);
    this.jdbcTemplate = jdbcTemplate;
    this.keyHolder = new GeneratedKeyHolder();
  }

  private long getOrCreateSystemEventType(@Nonnull final EventType type) {
    Preconditions.checkNotNull(type);
    final int affectedRows =
        jdbcTemplate.update(ADD_EVENT_TYPE_OR_RETURN_ID, convertSystemEventTypeToParameters(type), keyHolder);

    if (affectedRows == 0) {
      throw new RuntimeException("Fail to save event type " + type.name());
    }
    return ((Long) keyHolder.getKeys().get("ID"));
  }

  SystemEvent log(@Nonnull final SystemEvent event) {
    Preconditions.checkNotNull(event);
    final int affectedRows =
        jdbcTemplate.update(LOG, convertSystemEventToParameters(event), keyHolder);

    if (affectedRows == 0) {
      // LOGGER.error
      throw new RuntimeException("Fail to save event " + event);
    }
    event.setDatabaseId((Long) keyHolder.getKeys().get("ID"));
    return event;
  }

  @Nonnull
  private MapSqlParameterSource convertSystemEventToParameters(
      @Nonnull final SystemEvent event) {
    Preconditions.checkNotNull(event);
    final MapSqlParameterSource parameters = new MapSqlParameterSource();
    try {
      parameters
          .addValue("USERNAME", event.getUsername())
          .addValue("EVENT_TYPE", getOrCreateSystemEventType(event.getType()))
          .addValue("MESSAGE", event.getMessage())
          .addValue("DESCRIPTION", event.getDescription())
          .addValue("ACTION_TIME", event.getActionTime());
    } catch (final Exception e) {
      throw new RuntimeException("Fail to convert event", e);
    }
    Preconditions.checkNotNull(parameters);
    return parameters;
  }

  @Nonnull
  private MapSqlParameterSource convertSystemEventTypeToParameters(
      @Nonnull final EventType type) {
    Preconditions.checkNotNull(type);
    final MapSqlParameterSource parameters = new MapSqlParameterSource();
    try {
      parameters.addValue("NAME", type.name());
    } catch (final Exception e) {
      throw new RuntimeException("Fail to convert event", e);
    }
    Preconditions.checkNotNull(parameters);
    return parameters;
  }

  @Nonnull
  private SystemEvent convertResultSetToSystemEvent(@Nonnull final ResultSet resultSet)
      throws SQLException {
    Preconditions.checkNotNull(resultSet);

    final long id = resultSet.getLong("ID");
    final String username = resultSet.getString("USERNAME");
    final EventType eventType = EventType.valueOf(resultSet.getString("EVENT_TYPE"));
    final String message = resultSet.getString("MESSAGE");
    final String description = resultSet.getString("DESCRIPTION");
    final LocalDateTime actionTime = resultSet.getObject("ACTION_TIME", LocalDateTime.class);

    Preconditions.checkNotNull(username);
    Preconditions.checkNotNull(eventType);
    Preconditions.checkNotNull(message);
    Preconditions.checkNotNull(actionTime);
    return new SystemEvent(id, eventType, username, message, description, actionTime);
  }
}
