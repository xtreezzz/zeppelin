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
import java.util.List;
import javax.annotation.Nonnull;
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import org.springframework.jdbc.core.namedparam.SqlParameterSource;
import org.springframework.stereotype.Component;

@Component
public class FavoriteNotesDAO {

  private static final String GET_USER_FAV_NOTES = "" +
      "SELECT ID,\n" +
      "       NOTE_UUID,\n" +
      "       USER_NAME\n" +
      "FROM FAVORITE_NOTES\n" +
      "WHERE USER_NAME = :USER_NAME;";

  private static final String ADD_TO_FAV =  "" +
      "INSERT INTO FAVORITE_NOTES (NOTE_UUID,\n" +
      "                            USER_NAME)\n" +
      "VALUES (:NOTE_UUID,\n" +
      "        :USER_NAME);";

  private static final String DELETE = "" +
      "DELETE\n" +
      "FROM FAVORITE_NOTES\n" +
      "WHERE NOTE_UUID = :NOTE_UUID AND USER_NAME = :USER_NAME;";

  private final NamedParameterJdbcTemplate jdbcTemplate;


  public FavoriteNotesDAO(final NamedParameterJdbcTemplate jdbcTemplate) {
    this.jdbcTemplate = jdbcTemplate;
  }

  private static String mapRow(final ResultSet resultSet, final int i) throws SQLException {
    return resultSet.getString("NOTE_UUID");
  }

  public void persist(final @Nonnull String uuid, @Nonnull final String username) {
    final SqlParameterSource parameters = new MapSqlParameterSource()
        .addValue("NOTE_UUID", uuid)
        .addValue("USER_NAME", username);
    jdbcTemplate.update(ADD_TO_FAV, parameters);
  }

  public void remove(final @Nonnull String uuid, @Nonnull final String username) {
    final SqlParameterSource parameters = new MapSqlParameterSource()
        .addValue("NOTE_UUID", uuid)
        .addValue("USER_NAME", username);
    jdbcTemplate.update(DELETE, parameters);
  }

  public List<String> getAll(final String username) {
    final SqlParameterSource parameters = new MapSqlParameterSource()
        .addValue("USER_NAME", username);

    return jdbcTemplate.query(
        GET_USER_FAV_NOTES,
        parameters,
        FavoriteNotesDAO::mapRow);
  }

}
