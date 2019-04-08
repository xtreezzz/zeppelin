package org.apache.zeppelin.storage;

import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;
import org.apache.zeppelin.notebook.Scheduler;
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import org.springframework.jdbc.core.namedparam.SqlParameterSource;
import org.springframework.jdbc.support.GeneratedKeyHolder;
import org.springframework.jdbc.support.KeyHolder;
import org.springframework.stereotype.Component;

import java.lang.reflect.Type;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.LocalDateTime;
import java.util.List;

@Component
public class SchedulerDAO {

  private static final String PERSIST = "" +
          "INSERT INTO SCHEDULER(NOTE_ID,\n" +
          "                      ENABLED,\n" +
          "                      EXPRESSION,\n" +
          "                      USER_NAME,\n" +
          "                      USER_ROLES,\n" +
          "                      LAST_EXECUTION,\n" +
          "                      NEXT_EXECUTION)\n" +
          "VALUES (:NOTE_ID,\n" +
          "        :ENABLED,\n" +
          "        :EXPRESSION,\n" +
          "        :USER_NAME,\n" +
          "        :USER_ROLES,\n" +
          "        :LAST_EXECUTION,\n" +
          "        :NEXT_EXECUTION);";


  private static final String UPDATE = "" +
          "UPDATE SCHEDULER\n" +
          "SET ENABLED        = :ENABLED,\n" +
          "    EXPRESSION     = :EXPRESSION,\n" +
          "    USER_NAME      = :USER_NAME,\n" +
          "    USER_ROLES     = :USER_ROLES,\n" +
          "    LAST_EXECUTION = :LAST_EXECUTION,\n" +
          "    NEXT_EXECUTION = :NEXT_EXECUTION\n" +
          "WHERE ID = :ID;";

  private static final String SELECT_BY_ID = "" +
          "SELECT ID,\n" +
          "       NOTE_ID,\n" +
          "       ENABLED,\n" +
          "       EXPRESSION,\n" +
          "       USER_NAME,\n" +
          "       USER_ROLES,\n" +
          "       LAST_EXECUTION,\n" +
          "       NEXT_EXECUTION\n" +
          "FROM SCHEDULER\n" +
          "WHERE ID = :ID;";

  private static final String SELECT_BY_NOTE_ID = "" +
          "SELECT ID,\n" +
          "       NOTE_ID,\n" +
          "       ENABLED,\n" +
          "       EXPRESSION,\n" +
          "       USER_NAME,\n" +
          "       USER_ROLES,\n" +
          "       LAST_EXECUTION,\n" +
          "       NEXT_EXECUTION\n" +
          "FROM SCHEDULER\n" +
          "WHERE NOTE_ID = :NOTE_ID;";

  private static final String SELECT_READY_TO_EXECUTE = "" +
          "SELECT ID,\n" +
          "       NOTE_ID,\n" +
          "       ENABLED,\n" +
          "       EXPRESSION,\n" +
          "       USER_NAME,\n" +
          "       USER_ROLES,\n" +
          "       LAST_EXECUTION,\n" +
          "       NEXT_EXECUTION\n" +
          "FROM SCHEDULER\n" +
          "WHERE NEXT_EXECUTION < :NEXT_EXECUTION\n" +
          "AND ENABLED = 'TRUE'";

  private final NamedParameterJdbcTemplate namedParameterJdbcTemplate;

  private final static Gson gson = new Gson();

  public SchedulerDAO(final NamedParameterJdbcTemplate namedParameterJdbcTemplate) {
    this.namedParameterJdbcTemplate = namedParameterJdbcTemplate;
  }

  private static Scheduler mapRow(final ResultSet resultSet, final int i) throws SQLException {
    Type rolesListType = new TypeToken<List<String>>() {
    }.getType();

    final Long id = resultSet.getLong("ID");
    final Long noteId = resultSet.getLong("NOTE_ID");
    final boolean isEnabled = resultSet.getBoolean("ENABLED");
    final String expression = resultSet.getString("EXPRESSION");
    final String userName = resultSet.getString("USER_NAME");
    final List<String> userRoles = new Gson().fromJson(resultSet.getString("USER_ROLES"), rolesListType);

    final LocalDateTime lastExecution =
            null != resultSet.getTimestamp("LAST_EXECUTION")
                    ? resultSet.getTimestamp("LAST_EXECUTION").toLocalDateTime()
                    : null;
    final LocalDateTime nextExecution =
            null != resultSet.getTimestamp("NEXT_EXECUTION")
                    ? resultSet.getTimestamp("NEXT_EXECUTION").toLocalDateTime()
                    : null;

    return new Scheduler(
            id,
            noteId,
            isEnabled,
            expression,
            userName,
            userRoles,
            lastExecution,
            nextExecution
    );
  }

  public Scheduler persist(final Scheduler scheduler) {
    final KeyHolder holder = new GeneratedKeyHolder();
    final SqlParameterSource parameters = new MapSqlParameterSource()
            .addValue("NOTE_ID", scheduler.getNoteId())
            .addValue("ENABLED", scheduler.isEnabled())
            .addValue("EXPRESSION", scheduler.getExpression())
            .addValue("USER_NAME", scheduler.getUser())
            .addValue("USER_ROLES", gson.toJson(scheduler.getRoles()))
            .addValue("LAST_EXECUTION", scheduler.getLastExecution())
            .addValue("NEXT_EXECUTION", scheduler.getNextExecution());
    namedParameterJdbcTemplate.update(PERSIST, parameters, holder);

    return new Scheduler(
            (Long) holder.getKeys().get("id"),
            scheduler.getNoteId(),
            scheduler.isEnabled(),
            scheduler.getExpression(),
            scheduler.getUser(),
            scheduler.getRoles(),
            scheduler.getLastExecution(),
            scheduler.getNextExecution()
    );
  }

  public Scheduler update(final Scheduler scheduler) {
    final SqlParameterSource parameters = new MapSqlParameterSource()
            .addValue("ENABLED", scheduler.isEnabled())
            .addValue("EXPRESSION", scheduler.getExpression())
            .addValue("USER_NAME", scheduler.getUser())
            .addValue("USER_ROLES", gson.toJson(scheduler.getRoles()))
            .addValue("LAST_EXECUTION", scheduler.getLastExecution())
            .addValue("NEXT_EXECUTION", scheduler.getNextExecution())
            .addValue("ID", scheduler.getId());
    namedParameterJdbcTemplate.update(UPDATE, parameters);
    return scheduler;
  }

  public Scheduler get(final Long id) {
    final SqlParameterSource parameters = new MapSqlParameterSource()
            .addValue("ID", id);

    return namedParameterJdbcTemplate.query(
            SELECT_BY_ID,
            parameters,
            SchedulerDAO::mapRow)
            .stream()
            .findFirst()
            .orElse(null);
  }

  public Scheduler getByNote(final Long noteId) {
    final SqlParameterSource parameters = new MapSqlParameterSource()
            .addValue("NOTE_ID", noteId);

    return namedParameterJdbcTemplate.query(
            SELECT_BY_NOTE_ID,
            parameters,
            SchedulerDAO::mapRow)
            .stream()
            .findFirst()
            .orElse(null);
  }


  public List<Scheduler> getReadyToExecute(final LocalDateTime time) {
    final SqlParameterSource parameters = new MapSqlParameterSource()
            .addValue("NEXT_EXECUTION", time);

    return namedParameterJdbcTemplate.query(
            SELECT_READY_TO_EXECUTE,
            parameters,
            SchedulerDAO::mapRow);
  }
}
