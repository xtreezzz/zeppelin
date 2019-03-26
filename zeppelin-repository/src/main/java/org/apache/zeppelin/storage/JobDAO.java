package org.apache.zeppelin.storage;

import org.apache.zeppelin.notebook.Job;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import org.springframework.jdbc.core.namedparam.SqlParameterSource;
import org.springframework.jdbc.support.GeneratedKeyHolder;
import org.springframework.jdbc.support.KeyHolder;
import org.springframework.stereotype.Component;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.LocalDateTime;
import java.util.Comparator;
import java.util.LinkedList;
import java.util.List;

@Component
public class JobDAO {

  private static final String PERSIST_JOB = "" +
          "INSERT INTO JOB (BATCH_ID,\n" +
          "                 NOTE_ID,\n" +
          "                 PARAGRAPH_ID,\n" +
          "                 INDEX_NUMBER,\n" +
          "                 SHEBANG,\n" +
          "                 STATUS,\n" +
          "                 INTERPRETER_PROCESS_UUID,\n" +
          "                 INTERPRETER_JOB_UUID,\n" +
          "                 CREATED_AT,\n" +
          "                 STARTED_AT,\n" +
          "                 ENDED_AT)\n" +
          "VALUES (:BATCH_ID,\n" +
          "        :NOTE_ID,\n" +
          "        :PARAGRAPH_ID,\n" +
          "        :INDEX_NUMBER,\n" +
          "        :SHEBANG,\n" +
          "        :STATUS,\n" +
          "        :INTERPRETER_PROCESS_UUID,\n" +
          "        :INTERPRETER_JOB_UUID,\n" +
          "        :CREATED_AT,\n" +
          "        :STARTED_AT, :ENDED_AT);";

  private static final String UPDATE_JOB = "" +
          "UPDATE JOB\n" +
          "SET BATCH_ID                 = :BATCH_ID,\n" +
          "    NOTE_ID                  = :NOTE_ID,\n" +
          "    PARAGRAPH_ID             = :PARAGRAPH_ID,\n" +
          "    INDEX_NUMBER             = :INDEX_NUMBER,\n" +
          "    SHEBANG                  = :SHEBANG,\n" +
          "    STATUS                   = :STATUS,\n" +
          "    INTERPRETER_PROCESS_UUID = :INTERPRETER_PROCESS_UUID,\n" +
          "    INTERPRETER_JOB_UUID     = :INTERPRETER_JOB_UUID,\n" +
          "    CREATED_AT               = :CREATED_AT,\n" +
          "    STARTED_AT               = :STARTED_AT,\n" +
          "    ENDED_AT                 = :ENDED_AT\n" +
          "WHERE ID = :ID;";

  private static final String SELECT_JOB = "" +
          "SELECT ID,\n" +
          "       BATCH_ID,\n" +
          "       NOTE_ID,\n" +
          "       PARAGRAPH_ID,\n" +
          "       INDEX_NUMBER,\n" +
          "       SHEBANG,\n" +
          "       STATUS,\n" +
          "       INTERPRETER_PROCESS_UUID,\n" +
          "       INTERPRETER_JOB_UUID,\n" +
          "       CREATED_AT,\n" +
          "       STARTED_AT,\n" +
          "       ENDED_AT\n" +
          "FROM JOB\n" +
          "WHERE ID = :ID;";

  private static final String SELECT_JOB_BY_INTERPRETER_JOB_UUID = "" +
          "SELECT ID,\n" +
          "       BATCH_ID,\n" +
          "       NOTE_ID,\n" +
          "       PARAGRAPH_ID,\n" +
          "       INDEX_NUMBER,\n" +
          "       SHEBANG,\n" +
          "       STATUS,\n" +
          "       INTERPRETER_PROCESS_UUID,\n" +
          "       INTERPRETER_JOB_UUID,\n" +
          "       CREATED_AT,\n" +
          "       STARTED_AT,\n" +
          "       ENDED_AT\n" +
          "FROM JOB\n" +
          "WHERE INTERPRETER_JOB_UUID = :INTERPRETER_JOB_UUID;";

  private static final String SELECT_JOBS_BY_BATCH = "" +
          "SELECT ID,\n" +
          "       BATCH_ID,\n" +
          "       NOTE_ID,\n" +
          "       PARAGRAPH_ID,\n" +
          "       INDEX_NUMBER,\n" +
          "       SHEBANG,\n" +
          "       STATUS,\n" +
          "       INTERPRETER_PROCESS_UUID,\n" +
          "       INTERPRETER_JOB_UUID,\n" +
          "       CREATED_AT,\n" +
          "       STARTED_AT,\n" +
          "       ENDED_AT\n" +
          "FROM JOB\n" +
          "WHERE BATCH_ID = :BATCH_ID" +
          " ORDER BY INDEX_NUMBER;";

  private static final String SELECT_READY_TO_EXECUTE_JOBS = "" +
          "SELECT DISTINCT ON (J.BATCH_ID)\n" +
          "  J.ID,\n" +
          "  J.BATCH_ID,\n" +
          "  J.NOTE_ID,\n" +
          "  J.PARAGRAPH_ID,\n" +
          "  J.INDEX_NUMBER,\n" +
          "  J.SHEBANG,\n" +
          "  J.STATUS,\n" +
          "  J.INTERPRETER_PROCESS_UUID,\n" +
          "  J.INTERPRETER_JOB_UUID,\n" +
          "  J.CREATED_AT,\n" +
          "  J.STARTED_AT,\n" +
          "  J.ENDED_AT\n" +
          "FROM JOB_BATCH JB\n" +
          "  LEFT JOIN JOB J ON JB.ID = J.BATCH_ID\n" +
          "WHERE JB.STATUS IN ('PENDING', 'RUNNING')\n" +
          "      AND NOT EXISTS(SELECT * FROM JOB WHERE J.BATCH_ID = JB.ID AND J.STATUS IN('RUNNING', 'ERROR'))\n" +
          "ORDER BY J.BATCH_ID, J.INDEX_NUMBER;";


  private static final String SELECT_JOBS_WITH_DEAD_INTERPRETER = "" +
          "SELECT J.ID,\n" +
          "       J.BATCH_ID,\n" +
          "       J.NOTE_ID,\n" +
          "       J.PARAGRAPH_ID,\n" +
          "       J.INDEX_NUMBER,\n" +
          "       J.SHEBANG,\n" +
          "       J.STATUS,\n" +
          "       J.INTERPRETER_PROCESS_UUID,\n" +
          "       J.INTERPRETER_JOB_UUID,\n" +
          "       J.CREATED_AT,\n" +
          "       J.STARTED_AT,\n" +
          "       J.ENDED_AT\n" +
          "FROM JOB_BATCH JB\n" +
          "       LEFT JOIN JOB J ON JB.ID = J.BATCH_ID\n" +
          "WHERE JB.STATUS IN ('PENDING', 'RUNNING')\n" +
          "  AND J.INTERPRETER_PROCESS_UUID = :INTERPRETER_PROCESS_UUID;";

  private static final String SELECT_READY_TO_CANCEL_JOBS = "" +
          "SELECT J.ID,\n" +
          "       J.BATCH_ID,\n" +
          "       J.NOTE_ID,\n" +
          "       J.PARAGRAPH_ID,\n" +
          "       J.INDEX_NUMBER,\n" +
          "       J.SHEBANG,\n" +
          "       J.STATUS,\n" +
          "       J.INTERPRETER_PROCESS_UUID,\n" +
          "       J.INTERPRETER_JOB_UUID,\n" +
          "       J.CREATED_AT,\n" +
          "       J.STARTED_AT,\n" +
          "       J.ENDED_AT\n" +
          "FROM JOB_BATCH JB\n" +
          "       LEFT JOIN JOB J ON JB.ID = J.BATCH_ID\n" +
          "WHERE JB.STATUS IN ('ABORTING')\n" +
          "  AND J.STATUS IN('RUNNING');";

  private final static String RESTORE_JOBS_AFTER_SHUTDOWN = "" +
          "UPDATE JOB SET STATUS = 'PENDING',\n" +
          "               INTERPRETER_JOB_UUID = NULL,\n" +
          "               INTERPRETER_PROCESS_UUID = NULL\n" +
          "WHERE JOB.STATUS = 'RUNNING'";


  @Autowired
  private NamedParameterJdbcTemplate namedParameterJdbcTemplate;

  public Job persist(final Job job) {
    final KeyHolder holder = new GeneratedKeyHolder();
    final SqlParameterSource parameters = new MapSqlParameterSource()
            .addValue("BATCH_ID", job.getBatchId())
            .addValue("NOTE_ID", job.getNoteId())
            .addValue("PARAGRAPH_ID", job.getParagpaphId())
            .addValue("INDEX_NUMBER", job.getIndex())
            .addValue("SHEBANG", job.getShebang())
            .addValue("STATUS", job.getStatus().name())
            .addValue("INTERPRETER_PROCESS_UUID", job.getInterpreterProcessUUID())
            .addValue("INTERPRETER_JOB_UUID", job.getInterpreterJobUUID())
            .addValue("CREATED_AT", job.getCreatedAt())
            .addValue("STARTED_AT", job.getStartedAt())
            .addValue("ENDED_AT", job.getEndedAt());
    namedParameterJdbcTemplate.update(PERSIST_JOB, parameters, holder);

    job.setId((Long) holder.getKeys().get("id"));
    return job;
  }

  public Job update(final Job job) {
    final SqlParameterSource parameters = new MapSqlParameterSource()
            .addValue("BATCH_ID", job.getBatchId())
            .addValue("NOTE_ID", job.getNoteId())
            .addValue("PARAGRAPH_ID", job.getParagpaphId())
            .addValue("INDEX_NUMBER", job.getIndex())
            .addValue("SHEBANG", job.getShebang())
            .addValue("STATUS", job.getStatus().name())
            .addValue("INTERPRETER_PROCESS_UUID", job.getInterpreterProcessUUID())
            .addValue("INTERPRETER_JOB_UUID", job.getInterpreterJobUUID())
            .addValue("CREATED_AT", job.getCreatedAt())
            .addValue("STARTED_AT", job.getStartedAt())
            .addValue("ENDED_AT", job.getEndedAt())
            .addValue("ID", job.getId());
    namedParameterJdbcTemplate.update(UPDATE_JOB, parameters);
    return job;
  }

  private static Job mapRow(final ResultSet resultSet, final int i) throws SQLException {
    final Long id = resultSet.getLong("ID");
    final Long batch_id = resultSet.getLong("BATCH_ID");
    final Long noteId = resultSet.getLong("NOTE_ID");
    final Long paragraphId = resultSet.getLong("PARAGRAPH_ID");
    final Integer index = resultSet.getInt("INDEX_NUMBER");
    final String shebang = resultSet.getString("SHEBANG");
    final Job.Status status = Job.Status.valueOf(resultSet.getString("STATUS"));
    final String interpreter_process_uuid = resultSet.getString("INTERPRETER_PROCESS_UUID");
    final String interpreter_job_uuid = resultSet.getString("INTERPRETER_JOB_UUID");

    final LocalDateTime createdAt =
            null != resultSet.getTimestamp("CREATED_AT")
                    ? resultSet.getTimestamp("CREATED_AT").toLocalDateTime()
                    : null;
    final LocalDateTime startedAt =
            null != resultSet.getTimestamp("STARTED_AT")
                    ? resultSet.getTimestamp("STARTED_AT").toLocalDateTime()
                    : null;

    final LocalDateTime endedAt =
            null != resultSet.getTimestamp("ENDED_AT")
                    ? resultSet.getTimestamp("ENDED_AT").toLocalDateTime()
                    : null;

    final Job job = new Job();
    job.setId(id);
    job.setBatchId(batch_id);
    job.setNoteId(noteId);
    job.setParagpaphId(paragraphId);
    job.setIndex(index);
    job.setShebang(shebang);
    job.setStatus(status);
    job.setInterpreterProcessUUID(interpreter_process_uuid);
    job.setInterpreterJobUUID(interpreter_job_uuid);
    job.setCreatedAt(createdAt);
    job.setStartedAt(startedAt);
    job.setEndedAt(endedAt);
    return job;
  }

  public Job get(final Long jobId) {
    final SqlParameterSource parameters = new MapSqlParameterSource()
            .addValue("ID", jobId);

    return namedParameterJdbcTemplate.query(
            SELECT_JOB,
            parameters,
            JobDAO::mapRow)
            .stream()
            .findFirst()
            .orElse(null);
  }

  public Job getByInterpreterJobUUID(final String interpreterJobUUID) {
    final SqlParameterSource parameters = new MapSqlParameterSource()
            .addValue("INTERPRETER_JOB_UUID", interpreterJobUUID);

    return namedParameterJdbcTemplate.query(
            SELECT_JOB_BY_INTERPRETER_JOB_UUID,
            parameters,
            JobDAO::mapRow)
            .stream()
            .findFirst()
            .orElse(null);
  }

  public LinkedList<Job> loadNextPending() {
    final SqlParameterSource parameters = new MapSqlParameterSource();

    final LinkedList<Job> jobs = new LinkedList<>(namedParameterJdbcTemplate.query(
            SELECT_READY_TO_EXECUTE_JOBS,
            parameters,
            JobDAO::mapRow));
    jobs.sort(Comparator.comparing(Job::getIndex));
    return jobs;
  }

  public List<Job> loadByBatch(final Long batchId) {
    final SqlParameterSource parameters = new MapSqlParameterSource()
            .addValue("BATCH_ID", batchId);

    return namedParameterJdbcTemplate.query(
            SELECT_JOBS_BY_BATCH,
            parameters,
            JobDAO::mapRow);
  }

  public List<Job> loadJobsByInterpreterProcessUUID(final String interpreterProcessUUID) {
    final SqlParameterSource parameters = new MapSqlParameterSource()
            .addValue("INTERPRETER_PROCESS_UUID", interpreterProcessUUID);

    return namedParameterJdbcTemplate.query(
            SELECT_JOBS_WITH_DEAD_INTERPRETER,
            parameters,
            JobDAO::mapRow);
  }

  public List<Job> loadNextCancelling() {
    final SqlParameterSource parameters = new MapSqlParameterSource();

    return namedParameterJdbcTemplate.query(
            SELECT_READY_TO_CANCEL_JOBS,
            parameters,
            JobDAO::mapRow);
  }

  public void restoreState() {
    final SqlParameterSource parameters = new MapSqlParameterSource();
    namedParameterJdbcTemplate.update(
            RESTORE_JOBS_AFTER_SHUTDOWN,
            parameters);
  }
}
