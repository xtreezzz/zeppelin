package org.apache.zeppelin.storage;

import org.apache.zeppelin.notebook.JobBatch;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import org.springframework.jdbc.core.namedparam.SqlParameterSource;
import org.springframework.jdbc.support.GeneratedKeyHolder;
import org.springframework.jdbc.support.KeyHolder;
import org.springframework.stereotype.Component;

import java.time.LocalDateTime;

@Component
public class JobBatchDAO {

    @Autowired
    private NamedParameterJdbcTemplate namedParameterJdbcTemplate;

    private static final String PERSIST_BATCH = "INSERT INTO JOB_BATCH (NOTE_ID,\n" +
            "                       STATUS,\n" +
            "                       CREATED_AT,\n" +
            "                       STARTED_AT,\n" +
            "                       ENDED_AT)\n" +
            "            VALUES (:NOTE_ID,\n" +
            "                    :STATUS,\n" +
            "                    :CREATED_AT,\n" +
            "                    :STARTED_AT,\n" +
            "                    :ENDED_AT);";

    private static final String UPDATE_BATCH = "UPDATE JOB_BATCH\n" +
            "SET STATUS   = :STATUS,\n" +
            "  CREATED_AT = :CREATED_AT,\n" +
            "  STARTED_AT = :STARTED_AT,\n" +
            "  ENDED_AT   = :ENDED_AT\n" +
            "WHERE ID = :ID;";

    private static final String LOAD_BATCH_BY_ID = "SELECT * FROM job_batch WHERE ID = :ID;";


    public JobBatch persist(final JobBatch jobBatch) {
        final KeyHolder holder = new GeneratedKeyHolder();
        final SqlParameterSource parameters = new MapSqlParameterSource()
                .addValue("NOTE_ID", jobBatch.getNoteId())
                .addValue("STATUS", jobBatch.getStatus().name())
                .addValue("CREATED_AT", jobBatch.getCreatedAt())
                .addValue("STARTED_AT", jobBatch.getStartedAt())
                .addValue("ENDED_AT", jobBatch.getEndedAt());
        namedParameterJdbcTemplate.update(PERSIST_BATCH, parameters, holder);

        jobBatch.setId((Long) holder.getKeys().get("id"));
        return jobBatch;
    }

    public JobBatch update(final JobBatch jobBatch) {
        final SqlParameterSource parameters = new MapSqlParameterSource()
                .addValue("NOTE_ID", jobBatch.getNoteId())
                .addValue("STATUS", jobBatch.getStatus().name())
                .addValue("CREATED_AT", jobBatch.getCreatedAt())
                .addValue("STARTED_AT", jobBatch.getStartedAt())
                .addValue("ENDED_AT", jobBatch.getEndedAt())
                .addValue("ID", jobBatch.getId());
        namedParameterJdbcTemplate.update(UPDATE_BATCH, parameters);
        return jobBatch;
    }

    public JobBatch get(final Long batchId) {
        final SqlParameterSource parameters = new MapSqlParameterSource()
                .addValue("ID", batchId);
        return namedParameterJdbcTemplate.queryForObject(LOAD_BATCH_BY_ID, parameters, (resultSet, i) -> {
            final Long id = resultSet.getLong("ID");
            final Long noteId = resultSet.getLong("NOTE_ID");
            final JobBatch.Status status = JobBatch.Status.valueOf(resultSet.getString("STATUS"));

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

            final JobBatch jobBatch = new JobBatch();
            jobBatch.setId(id);
            jobBatch.setNoteId(noteId);
            jobBatch.setStatus(status);
            jobBatch.setCreatedAt(createdAt);
            jobBatch.setStartedAt(startedAt);
            jobBatch.setEndedAt(endedAt);
            return jobBatch;
        });
    }
}
