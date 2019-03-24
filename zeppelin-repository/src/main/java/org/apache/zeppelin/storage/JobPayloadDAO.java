package org.apache.zeppelin.storage;

import org.apache.zeppelin.notebook.JobPayload;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import org.springframework.jdbc.core.namedparam.SqlParameterSource;
import org.springframework.jdbc.support.GeneratedKeyHolder;
import org.springframework.jdbc.support.KeyHolder;
import org.springframework.stereotype.Component;

import java.sql.ResultSet;
import java.sql.SQLException;

@Component
public class JobPayloadDAO {

    @Autowired
    private NamedParameterJdbcTemplate namedParameterJdbcTemplate;


    private static final String PERISIT_PAYLOAD = "INSERT INTO JOB_PAYLOAD (JOB_ID, PAYLOAD) VALUES (:JOB_ID, :PAYLOAD);";

    private static final String LOAD_PAYLOAD_BY_ID = "SELECT * FROM JOB_PAYLOAD WHERE ID = :ID;";

    private static final String LOAD_PAYLOAD_BY_JOB_ID = "SELECT * FROM JOB_PAYLOAD WHERE JOB_ID = :JOB_ID;";


    public JobPayload persist(final JobPayload jobPayload) {
        final KeyHolder holder = new GeneratedKeyHolder();
        final SqlParameterSource parameters = new MapSqlParameterSource()
                .addValue("JOB_ID", jobPayload.getJobId())
                .addValue("PAYLOAD", jobPayload.getPayload());
        namedParameterJdbcTemplate.update(PERISIT_PAYLOAD, parameters, holder);

        jobPayload.setId((Long) holder.getKeys().get("id"));
        return jobPayload;
    }

    private static JobPayload mapRow(final ResultSet resultSet, final int i) throws SQLException {
        final Long id = resultSet.getLong("ID");
        final Long jobId = resultSet.getLong("JOB_ID");
        final String payload = resultSet.getString("PAYLOAD");

        final JobPayload jobPayload = new JobPayload();
        jobPayload.setId(id);
        jobPayload.setJobId(jobId);
        jobPayload.setPayload(payload);
        return jobPayload;
    }

    public JobPayload get(final Long payloadId) {
        final SqlParameterSource parameters = new MapSqlParameterSource()
                .addValue("ID", payloadId);

        return namedParameterJdbcTemplate.query(
                LOAD_PAYLOAD_BY_ID,
                parameters,
                JobPayloadDAO::mapRow)
                .stream()
                .findFirst()
                .orElse(null);
    }

    public JobPayload getByJobId(final Long identity) {
        final SqlParameterSource parameters = new MapSqlParameterSource()
                .addValue("JOB_ID", identity);

        return namedParameterJdbcTemplate.query(
                LOAD_PAYLOAD_BY_JOB_ID,
                parameters,
                JobPayloadDAO::mapRow)
                .stream()
                .findFirst()
                .orElse(null);
    }
}
