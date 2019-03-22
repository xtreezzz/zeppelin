package org.apache.zeppelin.storage;

import org.apache.zeppelin.notebook.JobResult;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import org.springframework.jdbc.core.namedparam.SqlParameterSource;
import org.springframework.jdbc.support.GeneratedKeyHolder;
import org.springframework.jdbc.support.KeyHolder;
import org.springframework.stereotype.Component;

import java.time.LocalDateTime;

@Component
public class JobResultDAO {

  @Autowired
  private NamedParameterJdbcTemplate namedParameterJdbcTemplate;


  private static final String PERISIT_PAYLOAD = "" +
          "INSERT INTO JOB_RESULT (JOB_ID,\n" +
          "                        CREATED_AT,\n" +
          "                        TYPE,\n" +
          "                        RESULT)\n" +
          "VALUES (:JOB_ID,\n" +
          "        :CREATED_AT,\n" +
          "        :TYPE,\n" +
          "        :RESULT);";

  private static final String LOAD_PAYLOAD_BY_ID = "SELECT ID,\n" +
          "       JOB_ID,\n" +
          "       CREATED_AT,\n" +
          "       TYPE,\n" +
          "       RESULT\n" +
          "FROM JOB_RESULT\n" +
          "WHERE ID = :ID;";


  public JobResult persist(final JobResult jobResult) {
    final KeyHolder holder = new GeneratedKeyHolder();
    final SqlParameterSource parameters = new MapSqlParameterSource()
            .addValue("JOB_ID", jobResult.getJobId())
            .addValue("CREATED_AT", jobResult.getCreatedAt())
            .addValue("TYPE", jobResult.getType())
            .addValue("RESULT", jobResult.getResult());
    namedParameterJdbcTemplate.update(PERISIT_PAYLOAD, parameters, holder);

    jobResult.setId((Long) holder.getKeys().get("id"));
    return jobResult;
  }

  public JobResult get(final Long jobResultId) {
    final SqlParameterSource parameters = new MapSqlParameterSource()
            .addValue("ID", jobResultId);
    return namedParameterJdbcTemplate.queryForObject(LOAD_PAYLOAD_BY_ID, parameters, (resultSet, i) -> {
      final Long id = resultSet.getLong("ID");
      final Long jobId = resultSet.getLong("JOB_ID");
      final LocalDateTime created_at = resultSet.getTimestamp("CREATED_AT").toLocalDateTime();
      final String type = resultSet.getString("TYPE");
      final String result = resultSet.getString("RESULT");

      final JobResult jobResult = new JobResult();
      jobResult.setId(id);
      jobResult.setJobId(jobId);
      jobResult.setCreatedAt(created_at);
      jobResult.setType(type);
      jobResult.setResult(result);
      return jobResult;
    });
  }
}
