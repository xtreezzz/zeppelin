package org.apache.zeppelin.storage;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.LoadingCache;
import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;
import org.apache.zeppelin.notebook.Paragraph;
import org.apache.zeppelin.notebook.display.GUI;
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
import java.util.Map;

import static org.apache.zeppelin.storage.Utils.generatePGjson;

@Component
public class ParagraphDAO {

  private final static String PERSIST_PARAGRAPH = "" +
          "INSERT INTO PARAGRAPHS (NOTE_ID,\n" +
          "                        UUID,\n" +
          "                        TITLE,\n" +
          "                        TEXT,\n" +
          "                        SHEBANG,\n" +
          "                        CREATED,\n" +
          "                        UPDATED,\n" +
          "                        POSITION,\n" +
          "                        JOB_ID,\n" +
          "                        CONFIG,\n" +
          "                        GUI,\n" +
          "                        REVISION_ID)\n" +
          "VALUES (:NOTE_ID,\n" +
          "        :UUID,\n" +
          "        :TITLE,\n" +
          "        :TEXT,\n" +
          "        :SHEBANG,\n" +
          "        :CREATED,\n" +
          "        :UPDATED,\n" +
          "        :POSITION,\n" +
          "        :JOB_ID,\n" +
          "        :CONFIG,\n" +
          "        :GUI,\n" +
          "        :REVISION_ID);";

  private final static String UPDATE_PARAGRAPH = "" +
          "UPDATE PARAGRAPHS\n" +
          "SET NOTE_ID     = :NOTE_ID,\n" +
          "    UUID        = :UUID,\n" +
          "    TITLE       = :TITLE,\n" +
          "    TEXT        = :TEXT,\n" +
          "    SHEBANG     = :SHEBANG,\n" +
          "    CREATED     = :CREATED,\n" +
          "    UPDATED     = :UPDATED,\n" +
          "    POSITION    = :POSITION,\n" +
          "    JOB_ID      = :JOB_ID,\n" +
          "    CONFIG      = :CONFIG,\n" +
          "    GUI         = :GUI,\n" +
          "    REVISION_ID = :REVISION_ID\n" +
          "WHERE ID = :ID;";


  private final static String DELETE_PARAGRAPH = "" +
          "DELETE\n" +
          "FROM PARAGRAPHS\n" +
          "WHERE ID = :ID;";

  private final static String SELECT_PARAGRAPH_BY_ID = "" +
          "SELECT ID,\n" +
          "       NOTE_ID,\n" +
          "       UUID,\n" +
          "       TITLE,\n" +
          "       TEXT,\n" +
          "       SHEBANG,\n" +
          "       CREATED,\n" +
          "       UPDATED,\n" +
          "       POSITION,\n" +
          "       JOB_ID,\n" +
          "       CONFIG,\n" +
          "       GUI,\n" +
          "       REVISION_ID\n" +
          "FROM PARAGRAPHS\n" +
          "  WHERE ID = :ID;";

  private final static String SELECT_PARAGRAPH_BY_UUID = "" +
          "SELECT ID,\n" +
          "       NOTE_ID,\n" +
          "       UUID,\n" +
          "       TITLE,\n" +
          "       TEXT,\n" +
          "       SHEBANG,\n" +
          "       CREATED,\n" +
          "       UPDATED,\n" +
          "       POSITION,\n" +
          "       JOB_ID,\n" +
          "       CONFIG,\n" +
          "       GUI,\n" +
          "       REVISION_ID\n" +
          "FROM PARAGRAPHS\n" +
          "  WHERE UUID = :UUID;";

  private final static String SELECT_PARAGRAPH_BY_NOTE_ID = "" +
          "SELECT ID,\n" +
          "       NOTE_ID,\n" +
          "       UUID,\n" +
          "       TITLE,\n" +
          "       TEXT,\n" +
          "       SHEBANG,\n" +
          "       CREATED,\n" +
          "       UPDATED,\n" +
          "       POSITION,\n" +
          "       JOB_ID,\n" +
          "       CONFIG,\n" +
          "       GUI,\n" +
          "       REVISION_ID\n" +
          "FROM PARAGRAPHS\n" +
          "WHERE NOTE_ID = :NOTE_ID\n" +
          "ORDER BY POSITION;";


  private final NamedParameterJdbcTemplate jdbcTemplate;

  public ParagraphDAO(final NamedParameterJdbcTemplate jdbcTemplate) {
    this.jdbcTemplate = jdbcTemplate;
  }

  private static Paragraph mapRow(final ResultSet resultSet, final int i) throws SQLException {

    final Type configType = new TypeToken<Map<String, Object>>() {}.getType();

    final Long id = resultSet.getLong("ID");
    final Long noteId = resultSet.getLong("NOTE_ID");
    final String uuid = resultSet.getString("UUID");
    final String title = resultSet.getString("TITLE");
    final String text = resultSet.getString("TEXT");
    final String shebang = resultSet.getString("SHEBANG");
    final LocalDateTime created =
            null != resultSet.getTimestamp("CREATED")
                    ? resultSet.getTimestamp("CREATED").toLocalDateTime()
                    : null;
    final LocalDateTime updated =
            null != resultSet.getTimestamp("UPDATED")
                    ? resultSet.getTimestamp("UPDATED").toLocalDateTime()
                    : null;
    final Integer position = resultSet.getInt("POSITION");
    final Long jobId =  null != resultSet.getString("JOB_ID")
            ? resultSet.getLong("JOB_ID")
            : null;

    final Map<String, Object> config = new Gson().fromJson(resultSet.getString("CONFIG"), configType);
    final GUI gui = new Gson().fromJson(resultSet.getString("GUI"), GUI.class);
    final Long revisionId =  null != resultSet.getString("REVISION_ID")
            ? resultSet.getLong("REVISION_ID")
            : null;

    return new Paragraph(
            id,
            noteId,
            uuid,
            title,
            text,
            shebang,
            created,
            updated,
            position,
            jobId,
            revisionId,
            config,
            gui);
  }

  public Paragraph persist(final Paragraph paragraph) {
    final KeyHolder holder = new GeneratedKeyHolder();
    final SqlParameterSource parameters = new MapSqlParameterSource()
            .addValue("NOTE_ID", paragraph.getNoteId())
            .addValue("UUID", paragraph.getUuid())
            .addValue("TITLE", paragraph.getTitle())
            .addValue("TEXT", paragraph.getText())
            .addValue("SHEBANG", paragraph.getShebang())
            .addValue("CREATED", paragraph.getCreated())
            .addValue("UPDATED", paragraph.getUpdated())
            .addValue("POSITION", paragraph.getPosition())
            .addValue("JOB_ID", paragraph.getJobId())
            .addValue("CONFIG", generatePGjson(paragraph.getConfig()))
            .addValue("GUI", generatePGjson(paragraph.getSettings()))
            .addValue("REVISION_ID", paragraph.getRevisionId());
    jdbcTemplate.update(PERSIST_PARAGRAPH, parameters, holder);

    paragraph.setId((Long) holder.getKeys().get("id"));
    return paragraph;
  }

  public Paragraph update(final Paragraph paragraph) {
    final SqlParameterSource parameters = new MapSqlParameterSource()
            .addValue("NOTE_ID", paragraph.getNoteId())
            .addValue("UUID", paragraph.getUuid())
            .addValue("TITLE", paragraph.getTitle())
            .addValue("TEXT", paragraph.getText())
            .addValue("SHEBANG", paragraph.getShebang())
            .addValue("CREATED", paragraph.getCreated())
            .addValue("UPDATED", paragraph.getUpdated())
            .addValue("POSITION", paragraph.getPosition())
            .addValue("JOB_ID", paragraph.getJobId())
            .addValue("CONFIG", generatePGjson(paragraph.getConfig()))
            .addValue("GUI", generatePGjson(paragraph.getSettings()))
            .addValue("REVISION_ID", paragraph.getRevisionId())
            .addValue("ID", paragraph.getId());
    jdbcTemplate.update(UPDATE_PARAGRAPH, parameters);
    return paragraph;
  }

  public Paragraph get(final Long id) {
    final SqlParameterSource parameters = new MapSqlParameterSource()
            .addValue("ID", id);

    return jdbcTemplate.query(
            SELECT_PARAGRAPH_BY_ID,
            parameters,
            ParagraphDAO::mapRow)
            .stream()
            .findFirst()
            .orElse(null);
  }

  public Paragraph get(final String uuid) {
    final SqlParameterSource parameters = new MapSqlParameterSource()
            .addValue("UUID", uuid);

    return jdbcTemplate.query(
            SELECT_PARAGRAPH_BY_UUID,
            parameters,
            ParagraphDAO::mapRow)
            .stream()
            .findFirst()
            .orElse(null);
  }

  public void remove(final Paragraph paragraph) {
    jdbcTemplate.update(DELETE_PARAGRAPH, new MapSqlParameterSource("ID", paragraph.getId()));
  }

  public List<Paragraph> getByNoteId(final Long noteId) {
    final SqlParameterSource parameters = new MapSqlParameterSource()
            .addValue("NOTE_ID", noteId);
    return jdbcTemplate.query(
            SELECT_PARAGRAPH_BY_NOTE_ID,
            parameters,
            ParagraphDAO::mapRow);
  }
}