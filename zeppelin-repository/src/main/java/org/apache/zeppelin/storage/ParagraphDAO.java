package org.apache.zeppelin.storage;

import static org.apache.zeppelin.storage.Utils.generatePGjson;

import com.google.gson.Gson;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.LocalDateTime;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.zeppelin.notebook.Note;
import org.apache.zeppelin.notebook.NoteRevision;
import org.apache.zeppelin.notebook.Paragraph;
import org.apache.zeppelin.notebook.display.GUI;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import org.springframework.stereotype.Component;

import javax.annotation.PreDestroy;

@Component
public class ParagraphDAO {



  private static final String GET_ALL_PARAGRAPHS = "SELECT * FROM paragraphs WHERE revision_id ISNULL ORDER BY position";
  private static final String GET_NOTE_PARAGRAPHS = "SELECT * FROM paragraphs WHERE db_note_id=:db_note_id AND revision_id ISNULL ORDER BY position";
  private static final String GET_NOTE_PARAGRAPHS_BY_REVISION = "SELECT * FROM paragraphs WHERE db_note_id=:db_note_id AND revision_id=:revision_id ORDER BY position";
  private static final String INSERT_PARAGRAPH = "INSERT INTO paragraphs(paragraph_id, db_note_id, revision_id, title, text, shebang, username, created, updated, config, gui, position) VALUES (:paragraph_id, :db_note_id, :revision_id, :title, :text, :shebang, :username, :created, :updated, :config, :gui, :position)";
  private static final String UPDATE_PARAGRAPH = "UPDATE paragraphs SET title=:title, text=:text, shebang=:shebang, username=:username, updated=:updated, config=:config, gui=:gui, job=:job, position=:position WHERE paragraph_id=:paragraph_id AND revision_id ISNULL";
  private static final String DELETE_PARAGRAPHS = "DELETE FROM paragraphs WHERE revision_id ISNULL AND db_note_id=:db_note_id AND paragraph_id NOT IN (:ids)";

  private final NamedParameterJdbcTemplate jdbcTemplate;

  private static final Gson gson = new Gson();

  public ParagraphDAO(final NamedParameterJdbcTemplate jdbcTemplate) {
    this.jdbcTemplate = jdbcTemplate;
  }

  void loadParagraphsAndFillInNotes(final List<Note> notes) {
    Map<Long, Note> noteMap = new HashMap<>(notes.size());
    for (final Note note : notes) {
      noteMap.put(note.getDatabaseId(), note);
    }

    jdbcTemplate
        .query(GET_ALL_PARAGRAPHS, resultSet -> {
          Long dbNoteId = resultSet.getLong("db_note_id");
          Paragraph paragraph = convertResultSetToParagraph(resultSet);
          noteMap.get(dbNoteId).getParagraphs().add(paragraph);
        });
  }

  List<Paragraph> getParagraphs(final Note note, final NoteRevision revision) {
    MapSqlParameterSource params = new MapSqlParameterSource()
        .addValue("db_note_id", note.getDatabaseId())
        .addValue("revision_id", revision == null ? null : revision.getId());

    String sqlScript = revision == null ? GET_NOTE_PARAGRAPHS : GET_NOTE_PARAGRAPHS_BY_REVISION;
    return jdbcTemplate
        .query(sqlScript, params, (resultSet, i) -> convertResultSetToParagraph(resultSet));
  }

  void saveNoteParagraphs(final Note note, final Long revisionId) {
    for (int i = 0; i < note.getParagraphs().size(); i++) {
      Paragraph paragraph = note.getParagraphs().get(i);

      MapSqlParameterSource params = convertParagraphToParameters(paragraph)
          .addValue("db_note_id", note.getDatabaseId())
          .addValue("revision_id", revisionId)
          .addValue("position", i);

      boolean paragraphMissing = false;
      if (revisionId == null) {
        paragraphMissing = jdbcTemplate.update(UPDATE_PARAGRAPH, params) == 0;
      }

      if (paragraphMissing || revisionId != null) {
        jdbcTemplate.update(INSERT_PARAGRAPH, params);
      }
    }

    //remove deleted paragraphs from database
    Set<String> existIds = note.getParagraphs().stream()
        .map(Paragraph::getId)
        .collect(Collectors.toSet());

    MapSqlParameterSource params = new MapSqlParameterSource("ids", existIds);
    params.addValue("db_note_id", note.getDatabaseId());
    jdbcTemplate.update(DELETE_PARAGRAPHS, params);
  }


  private Paragraph convertResultSetToParagraph(final ResultSet resultSet)
      throws SQLException {
    long databaseId = resultSet.getLong("id");
    String id = resultSet.getString("paragraph_id");
    String title = resultSet.getString("title");
    String text = resultSet.getString("text");
    String shebang = resultSet.getString("shebang");
    String user = resultSet.getString("username");
    LocalDateTime created = resultSet.getTimestamp("created").toLocalDateTime();
    LocalDateTime updated = resultSet.getTimestamp("updated").toLocalDateTime();
    String configJson = resultSet.getString("config");
    GUI gui = gson.fromJson(resultSet.getString("gui"), GUI.class);
      Long jobId = resultSet.getString("job") == null
              ? null
              : resultSet.getLong("job");

    Paragraph paragraph = new Paragraph(title, text, user, gui);
    paragraph.setId(id);
    paragraph.setDatabaseId(databaseId);
    paragraph.setShebang(shebang);
    paragraph.setCreated(created);
    paragraph.setUpdated(updated);
    paragraph.setConfig(gson.fromJson(configJson, paragraph.getConfig().getClass()));
    paragraph.setJobId(jobId);

    return paragraph;
  }

  private MapSqlParameterSource convertParagraphToParameters(final Paragraph p) {
    return new MapSqlParameterSource()
        .addValue("paragraph_id", p.getId())
        .addValue("title", p.getTitle())
        .addValue("text", p.getText())
        .addValue("shebang", p.getShebang())
        .addValue("username", p.getUser())
        .addValue("created", p.getCreated())
        .addValue("updated", p.getUpdated())
        .addValue("config", generatePGjson(p.getConfig()))
        .addValue("gui", generatePGjson(p.getSettings()))
        .addValue("job", p.getJobId());
  }
}