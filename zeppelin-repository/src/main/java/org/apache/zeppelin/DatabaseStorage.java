package org.apache.zeppelin;

import com.google.gson.Gson;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import org.apache.zeppelin.notebook.Note;
import org.apache.zeppelin.notebook.Paragraph;
import org.apache.zeppelin.notebook.display.GUI;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.dao.EmptyResultDataAccessException;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Component;

@Component
public class DatabaseStorage {

  @Autowired
  private JdbcTemplate jdbcTemplate;

  private static final Gson gson = new Gson();

  private static final String GET_ALL_NOTES = "SELECT * FROM notes;";
  private static final String GET_NOTE = "SELECT * FROM notes WHERE id = ?;";
  private static final String INSERT_NOTE = "INSERT INTO notes(id, path, default_interpreter_group) VALUES (?, ?, ?);";
  private static final String UPDATE_NOTE = "UPDATE notes SET path=?, default_interpreter_group=? WHERE id=?;";
  private static final String DELETE_NOTE = "DELETE FROM notes WHERE id = ?;";

  private static final String GET_ALL_PARAGRAPHS = "SELECT * FROM paragraphs";
  private static final String GET_PARAGRAPH = "SELECT * FROM paragraphs WHERE note_id = ?;";
  private static final String INSERT_PARAGRAPH = "INSERT INTO paragraphs(id, note_id, title, text, username, created, UPDATEd, settings) VALUES (?, ?, ?, ?, ?, ?, ?, ?);";
  private static final String UPDATE_PARAGRAPH = "UPDATE paragraphs SET title=?, text=?, username=?, updated=?, settings=? WHERE id=?;";

  private static final DateTimeFormatter paragraphDateFormatter = DateTimeFormatter
      .ofPattern("YYYY-MM-dd HH:mm:ss.SSS");

  public void addOrUpdateNote(final Note note) {
    final boolean noteMissing = jdbcTemplate
        .update(UPDATE_NOTE, note.getPath(), note.getDefaultInterpreterGroup(), note.getId()) == 0;

    if (noteMissing) {
      jdbcTemplate
          .update(INSERT_NOTE, note.getId(), note.getPath(), note.getDefaultInterpreterGroup());
    }

    note.getParagraphs().forEach(p -> saveParagraph(p, note.getId()));
  }

  public Note getNote(final String noteId) {
    try {
      final Note note = jdbcTemplate
          .queryForObject(GET_NOTE, new Object[]{noteId}, this::convertResultSetToNote);

      final List<Paragraph> paragraphs = jdbcTemplate.query(
          GET_PARAGRAPH,
          new Object[]{noteId},
          (resultSet, i) -> convertResultSetToParagraph(resultSet)
      );

      Objects.requireNonNull(note).getParagraphs().addAll(paragraphs);
      return note;
    } catch (final EmptyResultDataAccessException ignore) {
      return null;
    }
  }

  public boolean removeNote(final String noteId) {
    return jdbcTemplate.update(DELETE_NOTE, noteId) != 0;
  }

  public List<Note> getAllNotes() {
    final List<Note> notes = jdbcTemplate.query(GET_ALL_NOTES, this::convertResultSetToNote);
    final Map<String, Note> noteMap = new HashMap<>();
    for (final Note note : notes) {
      noteMap.put(note.getId(), note);
    }

    jdbcTemplate
        .query(GET_ALL_PARAGRAPHS, resultSet -> {
          final String note_id = resultSet.getString("note_id");
          final Paragraph paragraph = convertResultSetToParagraph(resultSet);
          noteMap.get(note_id).getParagraphs().add(paragraph);
        });

    return notes;
  }

  private Note convertResultSetToNote(final ResultSet resultSet, final int index)
      throws SQLException {
    final String noteId = resultSet.getString("id");
    final String notePath = resultSet.getString("path");
    final String interGroup = resultSet.getString("default_interpreter_group");
    final Note note = new Note(notePath, interGroup);
    note.setId(noteId);
    return note;
  }

  private Paragraph convertResultSetToParagraph(final ResultSet resultSet)
      throws SQLException {
    final String id = resultSet.getString("id");
    final String title = resultSet.getString("title");
    final String text = resultSet.getString("text");
    final String user = resultSet.getString("username");
    final LocalDateTime created = gson.fromJson(resultSet.getString("created"), LocalDateTime.class);
    final LocalDateTime updated = gson.fromJson(resultSet.getString("updated"), LocalDateTime.class);
    final String settingsJson = resultSet.getString("settings");

    final GUI settings = new Gson().fromJson(settingsJson, GUI.class);

    final Paragraph paragraph = new Paragraph(title, text, user, settings);
    paragraph.setId(id);
    paragraph.setCreated(created);
    paragraph.setUpdated(updated);

    return paragraph;
  }

  private void saveParagraph(final Paragraph p, final String noteId) {
    final String settingsJson = gson.toJson(p.getSettings());
    final String createdJson = gson.toJson(p.getCreated());
    final String updatedJson = gson.toJson(p.getUpdated());

    final boolean paragraphMissing = jdbcTemplate.update(
        UPDATE_PARAGRAPH,
        p.getTitle(),
        p.getText(),
        p.getUser(),
        updatedJson,
        settingsJson,
        p.getId()
    ) == 0;

    if (paragraphMissing) {
      jdbcTemplate.update(
          INSERT_PARAGRAPH,
          p.getId(),
          noteId,
          p.getTitle(),
          p.getText(),
          p.getUser(),
          createdJson,
          updatedJson,
          settingsJson
      );
    }
  }
}
