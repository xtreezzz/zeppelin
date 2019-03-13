package org.apache.zeppelin;

import com.google.gson.Gson;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
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

  private static final String GET_ALL_NOTES = "SELECT * FROM notes";
  private static final String GET_NOTE = "SELECT * FROM notes WHERE id = ?";
  private static final String INSERT_NOTE = "INSERT INTO notes(id, path, default_interpreter_group) VALUES (?, ?, ?)";
  private static final String UPDATE_NOTE = "UPDATE notes SET path=?, default_interpreter_group=? WHERE id=?";
  private static final String DELETE_NOTE = "DELETE FROM notes WHERE id = ?";

  private static final String GET_ALL_PARAGRAPHS = "SELECT * FROM paragraphs ORDER BY position";
  private static final String GET_PARAGRAPH = "SELECT * FROM paragraphs WHERE note_id = ? ORDER BY position";
  private static final String INSERT_PARAGRAPH = "INSERT INTO paragraphs(id, note_id, title, text, username, created, updated, config, settings, position) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";
  private static final String UPDATE_PARAGRAPH = "UPDATE paragraphs SET title=?, text=?, username=?, updated=?, config=?, settings=?, position=? WHERE id=?";

  public void createNote(final Note note) {
    final int affectedRows = jdbcTemplate
        .update(INSERT_NOTE, note.getId(), note.getPath(), note.getDefaultInterpreterGroup());
    if (affectedRows == 0) {
      throw new RuntimeException("Can't create note " + note.getId());
    }
    saveNoteParagraphs(note);
  }

  public void updateNote(final Note note) {
    final int affectedRows = jdbcTemplate
        .update(UPDATE_NOTE, note.getPath(), note.getDefaultInterpreterGroup(), note.getId());
    if (affectedRows == 0) {
      throw new RuntimeException("Can't update note " + note.getId());
    }
    saveNoteParagraphs(note);
  }

  public Note getNote(final String noteId) {
    try {
      final Note note = jdbcTemplate
          .queryForObject(GET_NOTE, new Object[]{noteId},
              (resultSet, i) -> convertResultSetToNote(resultSet));

      if (note == null) {
        throw new RuntimeException("Can't find note " + noteId);
      }

      final List<Paragraph> paragraphs = jdbcTemplate.query(
          GET_PARAGRAPH,
          new Object[]{noteId},
          (resultSet, i) -> convertResultSetToParagraph(resultSet)
      );

      note.getParagraphs().addAll(paragraphs);
      return note;
    } catch (final EmptyResultDataAccessException ignore) {
      return null;
    }
  }

  public boolean removeNote(final String noteId) {
    return jdbcTemplate.update(DELETE_NOTE, noteId) != 0;
  }

  public List<Note> getAllNotes() {
    final List<Note> notes = jdbcTemplate
        .query(GET_ALL_NOTES, (resultSet, i) -> convertResultSetToNote(resultSet));
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

  private Note convertResultSetToNote(final ResultSet resultSet)
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
    final LocalDateTime created = LocalDateTime
        .parse(resultSet.getString("created"), DateTimeFormatter.ISO_DATE_TIME);
    final LocalDateTime updated = LocalDateTime
        .parse(resultSet.getString("updated"), DateTimeFormatter.ISO_DATE_TIME);
    final String configJson = resultSet.getString("config");
    final String settingsJson = resultSet.getString("settings");

    final GUI settings = new Gson().fromJson(settingsJson, GUI.class);

    final Paragraph paragraph = new Paragraph(title, text, user, settings);
    paragraph.setId(id);
    paragraph.setCreated(created);
    paragraph.setUpdated(updated);
    paragraph.setConfig(gson.fromJson(configJson, paragraph.getConfig().getClass()));

    return paragraph;
  }

  private void saveNoteParagraphs(Note note) {
    for (int i = 0; i < note.getParagraphs().size(); i++) {
      final Paragraph paragraph = note.getParagraphs().get(i);
      saveParagraph(paragraph, note.getId(), i);
    }
  }

  private void saveParagraph(final Paragraph p, final String noteId, final int position) {
    final String configJson = gson.toJson(p.getConfig());
    final String settingsJson = gson.toJson(p.getSettings());

    final boolean paragraphMissing = jdbcTemplate.update(
        UPDATE_PARAGRAPH,
        p.getTitle(),
        p.getText(),
        p.getUser(),
        p.getUpdated().format(DateTimeFormatter.ISO_DATE_TIME),
        configJson,
        settingsJson,
        position,
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
          p.getCreated().format(DateTimeFormatter.ISO_DATE_TIME),
          p.getUpdated().format(DateTimeFormatter.ISO_DATE_TIME),
          configJson,
          settingsJson,
          position
      );
    }
  }
}
