package org.apache.zeppelin.storage;

import com.google.gson.Gson;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.LocalDateTime;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.zeppelin.notebook.Note;
import org.apache.zeppelin.notebook.Paragraph;
import org.apache.zeppelin.notebook.display.GUI;
import org.postgresql.util.PGobject;
import org.springframework.dao.EmptyResultDataAccessException;
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import org.springframework.jdbc.support.GeneratedKeyHolder;

public class NotebookDAO {

  private final NamedParameterJdbcTemplate jdbcTemplate;

  private static final Gson gson = new Gson();

  private static final String GET_ALL_NOTES = "SELECT * FROM notes";
  private static final String GET_NOTE = "SELECT * FROM notes WHERE note_id = :note_id";
  private static final String INSERT_NOTE = "INSERT INTO notes(note_id, path, permissions, gui) VALUES (:note_id, :path, :permissions, :gui)";
  private static final String UPDATE_NOTE = "UPDATE notes SET path=:path, permissions=:permissions, gui=:gui WHERE note_id=:note_id";
  private static final String DELETE_NOTE = "DELETE FROM notes WHERE note_id = :note_id";

  private static final String GET_ALL_PARAGRAPHS = "SELECT * FROM paragraphs ORDER BY position";
  private static final String GET_PARAGRAPH = "SELECT * FROM paragraphs WHERE db_note_id =:db_note_id ORDER BY position";
  private static final String INSERT_PARAGRAPH = "INSERT INTO paragraphs(paragraph_id, db_note_id, title, text, username, created, updated, config, gui, position) VALUES (:paragraph_id, :db_note_id, :title, :text, :username, :created, :updated, :config, :gui, :position)";
  private static final String UPDATE_PARAGRAPH = "UPDATE paragraphs SET title=:title, text=:text, username=:username, updated=:updated, config=:config, gui=:gui, position=:position WHERE paragraph_id=:paragraph_id";

  public NotebookDAO(final NamedParameterJdbcTemplate jdbcTemplate) {
    this.jdbcTemplate = jdbcTemplate;
  }

  void createNote(final Note note) {
    GeneratedKeyHolder holder = new GeneratedKeyHolder();
    int affectedRows = jdbcTemplate.update(INSERT_NOTE, convertNoteToParameters(note), holder);

    if (affectedRows == 0) {
      throw new RuntimeException("Can't create note " + note.getNoteId());
    }
    note.setDatabaseId((Long) holder.getKeys().get("id"));
    saveNoteParagraphs(note);
  }

  void updateNote(final Note note) {
    int affectedRows = jdbcTemplate.update(UPDATE_NOTE, convertNoteToParameters(note));
    if (affectedRows == 0) {
      throw new RuntimeException("Can't update note " + note.getNoteId());
    }
    saveNoteParagraphs(note);
  }

  public Note getNote(final String noteId) {
    try {
      Note note = jdbcTemplate.queryForObject(
          GET_NOTE,
          new MapSqlParameterSource("note_id", noteId),
          (resultSet, i) -> convertResultSetToNote(resultSet)
      );

      if (note == null) {
        throw new RuntimeException("Can't find note " + noteId);
      }

      List<Paragraph> paragraphs = jdbcTemplate.query(
          GET_PARAGRAPH,
          new MapSqlParameterSource("db_note_id", note.getDatabaseId()),
          (resultSet, i) -> convertResultSetToParagraph(resultSet)
      );

      note.getParagraphs().addAll(paragraphs);
      return note;
    } catch (EmptyResultDataAccessException ignore) {
      return null;
    }
  }

  boolean removeNote(final String noteId) {
    return jdbcTemplate.update(DELETE_NOTE, new MapSqlParameterSource("note_id", noteId)) != 0;
  }

  List<Note> getAllNotes() {
    List<Note> notes = jdbcTemplate
        .query(GET_ALL_NOTES, (resultSet, i) -> convertResultSetToNote(resultSet));
    Map<Long, Note> noteMap = new HashMap<>();
    for (final Note note : notes) {
      noteMap.put(note.getDatabaseId(), note);
    }

    jdbcTemplate
        .query(GET_ALL_PARAGRAPHS, resultSet -> {
          Long dbNoteId = resultSet.getLong("db_note_id");
          Paragraph paragraph = convertResultSetToParagraph(resultSet);
          noteMap.get(dbNoteId).getParagraphs().add(paragraph);
        });
    return notes;
  }

  private MapSqlParameterSource convertNoteToParameters(final Note note) {
    HashMap<String, Set<String>> permission = new HashMap<>(4);
    permission.put("owners", note.getOwners());
    permission.put("readers", note.getReaders());
    permission.put("runners", note.getRunners());
    permission.put("writers", note.getWriters());

    MapSqlParameterSource parameters = new MapSqlParameterSource();
    parameters
        .addValue("note_id", note.getNoteId())
        .addValue("path", note.getPath())
        .addValue("permissions", generatePGjson(permission))
        .addValue("gui", generatePGjson(note.getGuiConfiguration()));

    return parameters;
  }

  private MapSqlParameterSource convertParagraphToParameters(
      final Paragraph p,
      final long dbNoteId,
      final int position) {
    MapSqlParameterSource parameters = new MapSqlParameterSource();
    parameters
        .addValue("paragraph_id", p.getId())
        .addValue("db_note_id", dbNoteId)
        .addValue("title", p.getTitle())
        .addValue("text", p.getText())
        .addValue("username", p.getUser())
        .addValue("created", p.getCreated())
        .addValue("updated", p.getUpdated())
        .addValue("config", generatePGjson(p.getConfig()))
        .addValue("gui", generatePGjson(p.getSettings()))
        .addValue("position", position);

    return parameters;
  }

  private PGobject generatePGjson(final Object value) {
    try {
      PGobject pgObject = new PGobject();
      pgObject.setType("jsonb");
      pgObject.setValue(gson.toJson(value));
      return pgObject;
    } catch (SQLException e) {
      throw new RuntimeException("Can't generate postgres json", e);
    }
  }

  private Note convertResultSetToNote(final ResultSet resultSet)
      throws SQLException {
    long dbNoteId = resultSet.getLong("id");
    String noteId = resultSet.getString("note_id");
    String notePath = resultSet.getString("path");
    GUI gui = gson.fromJson(resultSet.getString("gui"), GUI.class);
    Map<String, Set<String>> permission = new HashMap<>();
    permission = gson.fromJson(resultSet.getString("permissions"), permission.getClass());

    Note note = new Note(notePath);
    note.setDatabaseId(dbNoteId);
    note.setNoteId(noteId);
    note.getOwners().addAll(permission.get("owners"));
    note.getOwners().addAll(permission.get("readers"));
    note.getOwners().addAll(permission.get("runners"));
    note.getOwners().addAll(permission.get("writers"));
    note.getGuiConfiguration().setParams(gui.getParams());
    note.getGuiConfiguration().setForms(gui.getForms());
    return note;
  }

  private Paragraph convertResultSetToParagraph(final ResultSet resultSet)
      throws SQLException {
    String id = resultSet.getString("paragraph_id");
    String title = resultSet.getString("title");
    String text = resultSet.getString("text");
    String user = resultSet.getString("username");
    LocalDateTime created = resultSet.getTimestamp("created").toLocalDateTime();
    LocalDateTime updated = resultSet.getTimestamp("updated").toLocalDateTime();
    String configJson = resultSet.getString("config");
    GUI gui = gson.fromJson(resultSet.getString("gui"), GUI.class);

    Paragraph paragraph = new Paragraph(title, text, user, gui);
    paragraph.setId(id);
    paragraph.setCreated(created);
    paragraph.setUpdated(updated);
    paragraph.setConfig(gson.fromJson(configJson, paragraph.getConfig().getClass()));

    return paragraph;
  }

  private void saveNoteParagraphs(final Note note) {
    for (int i = 0; i < note.getParagraphs().size(); i++) {
      Paragraph paragraph = note.getParagraphs().get(i);
      saveParagraph(paragraph, note.getDatabaseId(), i);
    }
  }

  private void saveParagraph(final Paragraph p, final long noteId, final int position) {
    MapSqlParameterSource parameters = convertParagraphToParameters(p, noteId, position);

    boolean paragraphMissing = jdbcTemplate.update(UPDATE_PARAGRAPH, parameters) == 0;
    if (paragraphMissing) {
      jdbcTemplate.update(INSERT_PARAGRAPH, parameters);
    }
  }
}
