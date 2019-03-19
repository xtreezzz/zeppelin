package org.apache.zeppelin.storage;

import static org.apache.zeppelin.storage.Utils.generatePGjson;

import com.google.gson.Gson;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.zeppelin.notebook.Note;
import org.apache.zeppelin.notebook.display.GUI;
import org.springframework.dao.EmptyResultDataAccessException;
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import org.springframework.jdbc.support.GeneratedKeyHolder;

public class NotebookDAO {

  private final NamedParameterJdbcTemplate jdbcTemplate;
  private final ParagraphDAO paragraphDAO;

  private static final Gson gson = new Gson();

  private static final String GET_ALL_NOTES = "SELECT * FROM notes";
  private static final String GET_NOTE = "SELECT * FROM notes WHERE note_id = :note_id";
  private static final String INSERT_NOTE = "INSERT INTO notes(note_id, path, permissions, gui) VALUES (:note_id, :path, :permissions, :gui)";
  private static final String UPDATE_NOTE = "UPDATE notes SET path=:path, permissions=:permissions, gui=:gui WHERE note_id=:note_id";
  private static final String DELETE_NOTE = "DELETE FROM notes WHERE note_id = :note_id";


  public NotebookDAO(final NamedParameterJdbcTemplate jdbcTemplate, final ParagraphDAO paragraphDAO) {
    this.jdbcTemplate = jdbcTemplate;
    this.paragraphDAO = paragraphDAO;
  }

  void createNote(final Note note) {
    GeneratedKeyHolder holder = new GeneratedKeyHolder();
    jdbcTemplate.update(INSERT_NOTE, convertNoteToParameters(note), holder);
    note.setDatabaseId((Long) holder.getKeys().get("id"));
    paragraphDAO.saveNoteParagraphs(note, null);
  }

  void updateNote(final Note note) {

    if (note.getRevision() != null) {
      throw new RuntimeException("Can't update note revision");
    }

    int affectedRows = jdbcTemplate.update(UPDATE_NOTE, convertNoteToParameters(note));
    if (affectedRows == 0) {
      throw new RuntimeException("Can't update note " + note.getNoteId());
    }
    paragraphDAO.saveNoteParagraphs(note, null);
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

      note.getParagraphs().addAll(paragraphDAO.getParagraphs(note, null));

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

    paragraphDAO.loadParagraphsAndFillInNotes(notes);
    return notes;
  }

  private MapSqlParameterSource convertNoteToParameters(final Note note) {
    HashMap<String, Set<String>> permission = new HashMap<>(4);
    permission.put("owners", note.getOwners());
    permission.put("readers", note.getReaders());
    permission.put("runners", note.getRunners());
    permission.put("writers", note.getWriters());

    return new MapSqlParameterSource()
        .addValue("note_id", note.getNoteId())
        .addValue("path", note.getPath())
        .addValue("permissions", generatePGjson(permission))
        .addValue("gui", generatePGjson(note.getGuiConfiguration()));
  }

  private Note convertResultSetToNote(final ResultSet resultSet)
      throws SQLException {
    long dbNoteId = resultSet.getLong("id");
    String noteId = resultSet.getString("note_id");
    String notePath = resultSet.getString("path");
    GUI gui = gson.fromJson(resultSet.getString("gui"), GUI.class);
    Map<String, Set<String>> permission = new HashMap<>(4);
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
}
