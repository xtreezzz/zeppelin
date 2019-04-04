package org.apache.zeppelin.storage;

import static org.apache.zeppelin.storage.Utils.generatePGjson;

import com.google.gson.Gson;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.zeppelin.notebook.Job;
import org.apache.zeppelin.notebook.Note;
import org.apache.zeppelin.notebook.display.GUI;
import org.springframework.dao.EmptyResultDataAccessException;
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import org.springframework.jdbc.core.namedparam.SqlParameterSource;
import org.springframework.jdbc.support.GeneratedKeyHolder;
import org.springframework.jdbc.support.KeyHolder;
import org.springframework.stereotype.Component;

@Component
public class NoteDAO {

  private static final String PERSIST_NOTE = "" +
          "INSERT INTO NOTES (UUID,\n" +
          "                   PATH,\n" +
          "                   PERMISSIONS,\n" +
          "                   GUI,\n" +
          "                   JOB_BATCH_ID)\n" +
          "VALUES (:UUID,\n" +
          "        :PATH,\n" +
          "        :PERMISSIONS,\n" +
          "        :GUI,\n" +
          "        :JOB_BATCH_ID);";

  private static final String UPDATE_NOTE = "" +
          "UPDATE NOTES\n" +
          "SET UUID         = :UUID,\n" +
          "    PATH         = :PATH,\n" +
          "    PERMISSIONS  = :PERMISSIONS,\n" +
          "    GUI          = :GUI,\n" +
          "    JOB_BATCH_ID =:JOB_BATCH_ID\n" +
          "WHERE ID = :ID;";


  private static final String GET_NOTE_BY_ID = "" +
          "SELECT ID,\n" +
          "       UUID,\n" +
          "       PATH,\n" +
          "       PERMISSIONS,\n" +
          "       GUI,\n" +
          "       JOB_BATCH_ID\n" +
          "FROM NOTES\n" +
          "WHERE ID = :ID;";

  private static final String GET_NOTE_BY_UUID = "" +
          "SELECT ID,\n" +
          "       UUID,\n" +
          "       PATH,\n" +
          "       PERMISSIONS,\n" +
          "       GUI,\n" +
          "       JOB_BATCH_ID\n" +
          "FROM NOTES\n" +
          "WHERE UUID = :UUID;";

  private static final String GET_ALL_NOTES = "" +
          "SELECT ID,\n" +
          "       UUID,\n" +
          "       PATH,\n" +
          "       PERMISSIONS,\n" +
          "       GUI,\n" +
          "       JOB_BATCH_ID\n" +
          "FROM NOTES\n";

  private static final String DELETE_NOTE = "" +
          "DELETE\n" +
          "FROM NOTES\n" +
          "WHERE ID = :ID;";

  private final NamedParameterJdbcTemplate jdbcTemplate;

  private static final Gson gson = new Gson();

  public NoteDAO(final NamedParameterJdbcTemplate jdbcTemplate) {
    this.jdbcTemplate = jdbcTemplate;
  }


  private static Note mapRow(final ResultSet resultSet, final int i) throws SQLException {
    long dbNoteId = resultSet.getLong("id");
    String noteId = resultSet.getString("UUID");
    String notePath = resultSet.getString("path");
    GUI gui = gson.fromJson(resultSet.getString("gui"), GUI.class);
    Map<String, Set<String>> permission = new HashMap<>(4);
    permission = gson.fromJson(resultSet.getString("permissions"), permission.getClass());

    Note note = new Note(notePath);
    note.setId(dbNoteId);
    note.setUuid(noteId);
    note.getOwners().addAll(permission.get("owners"));
    note.getOwners().addAll(permission.get("readers"));
    note.getOwners().addAll(permission.get("runners"));
    note.getOwners().addAll(permission.get("writers"));
    note.getGuiConfiguration().setParams(gui.getParams());
    note.getGuiConfiguration().setForms(gui.getForms());
    return note;
  }

  public Note persist(final Note note) {
    HashMap<String, Set<String>> permission = new HashMap<>();
    permission.put("owners", note.getOwners());
    permission.put("readers", note.getReaders());
    permission.put("runners", note.getRunners());
    permission.put("writers", note.getWriters());

    final KeyHolder holder = new GeneratedKeyHolder();
    final SqlParameterSource parameters = new MapSqlParameterSource()
            .addValue("UUID", note.getUuid())
            .addValue("PATH", note.getPath())
            .addValue("PERMISSIONS", generatePGjson(permission))
            .addValue("GUI", generatePGjson(note.getGuiConfiguration()))
            .addValue("JOB_BATCH_ID", note.getBatchJobId());
    jdbcTemplate.update(PERSIST_NOTE, parameters, holder);

    note.setId((Long) holder.getKeys().get("id"));
    return note;
  }

  public Note update(final Note note) {
    HashMap<String, Set<String>> permission = new HashMap<>();
    permission.put("owners", note.getOwners());
    permission.put("readers", note.getReaders());
    permission.put("runners", note.getRunners());
    permission.put("writers", note.getWriters());

    final KeyHolder holder = new GeneratedKeyHolder();
    final SqlParameterSource parameters = new MapSqlParameterSource()
            .addValue("UUID", note.getUuid())
            .addValue("PATH", note.getPath())
            .addValue("PERMISSIONS", generatePGjson(permission))
            .addValue("GUI", generatePGjson(note.getGuiConfiguration()))
            .addValue("JOB_BATCH_ID", note.getBatchJobId())
            .addValue("ID", note.getId());

    jdbcTemplate.update(UPDATE_NOTE, parameters, holder);
    return note;
  }

  public Note get(final Long noteId) {
    final SqlParameterSource parameters = new MapSqlParameterSource()
            .addValue("ID", noteId);

    return jdbcTemplate.query(
            GET_NOTE_BY_ID,
            parameters,
            NoteDAO::mapRow)
            .stream()
            .findFirst()
            .orElse(null);
  }

  public Note get(final String uuid) {
    final SqlParameterSource parameters = new MapSqlParameterSource()
            .addValue("UUID", uuid);

    return jdbcTemplate.query(
            GET_NOTE_BY_UUID,
            parameters,
            NoteDAO::mapRow)
            .stream()
            .findFirst()
            .orElse(null);
  }

  public void remove(final Note note) {
    jdbcTemplate.update(DELETE_NOTE, new MapSqlParameterSource("ID", note.getId()));
  }

  public List<Note> getAllNotes() {
    final SqlParameterSource parameters = new MapSqlParameterSource();
    return jdbcTemplate.query(
            GET_ALL_NOTES,
            parameters,
            NoteDAO::mapRow);
  }
}
