package org.apache.zeppelin;

import org.apache.zeppelin.notebook.Note;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Component;

@Component
public class DatabaseStorage {

  @Autowired
  private JdbcTemplate jdbcTemplate;

  private static final String UPDATE_SQL = "update notes set path=?, default_interpreter_group=? where note_id=?";
  private static final String INSERT_SQL = "insert into notes(note_id, path, default_interpreter_group) values (?, ?, ?, ?)";

  public void addOrUpdateNote(Note note) {
    boolean noteMissing = jdbcTemplate
        .update(UPDATE_SQL, note.getPath(), note.getDefaultInterpreterGroup(), note.getId()) == 0;

    if (noteMissing) {
      jdbcTemplate.update(INSERT_SQL, note.getPath(), note.getDefaultInterpreterGroup(), note.getId());
    }
  }
}
