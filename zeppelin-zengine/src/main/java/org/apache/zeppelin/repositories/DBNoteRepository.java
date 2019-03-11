package org.apache.zeppelin.repositories;

import java.util.List;
import org.apache.zeppelin.DatabaseStorage;
import org.apache.zeppelin.notebook.Note;
import org.apache.zeppelin.notebook.NoteInfo;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

@Component
public class DBNoteRepository implements NoteRepository{

  @Autowired
  DatabaseStorage storage;

  @Override
  public Note getNote(String noteId) {
    throw new RuntimeException("Not implemented!");
  }

  @Override
  public List<Note> getAllNotes() {
    throw new RuntimeException("Not implemented!");
  }

  @Override
  public List<NoteInfo> getNotesInfo() {
    throw new RuntimeException("Not implemented!");
  }

  @Override
  public Note persistNote(Note note) {
    storage.addOrUpdateNote(note);
    return note;
  }

  @Override
  public Note updateNote(Note note) {
    throw new RuntimeException("Not implemented!");
  }

  @Override
  public boolean removeNote(String noteId) {
    throw new RuntimeException("Not implemented!");
  }
}
