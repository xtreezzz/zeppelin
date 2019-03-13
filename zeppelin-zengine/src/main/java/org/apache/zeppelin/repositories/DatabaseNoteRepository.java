package org.apache.zeppelin.repositories;

import java.util.List;
import java.util.stream.Collectors;
import org.apache.zeppelin.DatabaseStorage;
import org.apache.zeppelin.notebook.Note;
import org.apache.zeppelin.notebook.NoteInfo;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

@Component
public class DatabaseNoteRepository implements NoteRepository {

  @Autowired
  DatabaseStorage storage;

  @Override
  public Note getNote(final String noteId) {
    return storage.getNote(noteId);
  }

  @Override
  public List<Note> getAllNotes() {
    return storage.getAllNotes();
  }

  @Override
  public List<NoteInfo> getNotesInfo() {
    return storage.getAllNotes().stream()
        .map(note -> new NoteInfo(note.getId(), note.getPath()))
        .collect(Collectors.toList());
  }

  @Override
  public Note persistNote(final Note note) {
    storage.createNote(note);
    return note;
  }

  @Override
  public Note updateNote(final Note note) {
    storage.updateNote(note);
    return note;
  }

  @Override
  public boolean removeNote(final String noteId) {
    return storage.removeNote(noteId);
  }
}
