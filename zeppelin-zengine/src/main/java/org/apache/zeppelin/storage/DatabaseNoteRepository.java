package org.apache.zeppelin.storage;

import java.util.List;
import java.util.stream.Collectors;
import org.apache.zeppelin.notebook.Note;
import org.apache.zeppelin.notebook.NoteInfo;

public class DatabaseNoteRepository {

  private final NotebookDAO storage;

  public DatabaseNoteRepository(final NotebookDAO storage) {
    this.storage = storage;
  }

  public Note getNote(final String noteId) {
    return storage.getNote(noteId);
  }

  public List<Note> getAllNotes() {
    return storage.getAllNotes();
  }

  public List<NoteInfo> getNotesInfo() {
    return storage.getAllNotes().stream()
        .map(note -> new NoteInfo(note.getId(), note.getPath()))
        .collect(Collectors.toList());
  }

  public Note persistNote(final Note note) {
    storage.createNote(note);
    return note;
  }

  public Note updateNote(final Note note) {
    storage.updateNote(note);
    return note;
  }

  public boolean removeNote(final String noteId) {
    return storage.removeNote(noteId);
  }
}
