package org.apache.zeppelin.repositories;

import java.util.List;
import org.apache.zeppelin.notebook.Note;
import org.apache.zeppelin.notebook.NoteInfo;

public interface NoteRepository {
  public Note getNote(final String noteId);
  public List<Note> getAllNotes();
  public List<NoteInfo> getNotesInfo();
  public Note persistNote(final Note note);
  public Note updateNote(final Note note);
  public boolean removeNote(final String noteId);
}
