package org.apache.zeppelin.storage;

import java.util.List;
import java.util.stream.Collectors;
import org.apache.zeppelin.notebook.Note;
import org.apache.zeppelin.notebook.NoteInfo;
import org.apache.zeppelin.notebook.NoteRevision;

//TODO(SAN) move to zeppelin-repository
public class DatabaseNoteRepository {

  private final NotebookDAO notebookDAO;
  private final NoteRevisionDAO noteRevisionDAO;

  public DatabaseNoteRepository(
      final NotebookDAO notebookDAO,
      final NoteRevisionDAO noteRevisionDAO) {

    this.notebookDAO = notebookDAO;
    this.noteRevisionDAO = noteRevisionDAO;
  }

  public Note getNote(final String noteId) {
    return notebookDAO.getNote(noteId);
  }

  public List<Note> getAllNotes() {
    return notebookDAO.getAllNotes();
  }

  public List<NoteInfo> getNotesInfo() {
    return notebookDAO.getAllNotes().stream()
        .map(note -> new NoteInfo(note.getNoteId(), note.getPath()))
        .collect(Collectors.toList());
  }

  public Note persistNote(final Note note) {
    notebookDAO.createNote(note);
    return note;
  }

  public Note updateNote(final Note note) {
    notebookDAO.updateNote(note);
    return note;
  }

  public boolean removeNote(final String noteId) {
    return notebookDAO.removeNote(noteId);
  }

  public void createRevision(final Note note, final String message) {
    noteRevisionDAO.createRevision(note, message);
  }

  public List<NoteRevision> getRevisions(final long noteId) {
    return noteRevisionDAO.getRevisions(noteId);
  }

  public void checkoutRevision(final Note note, final Long revisionId) {
    noteRevisionDAO.checkoutRevision(note, revisionId);
  }

  public void applyRevision(final Note note, final Long revisionId) {
    noteRevisionDAO.applyRevision(note, revisionId);
  }
}
