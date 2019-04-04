package org.apache.zeppelin;

import org.apache.zeppelin.notebook.Note;
import org.apache.zeppelin.notebook.Paragraph;
import org.apache.zeppelin.storage.NoteDAO;
import org.apache.zeppelin.storage.ParagraphDAO;
import org.springframework.stereotype.Component;

import java.util.ArrayList;
import java.util.List;

@Component
public class NoteService {

  private final NoteDAO noteDAO;
  private final ParagraphDAO paragraphDAO;

  public NoteService(final NoteDAO noteDAO,
                     final ParagraphDAO paragraphDAO) {
    this.noteDAO = noteDAO;
    this.paragraphDAO = paragraphDAO;
  }

  public List<Note> getAllNotes() {
    return noteDAO.getAllNotes();
  }

  public Note getNote(final String uuid) {
    return noteDAO.get(uuid);
  }

  public Note getNote(final Long noteid) {
    return noteDAO.get(noteid);
  }

  public Note persistNote(final Note note) {
    return noteDAO.persist(note);
  }

  public Note updateNote(final Note note) {
    return noteDAO.update(note);
  }

  public void deleteNote(final Note note) {
    noteDAO.remove(note);
  }

  public List<Paragraph> getParapraphs(final Note note) {
    return paragraphDAO.getByNoteId(note.getId());
  }

  public Paragraph persistParapraph(final Note note, final Paragraph paragraph) {
    return paragraphDAO.persist(paragraph);
  }

  public Paragraph updateParapraph(final Note note, final Paragraph paragraph) {
    return paragraphDAO.update(paragraph);
  }

  public void removeParagraph(final Note note, final Paragraph paragraph) {
    paragraphDAO.remove(paragraph);
  }

}
