package org.apache.zeppelin;

import org.apache.zeppelin.notebook.Note;
import org.apache.zeppelin.notebook.Paragraph;
import org.apache.zeppelin.storage.NoteDAO;
import org.apache.zeppelin.storage.ParagraphDAO;
import org.springframework.stereotype.Component;

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
    final Note saved = noteDAO.persist(note);

    EventService.publish(EventService.Type.NOTE_CREATED, note);

    return saved;
  }

  public Note updateNote(final Note note) {
    final Note updated = noteDAO.update(note);

    EventService.publish(EventService.Type.NOTE_UPDATED, note);

    return updated;
  }

  public void deleteNote(final Note note) {
    noteDAO.remove(note);
    EventService.publish(EventService.Type.NOTE_REMOVED, note);
  }

  public List<Paragraph> getParapraphs(final Note note) {
    return paragraphDAO.getByNoteId(note.getId());
  }

  public Paragraph persistParagraph(final Note note, final Paragraph paragraph) {
    final Paragraph savedParagraph = paragraphDAO.persist(paragraph);

    EventService.publish(EventService.Type.PARAGRAPH_ADDED, note, savedParagraph);

    return savedParagraph;
  }

  public Paragraph updateParapraph(final Note note, final Paragraph paragraph) {
    final Paragraph savedParagraph = paragraphDAO.update(paragraph);

    EventService.publish(EventService.Type.PARAGRAPH_UPDATED, note, savedParagraph);

    return paragraphDAO.update(paragraph);
  }

  public void removeParagraph(final Note note, final Paragraph paragraph) {
    paragraphDAO.remove(paragraph);
    EventService.publish(EventService.Type.PARAGRAPH_REMOVED, note, paragraph);
  }

}
