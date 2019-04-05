package org.apache.zeppelin;

import org.apache.zeppelin.externalDTO.ParagraphDTO;
import org.apache.zeppelin.notebook.Note;
import org.apache.zeppelin.notebook.Paragraph;
import org.apache.zeppelin.storage.FullParagraphDAO;
import org.apache.zeppelin.storage.NoteDAO;
import org.apache.zeppelin.storage.ParagraphDAO;
import org.springframework.stereotype.Component;

import java.util.List;

@Component
public class NoteService {

  private final NoteDAO noteDAO;
  private final ParagraphDAO paragraphDAO;
  private final FullParagraphDAO fullParagraphDAO;

  public NoteService(final NoteDAO noteDAO,
                     final ParagraphDAO paragraphDAO,
                     final FullParagraphDAO fullParagraphDAO) {
    this.noteDAO = noteDAO;
    this.paragraphDAO = paragraphDAO;
    this.fullParagraphDAO = fullParagraphDAO;
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
    final ParagraphDTO before = fullParagraphDAO.getById(paragraph.getId());

    final Paragraph savedParagraph = paragraphDAO.persist(paragraph);

    final ParagraphDTO after = fullParagraphDAO.getById(paragraph.getId());
    EventService.publish(note.getId(), before, after);

    return savedParagraph;
  }

  public Paragraph updateParapraph(final Note note, final Paragraph paragraph) {
    final ParagraphDTO before = fullParagraphDAO.getById(paragraph.getId());

    final Paragraph savedParagraph = paragraphDAO.update(paragraph);

    final ParagraphDTO after = fullParagraphDAO.getById(paragraph.getId());
    EventService.publish(note.getId(), before, after);

    return savedParagraph;
  }

  public void removeParagraph(final Note note, final Paragraph paragraph) {
    final ParagraphDTO before = fullParagraphDAO.getById(paragraph.getId());

    paragraphDAO.remove(paragraph);

    final ParagraphDTO after = fullParagraphDAO.getById(paragraph.getId());
    EventService.publish(note.getId(), before, after);
  }

}
