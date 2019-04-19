/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package ru.tinkoff.zeppelin.engine;

import org.apache.zeppelin.storage.FullParagraphDAO;
import org.apache.zeppelin.storage.NoteDAO;
import org.apache.zeppelin.storage.ParagraphDAO;
import org.springframework.stereotype.Component;
import ru.tinkoff.zeppelin.core.externalDTO.ParagraphDTO;
import ru.tinkoff.zeppelin.core.notebook.Note;
import ru.tinkoff.zeppelin.core.notebook.Paragraph;

import java.util.List;

/**
 * Service for operations on storage
 *
 * @author Andrey Koshkin
 * @version 1.0
 * @since 1.0
 */
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

  public List<Paragraph> getParagraphs(final Note note) {
    return paragraphDAO.getByNoteId(note.getId());
  }

  public Paragraph persistParagraph(final Note note, final Paragraph paragraph) {
    final ParagraphDTO before = fullParagraphDAO.getById(paragraph.getId());

    final Paragraph savedParagraph = paragraphDAO.persist(paragraph);

    final ParagraphDTO after = fullParagraphDAO.getById(paragraph.getId());
    EventService.publish(note.getId(), before, after);

    return savedParagraph;
  }

  public Paragraph persistParagraphSilently(final Paragraph paragraph) {
    return paragraphDAO.persist(paragraph);
  }

  public Paragraph updateParagraph(final Note note, final Paragraph paragraph) {
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
