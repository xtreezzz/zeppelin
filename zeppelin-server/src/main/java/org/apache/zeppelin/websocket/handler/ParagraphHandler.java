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

package org.apache.zeppelin.websocket.handler;

import org.apache.commons.lang3.StringUtils;
import org.apache.zeppelin.EventService;
import org.apache.zeppelin.NoteService;
import org.apache.zeppelin.externalDTO.ParagraphDTO;
import org.apache.zeppelin.notebook.Note;
import org.apache.zeppelin.notebook.Paragraph;
import org.apache.zeppelin.notebook.display.GUI;
import org.apache.zeppelin.rest.exception.BadRequestException;
import org.apache.zeppelin.service.ServiceContext;
import org.apache.zeppelin.storage.FullParagraphDAO;
import org.apache.zeppelin.websocket.ConnectionManager;
import org.apache.zeppelin.websocket.Operation;
import org.apache.zeppelin.websocket.SockMessage;
import org.apache.zeppelin.websocket.dto.NoteDTOConverter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import org.springframework.web.socket.WebSocketSession;

import java.io.IOException;
import java.time.LocalDateTime;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Component
public class ParagraphHandler extends AbstractHandler {

  private static final Logger LOG = LoggerFactory.getLogger(ParagraphHandler.class);

  private final FullParagraphDAO fullParagraphDAO;

  @Autowired
  public ParagraphHandler(final NoteService noteService,
                          final ConnectionManager connectionManager,
                          final FullParagraphDAO fullParagraphDAO) {
    super(connectionManager, noteService);
    this.fullParagraphDAO = fullParagraphDAO;
  }

  public void updateParagraph(final WebSocketSession conn, final SockMessage fromMessage) throws IOException {
    final ServiceContext serviceContext = getServiceContext(fromMessage);

    final Note note = safeLoadNote("noteId", fromMessage, Permission.WRITER, serviceContext, conn);
    final Paragraph paragraph = safeLoadParagraph("id", fromMessage, note);

    //final ParagraphDTO before = fullParagraphDAO.getById(paragraph.getId());

    final String title = fromMessage.getNotNull("title");
    final String shebang = fromMessage.getNotNull("shebang");
    final String text = fromMessage.getNotNull("paragraph");
    final Map<String, Object> config = fromMessage.getNotNull("config");
    final Map<String, Object> params = fromMessage.getNotNull("params");

    paragraph.setConfig(config);
    paragraph.getSettings().setParams(params);
    paragraph.setTitle(title);
    paragraph.setText(text);
    paragraph.setShebang(shebang);

    noteService.updateParapraph(note, paragraph);
  }

  public void removeParagraph(final WebSocketSession conn, final SockMessage fromMessage) throws IOException {
    final ServiceContext serviceContext = getServiceContext(fromMessage);

    final Note note = safeLoadNote("noteId", fromMessage, Permission.WRITER, serviceContext, conn);
    final Paragraph p = safeLoadParagraph("id", fromMessage, note);

    noteService.removeParagraph(note, p);
    final List<Paragraph> paragraphs = noteService.getParapraphs(note);
    paragraphs.sort(Comparator.comparingInt(Paragraph::getPosition));

    for (int i = 0; i < paragraphs.size(); i++) {
      final Paragraph paragraph = paragraphs.get(i);
      if (paragraph.getPosition() == i) {
        continue;
      }
      paragraph.setPosition(i);
      noteService.updateParapraph(note, paragraph);
    }
  }

  public void clearParagraphOutput(final WebSocketSession conn, final SockMessage fromMessage) throws IOException {
    final ServiceContext serviceContext = getServiceContext(fromMessage);

    final Note note = safeLoadNote("noteId", fromMessage, Permission.WRITER, serviceContext, conn);
    final Paragraph p = safeLoadParagraph("id", fromMessage, note);

    p.setJobId(null);
    noteService.updateParapraph(note, p);
  }

  public void moveParagraph(final WebSocketSession conn, final SockMessage fromMessage) throws IOException {
    final ServiceContext serviceContext = getServiceContext(fromMessage);

    final Note note = safeLoadNote("noteId", fromMessage, Permission.WRITER, serviceContext, conn);
    final Paragraph paragraphFrom = safeLoadParagraph("id", fromMessage, note);
    final int indexFrom = paragraphFrom.getPosition();
    final int indexTo = ((Double) fromMessage.getNotNull("index")).intValue();

    final List<Paragraph> paragraphs = noteService.getParapraphs(note);
    if (indexTo < 0 || indexTo > paragraphs.size()) {
      throw new BadRequestException("newIndex " + indexTo + " is out of bounds");
    }

    paragraphs.forEach(p -> p.setPosition(p.getPosition() * 10));
    paragraphs.stream()
            .filter(p -> p.getId().equals(paragraphFrom.getId()))
            .forEach(p -> p.setPosition(indexTo * 10 - (indexFrom - indexTo) % 10));
    paragraphs.sort(Comparator.comparingInt(Paragraph::getPosition));

    for (int i = 0; i < paragraphs.size(); i++) {
      final Paragraph paragraph = paragraphs.get(i);
      paragraph.setPosition(i);
      noteService.updateParapraph(note, paragraph);
    }
  }

  public String insertParagraph(final WebSocketSession conn, final SockMessage fromMessage) throws IOException {
    final ServiceContext serviceContext = getServiceContext(fromMessage);

    final Note note = safeLoadNote("noteId", fromMessage, Permission.WRITER, serviceContext, conn);
    final int index = ((Double) fromMessage.getNotNull("index")).intValue();

    final List<Paragraph> paragraphs = noteService.getParapraphs(note);
    if (index < 0 || index > paragraphs.size()) {
      throw new BadRequestException("newIndex " + index + " is out of bounds");
    }

    for (int i = index; i < paragraphs.size(); i++) {
      final Paragraph p = paragraphs.get(i);
      p.setPosition(i + 1);
      noteService.updateParapraph(note, p);
    }

    final Paragraph paragraph = new Paragraph();
    paragraph.setId(null);
    paragraph.setNoteId(note.getId());
    paragraph.setTitle(StringUtils.EMPTY);
    paragraph.setText(StringUtils.EMPTY);
    paragraph.setShebang(null);
    paragraph.setCreated(LocalDateTime.now());
    paragraph.setUpdated(LocalDateTime.now());
    paragraph.setPosition(index);
    paragraph.setJobId(null);
    paragraph.setConfig(new HashMap<>());
    paragraph.setSettings(new GUI());
    noteService.persistParagraph(note, paragraph);

    return paragraph.getUuid();
  }

  public void copyParagraph(final WebSocketSession conn, final SockMessage fromMessage) throws IOException {
    final String paragraphId = insertParagraph(conn, fromMessage);

    if (paragraphId == null) {
      throw new BadRequestException("paragraphId is not defined");
    }
    fromMessage.put("id", paragraphId);

    updateParagraph(conn, fromMessage);
  }
}
