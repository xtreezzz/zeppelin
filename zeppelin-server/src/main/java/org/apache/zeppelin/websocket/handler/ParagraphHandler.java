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

import org.apache.zeppelin.NoteService;
import org.apache.zeppelin.notebook.Note;
import org.apache.zeppelin.notebook.Paragraph;
import org.apache.zeppelin.service.ServiceContext;
import org.apache.zeppelin.websocket.ConnectionManager;
import org.apache.zeppelin.websocket.SockMessage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import org.springframework.web.socket.WebSocketSession;

import java.io.IOException;
import java.util.Comparator;
import java.util.List;
import java.util.Map;

@Component
public class ParagraphHandler extends AbstractHandler {

  private static final Logger LOG = LoggerFactory.getLogger(ParagraphHandler.class);

  private final Boolean collaborativeModeEnable;

  @Autowired
  public ParagraphHandler(final NoteService noteService,
                          final ConnectionManager connectionManager) {
    super(connectionManager, noteService);
    this.collaborativeModeEnable = false;
  }

  public void updateParagraph(final WebSocketSession conn, final SockMessage fromMessage) throws IOException {
    final ServiceContext serviceContext = getServiceContext(fromMessage);

    final Note note = safeLoadNote("noteId", fromMessage, Permission.WRITER, serviceContext, conn);
    final Paragraph paragraph = safeLoadParagraph("id", fromMessage, note);

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
//    final ServiceContext serviceContext = getServiceContext(fromMessage);
//
//    final Note note = safeLoadNote("noteId", fromMessage, Permission.WRITER, serviceContext, conn);
//    final Paragraph p = safeLoadParagraph("id", fromMessage, note);
//    final int index = ((Double) fromMessage.getNotNull("index")).intValue();
//
//    if (index < 0 || index > note.getParagraphs().size()) {
//      throw new BadRequestException("newIndex " + index + " is out of bounds");
//    }
//
//    final List<Paragraph> paragraphs = note.getParagraphs();
//    Collections.swap(paragraphs, index, paragraphs.indexOf(p));
//    noteService.update(note);
//
//    final SockMessage message = new SockMessage(Operation.PARAGRAPH_MOVED)
//            .put("id", p.getParagraphId())
//            .put("index", index);
//    connectionManager.broadcast(note.getUuid(), message);
  }

  public String insertParagraph(final WebSocketSession conn, final SockMessage fromMessage) throws IOException {
//    final ServiceContext serviceContext = getServiceContext(fromMessage);
//
//    final Note note = safeLoadNote("noteId", fromMessage, Permission.WRITER, serviceContext, conn);
//    final int index = ((Double) fromMessage.getNotNull("index")).intValue();
//
//    if (index < 0 || index > note.getParagraphs().size()) {
//      throw new BadRequestException("newIndex " + index + " is out of bounds");
//    }
//
//    final Paragraph p = new Paragraph("", "", serviceContext.getAutheInfo().getUser(), new GUI());
//    note.getParagraphs().add(index, p);
//
//    noteService.update(note);
//    connectionManager.broadcast(note.getUuid(), new SockMessage(Operation.PARAGRAPH_ADDED).put("paragraph", p).put("index", index));
//    return p.getParagraphId();
    return "3";
  }

  public void copyParagraph(final WebSocketSession conn, final SockMessage fromMessage) throws IOException {
//    final String paragraphId = insertParagraph(conn, fromMessage);
//
//    if (paragraphId == null) {
//      throw new BadRequestException("paragraphId is not defined");
//    }
//    fromMessage.put("id", paragraphId);
//
//    updateParagraph(conn, fromMessage);
  }
}
