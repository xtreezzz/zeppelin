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

import com.google.common.collect.Lists;
import java.io.IOException;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import org.apache.commons.lang3.StringUtils;
import org.apache.zeppelin.configuration.ZeppelinConfiguration;
import org.apache.zeppelin.notebook.Note;
import org.apache.zeppelin.notebook.Paragraph;
import org.apache.zeppelin.notebook.display.GUI;
import org.apache.zeppelin.storage.DatabaseNoteRepository;
import org.apache.zeppelin.rest.exception.BadRequestException;
import org.apache.zeppelin.service.ServiceContext;
import org.apache.zeppelin.websocket.ConnectionManager;
import org.apache.zeppelin.websocket.Operation;
import org.apache.zeppelin.websocket.SockMessage;
import org.bitbucket.cowwoc.diffmatchpatch.DiffMatchPatch;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import org.springframework.web.socket.WebSocketSession;

@Component
public class ParagraphHandler extends AbstractHandler {

  private static final Logger LOG = LoggerFactory.getLogger(ParagraphHandler.class);

  private final Boolean collaborativeModeEnable;

  @Autowired
  public ParagraphHandler(final DatabaseNoteRepository noteRepository,
                          final ConnectionManager connectionManager) {
    super(connectionManager, noteRepository);
    this.collaborativeModeEnable = false;
  }

  //TODO(egorklimov): config removed from paragraph
  public void updateParagraph(final WebSocketSession conn, final SockMessage fromMessage) throws IOException {
    final ServiceContext serviceContext = getServiceContext(fromMessage);

    final Note note = safeLoadNote("noteId", fromMessage, Permission.WRITER, serviceContext, conn);
    final Paragraph p = safeLoadParagraph("id", fromMessage, note);

    final String title = fromMessage.getNotNull("title");
    final String text = fromMessage.getNotNull("paragraph");
    final Map<String, Object> config = fromMessage.getNotNull("config");
    final Map<String, Object> params = fromMessage.getNotNull("params");

    p.setConfig(config);
    p.getSettings().setParams(params);
    p.setTitle(title);
    p.setText(text);

    noteRepository.updateNote(note);
    connectionManager.broadcast(note.getNoteId(), new SockMessage(Operation.PARAGRAPH).put("paragraph", p));
  }

  public void patchParagraph(final WebSocketSession conn, final SockMessage fromMessage) throws IOException {
    final ServiceContext serviceContext = getServiceContext(fromMessage);

    if (!collaborativeModeEnable) {
      return;
    }

    final Note note = safeLoadNote("noteId", fromMessage, Permission.WRITER, serviceContext, conn);
    final Paragraph p = safeLoadParagraph("id", fromMessage, note);

    final String paragraphId = fromMessage.getNotNull("id");
    final String patchText = fromMessage.getNotNull("patch");

    String paragraphText = p.getText() == null ? StringUtils.EMPTY : p.getText();

    final DiffMatchPatch dmp = new DiffMatchPatch();
    final LinkedList<DiffMatchPatch.Patch> patches = Lists.newLinkedList(dmp.patchFromText(patchText));
    paragraphText = (String) dmp.patchApply(patches, paragraphText)[0];

    p.setText(paragraphText);
    noteRepository.updateNote(note);

    final SockMessage message = new SockMessage(Operation.PATCH_PARAGRAPH)
            .put("patch", patchText)
            .put("paragraphId", paragraphId);
    connectionManager.broadcast(note.getNoteId(), message);
  }

  public void removeParagraph(final WebSocketSession conn, final SockMessage fromMessage) throws IOException {
    final ServiceContext serviceContext = getServiceContext(fromMessage);

    final Note note = safeLoadNote("noteId", fromMessage, Permission.WRITER, serviceContext, conn);
    final Paragraph p = safeLoadParagraph("id", fromMessage, note);

    note.getParagraphs().removeIf(paragraph -> paragraph.getId().equals(p.getId()));

    noteRepository.updateNote(note);

    final SockMessage message = new SockMessage(Operation.PARAGRAPH_REMOVED)
            .put("id", p.getId());
    connectionManager.broadcast(note.getNoteId(), message);
  }

  public void clearParagraphOutput(final WebSocketSession conn, final SockMessage fromMessage) throws IOException {
    final ServiceContext serviceContext = getServiceContext(fromMessage);

    final Note note = safeLoadNote("noteId", fromMessage, Permission.WRITER, serviceContext, conn);
    final Paragraph p = safeLoadParagraph("id", fromMessage, note);

    //note.clearParagraphOutput(p.getNoteId());
    connectionManager.broadcast(note.getNoteId(), new SockMessage(Operation.PARAGRAPH).put("paragraph", p));
  }

  public void moveParagraph(final WebSocketSession conn, final SockMessage fromMessage) throws IOException {
    final ServiceContext serviceContext = getServiceContext(fromMessage);

    final Note note = safeLoadNote("noteId", fromMessage, Permission.WRITER, serviceContext, conn);
    final Paragraph p = safeLoadParagraph("id", fromMessage, note);
    final int index = ((Double) fromMessage.getNotNull("index")).intValue();

    if (index < 0 || index > note.getParagraphs().size()) {
      throw new BadRequestException("newIndex " + index + " is out of bounds");
    }

    final List<Paragraph> paragraphs = note.getParagraphs();
    Collections.swap(paragraphs, index, paragraphs.indexOf(p));
    noteRepository.updateNote(note);

    final SockMessage message = new SockMessage(Operation.PARAGRAPH_MOVED)
            .put("id", p.getId())
            .put("index", index);
    connectionManager.broadcast(note.getNoteId(), message);
  }

  //TODO(egorklimov): config removed from paragraph
  public String insertParagraph(final WebSocketSession conn, final SockMessage fromMessage) throws IOException {
    final ServiceContext serviceContext = getServiceContext(fromMessage);

    final Note note = safeLoadNote("noteId", fromMessage, Permission.WRITER, serviceContext, conn);
    final int index = ((Double) fromMessage.getNotNull("index")).intValue();

    if (index < 0 || index > note.getParagraphs().size()) {
      throw new BadRequestException("newIndex " + index + " is out of bounds");
    }

    final Paragraph p = new Paragraph("", "", serviceContext.getAutheInfo().getUser(), new GUI());
    note.getParagraphs().add(index, p);

    noteRepository.updateNote(note);
    connectionManager.broadcast(note.getNoteId(), new SockMessage(Operation.PARAGRAPH_ADDED).put("paragraph", p).put("index", index));
    return p.getId();
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
