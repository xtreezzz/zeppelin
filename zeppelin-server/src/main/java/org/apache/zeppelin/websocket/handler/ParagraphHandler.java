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
import org.apache.commons.lang3.StringUtils;
import org.apache.zeppelin.conf.ZeppelinConfiguration;
import org.apache.zeppelin.notebook.Note;
import org.apache.zeppelin.notebook.NotePermissionsService;
import org.apache.zeppelin.notebook.Notebook;
import org.apache.zeppelin.notebook.ParagraphJob;
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

import java.io.IOException;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.Map;

@Component
public class ParagraphHandler extends AbstractHandler {

  private static final Logger LOG = LoggerFactory.getLogger(ParagraphHandler.class);

  private final Boolean collaborativeModeEnable;

  @Autowired
  public ParagraphHandler(final NotePermissionsService notePermissionsService,
                          final Notebook notebook,
                          final ConnectionManager connectionManager) {
    super(notePermissionsService, notebook, connectionManager);
    this.collaborativeModeEnable = ZeppelinConfiguration
            .create()
            .isZeppelinNotebookCollaborativeModeEnable();
  }

  public void updateParagraph(final WebSocketSession conn, final SockMessage fromMessage) throws IOException {
    final ServiceContext serviceContext = getServiceContext(fromMessage);

    final Note note = safeLoadNote("noteId", fromMessage, Permission.WRITER, serviceContext, conn);
    final ParagraphJob p = safeLoadParagraph("id", fromMessage, note);

    final String title = fromMessage.safeGetType("title", LOG);
    final String text = fromMessage.safeGetType("paragraph", LOG);
    final Map<String, Object> params = fromMessage.safeGetType("params", LOG);
    final Map<String, Object> config = fromMessage.safeGetType("config", LOG);

    p.settings.setParams(params);
    p.setConfig(config);
    p.setTitle(title);
    p.setText(text);

    notebook.saveNote(note);
    connectionManager.broadcast(note.getId(), new SockMessage(Operation.PARAGRAPH).put("paragraph", p));
  }

  public void patchParagraph(final WebSocketSession conn, final SockMessage fromMessage) throws IOException {
    final ServiceContext serviceContext = getServiceContext(fromMessage);

    if (!collaborativeModeEnable) {
      return;
    }

    final Note note = safeLoadNote("noteId", fromMessage, Permission.WRITER, serviceContext, conn);
    final ParagraphJob p = safeLoadParagraph("id", fromMessage, note);

    final String paragraphId = fromMessage.safeGetType("id", LOG);
    final String patchText = fromMessage.safeGetType("patch", LOG);

    String paragraphText = p.getText() == null ? StringUtils.EMPTY : p.getText();

    final DiffMatchPatch dmp = new DiffMatchPatch();
    final LinkedList<DiffMatchPatch.Patch> patches = Lists.newLinkedList(dmp.patchFromText(patchText));
    paragraphText = (String) dmp.patchApply(patches, paragraphText)[0];

    p.setText(paragraphText);
    notebook.saveNote(note);

    final SockMessage message = new SockMessage(Operation.PATCH_PARAGRAPH)
            .put("patch", patchText)
            .put("paragraphId", paragraphId);
    connectionManager.broadcast(note.getId(), message);
  }

  public void removeParagraph(final WebSocketSession conn, final SockMessage fromMessage) throws IOException {
    final ServiceContext serviceContext = getServiceContext(fromMessage);

    final Note note = safeLoadNote("noteId", fromMessage, Permission.WRITER, serviceContext, conn);
    final ParagraphJob p = safeLoadParagraph("id", fromMessage, note);

    note.removeParagraph(serviceContext.getAutheInfo().getUser(), p.getId());
    notebook.saveNote(note);

    final SockMessage message = new SockMessage(Operation.PARAGRAPH_REMOVED)
            .put("id", p.getId());
    connectionManager.broadcast(note.getId(), message);
  }

  public void clearParagraphOutput(final WebSocketSession conn, final SockMessage fromMessage) throws IOException {
    final ServiceContext serviceContext = getServiceContext(fromMessage);

    final Note note = safeLoadNote("noteId", fromMessage, Permission.WRITER, serviceContext, conn);
    final ParagraphJob p = safeLoadParagraph("id", fromMessage, note);

    note.clearParagraphOutput(p.getId());
    connectionManager.broadcast(note.getId(), new SockMessage(Operation.PARAGRAPH).put("paragraph", p));
  }

  public void moveParagraph(final WebSocketSession conn, final SockMessage fromMessage) throws IOException {
    final ServiceContext serviceContext = getServiceContext(fromMessage);

    final Note note = safeLoadNote("noteId", fromMessage, Permission.WRITER, serviceContext, conn);
    final ParagraphJob p = safeLoadParagraph("id", fromMessage, note);
    final Integer index = fromMessage.safeGetType("index", LOG);

    if (index > 0 && index >= note.getParagraphCount()) {
      throw new BadRequestException("newIndex " + index + " is out of bounds");
    }

    note.moveParagraph(p.getId(), index);
    notebook.saveNote(note);

    final SockMessage message = new SockMessage(Operation.PARAGRAPH_MOVED)
            .put("id", p.getId())
            .put("index", index);
    connectionManager.broadcast(note.getId(), message);
  }

  public String insertParagraph(final WebSocketSession conn, final SockMessage fromMessage) throws IOException {
    final ServiceContext serviceContext = getServiceContext(fromMessage);

    final Note note = safeLoadNote("noteId", fromMessage, Permission.WRITER, serviceContext, conn);
    final Integer index = fromMessage.safeGetType("index", LOG);
    final Map<String, Object> config = fromMessage.getType("config", LOG) != null
            ? fromMessage.getType("config", LOG)
            : new HashMap<>();

    if (index > 0 && index >= note.getParagraphCount()) {
      throw new BadRequestException("newIndex " + index + " is out of bounds");
    }

    final ParagraphJob newPara = note.insertNewParagraph(index, serviceContext.getAutheInfo());
    newPara.setConfig(config);
    notebook.saveNote(note);
    connectionManager.broadcast(note.getId(), new SockMessage(Operation.PARAGRAPH_ADDED).put("paragraph", newPara).put("index", index));
    return newPara.getId();
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
