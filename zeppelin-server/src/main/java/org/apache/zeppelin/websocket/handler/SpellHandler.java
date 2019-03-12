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

import org.apache.zeppelin.repositories.FileSystemNoteRepository;
import org.apache.zeppelin.notebook.Note;
import org.apache.zeppelin.notebook.Paragraph;
import org.apache.zeppelin.service.ServiceContext;
import org.apache.zeppelin.websocket.ConnectionManager;
import org.apache.zeppelin.websocket.Operation;
import org.apache.zeppelin.websocket.SockMessage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import org.springframework.web.socket.WebSocketSession;

import java.io.IOException;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Map;


@Component
public class SpellHandler extends AbstractHandler {

  private static final Logger LOG = LoggerFactory.getLogger(SpellHandler.class);

  @Autowired
  public SpellHandler(final FileSystemNoteRepository fileSystemNoteRepository,
                      final ConnectionManager connectionManager) {
    super(connectionManager, fileSystemNoteRepository);
  }

  //TODO(KOT): check "noteId"
  //TODO(egorklimov): authInfo, config, result removed fron paragraph
  public void broadcastSpellExecution(final WebSocketSession session, final SockMessage fromMessage) throws IOException {
    final ServiceContext serviceContext = getServiceContext(fromMessage);

    final Note note = safeLoadNote("noteId", fromMessage, Permission.RUNNER, serviceContext, session);
    final Paragraph p = safeLoadParagraph("id", fromMessage, note);

    final String text = fromMessage.safeGetType("paragraph", LOG);
    final String title = fromMessage.safeGetType("title", LOG);
    final Map<String, Object> params = fromMessage.safeGetType("params", LOG);
    final String dateStarted = fromMessage.safeGetType("dateStarted", LOG);
    final String dateFinished = fromMessage.safeGetType("dateFinished", LOG);

    DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss.SSSX");

    p.setText(text);
    p.setTitle(title);
    p.getSettings().setParams(params);

    // Spell uses ISO 8601 formatted string generated from moment
    p.setCreated(LocalDateTime.parse(dateStarted, formatter));

    p.setUpdated(LocalDateTime.parse(dateFinished, formatter));

    addNewParagraphIfLastParagraphIsExecuted(note, p);
    noteRepository.updateNote(note);
    connectionManager.broadcast(note.getId(), new SockMessage(Operation.RUN_PARAGRAPH_USING_SPELL).put("paragraph", p));
  }


  private void addNewParagraphIfLastParagraphIsExecuted(final Note note, final Paragraph p) {
    // if it's the last paragraph and not empty, let's add a new one
    //TODO(egorklimov): Should get ParagraphJobContext
    //    if (!(Strings.isNullOrEmpty(p.getText()) ||
    //            Strings.isNullOrEmpty(p.getScriptText())) &&
    //            note.isLastParagraph(p.getId())) {
    //      note.addParagraph(p.getAuthenticationInfo());
  }

}
