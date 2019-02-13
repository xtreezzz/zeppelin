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

import org.apache.zeppelin.annotation.ZeppelinApi;
import org.apache.zeppelin.display.GUI;
import org.apache.zeppelin.notebook.Note;
import org.apache.zeppelin.notebook.Notebook;
import org.apache.zeppelin.notebook.NotebookAuthorization;
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
import java.util.Map;

@Component
public class NoteFormsHandler extends AbstractHandler {

  private static final Logger LOG = LoggerFactory.getLogger(NoteFormsHandler.class);

  @Autowired
  public NoteFormsHandler(final NotebookAuthorization notebookAuthorization,
                          final Notebook notebook,
                          final ConnectionManager connectionManager) {
    super(notebookAuthorization, notebook, connectionManager);
  }

  @ZeppelinApi
  public void saveNoteForms(final WebSocketSession conn, final SockMessage fromSockMessage) throws IOException {
    final ServiceContext serviceContext = getServiceContext(fromSockMessage);

    final Note note = safeLoadNote("noteId", fromSockMessage, Permission.WRITER, serviceContext, conn);
    final Map<String, Object> noteParams = fromSockMessage.safeGetType("noteParams", LOG);

    //TODO(KOT): wrong field
    note.setNoteParams(noteParams);
    notebook.saveNote(note, serviceContext.getAutheInfo());
    broadcastNoteForms(note);
  }

  @ZeppelinApi
  public void removeNoteForms(final WebSocketSession conn, final SockMessage fromSockMessage) throws IOException {
    final ServiceContext serviceContext = getServiceContext(fromSockMessage);

    final Note note = safeLoadNote("noteId", fromSockMessage, Permission.WRITER, serviceContext, conn);
    final String formName = fromSockMessage.safeGetType("formName", LOG);

    note.getNoteForms().remove(formName);
    note.getNoteParams().remove(formName);
    notebook.saveNote(note, serviceContext.getAutheInfo());

    broadcastNoteForms(note);
  }

  private void broadcastNoteForms(final Note note) {
    final GUI formsSettings = new GUI();
    formsSettings.setForms(note.getNoteForms());
    formsSettings.setParams(note.getNoteParams());
    connectionManager.broadcast(note.getId(), new SockMessage(Operation.SAVE_NOTE_FORMS).put("formsData", formsSettings));
  }
}