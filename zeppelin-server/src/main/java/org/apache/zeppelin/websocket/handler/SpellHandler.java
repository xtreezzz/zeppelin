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

import com.google.common.base.Strings;
import org.apache.zeppelin.interpreter.InterpreterResult;
import org.apache.zeppelin.notebook.Note;
import org.apache.zeppelin.notebook.NotePermissionsService;
import org.apache.zeppelin.notebook.Notebook;
import org.apache.zeppelin.notebook.core.Paragraph;
import org.apache.zeppelin.scheduler.Job;
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
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Map;


@Component
public class SpellHandler extends AbstractHandler {

  private static final Logger LOG = LoggerFactory.getLogger(SpellHandler.class);

  @Autowired
  public SpellHandler(final NotePermissionsService notePermissionsService,
                      final Notebook notebook,
                      final ConnectionManager connectionManager) {
    super(notePermissionsService, notebook, connectionManager);
  }

  //TODO(KOT): check "noteId"
  public void broadcastSpellExecution(final WebSocketSession session, final SockMessage fromMessage) throws IOException {
    final ServiceContext serviceContext = getServiceContext(fromMessage);

    final Note note = safeLoadNote("noteId", fromMessage, Permission.RUNNER, serviceContext, session);
    final Paragraph p = safeLoadParagraph("id", fromMessage, note);

    final String text = fromMessage.safeGetType("paragraph", LOG);
    final String title = fromMessage.safeGetType("title", LOG);
    final Job.Status status = fromMessage.safeGetType("status", LOG);
    final Map<String, Object> params = fromMessage.safeGetType("params", LOG);
    final Map<String, Object> config = fromMessage.safeGetType("config", LOG);
    final InterpreterResult interpreterResult = fromMessage.safeGetType("results", LOG);
    final String errorSockMessage = fromMessage.safeGetType("errorSockMessage", LOG);
    final String dateStarted = fromMessage.safeGetType("dateStarted", LOG);
    final String dateFinished = fromMessage.safeGetType("dateFinished", LOG);

    final SimpleDateFormat df = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSSX");

    p.setText(text);
    p.setTitle(title);
    p.setAuthenticationInfo(serviceContext.getAutheInfo());
    p.settings.setParams(params);
    p.setConfig(config);
    p.setResult(interpreterResult);
    p.setErrorMessage(errorSockMessage);
    p.setStatusWithoutNotification(status);

    // Spell uses ISO 8601 formatted string generated from moment
    try {
      p.setDateStarted(df.parse(dateStarted));
    } catch (final ParseException e) {
      LOG.error("Failed parse dateStarted", e);
    }

    try {
      p.setDateFinished(df.parse(dateFinished));
    } catch (final ParseException e) {
      LOG.error("Failed parse dateFinished", e);
    }

    addNewParagraphIfLastParagraphIsExecuted(note, p);
    notebook.saveNote(note);
    connectionManager.broadcast(note.getId(), new SockMessage(Operation.RUN_PARAGRAPH_USING_SPELL).put("paragraph", p));
  }


  private void addNewParagraphIfLastParagraphIsExecuted(final Note note, final Paragraph p) {
    // if it's the last paragraph and not empty, let's add a new one
    if (!(Strings.isNullOrEmpty(p.getText()) ||
            Strings.isNullOrEmpty(p.getScriptText())) &&
            note.isLastParagraph(p.getId())) {
      note.addNewParagraph(p.getAuthenticationInfo());
    }
  }

}
