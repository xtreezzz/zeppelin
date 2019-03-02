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
import org.apache.zeppelin.notebook.Note;
import org.apache.zeppelin.notebook.NotePermissionsService;
import org.apache.zeppelin.notebook.Notebook;
import org.apache.zeppelin.notebook.core.Paragraph;
import org.apache.zeppelin.service.ServiceContext;
import org.apache.zeppelin.websocket.ConnectionManager;
import org.apache.zeppelin.websocket.SockMessage;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import org.springframework.web.socket.WebSocketSession;

import java.io.IOException;

@Component
public class RunnerHandler extends AbstractHandler {

  @Autowired
  public RunnerHandler(final NotePermissionsService notePermissionsService, final Notebook notebook, final ConnectionManager connectionManager) {
    super(notePermissionsService, notebook, connectionManager);
  }

  public void stopNoteExecution(final WebSocketSession conn,
                                final SockMessage fromMessage) {
    final ServiceContext serviceContext = getServiceContext(fromMessage);

    final Note note = safeLoadNote("noteId", fromMessage, Permission.RUNNER, serviceContext, conn);

    // prevent the run of new paragraphs
    note.abortExecution();

    // abort running paragraphs
    for (final Paragraph paragraph : note.getParagraphs()) {
      if (paragraph.isRunning()) {
        paragraph.abort();
        break;
      }
    }
  }

  public void runAllParagraphs(final WebSocketSession conn, final SockMessage fromSockMessage) throws IOException {
    /*
    final ServiceContext serviceContext = getServiceContext(fromSockMessage);

    final String noteId = (String) fromSockMessage.get("noteId");
    List<Map<String, Object>> paragraphs =
            gson.fromJson(String.valueOf(fromSockMessage.data.get("paragraphs")),
                    new TypeToken<List<Map<String, Object>>>() {}.getType());


    checkPermission(noteId, Permission.RUNNER, serviceContext);

    Note note = notebook.getNote(noteId);
    if (note == null) {
      throw new NoteNotFoundException(noteId);
    }

    note.setRunning(true);
    try {
      for (Map<String, Object> raw : paragraphs) {
        String paragraphId = (String) raw.get("id");
        if (paragraphId == null) {
          continue;
        }
        String text = (String) raw.get("paragraph");
        String title = (String) raw.get("title");
        Map<String, Object> params = (Map<String, Object>) raw.get("params");
        Map<String, Object> config = (Map<String, Object>) raw.get("config");

        if (!runParagraph(noteId, paragraphId, title, text, params, config, false, true, serviceContext)) {
          // stop execution when one paragraph fails.
          break;
        }
      }
    } finally {
      note.setRunning(false);
    }
    */
  }


  public void runParagraph(final WebSocketSession conn, final SockMessage fromSockMessage) throws IOException {
    /*
    String paragraphId = (String) fromSockMessage.get("id");
    String noteId = connectionManager.getAssociatedNoteId(conn);
    String text = (String) fromSockMessage.get("paragraph");
    String title = (String) fromSockMessage.get("title");
    Map<String, Object> params = (Map<String, Object>) fromSockMessage.get("params");
    Map<String, Object> config = (Map<String, Object>) fromSockMessage.get("config");
    notebookService.runParagraph(noteId, paragraphId, title, text, params, config,
            false, false, getServiceContext(fromSockMessage),
            new NotebookServer.WebSocketServiceCallback<Paragraph>(conn) {
              @Override
              public void onSuccess(Paragraph p, ServiceContext context) throws IOException {
                super.onSuccess(p, context);
                if (p.getNote().isPersonalizedMode()) {
                  Paragraph p2 = p.getNote().clearPersonalizedParagraphOutput(paragraphId,
                          context.getAutheInfo().getUser());
                  unicastParagraph(p.getNote(), p2, context.getAutheInfo().getUser());
                }

                // if it's the last paragraph and not empty, let's add a new one
                boolean isTheLastParagraph = p.getNote().isLastParagraph(paragraphId);
                if (!(Strings.isNullOrEmpty(p.getText()) ||
                        Strings.isNullOrEmpty(p.getScriptText())) &&
                        isTheLastParagraph) {
                  Paragraph newPara = p.getNote().addNewParagraph(p.getAuthenticationInfo());
                  //connectionManager.broadcastNewParagraph(p.getNote(), newPara);
                  connectionManager.broadcast(note.getId(), new SockMessage(Operation.PARAGRAPH_ADDED).put("paragraph", newPara).put("index", index));
                }
              }
            });
  }
  public void unicastParagraph(Note note, Paragraph p, String user) {
    if (!note.isPersonalizedMode() || p == null || user == null) {
      return;
    }

    if (!userSocketMap.containsKey(user)) {
      LOGGER.warn("Failed to send unicast. user {} that is not in connections map", user);
      return;
    }

    for (NotebookSocket conn : userSocketMap.get(user)) {
      Message m = new Message(Message.OP.PARAGRAPH).put("paragraph", p);
      unicast(m, conn);
    }
    */
  }
/*
  public boolean runParagraph(String noteId,
                              String paragraphId,
                              String title,
                              String text,
                              Map<String, Object> params,
                              Map<String, Object> config,
                              boolean failIfDisabled,
                              boolean blocking,
                              ServiceContext serviceContext) throws IOException {


    checkPermission(noteId, Permission.RUNNER, serviceContext);

    Note note = notebook.getNote(noteId);
    if (note == null) {
      throw new NoteNotFoundException(noteId, conn);
    }

    Paragraph p = note.getParagraph(paragraphId);
    if (p == null) {
      throw new ParagraphNotFoundException(paragraphId);
    }

    if (failIfDisabled && !p.isEnabled()) {
      throw new IOException("paragraph is disabled.");
    }

    p.setText(text);
    p.setTitle(title);
    p.setAuthenticationInfo(serviceContext.getAutheInfo());
    p.settings.setParams(params);
    p.setConfig(config);

    try {
      notebook.saveNote(note, serviceContext.getAutheInfo());
      return note.run(p.getId(), blocking);
    } catch (Exception ex) {
      LOGGER.error("Exception from run", ex);
      p.setReturn(new InterpreterResult(InterpreterResult.Code.ERROR, ex.getSockMessage()), ex);
      p.setStatus(Job.Status.ERROR);
      // don't call callback.onFailure, we just need to display the error message
      // in paragraph result section instead of pop up the error window.
      return false;
    }

  }
 */
  @ZeppelinApi
  public void cancelParagraph(final WebSocketSession conn, final SockMessage fromSockMessage) throws IOException {
    /*
    final ServiceContext serviceContext = getServiceContext(fromSockMessage);

    final String paragraphId = (String) fromSockMessage.get("id");
    final String noteId = connectionManager.getAssociatedNoteId(conn) != null
            ? connectionManager.getAssociatedNoteId(conn)
            : (String) fromSockMessage.get("noteId");
    checkPermission(noteId, Permission.RUNNER, serviceContext);
    final Note note = notebook.getNote(noteId);
    if (note == null) {
      throw new NoteNotFoundException(noteId, conn);
    }

    Paragraph p = note.getParagraph(paragraphId);
    if (p == null) {
      throw new ParagraphNotFoundException(paragraphId);
    }
    p.abort();
    */
  }

}
