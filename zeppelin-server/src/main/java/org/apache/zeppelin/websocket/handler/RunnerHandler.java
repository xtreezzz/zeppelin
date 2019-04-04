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
import org.apache.zeppelin.NoteExecutorService;
import org.apache.zeppelin.NoteService;
import org.apache.zeppelin.annotation.ZeppelinApi;
import org.apache.zeppelin.notebook.Note;
import org.apache.zeppelin.notebook.Paragraph;
import org.apache.zeppelin.service.ServiceContext;
import org.apache.zeppelin.websocket.ConnectionManager;
import org.apache.zeppelin.websocket.Operation;
import org.apache.zeppelin.websocket.SockMessage;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import org.springframework.web.socket.WebSocketSession;

import java.io.IOException;

@Component
public class RunnerHandler extends AbstractHandler {

  private final NoteExecutorService noteExecutorService;


  @Autowired
  public RunnerHandler(final ConnectionManager connectionManager,
                       final NoteService noteService,
                       final NoteExecutorService noteExecutorService) {
    super(connectionManager, noteService);
    this.noteExecutorService = noteExecutorService;
  }


  public void stopNoteExecution(final WebSocketSession conn,
                                final SockMessage fromMessage) {
    final ServiceContext serviceContext = getServiceContext(fromMessage);
    /*

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
    */
  }

  public void runAllParagraphs(final WebSocketSession conn, final SockMessage fromMessage) {
    final ServiceContext serviceContext = getServiceContext(fromMessage);
    final Note note = safeLoadNote("noteId", fromMessage, Permission.RUNNER, serviceContext, conn);
    noteExecutorService.run(note, noteService.getParapraphs(note));
  }


  public void runParagraph(final WebSocketSession conn, final SockMessage fromMessage)
      throws InterruptedException {
    final ServiceContext serviceContext = getServiceContext(fromMessage);

    final Note note = safeLoadNote("noteId", fromMessage, Permission.WRITER, serviceContext, conn);
    final Paragraph p = safeLoadParagraph("id", fromMessage, note);

    noteExecutorService.run(note, Lists.newArrayList(p));
    connectionManager.broadcast(note.getUuid(), new SockMessage(Operation.PARAGRAPH).put("paragraph", p));
  }

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
