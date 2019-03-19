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

import java.io.IOException;
import org.apache.zeppelin.notebook.Note;
import org.apache.zeppelin.service.ServiceContext;
import org.apache.zeppelin.storage.DatabaseNoteRepository;
import org.apache.zeppelin.websocket.ConnectionManager;
import org.apache.zeppelin.websocket.Operation;
import org.apache.zeppelin.websocket.SockMessage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import org.springframework.web.socket.WebSocketSession;

@Component
public class NoteRevisionHandler extends AbstractHandler {

  private static final Logger LOG = LoggerFactory.getLogger(NoteRevisionHandler.class);

  @Autowired
  public NoteRevisionHandler(final ConnectionManager connectionManager, final DatabaseNoteRepository noteRepository) {
    super(connectionManager, noteRepository);
  }

  public void checkpointNote(final WebSocketSession conn, final SockMessage fromMessage) throws IOException {
    final ServiceContext serviceContext = getServiceContext(fromMessage);

    final Note note = safeLoadNote("noteId", fromMessage, Permission.WRITER, serviceContext, conn);
    final String message = fromMessage.getNotNull("commitMessage");

    noteRepository.createRevision(note, message);
    listRevisionHistory(conn, fromMessage);
  }

  public void listRevisionHistory(final WebSocketSession conn, final SockMessage fromMessage) throws IOException {
    final ServiceContext serviceContext = getServiceContext(fromMessage);

    final Note note = safeLoadNote("noteId", fromMessage, Permission.READER, serviceContext, conn);

    final SockMessage message = new SockMessage(Operation.LIST_REVISION_HISTORY)
            .put("revisionList", noteRepository.getRevisions(note.getDatabaseId()));
    conn.sendMessage(message.toSend());
  }

  public void getNoteByRevision(final WebSocketSession conn, final SockMessage fromMessage) throws IOException {
    final ServiceContext serviceContext = getServiceContext(fromMessage);

    final Note note = safeLoadNote("noteId", fromMessage, Permission.READER, serviceContext, conn);
    final Long revisionId = Long.parseLong(fromMessage.getNotNull("revisionId"));

    noteRepository.checkoutRevision(note, revisionId);

    final Note revisionNote = note;//notebook.getNoteByRevision(note.getNoteId(), note.getPath(), revisionId);
    final SockMessage message = new SockMessage(Operation.NOTE_REVISION)
            .put("noteId", note.getNoteId())
            .put("revisionId", revisionId)
            .put("note", revisionNote);
    conn.sendMessage(message.toSend());
  }

  public void getNoteByRevisionForCompare(final WebSocketSession conn, final SockMessage fromMessage) throws IOException {
    final ServiceContext serviceContext = getServiceContext(fromMessage);

    final Note note = safeLoadNote("noteId", fromMessage, Permission.READER, serviceContext, conn);
    final String position = fromMessage.getNotNull("position");
    final Long revisionId = ((Double) fromMessage.getNotNull("revisionId")).longValue();

    if ("Head".equals(revisionId)) {
      noteRepository.checkoutRevision(note, null);
    } else {
      noteRepository.checkoutRevision(note, revisionId);
    }

    final SockMessage message = new SockMessage(Operation.NOTE_REVISION_FOR_COMPARE)
            .put("noteId", note.getNoteId())
            .put("revisionId", revisionId)
            .put("position", position)
            .put("note", note);
    conn.sendMessage(message.toSend());
  }

  public void setNoteRevision(final WebSocketSession conn, final SockMessage fromMessage) throws IOException {
    final ServiceContext serviceContext = getServiceContext(fromMessage);

    final Note note = safeLoadNote("noteId", fromMessage, Permission.WRITER, serviceContext, conn);
    final Long revisionId = Long.parseLong(fromMessage.getNotNull("revisionId"));

    noteRepository.applyRevision(note, revisionId);

    final SockMessage message = new SockMessage(Operation.SET_NOTE_REVISION)
            .put("status", true);
    conn.sendMessage(message.toSend());
    connectionManager.broadcast(note.getNoteId(), new SockMessage(Operation.NOTE).put("note", note));
  }
}
