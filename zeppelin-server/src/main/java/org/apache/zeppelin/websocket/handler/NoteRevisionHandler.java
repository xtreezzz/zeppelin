/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http:www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.zeppelin.websocket.handler;

import java.io.IOException;
import java.security.InvalidParameterException;
import org.apache.zeppelin.realm.AuthenticationInfo;
import org.apache.zeppelin.realm.AuthorizationService;
import org.apache.zeppelin.websocket.ConnectionManager;
import org.apache.zeppelin.websocket.Operation;
import org.apache.zeppelin.websocket.SockMessage;
import org.apache.zeppelin.websocket.dto.NoteDTOConverter;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import org.springframework.web.socket.WebSocketSession;
import ru.tinkoff.zeppelin.core.externalDTO.NoteDTO;
import ru.tinkoff.zeppelin.core.notebook.Note;
import ru.tinkoff.zeppelin.core.notebook.NoteRevision;
import ru.tinkoff.zeppelin.engine.NoteService;

@Component
public class NoteRevisionHandler extends AbstractHandler {

  private final NoteDTOConverter noteDTOConverter;

  @Autowired
  public NoteRevisionHandler(
      final ConnectionManager connectionManager,
      final NoteService noteService,
      final NoteDTOConverter noteDTOConverter) {
    super(connectionManager, noteService);
    this.noteDTOConverter = noteDTOConverter;
  }

  public void checkpointNote(final WebSocketSession conn, final SockMessage fromMessage)
      throws IOException {
    final AuthenticationInfo authenticationInfo = AuthorizationService.getAuthenticationInfo();
    final Note note = safeLoadNote(
        "noteId",
        fromMessage,
        Permission.WRITER,
        authenticationInfo,
        conn
    );

    final String message = fromMessage.getNotNull("commitMessage");
    noteService.persistRevision(note, message);

    listRevisionHistory(conn, fromMessage);
  }

  public void listRevisionHistory(final WebSocketSession conn, final SockMessage fromMessage)
      throws IOException {
    final AuthenticationInfo authenticationInfo = AuthorizationService.getAuthenticationInfo();
    final Note note = safeLoadNote(
        "noteId",
        fromMessage,
        Permission.READER,
        authenticationInfo,
        conn);

    final SockMessage message = new SockMessage(Operation.LIST_REVISION_HISTORY);
    message.put("revisionList", noteService.getRevisions(note));
    conn.sendMessage(message.toSend());
  }

  public void getNoteByRevision(final WebSocketSession conn, final SockMessage fromMessage)
      throws IOException {
    final AuthenticationInfo authenticationInfo = AuthorizationService.getAuthenticationInfo();
    final Note note = safeLoadNote(
        "noteId",
        fromMessage,
        Permission.READER,
        authenticationInfo,
        conn
    );

    final long revisionId = Long.parseLong(fromMessage.getNotNull("revisionId"));
    NoteRevision noteRevision = getNoteRevision(note, revisionId);

    note.setRevision(noteRevision);
    NoteDTO noteDTO = noteDTOConverter.convertNoteToDTO(note);

    final SockMessage message = new SockMessage(Operation.NOTE_REVISION)
        .put("noteId", note.getUuid())
        .put("revisionId", revisionId)
        .put("note", noteDTO);
    conn.sendMessage(message.toSend());
  }

  public void getNoteByRevisionForCompare(final WebSocketSession conn,
      final SockMessage fromMessage) throws IOException {
    final AuthenticationInfo authenticationInfo = AuthorizationService.getAuthenticationInfo();
    final Note note = safeLoadNote(
        "noteId",
        fromMessage,
        Permission.READER,
        authenticationInfo,
        conn
    );

    final String position = fromMessage.getNotNull("position");
    final String revisionId = fromMessage.getNotNull("revisionId").toString();

    if (!"Head".equals(revisionId)) {
      NoteRevision noteRevision = getNoteRevision(note, new Double(revisionId).longValue());
      note.setRevision(noteRevision);
    }

    NoteDTO noteDTO = noteDTOConverter.convertNoteToDTO(note);

    final SockMessage message = new SockMessage(Operation.NOTE_REVISION_FOR_COMPARE);
    message.put("noteId", note.getUuid());
    message.put("revisionId", revisionId);
    message.put("position", position);
    message.put("note", noteDTO);

    conn.sendMessage(message.toSend());
  }

  public void setNoteRevision(final WebSocketSession conn, final SockMessage fromMessage)
      throws IOException {
    final AuthenticationInfo authenticationInfo = AuthorizationService.getAuthenticationInfo();
    final Note note = safeLoadNote(
        "noteId",
        fromMessage,
        Permission.WRITER,
        authenticationInfo,
        conn
    );

    final long revisionId = Long.parseLong(fromMessage.getNotNull("revisionId"));
    NoteRevision noteRevision = getNoteRevision(note, revisionId);
    noteService.restoreNoteToRevision(note, noteRevision);
    NoteDTO noteDTO = noteDTOConverter.convertNoteToDTO(note);

    final SockMessage message = new SockMessage(Operation.SET_NOTE_REVISION);
    message.put("status", true);

    conn.sendMessage(message.toSend());
    connectionManager.broadcast(
        noteDTO.getDatabaseId(),
        new SockMessage(Operation.NOTE).put("note", noteDTO)
    );
  }

  private NoteRevision getNoteRevision(final Note note, final long revisionId) {
    return noteService.getRevisions(note).stream()
        .filter(r -> r.getId() == revisionId)
        .findAny()
        .orElseThrow(
            () -> new InvalidParameterException("Revision with id " + revisionId + " not found"));
  }
}
