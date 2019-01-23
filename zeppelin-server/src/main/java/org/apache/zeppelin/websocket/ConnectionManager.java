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

package org.apache.zeppelin.websocket;


import org.apache.zeppelin.conf.ZeppelinConfiguration;
import org.apache.zeppelin.display.GUI;
import org.apache.zeppelin.notebook.Note;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;
import org.springframework.web.socket.WebSocketSession;

import java.util.HashSet;
import java.util.Map;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;

/**
 * Manager class for managing websocket connections
 */
@Component
public class ConnectionManager {

  private static final Logger LOGGER = LoggerFactory.getLogger(ConnectionManager.class);

  private final Queue<WebSocketSession> activeSessions = new ConcurrentLinkedQueue<>();
  // noteId -> connection
  private final Map<String, Queue<WebSocketSession>> noteSocketMap = new ConcurrentHashMap<>();

  private final Boolean collaborativeModeEnable = ZeppelinConfiguration
          .create()
          .isZeppelinNotebookCollaborativeModeEnable();


  public String getAssociatedNoteId(final WebSocketSession socket) {
    final Set<String> noteIds = noteSocketMap.keySet();
    for (final String noteId : noteIds) {
      final Queue<WebSocketSession> sockets = noteSocketMap.get(noteId);
      if (sockets.contains(socket)) {
        return noteId;
      }
    }
    return null;
  }


  public void addSession(final WebSocketSession session) {
    activeSessions.add(session);
  }

  public void removeSession(final WebSocketSession session) {
    activeSessions.remove(session);
  }

  public void addSubscriberToNode(final String noteId, final WebSocketSession socket) {
    LOGGER.debug("Add connection {} to note: {}", socket, noteId);
    // make sure a socket relates only an single note.
    removeSubscribersFromAllNote(socket);

    final Queue<WebSocketSession> sessions = noteSocketMap.computeIfAbsent(noteId, k -> new ConcurrentLinkedQueue<>());
    if (!sessions.contains(socket)) {
      sessions.add(socket);
    }
    checkCollaborativeStatus(noteId, sessions);
  }

  public void removeSubscriberFromNote(final String noteId, final WebSocketSession socket) {
    LOGGER.debug("Remove connection {} from note: {}", socket, noteId);
    final Queue<WebSocketSession> sessions = noteSocketMap.get(noteId);
    if (sessions != null) {
      sessions.remove(socket);
    }
    checkCollaborativeStatus(noteId, sessions);
  }

  public void removeNoteSubscribers(final String noteId) {
    noteSocketMap.remove(noteId);
  }

  public void removeSubscribersFromAllNote(final WebSocketSession socket) {
    final Set<String> noteIds = noteSocketMap.keySet();
    for (final String noteId : noteIds) {
      removeSubscriberFromNote(noteId, socket);
    }
  }

  private void checkCollaborativeStatus(final String noteId, final Queue<WebSocketSession> socketList) {
    if (!collaborativeModeEnable) {
      return;
    }

    final boolean collaborativeStatusNew = socketList.size() > 1;

    final SockMessage message = new SockMessage(Operation.COLLABORATIVE_MODE_STATUS)
            .put("status", collaborativeStatusNew);

    if (collaborativeStatusNew) {
      final HashSet<String> userList = new HashSet<>();
      for (final WebSocketSession noteSocket : socketList) {
        userList.add(noteSocket.getPrincipal().getName());
      }
      message.put("users", userList);
    }
    broadcast(noteId, message);
  }

  public void broadcast(final SockMessage m) {
    for (final WebSocketSession session : activeSessions) {
      try {
        session.sendMessage(m.toSend());
      } catch (final Exception e) {
        LOGGER.error("Send error: " + m, e);
      }
    }
  }

  public void broadcast(final String noteId, final SockMessage m) {
    final Queue<WebSocketSession> sessions = noteSocketMap.get(noteId);
    if (sessions == null || sessions.size() == 0) {
      return;
    }
    LOGGER.debug("SEND >> " + m);
    for (final WebSocketSession session : sessions) {
      try {
        session.sendMessage(m.toSend());
      } catch (final Exception e) {
        LOGGER.error("Send error: " + m, e);
      }
    }
  }


  //TODO(KOT): вернуть обратно
  private void broadcastNoteForms(final Note note) {
    final GUI formsSettings = new GUI();
    formsSettings.setForms(note.getNoteForms());
    formsSettings.setParams(note.getNoteParams());
  //broadcast(note.getId(), new Message(Message.OP.SAVE_NOTE_FORMS)
  //          .put("formsData", formsSettings));
  }
}
