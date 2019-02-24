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

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.reflect.TypeToken;
import org.apache.zeppelin.display.Input;
import org.apache.zeppelin.notebook.*;
import org.apache.zeppelin.rest.exception.ForbiddenException;
import org.apache.zeppelin.rest.exception.NoteNotFoundException;
import org.apache.zeppelin.rest.exception.ParagraphNotFoundException;
import org.apache.zeppelin.service.ServiceContext;
import org.apache.zeppelin.user.AuthenticationInfo;
import org.apache.zeppelin.websocket.ConnectionManager;
import org.apache.zeppelin.websocket.SockMessage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.web.socket.WebSocketSession;

import java.util.Date;
import java.util.HashSet;
import java.util.Set;

public abstract class AbstractHandler {

  private static final Logger LOG = LoggerFactory.getLogger(AbstractHandler.class);

  protected static Gson gson = new GsonBuilder()
          .setDateFormat("yyyy-MM-dd'T'HH:mm:ssZ")
          .registerTypeAdapter(Date.class, new NotebookImportDeserializer())
          .setPrettyPrinting()
          .registerTypeAdapterFactory(Input.TypeAdapterFactory).create();

  private final NotePermissionsService notePermissionsService;
  protected final Notebook notebook;
  protected final ConnectionManager connectionManager;

  public AbstractHandler(final NotePermissionsService notePermissionsService,
                         final Notebook notebook,
                         final ConnectionManager connectionManager) {
    this.notePermissionsService = notePermissionsService;
    this.notebook = notebook;
    this.connectionManager = connectionManager;
  }

  protected ServiceContext getServiceContext(final SockMessage message) {
    final AuthenticationInfo authInfo = new AuthenticationInfo(message.principal, message.roles, message.ticket);
    final Set<String> userAndRoles = new HashSet<>();
    userAndRoles.add(message.principal);
    if (message.roles != null && !message.roles.equals("")) {
      final HashSet<String> roles = gson.fromJson(message.roles, new TypeToken<HashSet<String>>() {
      }.getType());
      if (roles != null) {
        userAndRoles.addAll(roles);
      }
    }
    return new ServiceContext(authInfo, userAndRoles);
  }


  protected Note safeLoadNote(final String paramName,
                              final SockMessage message,
                              final Permission permission,
                              final ServiceContext serviceContext,
                              final WebSocketSession conn) {
    final String noteId = connectionManager.getAssociatedNoteId(conn) != null
            ? connectionManager.getAssociatedNoteId(conn)
            : message.safeGetType(paramName, LOG);

    checkPermission(noteId, permission, serviceContext);
    final Note note = notebook.getNote(noteId);
    if (note == null) {
      throw new NoteNotFoundException(noteId);
    }
    return note;
  }

  protected ParagraphJob safeLoadParagraph(final String paramName,
                                        final SockMessage fromSockMessage,
                                        final Note note) {
    final String paragraphId = fromSockMessage.safeGetType(paramName, LOG);
    final ParagraphJob p = note.getParagraph(paragraphId);
    if (p == null) {
      throw new ParagraphNotFoundException(paragraphId);
    }
    return p;
  }


  public enum Permission {
    READER,
    WRITER,
    RUNNER,
    OWNER,
    ANY
  }

  protected void checkPermission(final String noteId,
                                 final Permission permission,
                                 final ServiceContext context) {
    Note target = notebook.getNote(noteId);
    if (permission == Permission.ANY || target == null) {
      return;
    }
    boolean isAllowed = false;
    Set<String> allowed = null;
    switch (permission) {
      case READER:
        isAllowed = notePermissionsService.isReader(noteId, context.getUserAndRoles());
        allowed = target.getReaders();
        break;
      case WRITER:
        isAllowed = notePermissionsService.isWriter(noteId, context.getUserAndRoles());
        allowed = target.getWriters();
        break;
      case RUNNER:
        isAllowed = notePermissionsService.isRunner(noteId, context.getUserAndRoles());
        allowed = target.getRunners();
        break;
      case OWNER:
        isAllowed = notePermissionsService.isOwner(noteId, context.getUserAndRoles());
        allowed = target.getOwners();
        break;
    }
    if (!isAllowed) {
      final String errorMsg = "Insufficient privileges to " + permission + " note.\n" +
              "Allowed users or roles: " + allowed + "\n" + "But the user " +
              context.getAutheInfo().getUser() + " belongs to: " + context.getUserAndRoles();
      throw new ForbiddenException(errorMsg);
    }
  }

}
