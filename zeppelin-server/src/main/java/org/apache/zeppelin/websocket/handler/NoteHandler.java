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

import org.apache.commons.lang.StringUtils;
import org.apache.zeppelin.annotation.ZeppelinApi;
import org.apache.zeppelin.conf.ZeppelinConfiguration;
import org.apache.zeppelin.notebook.*;
import org.apache.zeppelin.service.ServiceContext;
import org.apache.zeppelin.user.AuthenticationInfo;
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
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.apache.zeppelin.conf.ZeppelinConfiguration.ConfVars.ZEPPELIN_NOTEBOOK_HOMESCREEN;

@Component
public class NoteHandler extends AbstractHandler {

  private static final Logger LOG = LoggerFactory.getLogger(NoteHandler.class);

  private final AngularObjectsHandler angularObjectsService;
  private final ZeppelinConfiguration zeppelinConfiguration;

  @Autowired
  public NoteHandler(final NotebookAuthorization notebookAuthorization,
                     final Notebook notebook,
                     final ConnectionManager connectionManager,
                     final AngularObjectsHandler angularObjectsService,
                     final ZeppelinConfiguration zeppelinConfiguration) {
    super(notebookAuthorization, notebook, connectionManager);
    this.angularObjectsService = angularObjectsService;
    this.zeppelinConfiguration = zeppelinConfiguration;
  }


  public void listNotesInfo(final WebSocketSession conn, final SockMessage fromMessage) throws IOException {
    final ServiceContext serviceContext = getServiceContext(fromMessage);

    final List<NoteInfo> notesInfo = notebook.getNotesInfo(serviceContext.getUserAndRoles());
    conn.sendMessage(new SockMessage(Operation.NOTES_INFO).put("notes", notesInfo).toSend());
  }

  public void broadcastReloadedNoteList(final SockMessage fromMessage) {
    final ServiceContext serviceContext = getServiceContext(fromMessage);
    broadcastNoteList(serviceContext.getUserAndRoles());
  }

  public void getHomeNote(final WebSocketSession conn, final SockMessage fromMessage) throws IOException {
    final ServiceContext serviceContext = getServiceContext(fromMessage);

    final String noteId = notebook.getConf().getString(ZEPPELIN_NOTEBOOK_HOMESCREEN);

    checkPermission(noteId, Permission.READER, serviceContext);
    final Note note = notebook.getNote(noteId);
    if (note != null) {
      connectionManager.addSubscriberToNode(note.getId(), conn);
      conn.sendMessage(new SockMessage(Operation.NOTE).put("note", note).toSend());
      angularObjectsService.sendAllAngularObjects(note, serviceContext.getAutheInfo().getUser(), conn);
    } else {
      connectionManager.removeSubscribersFromAllNote(conn);
      conn.sendMessage(new SockMessage(Operation.NOTE).put("note", null).toSend());
    }
  }


  //TODO(KOT) WRONG RESULT!!!!
  public void getNote(final WebSocketSession conn, final SockMessage fromMessage) throws IOException {
    final ServiceContext serviceContext = getServiceContext(fromMessage);

    final Note note = safeLoadNote("id", fromMessage, Permission.READER, serviceContext, conn);

    final Note resultNote = note.isPersonalizedMode()
            ? note.getUserNote(serviceContext.getAutheInfo().getUser())
            : note;

    connectionManager.addSubscriberToNode(resultNote.getId(), conn);
    conn.sendMessage(new SockMessage(Operation.NOTE).put("note", note).toSend());
    angularObjectsService.sendAllAngularObjects(note, serviceContext.getAutheInfo().getUser(), conn);
  }

  public void updateNote(final WebSocketSession conn, final SockMessage fromMessage) throws IOException {
    final ServiceContext serviceContext = getServiceContext(fromMessage);

    final Note note = safeLoadNote("id", fromMessage, Permission.READER, serviceContext, conn);
    final String name = fromMessage.safeGetType("name", LOG);
    final Map<String, Object> config = fromMessage.safeGetType("config", LOG);

    if (!(Boolean) note.getConfig().get("isZeppelinNotebookCronEnable")) {
      config.remove("cron");
    }
    final boolean cronUpdated = isCronUpdated(config, note.getConfig());
    note.setName(name);
    note.setConfig(config);
    if (cronUpdated) {
      notebook.refreshCron(note.getId());
    }

    notebook.saveNote(note);

    final SockMessage message = new SockMessage(Operation.NOTE_UPDATED)
            .put("name", name)
            .put("config", config)
            .put("info", note.getInfo());
    connectionManager.broadcast(note.getId(), message);

    broadcastNoteList(serviceContext.getUserAndRoles());
  }


  @ZeppelinApi
  public void renameNote(final WebSocketSession conn, final SockMessage fromMessage) throws IOException {
    final ServiceContext serviceContext = getServiceContext(fromMessage);

    final Note note = safeLoadNote("id", fromMessage, Permission.OWNER, serviceContext, conn);
    final String name = fromMessage.safeGetType("name", LOG);
    final boolean isRelativePath = fromMessage.getType("relative", LOG) != null && (Boolean) fromMessage.getType("relative", LOG);

    String newName = StringUtils.EMPTY;
    if (isRelativePath && !note.getParentPath().equals("/")) {
      newName = note.getParentPath() + "/" + name;
    } else {
      if (!name.startsWith("/")) {
        newName = "/" + name;
      }
    }
    note.setCronSupported(notebook.getConf());
    notebook.moveNote(note.getId(), newName);
    connectionManager.broadcast(note.getId(), new SockMessage(Operation.NOTE).put("note", note));
    broadcastNoteList(serviceContext.getUserAndRoles());
  }

  public void deleteNote(final WebSocketSession conn, final SockMessage fromMessage) throws IOException {
    final ServiceContext serviceContext = getServiceContext(fromMessage);

    final Note note = safeLoadNote("id", fromMessage, Permission.OWNER, serviceContext, conn);
    notebook.removeNote(note.getId(), serviceContext.getAutheInfo());
    connectionManager.removeNoteSubscribers(note.getId());
    broadcastNoteList(serviceContext.getUserAndRoles());
  }


  @ZeppelinApi
  public void createNote(final WebSocketSession conn, final SockMessage fromMessage) throws IOException {
    final ServiceContext serviceContext = getServiceContext(fromMessage);

    final String noteName = fromMessage.safeGetType("name", LOG);
    final String defaultInterpreterGroup = fromMessage.getType("defaultInterpreterGroup", LOG) == null
            ? fromMessage.safeGetType("defaultInterpreterGroup", LOG)
            : zeppelinConfiguration.getString(ZeppelinConfiguration.ConfVars.ZEPPELIN_INTERPRETER_GROUP_DEFAULT);

    try {
      final Note note = notebook.createNote(normalizeNotePath(noteName),
              defaultInterpreterGroup,
              serviceContext.getAutheInfo());

      // it's an empty note. so add one paragraph
      note.addNewParagraph(serviceContext.getAutheInfo());
      notebook.saveNote(note);

      connectionManager.addSubscriberToNode(note.getId(), conn);
      publishNote(note, conn, serviceContext);
    } catch (final IOException e) {
      throw new IOException("Failed to create note.", e);
    }
  }

  @ZeppelinApi
  public void cloneNote(final WebSocketSession conn, final SockMessage fromMessage) throws IOException {
    final ServiceContext serviceContext = getServiceContext(fromMessage);

    final Note note = safeLoadNote("id", fromMessage, Permission.READER, serviceContext, conn);
    final String name = fromMessage.safeGetType("name", LOG);

    final String newNoteName = StringUtils.isBlank(name)
            ? "/Cloned Note_" + note.getId()
            : name;

    final Note newNote = notebook.cloneNote(note.getId(), normalizeNotePath(newNoteName), serviceContext.getAutheInfo());

    connectionManager.addSubscriberToNode(newNote.getId(), conn);
    publishNote(newNote, conn, serviceContext);
  }

  public void importNote(final SockMessage fromMessage) throws IOException {
    final ServiceContext serviceContext = getServiceContext(fromMessage);

    final String noteName = (String) ((Map) fromMessage.safeGetType("note", LOG)).get("name");
    final String noteJson = gson.toJson(fromMessage.safeGetType("note", LOG));

    final String resultNoteName = noteName == null
            ? noteName
            : normalizeNotePath(noteName);

    final Note note = notebook.importNote(noteJson, resultNoteName, serviceContext.getAutheInfo());

    connectionManager.broadcast(note.getId(), new SockMessage(Operation.NOTE).put("note", note));
    broadcastNoteList(serviceContext.getUserAndRoles());
  }

  public void moveNoteToTrash(final WebSocketSession conn, final SockMessage fromMessage) throws IOException {
    final ServiceContext serviceContext = getServiceContext(fromMessage);

    final Note note = safeLoadNote("id", fromMessage, Permission.OWNER, serviceContext, conn);

    String destNotePath = "/" + NoteManager.TRASH_FOLDER + note.getPath();
    if (notebook.containsNote(destNotePath)) {
      final DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");
      destNotePath = destNotePath + " " + formatter.format(LocalDateTime.now());
    }
    notebook.moveNote(note.getId(), destNotePath);
    connectionManager.broadcast(note.getId(), new SockMessage(Operation.NOTE).put("note", note));
    broadcastNoteList(serviceContext.getUserAndRoles());
  }

  public void restoreNote(final WebSocketSession conn, final SockMessage fromMessage) throws IOException {
    final ServiceContext serviceContext = getServiceContext(fromMessage);

    final Note note = safeLoadNote("id", fromMessage, Permission.OWNER, serviceContext, conn);

    if (!note.getPath().startsWith("/" + NoteManager.TRASH_FOLDER)) {
      throw new IOException("Can not restore this note " + note.getPath() + " as it is not in trash folder");
    }

    try {
      final String destNotePath = note.getPath().replace("/" + NoteManager.TRASH_FOLDER, "");
      notebook.moveNote(note.getId(), destNotePath);
      connectionManager.broadcast(note.getId(), new SockMessage(Operation.NOTE).put("note", note));
      broadcastNoteList(serviceContext.getUserAndRoles());
    } catch (final IOException e) {
      throw new IOException("Fail to restore note: " + note.getId(), e);
    }
  }

  public void restoreFolder(final SockMessage fromMessage) throws IOException {
    final ServiceContext serviceContext = getServiceContext(fromMessage);

    final String folderPath = "/" + fromMessage.safeGetType("id", LOG);

    if (!folderPath.startsWith("/" + NoteManager.TRASH_FOLDER)) {
      throw new IOException("Can not restore this folder: " + folderPath + " as it is not in trash folder");
    }
    try {
      final String destFolderPath = folderPath.replace("/" + NoteManager.TRASH_FOLDER, "");
      notebook.moveFolder(folderPath, destFolderPath);
      broadcastNoteList(serviceContext.getUserAndRoles());
    } catch (final IOException e) {
      throw new IOException("Fail to restore folder: " + folderPath, e);
    }
  }

  public void renameFolder(final SockMessage fromMessage) throws IOException {
    final ServiceContext serviceContext = getServiceContext(fromMessage);

    final String oldFolderId = fromMessage.safeGetType("id", LOG);
    final String newFolderId = fromMessage.safeGetType("name", LOG);
    //TODO(zjffdu) folder permission check

    try {
      notebook.moveFolder(oldFolderId, newFolderId);
      broadcastNoteList(serviceContext.getUserAndRoles());
    } catch (final IOException e) {
      throw new IOException("Fail to rename folder: " + oldFolderId, e);
    }
  }

  public void moveFolderToTrash(final SockMessage fromMessage) throws IOException {
    final ServiceContext serviceContext = getServiceContext(fromMessage);

    final String folderPath = fromMessage.safeGetType("id", LOG);
    //TODO(zjffdu) folder permission check
    //TODO(zjffdu) folderPath is relative path, need to fix it in frontend

    String destFolderPath = "/" + NoteManager.TRASH_FOLDER + "/" + folderPath;
    if (notebook.containsNote(destFolderPath)) {
      final DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");
      destFolderPath = destFolderPath + " " + formatter.format(LocalDateTime.now());
    }

    notebook.moveFolder("/" + folderPath, destFolderPath);
    broadcastNoteList(serviceContext.getUserAndRoles());
  }

  public void removeFolder(final SockMessage fromMessage) throws IOException {
    final ServiceContext serviceContext = getServiceContext(fromMessage);

    final String folderPath = "/" + fromMessage.safeGetType("id", LOG);
    try {
      notebook.removeFolder(folderPath);
      final List<NoteInfo> notesInfo = notebook.getNotesInfo(serviceContext.getUserAndRoles());
      for (final NoteInfo noteInfo : notesInfo) {
        connectionManager.removeNoteSubscribers(noteInfo.getId());
      }
      broadcastNoteList(serviceContext.getUserAndRoles());
    } catch (final IOException e) {
      throw new IOException("Fail to remove folder: " + folderPath, e);
    }
  }

  public void emptyTrash(final SockMessage fromMessage) throws IOException {
    try {
      notebook.emptyTrash();
    } catch (final IOException e) {
      throw new IOException("Fail to clear trash folder", e);
    }
  }

  public void restoreAll(final SockMessage fromMessage) throws IOException {
    final ServiceContext serviceContext = getServiceContext(fromMessage);

    try {
      notebook.restoreAll();
      broadcastNoteList(serviceContext.getUserAndRoles());
    } catch (final IOException e) {
      throw new IOException("Fail to restore all", e);
    }
  }

  public void updatePersonalizedMode(final WebSocketSession conn, final SockMessage fromMessage) throws IOException {
    final ServiceContext serviceContext = getServiceContext(fromMessage);

    final Note note = safeLoadNote("id", fromMessage, Permission.WRITER, serviceContext, conn);
    final boolean isPersonalized = fromMessage.getType("personalized", LOG).equals("true");

    note.setPersonalizedMode(isPersonalized);
    notebook.saveNote(note);
    connectionManager.broadcast(note.getId(), new SockMessage(Operation.NOTE).put("note", note));
  }

  public void clearAllParagraphOutput(final WebSocketSession conn, final SockMessage fromMessage) throws IOException {
    final ServiceContext serviceContext = getServiceContext(fromMessage);

    final Note note = safeLoadNote("id", fromMessage, Permission.WRITER, serviceContext, conn);
    note.clearAllParagraphOutput();
    connectionManager.broadcast(note.getId(), new SockMessage(Operation.NOTE).put("note", note));
  }

  private void broadcastNoteList(final Set<String> userAndRoles) {
    final List<NoteInfo> notesInfo = notebook.getNotesInfo(userAndRoles);
    connectionManager.broadcast( new SockMessage(Operation.NOTES_INFO).put("notes", notesInfo));
  }

  //TODO(KOT) check logic
  private boolean isCronUpdated(final Map<String, Object> configA, final Map<String, Object> configB) {
    if (configA.get("cron") != null
            && configB.get("cron") != null
            && configA.get("cron").equals(configB.get("cron"))) {
      return true;
    } else if (configA.get("cron") == null && configB.get("cron") == null) {
      return false;
    } else return configA.get("cron") != null || configB.get("cron") != null;
  }

  private String normalizeNotePath(String notePath) throws IOException {
    if (StringUtils.isBlank(notePath)) {
      notePath = "/Untitled Note";
    }
    if (!notePath.startsWith("/")) {
      notePath = "/" + notePath;
    }

    notePath = notePath.replace("\r", " ").replace("\n", " ");
    final int pos = notePath.lastIndexOf("/");
    if ((notePath.length() - pos) > 255) {
      throw new IOException("Note name must be less than 255");
    }

    if (notePath.contains("..")) {
      throw new IOException("Note name can not contain '..'");
    }
    return notePath;
  }

  private void publishNote(final Note note,
                           final WebSocketSession conn,
                           final ServiceContext serviceContext) throws IOException {
    conn.sendMessage(new SockMessage(Operation.NEW_NOTE).put("note", note).toSend());
    broadcastNoteList(serviceContext.getUserAndRoles());
  }
}
