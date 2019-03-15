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

import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.commons.lang3.NotImplementedException;
import org.apache.commons.lang3.StringUtils;
import org.apache.zeppelin.annotation.ZeppelinApi;
import org.apache.zeppelin.configuration.ZeppelinConfiguration;
import org.apache.zeppelin.notebook.Note;
import org.apache.zeppelin.notebook.NoteCronConfiguration;
import org.apache.zeppelin.notebook.NoteInfo;
import org.apache.zeppelin.notebook.Paragraph;
import org.apache.zeppelin.notebook.display.GUI;
import org.apache.zeppelin.storage.DatabaseNoteRepository;
import org.apache.zeppelin.service.ServiceContext;
import org.apache.zeppelin.websocket.ConnectionManager;
import org.apache.zeppelin.websocket.Operation;
import org.apache.zeppelin.websocket.SockMessage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import org.springframework.web.socket.WebSocketSession;


@Component
public class NoteHandler extends AbstractHandler {

  private static final Logger LOG = LoggerFactory.getLogger(NoteHandler.class);
  private static final String TRASH_FOLDER = "~Trash";

  private final ZeppelinConfiguration zeppelinConfiguration;

  @Autowired
  public NoteHandler(final DatabaseNoteRepository noteRepository,
                     final ConnectionManager connectionManager,
                     final ZeppelinConfiguration zeppelinConfiguration) {
    super(connectionManager, noteRepository);
    this.zeppelinConfiguration = zeppelinConfiguration;
  }


  public void listNotesInfo(final WebSocketSession conn, final SockMessage fromMessage) throws IOException {
    final ServiceContext serviceContext = getServiceContext(fromMessage);

    final List<NoteInfo> notesInfo = noteRepository.getNotesInfo();
    //final List<NoteInfo> notesInfo = zeppelinRepository.getNotesInfo(serviceContext.getUserAndRoles());
    conn.sendMessage(new SockMessage(Operation.NOTES_INFO).put("notes", notesInfo).toSend());
  }

  public void broadcastReloadedNoteList(final SockMessage fromMessage) {
    final ServiceContext serviceContext = getServiceContext(fromMessage);
    broadcastNoteList(serviceContext.getUserAndRoles());
  }

  public void getHomeNote(final WebSocketSession conn, final SockMessage fromMessage) throws IOException {
    final ServiceContext serviceContext = getServiceContext(fromMessage);

    final String noteId = zeppelinConfiguration.getString(ZeppelinConfiguration.ConfVars.ZEPPELIN_NOTEBOOK_HOMESCREEN);

    checkPermission(noteId, Permission.READER, serviceContext);
    final Note note = noteRepository.getNote(noteId);
    if (note != null) {
      connectionManager.addSubscriberToNode(note.getId(), conn);
      conn.sendMessage(new SockMessage(Operation.NOTE).put("note", note).toSend());
    } else {
      connectionManager.removeSubscribersFromAllNote(conn);
      conn.sendMessage(new SockMessage(Operation.NOTE).put("note", null).toSend());
    }
  }


  //TODO(KOT) WRONG RESULT!!!!
  public void getNote(final WebSocketSession conn, final SockMessage fromMessage) throws IOException {
    final ServiceContext serviceContext = getServiceContext(fromMessage);

    final Note note = safeLoadNote("id", fromMessage, Permission.READER, serviceContext, conn);

    //TODO(egorklimov): персональный режим временно убран 
    final Note resultNote = note;

    connectionManager.addSubscriberToNode(resultNote.getId(), conn);
    conn.sendMessage(new SockMessage(Operation.NOTE).put("note", note).toSend());
  }

  //TODO(egorklimov) check passing personalized mode flag
  public void updateNote(final WebSocketSession conn, final SockMessage fromMessage) throws IOException {
    final ServiceContext serviceContext = getServiceContext(fromMessage);

    final Note note = safeLoadNote("id", fromMessage, Permission.READER, serviceContext, conn);
    final String path = fromMessage.getNotNull("path");
    final Map config =  fromMessage.getOrDefault("config", null);

    note.setPath(path);
    if (config != null) {
      note.setNoteCronConfiguration(new NoteCronConfiguration(config));
    }
    //if (!note.getNoteCronConfiguration().equals(config)) {
    //  zeppelinRepository.refreshCron(note.getId());
    //}

    noteRepository.updateNote(note);

    final SockMessage message = new SockMessage(Operation.NOTE_UPDATED)
            .put("path", path)
            .put("config", config)
            .put("runningStatus", note.isRunning());
    connectionManager.broadcast(note.getId(), message);
    broadcastNoteList(serviceContext.getUserAndRoles());
  }

  public void deleteNote(final WebSocketSession conn, final SockMessage fromMessage) throws IOException {
    final ServiceContext serviceContext = getServiceContext(fromMessage);

    final Note note = safeLoadNote("id", fromMessage, Permission.OWNER, serviceContext, conn);
    noteRepository.removeNote(note.getId());
    connectionManager.removeNoteSubscribers(note.getId());
    broadcastNoteList(serviceContext.getUserAndRoles());
  }


  @ZeppelinApi
  public void createNote(final WebSocketSession conn, final SockMessage fromMessage) throws IOException {
    final ServiceContext serviceContext = getServiceContext(fromMessage);

    String notePath = fromMessage.getNotNull("path");
    if (!notePath.startsWith(File.separator)) {
      notePath = File.separator + notePath;
    }

    try {
      final Note note = new Note(notePath);

      // it's an empty note. so add one paragraph
      //note.addParagraph(serviceContext.getAutheInfo());
      final Paragraph paragraph = new Paragraph("", "", serviceContext.getAutheInfo().getUser(), new GUI());
      note.getParagraphs().add(paragraph);

      noteRepository.persistNote(note);

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
    String path = fromMessage.getNotNull("name");
    if (!path.startsWith(File.separator)) {
      path = File.separator + path;
    }

    final Note cloneNote = new Note(path);

    // clone all paragraphs
    note.getParagraphs().forEach(p -> {
      final Paragraph newParag =
          new Paragraph(p.getTitle(), p.getText(), p.getUser(), p.getSettings());
      newParag.setConfig(p.getConfig());
      newParag.setCreated(p.getCreated());
      newParag.setUpdated(p.getUpdated());
      cloneNote.getParagraphs().add(newParag);
    });

    noteRepository.persistNote(cloneNote);

    connectionManager.addSubscriberToNode(cloneNote.getId(), conn);
    publishNote(cloneNote, conn, serviceContext);
  }

  public void importNote(final SockMessage fromMessage) throws IOException {
    final ServiceContext serviceContext = getServiceContext(fromMessage);

    final String noteName = (String) ((Map) fromMessage.getNotNull("note")).get("name");
    final String noteJson = gson.toJson(fromMessage.getNotNull("note"));

    final String resultNoteName = noteName == null
            ? noteName
            : normalizeNotePath(noteName);

    //final Note note = zeppelinRepository.importNote(noteJson, resultNoteName, serviceContext.getAutheInfo());

    //connectionManager.broadcast(note.getId(), new SockMessage(Operation.NOTE).put("note", note));
    //broadcastNoteList(serviceContext.getUserAndRoles());
  }

  public void moveNoteToTrash(final WebSocketSession conn, final SockMessage fromMessage) throws IOException {
    final ServiceContext serviceContext = getServiceContext(fromMessage);

    final Note note = safeLoadNote("id", fromMessage, Permission.OWNER, serviceContext, conn);

    //TODO(SAN) вынести потом '~Trash' куда-нибудь
    note.setPath("/" + TRASH_FOLDER + note.getPath());
    noteRepository.updateNote(note);
    connectionManager.broadcast(note.getId(), new SockMessage(Operation.NOTE).put("note", note));
    broadcastNoteList(serviceContext.getUserAndRoles());
  }

  public void restoreNote(final WebSocketSession conn, final SockMessage fromMessage) throws IOException {
    final ServiceContext serviceContext = getServiceContext(fromMessage);

    final Note note = safeLoadNote("id", fromMessage, Permission.OWNER, serviceContext, conn);

    if (!note.getPath().startsWith("/" + TRASH_FOLDER)) {
      throw new IOException("Can not restore this note " + note.getPath() + " as it is not in trash folder");
    }

    final String destNotePath = note.getPath().replace("/" + TRASH_FOLDER, "");
    note.setPath(destNotePath);
    noteRepository.updateNote(note);
    connectionManager.broadcast(note.getId(), new SockMessage(Operation.NOTE).put("note", note));
    broadcastNoteList(serviceContext.getUserAndRoles());
  }

  public void restoreFolder(final SockMessage fromMessage) throws IOException {
    final ServiceContext serviceContext = getServiceContext(fromMessage);

    /*final String folderPath = "/" + fromMessage.safeGetType("id", LOG);

    if (!folderPath.startsWith("/" + NoteManager.TRASH_FOLDER)) {
      throw new IOException("Can not restore this folder: " + folderPath + " as it is not in trash folder");
    }
    try {
      final String destFolderPath = folderPath.replace("/" + NoteManager.TRASH_FOLDER, "");
      zeppelinRepository.moveFolder(folderPath, destFolderPath);
      broadcastNoteList(serviceContext.getUserAndRoles());
    } catch (final IOException e) {
      throw new IOException("Fail to restore folder: " + folderPath, e);
    }*/
  }

  public void renameFolder(final SockMessage fromMessage) throws IOException {
    final ServiceContext serviceContext = getServiceContext(fromMessage);

    final String oldFolderId = fromMessage.getNotNull("id");
    final String newFolderId = fromMessage.getNotNull("name");
    //TODO(zjffdu) folder permission check

    //try {
      //zeppelinRepository.moveFolder(oldFolderId, newFolderId);
      broadcastNoteList(serviceContext.getUserAndRoles());
    //} catch (final IOException e) {
     // throw new IOException("Fail to rename folder: " + oldFolderId, e);
    //}
  }

  public void moveFolderToTrash(final SockMessage fromMessage) throws IOException {
    final ServiceContext serviceContext = getServiceContext(fromMessage);

    final String folderPath = fromMessage.getNotNull("id");
    //TODO(zjffdu) folder permission check
    //TODO(zjffdu) folderPath is relative path, need to fix it in frontend

    //String destFolderPath = "/" + NoteManager.TRASH_FOLDER + "/" + folderPath;
    //if (zeppelinRepository.containsNote(destFolderPath)) {
    //  final DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");
    //  destFolderPath = destFolderPath + " " + formatter.format(LocalDateTime.now());
    //}

    //zeppelinRepository.moveFolder("/" + folderPath, destFolderPath);
    broadcastNoteList(serviceContext.getUserAndRoles());
  }

  public void removeFolder(final SockMessage fromMessage) throws IOException {
    final ServiceContext serviceContext = getServiceContext(fromMessage);

    final String folderPath = "/" + fromMessage.getNotNull("id");
    //try {
      //zeppelinRepository.removeFolder(folderPath);
      final List<NoteInfo> notesInfo = noteRepository.getNotesInfo();
      //final List<NoteInfo> notesInfo = zeppelinRepository.getNotesInfo(serviceContext.getUserAndRoles());
      for (final NoteInfo noteInfo : notesInfo) {
        connectionManager.removeNoteSubscribers(noteInfo.getId());
      }
      broadcastNoteList(serviceContext.getUserAndRoles());
    //} catch (final IOException e) {
    //  throw new IOException("Fail to remove folder: " + folderPath, e);
    //}
  }

  public void emptyTrash(final SockMessage fromMessage) throws IOException {
    //try {
      //zeppelinRepository.emptyTrash();
    //} catch (final IOException e) {
      //throw new IOException("Fail to clear trash folder", e);
    //}
  }

  public void restoreAll(final SockMessage fromMessage) throws IOException {
    final ServiceContext serviceContext = getServiceContext(fromMessage);

    //try {
      //zeppelinRepository.restoreAll();
    //  broadcastNoteList(serviceContext.getUserAndRoles());
    //} catch (final IOException e) {
    //  throw new IOException("Fail to restore all", e);
    //}
  }

  public void updatePersonalizedMode(final WebSocketSession conn, final SockMessage fromMessage) throws IOException {
    throw new NotImplementedException("Personalized mode removed");
    //    final ServiceContext serviceContext = getServiceContext(fromMessage);
    //
    //    final Note note = safeLoadNote("id", fromMessage, Permission.WRITER, serviceContext, conn);
    //    final boolean isPersonalized = fromMessage.getType("personalized", LOG).equals("true");
    //
    //    note.setPersonalizedMode(isPersonalized);
    //    zeppelinRepository.saveNote(note);
    //    connectionManager.broadcast(note.getId(), new SockMessage(Operation.NOTE).put("note", note));
  }

  public void clearAllParagraphOutput(final WebSocketSession conn, final SockMessage fromMessage) throws IOException {
    final ServiceContext serviceContext = getServiceContext(fromMessage);

    final Note note = safeLoadNote("id", fromMessage, Permission.WRITER, serviceContext, conn);
    //note.clearAllParagraphOutput();
    connectionManager.broadcast(note.getId(), new SockMessage(Operation.NOTE).put("note", note));
  }

  private void broadcastNoteList(final Set<String> userAndRoles) {
    final List<NoteInfo> notesInfo = noteRepository.getNotesInfo();
    //final List<NoteInfo> notesInfo = zeppelinRepository.getNotesInfo(userAndRoles);
    connectionManager.broadcast( new SockMessage(Operation.NOTES_INFO).put("notes", notesInfo));
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
