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

import org.apache.commons.lang3.StringUtils;
import org.apache.zeppelin.configuration.ZeppelinConfiguration;
import org.apache.zeppelin.notebook.display.GUI;
import org.apache.zeppelin.service.ServiceContext;
import org.apache.zeppelin.websocket.ConnectionManager;
import org.apache.zeppelin.websocket.Operation;
import org.apache.zeppelin.websocket.SockMessage;
import org.apache.zeppelin.websocket.dto.NoteDTOConverter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import org.springframework.web.socket.WebSocketSession;
import ru.tinkoff.zeppelin.core.externalDTO.NoteDTO;
import ru.tinkoff.zeppelin.core.notebook.Note;
import ru.tinkoff.zeppelin.core.notebook.NoteInfo;
import ru.tinkoff.zeppelin.core.notebook.Paragraph;
import ru.tinkoff.zeppelin.engine.NoteService;

import java.io.File;
import java.io.IOException;
import java.time.LocalDateTime;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;


@Component
public class NoteHandler extends AbstractHandler {

  private static final Logger LOG = LoggerFactory.getLogger(NoteHandler.class);
  private static final String TRASH_FOLDER = "~Trash";

  private final ZeppelinConfiguration zeppelinConfiguration;
  private final NoteDTOConverter noteDTOConverter;

  @Autowired
  public NoteHandler(final NoteService noteService,
                     final ConnectionManager connectionManager,
                     final ZeppelinConfiguration zeppelinConfiguration,
                     final NoteDTOConverter noteDTOConverter) {
    super(connectionManager, noteService);
    this.zeppelinConfiguration = zeppelinConfiguration;
    this.noteDTOConverter = noteDTOConverter;
  }


  public void listNotesInfo(final WebSocketSession conn, final SockMessage fromMessage) throws IOException {
    final ServiceContext serviceContext = getServiceContext(fromMessage);

    final List<Note> notes = noteService.getAllNotes();
    // TODO: filter by access rights

    final List<NoteInfo> notesInfo = notes
            .stream()
            .map(NoteInfo::new)
            .collect(Collectors.toList());

    conn.sendMessage(new SockMessage(Operation.NOTES_INFO).put("notes", notesInfo).toSend());
  }

  public void getHomeNote(final WebSocketSession conn, final SockMessage fromMessage) throws IOException {
    final ServiceContext serviceContext = getServiceContext(fromMessage);

    final String noteId = zeppelinConfiguration.getString(ZeppelinConfiguration.ConfVars.ZEPPELIN_NOTEBOOK_HOMESCREEN);

    checkPermission(0L, Permission.READER, serviceContext);
    final Note note = noteService.getNote(noteId);
    if (note != null) {
      connectionManager.addSubscriberToNode(note.getId(), conn);
      conn.sendMessage(new SockMessage(Operation.NOTE).put("note", note).toSend());
    } else {
      connectionManager.removeSubscribersFromAllNote(conn);
      conn.sendMessage(new SockMessage(Operation.NOTE).put("note", null).toSend());
    }
  }

  public void getNote(final WebSocketSession conn, final SockMessage fromMessage) throws IOException {
    final ServiceContext serviceContext = getServiceContext(fromMessage);
    final Note note = safeLoadNote("id", fromMessage, Permission.READER, serviceContext, conn);
    connectionManager.addSubscriberToNode(note.getId(), conn);
    final NoteDTO noteDTO = noteDTOConverter.convertNoteToDTO(note);
    conn.sendMessage(new SockMessage(Operation.NOTE).put("note", noteDTO).toSend());
  }

  public void updateNote(final WebSocketSession conn, final SockMessage fromMessage) throws IOException {
    final ServiceContext serviceContext = getServiceContext(fromMessage);
    final Note note = safeLoadNote("id", fromMessage, Permission.READER, serviceContext, conn);
    final String path = fromMessage.getNotNull("path");
    final Map config = fromMessage.getOrDefault("config", null);

    note.setPath(path);

    noteService.updateNote(note);
  }

  public void deleteNote(final WebSocketSession conn, final SockMessage fromMessage) throws IOException {
    final ServiceContext serviceContext = getServiceContext(fromMessage);

    final Note note = safeLoadNote("id", fromMessage, Permission.OWNER, serviceContext, conn);
    noteService.deleteNote(note);

    connectionManager.removeNoteSubscribers(note.getId());
  }

  public void createNote(final WebSocketSession conn, final SockMessage fromMessage) throws IOException {
    final ServiceContext serviceContext = getServiceContext(fromMessage);

    String notePath = fromMessage.getNotNull("path");
    if (!notePath.startsWith(File.separator)) {
      notePath = File.separator + notePath;
    }

    try {
      final Note note = new Note(notePath);
      noteService.persistNote(note);

      // it's an empty note. so add one paragraph
      final Paragraph paragraph = new Paragraph();
      paragraph.setId(null);
      paragraph.setNoteId(note.getId());
      paragraph.setTitle(StringUtils.EMPTY);
      paragraph.setText(StringUtils.EMPTY);
      paragraph.setShebang(null);
      paragraph.setCreated(LocalDateTime.now());
      paragraph.setUpdated(LocalDateTime.now());
      paragraph.setPosition(0);
      paragraph.setJobId(null);
      paragraph.setConfig(new HashMap<>());
      paragraph.setSettings(new GUI());
      noteService.persistParagraph(note, paragraph);

      connectionManager.addSubscriberToNode(note.getId(), conn);
      conn.sendMessage(new SockMessage(Operation.NEW_NOTE).put("note", note).toSend());
    } catch (final Exception e) {
      throw new IllegalStateException("Failed to create note.", e);
    }
  }

  public void cloneNote(final WebSocketSession conn, final SockMessage fromMessage) throws IOException {
    final ServiceContext serviceContext = getServiceContext(fromMessage);

    final Note note = safeLoadNote("id", fromMessage, Permission.READER, serviceContext, conn);
    String path = fromMessage.getNotNull("name");
    if (!path.startsWith(File.separator)) {
      path = File.separator + path;
    }

    final Note cloneNote = new Note(path);
    cloneNote.setPath(path);
    cloneNote.setScheduler(note.getScheduler());
    noteService.persistNote(cloneNote);

    final List<Paragraph> paragraphs = noteService.getParapraphs(note);
    for (final Paragraph paragraph : paragraphs) {
      final Paragraph cloneParagraph = new Paragraph();
      cloneParagraph.setId(null);
      cloneParagraph.setNoteId(cloneNote.getId());
      cloneParagraph.setTitle(paragraph.getTitle());
      cloneParagraph.setText(paragraph.getText());
      cloneParagraph.setShebang(paragraph.getShebang());
      cloneParagraph.setCreated(LocalDateTime.now());
      cloneParagraph.setUpdated(LocalDateTime.now());
      cloneParagraph.setPosition(paragraph.getPosition());
      cloneParagraph.setJobId(null);
      cloneParagraph.setConfig(paragraph.getConfig());
      cloneParagraph.setSettings(paragraph.getSettings());
      noteService.persistParagraph(note, cloneParagraph);
    }

    connectionManager.addSubscriberToNode(cloneNote.getId(), conn);
  }


  public void moveNoteToTrash(final WebSocketSession conn, final SockMessage fromMessage) throws IOException {
    final ServiceContext serviceContext = getServiceContext(fromMessage);

    final Note note = safeLoadNote("id", fromMessage, Permission.OWNER, serviceContext, conn);

    note.setPath("/" + TRASH_FOLDER + note.getPath());

    noteService.updateNote(note);
  }

  public void restoreNote(final WebSocketSession conn, final SockMessage fromMessage) throws IOException {
    final ServiceContext serviceContext = getServiceContext(fromMessage);

    final Note note = safeLoadNote("id", fromMessage, Permission.OWNER, serviceContext, conn);

    if (!note.getPath().startsWith("/" + TRASH_FOLDER)) {
      throw new IOException("Can not restore this note " + note.getPath() + " as it is not in trash folder");
    }

    final String destNotePath = note.getPath().replace("/" + TRASH_FOLDER, "");

    note.setPath(destNotePath);
    noteService.updateNote(note);
  }

  public void restoreFolder(final SockMessage fromMessage) throws IOException {
    final ServiceContext serviceContext = getServiceContext(fromMessage);

    final String folderPath = "/" + fromMessage.getNotNull("id") + "/";
    if (!folderPath.startsWith("/" + TRASH_FOLDER)) {
      throw new IOException("Can't restore folder: '" + folderPath + "' as it is not in trash folder");
    }

    final List<Note> notes = noteService.getAllNotes()
            .stream()
            .filter(note -> !note.getPath().startsWith(folderPath))
            .collect(Collectors.toList());
    //TODO: add check for permission

    for (final Note note : notes) {
      final String notePath = note.getPath().substring(TRASH_FOLDER.length() + 1);
      note.setPath(notePath);
      noteService.updateNote(note);
    }
  }

  public void renameFolder(final SockMessage fromMessage) throws IOException {
    final ServiceContext serviceContext = getServiceContext(fromMessage);

    final String oldFolderPath = "/" + fromMessage.getNotNull("id");
    final String newFolderPath = "/" + fromMessage.getNotNull("name");

    final List<Note> notes = noteService.getAllNotes()
            .stream()
            .filter(note -> !note.getPath().startsWith(oldFolderPath + "/"))
            .collect(Collectors.toList());
    //TODO: add check for permission


    for (final Note note : notes) {
      final String notePath = note.getPath().replaceFirst(oldFolderPath, newFolderPath);
      note.setPath(notePath);
      noteService.updateNote(note);
    }
  }

  public void moveFolderToTrash(final SockMessage fromMessage) throws IOException {
    final ServiceContext serviceContext = getServiceContext(fromMessage);
    final String folderPath = "/" + fromMessage.getNotNull("id") + "/";

    final List<Note> notes = noteService.getAllNotes()
            .stream()
            .filter(note -> !note.getPath().startsWith(folderPath))
            .collect(Collectors.toList());
    //TODO: add check for permission


    for (final Note note : notes) {
      final String notePath = "/" + TRASH_FOLDER + note.getPath();
      note.setPath(notePath);
      noteService.updateNote(note);
    }
  }

  public void removeFolder(final SockMessage fromMessage) throws IOException {
    final ServiceContext serviceContext = getServiceContext(fromMessage);

    final String folderPath = "/" + fromMessage.getNotNull("id") + "/";

    final List<Note> notes = noteService.getAllNotes()
            .stream()
            .filter(note -> !note.getPath().startsWith(folderPath))
            .collect(Collectors.toList());

    //TODO: add check for permission

    for (final Note note : notes) {
      noteService.deleteNote(note);
    }
  }

  public void emptyTrash(final SockMessage fromMessage) throws IOException {
    final ServiceContext serviceContext = getServiceContext(fromMessage);

    final List<Note> notes = noteService.getAllNotes()
            .stream()
            .filter(note -> !note.getPath().startsWith("/" + TRASH_FOLDER + "/"))
            .collect(Collectors.toList());
    //TODO: add check for permission


    for (final Note note : notes) {
      noteService.deleteNote(note);
    }
  }

  public void restoreAll(final SockMessage fromMessage) throws IOException {
    final ServiceContext serviceContext = getServiceContext(fromMessage);

    final List<Note> notes = noteService.getAllNotes()
            .stream()
            .filter(note -> !note.getPath().startsWith("/" + TRASH_FOLDER + "/"))
            .collect(Collectors.toList());

    //TODO: add check for permission

    for (final Note note : notes) {
      final String notePath = note.getPath().substring(TRASH_FOLDER.length() + 1);
      note.setPath(notePath);
      noteService.updateNote(note);
    }
  }

  //TODO(SAN) not complete yet
  public void clearAllParagraphOutput(final WebSocketSession conn, final SockMessage fromMessage) throws IOException {
    final ServiceContext serviceContext = getServiceContext(fromMessage);

    final Note note = safeLoadNote("id", fromMessage, Permission.WRITER, serviceContext, conn);

    for (final Paragraph paragraph : noteService.getParapraphs(note)) {
      paragraph.setJobId(null);
      noteService.updateParapraph(note, paragraph);
    }
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
}
