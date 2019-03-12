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

package org.apache.zeppelin.notebook.repo;

import org.apache.zeppelin.annotation.ZeppelinApi;
import org.apache.zeppelin.conf.ZeppelinConfiguration;
import org.apache.zeppelin.notebook.Note;
import org.apache.zeppelin.notebook.NoteInfo;
import org.apache.zeppelin.user.AuthenticationInfo;

import java.io.IOException;
import java.util.List;
import java.util.Map;

/**
 * Notebook repository (persistence layer) abstraction
 */
public interface NotebookRepo {

  void init(ZeppelinConfiguration zConf) throws IOException;

  /**
   * Lists notebook information about all notebooks in storage. This method should only read
   * the metadata of note, rather than reading all notes which usually takes long time.
   *
   * @param subject contains user information.
   * @return
   * @throws IOException
   */
  @ZeppelinApi
  Map<String, NoteInfo> list(AuthenticationInfo subject) throws IOException;

  /**
   * Get the notebook with the given id and given notePath.
   *
   * @param noteId   is note id.
   * @param notePath is note path
   * @param subject  contains user information.
   * @return
   * @throws IOException
   */
  @ZeppelinApi
  Note get(String noteId, String notePath, AuthenticationInfo subject) throws IOException;

  /**
   * Save given note in storage
   *
   * @param note    is the note itself.
   * @param subject contains user information.
   * @throws IOException
   */
  @ZeppelinApi
  void save(Note note, AuthenticationInfo subject) throws IOException;

  /**
   *
   * Move given note to another path
   *
   * @param noteId
   * @param notePath
   * @param newNotePath
   * @throws IOException
   */
  @ZeppelinApi
  void move(String noteId, String notePath, String newNotePath,
            AuthenticationInfo subject) throws IOException;

  /**
   * Move folder to another path
   *
   * @param folderPath
   * @param newFolderPath
   * @param subject
   * @throws IOException
   */
  void move(String folderPath, String newFolderPath,
            AuthenticationInfo subject) throws IOException;

  /**
   * Remove note with given id and notePath
   *
   * @param noteId   is note id.
   * @param notePath is note path
   * @param subject  contains user information.
   * @throws IOException
   */
  @ZeppelinApi
  void remove(String noteId, String notePath, AuthenticationInfo subject) throws IOException;

  /**
   * Remove folder
   *
   * @param folderPath
   * @param subject
   * @throws IOException
   */
  @ZeppelinApi
  void remove(String folderPath, AuthenticationInfo subject) throws IOException;

  /**
   * Release any underlying resources
   */
  @ZeppelinApi
  void close();


  /**
   * Get NotebookRepo settings got the given user.
   *
   * @param subject
   * @return
   */
  @ZeppelinApi
  List<NotebookRepoSettingsInfo> getSettings(AuthenticationInfo subject);

  /**
   * update notebook repo settings.
   *
   * @param settings
   * @param subject
   */
  @ZeppelinApi
  void updateSettings(Map<String, String> settings, AuthenticationInfo subject);

  default String buildNoteFileName(String noteId, String notePath) throws IOException {
    if (!notePath.startsWith("/")) {
      throw new IOException("Invalid notePath: " + notePath);
    }
    int pos = notePath.lastIndexOf('/');
    String path = notePath.substring(0, pos + 1) + "_" + notePath.substring(pos + 1);
    return (path + "_" + noteId + ".zpln").substring(1);
  }

  default String buildNoteFileName(Note note) throws IOException {
    return buildNoteFileName(note.getId(), note.getPath());
  }

  default String buildNoteFileNameWithoutPrefix(String noteId, String notePath) throws IOException {
    if (!notePath.startsWith("/")) {
      throw new IOException("Invalid notePath: " + notePath);
    }
    return (notePath + "_" + noteId + ".zpln").substring(1);
  }

  default String buildNoteTempFileName(Note note) {
    int pos = note.getPath().lastIndexOf('/');
    String path = note.getPath().substring(0, pos + 1) + "_" + note.getPath().substring(pos + 1);
    return (path + "_" + note.getId() + ".tmp").substring(1);
  }

  default String getNoteId(String noteFileName) throws IOException {
    int separatorIndex = noteFileName.lastIndexOf("_");
    if (separatorIndex == -1) {
      throw new IOException(
          "Invalid note name, no '_' in note name: " + noteFileName);
    }
    try {
      int dotIndex = noteFileName.lastIndexOf(".");
      return noteFileName.substring(separatorIndex + 1, dotIndex);
    } catch (StringIndexOutOfBoundsException e) {
      throw new IOException("Invalid note name: " + noteFileName);
    }
  }

  default String getNotePath(String rootNoteFolder, String noteFileName)
      throws IOException {
    int index = noteFileName.lastIndexOf('_');
    if (index == -1) {
      throw new IOException(
          "Invalid note name, no '_' in note name: " + noteFileName);
    }
    try {
      // If note saved in new format (with '_' prefix) method should return path without '_'
      // get full_path_to_note {NOTEBOOK_DIR}/{FULL_PATH_TO_NOTE}_{id}.zpln
      String fullNotePath = noteFileName.substring(rootNoteFolder.length(), index);
      // get the filename from path_to_note
      String noteName = fullNotePath.substring(fullNotePath.lastIndexOf('/') + 1);
      // get the index of first '_' in the name
      int prefix = noteName.indexOf('_');
      // note name should start with '_' in new format
      if (prefix == 0) {
        // construct path: {CURRENT_NOTE_DIR} + {NOTE_NAME}
        String noteDir =
            fullNotePath.substring(0, fullNotePath.lastIndexOf('/') + 1);
        return noteDir + noteName.substring(1);
      }
      // otherwise note saved in old format without prefix
      return noteFileName.substring(rootNoteFolder.length(), index);
    } catch (StringIndexOutOfBoundsException e) {
      throw new IOException("Invalid note name: " + noteFileName);
    }
  }
}
