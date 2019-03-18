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

package org.apache.zeppelin.repository;

import org.apache.zeppelin.annotation.ZeppelinApi;
import org.apache.zeppelin.configuration.ZeppelinConfiguration;
import org.apache.zeppelin.notebook.Note;
import org.apache.zeppelin.notebook.NoteInfo;

import java.io.IOException;
import java.util.List;
import java.util.Map;

/**
 * Notebook repository (persistence layer) abstraction
 */
public interface NotebookRepo {

  /**
   * Notebook repository settings. This represent a structure of a notebook repository settings that will mostly
   * used in the frontend.
   *
   */
   class Settings {

    /**
     * Type of value, It can be text or list.
     */
    public enum Type {
      INPUT, DROPDOWN
    }

    public Type type;
    public List<Map<String, String>> value;
    public String selected;
    public String name;
  }

  void init(ZeppelinConfiguration zConf) throws IOException;

  /**
   * Lists notebook information about all notebooks in storage. This method should only read
   * the metadata of note, rather than reading all notes which usually takes long time.
   *
   * @return
   * @throws IOException
   */
  @ZeppelinApi
  Map<String, NoteInfo> list() throws IOException;

  /**
   * Get the notebook with the given id and given notePath.
   *
   * @param noteId   is note id.
   * @param notePath is note path
   * @return
   * @throws IOException
   */
  @ZeppelinApi
  Note get(String noteId, String notePath) throws IOException;

  /**
   * Save given note in storage
   *
   * @param note    is the note itself.
   * @throws IOException
   */
  @ZeppelinApi
  void save(Note note) throws IOException;

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
  void move(String noteId, String notePath, String newNotePath) throws IOException;

  /**
   * Move folder to another path
   *
   * @param folderPath
   * @param newFolderPath
   * @throws IOException
   */
  void move(String folderPath, String newFolderPath) throws IOException;

  /**
   * Remove note with given id and notePath
   *
   * @param noteId   is note id.
   * @param notePath is note path
   * @throws IOException
   */
  @ZeppelinApi
  void remove(String noteId, String notePath) throws IOException;

  /**
   * Remove folder
   *
   * @param folderPath
   * @throws IOException
   */
  @ZeppelinApi
  void remove(String folderPath) throws IOException;

  /**
   * Release any underlying resources
   */
  @ZeppelinApi
  void close();


  /**
   * Get NotebookRepo settings got the given user.
   *
   * @return
   */
  @ZeppelinApi
  List<NotebookRepo.Settings> getSettings();

  /**
   * update notebook repository settings.
   *
   * @param settings
   */
  @ZeppelinApi
  void updateSettings(Map<String, String> settings);

  default String buildNoteFileName(final String noteId, final String notePath) throws IOException {
    if (!notePath.startsWith("/")) {
      throw new IOException("Invalid notePath: " + notePath);
    }
    return (notePath + "_" + noteId + ".zpln").substring(1);
  }

  default String buildNoteFileName(final Note note) throws IOException {
    return buildNoteFileName(note.getNoteId(), note.getPath());
  }

  default String buildNoteTempFileName(final Note note) {
    return (note.getPath() + "_" + note.getNoteId() + ".tmp").substring(1);
  }

  default String getNoteId(final String noteFileName) throws IOException {
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

  default String getNotePath(final String rootNoteFolder, final String noteFileName)
      throws IOException {
    int index = noteFileName.lastIndexOf("_");
    if (index == -1) {
      throw new IOException(
          "Invalid note name, no '_' in note name: " + noteFileName);
    }
    try {
      return noteFileName.substring(rootNoteFolder.length(), index);
    } catch (StringIndexOutOfBoundsException e) {
      throw new IOException("Invalid note name: " + noteFileName);
    }
  }
}
