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

package org.apache.zeppelin.notebook;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import java.io.File;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import org.apache.zeppelin.notebook.display.GUI;
import org.apache.zeppelin.notebook.display.Input;
import org.apache.zeppelin.utils.IdHashes;

/**
 * Represent the note of Zeppelin. All the note and its paragraph operations are done
 * via this class.
 */
public class Note implements Serializable {

  private static final Gson gson = new GsonBuilder()
      .setPrettyPrinting()
      .registerTypeAdapterFactory(Input.TypeAdapterFactory)
      .create();

  private final static String TRASH_FOLDER = "~Trash";


  private String id;
  private String name;
  private String path;

  /**
   * form and parameter guiConfiguration
   * see https://github.com/apache/zeppelin/pull/2641
   */
  private final GUI guiConfiguration;

  private final List<Paragraph> paragraphs;

  private boolean isRunning = false;

  private NoteCronConfiguration noteCronConfiguration;

  /********************************** user permissions info *************************************/
  private final Set<String> owners;
  private final Set<String> readers;
  private final Set<String> runners;
  private final Set<String> writers;

  public Note(final String path) {
    this(null, path);
    this.name = path.substring(path.lastIndexOf(File.separator) + 1);
  }

  public Note(final String name, final String path) {
    this.id = IdHashes.generateId();
    this.name = name;
    this.path = path;

    this.guiConfiguration = new GUI();

    this.owners = new HashSet<>();
    this.readers = new HashSet<>();
    this.runners = new HashSet<>();
    this.writers = new HashSet<>();

    this.paragraphs = new ArrayList<>();
    this.noteCronConfiguration = new NoteCronConfiguration();
  }

  public String getId() {
    return id;
  }

  public void setId(final String id) {
    this.id = id;
  }

  public String getName() {
    return name;
  }

  public GUI getGuiConfiguration() {
    return guiConfiguration;
  }

  public Paragraph getParagraph(final String paragraphId) {
    for (final Paragraph p : paragraphs) {
      if (p.getId().equals(paragraphId)) {
        return p;
      }
    }
    return null;
  }

  public List<Paragraph> getParagraphs() {
    return paragraphs;
  }

  public boolean isRunning() {
    return isRunning;
  }

  public void setRunning(final boolean running) {
    isRunning = running;
  }

  public void setNoteCronConfiguration(final NoteCronConfiguration noteCronConfiguration) {
    this.noteCronConfiguration = noteCronConfiguration;
  }

  public NoteCronConfiguration getNoteCronConfiguration() {
    return noteCronConfiguration;
  }

  public Set<String> getOwners() {
    return owners;
  }

  public Set<String> getReaders() {
    return readers;
  }

  public Set<String> getRunners() {
    return runners;
  }

  public Set<String> getWriters() {
    return writers;
  }

  public String getPath() {
    return path;
  }

  public void setPath(final String path) {
    this.path = path;
    this.name = path.substring(path.lastIndexOf(File.separator) + 1);
  }

  public boolean isTrashed() {
    return this.path.startsWith("/" + TRASH_FOLDER);
  }


  @Override
  public String toString() {
    if (this.path != null) {
      return this.path;
    } else {
      return "/" + this.name;
    }
  }

  public static Note fromJson(final String json) {
    return gson.fromJson(json, Note.class);
  }

  @Override
  public boolean equals(final Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    Note note = (Note) o;

    if (paragraphs != null ? !paragraphs.equals(note.paragraphs) : note.paragraphs != null) {
      return false;
    }
    //TODO(zjffdu) exclude path because FolderView.index use Note as key and consider different path
    //as same note
    //    if (path != null ? !path.equals(note.path) : note.path != null) return false;
    if (id != null ? !id.equals(note.id) : note.id != null) {
      return false;
    }
    if (noteCronConfiguration != null ? !noteCronConfiguration.equals(note.noteCronConfiguration) : note.noteCronConfiguration != null) {
      return false;
    }
    return isRunning == note.isRunning;

  }

  @Override
  public int hashCode() {
    int result = paragraphs != null ? paragraphs.hashCode() : 0;
    result = 31 * result + (id != null ? id.hashCode() : 0);
    result = 31 * result + (noteCronConfiguration != null ? noteCronConfiguration.hashCode() : 0);
    return result;
  }
}
