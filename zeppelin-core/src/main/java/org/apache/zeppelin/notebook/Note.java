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
import org.apache.zeppelin.notebook.display.AngularObject;
import org.apache.zeppelin.notebook.display.GUI;
import org.apache.zeppelin.notebook.display.Input;
import org.apache.zeppelin.utils.IdHashes;

import java.io.Serializable;
import java.util.*;

/**
 * Represent the note of Zeppelin. All the note and its paragraph operations are done
 * via this class.
 */
//TODO(egorklimov):
// * Убрал toJson, сериализация в json должна производиться явно
// * Убрал все что связано с исполнением
// * Убрал персонализированный мод
public class Note implements Serializable {

  private static Gson gson = new GsonBuilder()
      .setPrettyPrinting()
      .registerTypeAdapterFactory(Input.TypeAdapterFactory)
      .create();

  private final static String TRASH_FOLDER = "~Trash";


  private String id;
  private String name = "";
  private String defaultInterpreterGroup;

  /**
   * form and parameter guiConfiguration
   * see https://github.com/apache/zeppelin/pull/2641
   */
  private final GUI guiConfiguration;

  private final List<Paragraph> paragraphs;

  /**
   *
   * TODO(egorklimov): переложить ZeppelinConfiguration в zeppelin-core
   * ZeppelinConfiguration - не должен быть компонентом, просто класс с контсруктором
   * в Zeppelin Server сделать бин и инжектить его в контекст как бин
   * Везде где ZeppelinConfiguration заводим enum
   * в get передаем enum (параметр) доставать стринг
   * Упростить getter'ы чтобы туда передавался enum
   * zeppelin-core
   *
   * Убрать Gson везде
   * Убрать метод toJson везде
   * где нужен json new Gson.toJson()
   * Делаем ЯВНУЮ сериализацию!
   *
   * Cron в отдельный сервис
   * CronJob - расписание шедуллера, id ноута и параграфа и контекст
   * внутри примитивы и строки
   * никаких. Нельзя кидать в thread Note и Paragraph, только id
   *
   * OLD - в пекло, нужно все выпилить
   *
   * Если ноут нужен - загруси его через NoteManager
   *
   * runNote, runParagraph - удалить
   *
   * ApplicationState и InterpreterResult отпиливать от ноута и параграфа
   *
   * Сервисы по загрузке InterpreterResult, personalizedMode
   *
   * NotebookService
   *  #clon
   *  #move
   *  #clone убрать
   *  #Вызов заккоментить и кидать NotImplementedException
   */

  /**
   * see https://issues.apache.org/jira/browse/ZEPPELIN-25
   */
  private final Map<String, List<AngularObject>> angularObjects;

  private boolean isRunning = false;

  // При сериализации и десериадизации может быть проблема
  // перетащить в конструктор, поставить falce по дефолту
  private NoteCronConfiguration noteCronConfiguration;

  /********************************** user permissions info *************************************/
  private final Set<String> owners;
  private final Set<String> readers;
  private final Set<String> runners;
  private final Set<String> writers;

  private transient String path;


  public Note(final String name, final String path, final String defaultInterpreterGroup) {
    this.id = IdHashes.generateId();
    this.name = name;
    this.path = path;
    this.angularObjects = new HashMap<>();
    this.defaultInterpreterGroup = defaultInterpreterGroup;

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

  public void setId(String id) {
    this.id = id;
  }

  public String getName() {
    return name;
  }

  public void setName(String name) {
    this.name = name;
  }

  public String getDefaultInterpreterGroup() {
    return defaultInterpreterGroup;
  }

  public void setDefaultInterpreterGroup(String defaultInterpreterGroup) {
    this.defaultInterpreterGroup = defaultInterpreterGroup;
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

  public Map<String, List<AngularObject>> getAngularObjects() {
    return angularObjects;
  }

  public boolean isRunning() {
    return isRunning;
  }

  public void setRunning(boolean running) {
    isRunning = running;
  }

  public void setNoteCronConfiguration(NoteCronConfiguration noteCronConfiguration) {
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

  public void setPath(String path) {
    this.path = path;
  }

  public boolean isTrashed() {
    return this.path.startsWith("/" + TRASH_FOLDER);
  }


  @Override
  public String toString() {
    if (this.path != null) {
      return this.path + this.name;
    } else {
      return "/" + this.name;
    }
  }

  public static Note fromJson(final String json) {
    Note note = gson.fromJson(json, Note.class);
    return note;
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
    if (angularObjects != null ?
        !angularObjects.equals(note.angularObjects) : note.angularObjects != null) {
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
    result = 31 * result + (angularObjects != null ? angularObjects.hashCode() : 0);
    result = 31 * result + (noteCronConfiguration != null ? noteCronConfiguration.hashCode() : 0);
    return result;
  }
}
