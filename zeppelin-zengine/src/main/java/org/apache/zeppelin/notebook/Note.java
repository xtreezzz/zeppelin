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

import com.google.common.annotations.VisibleForTesting;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.commons.lang3.NotImplementedException;
import org.apache.zeppelin.conf.ZeppelinConfiguration;
import org.apache.zeppelin.display.AngularObject;
import org.apache.zeppelin.display.AngularObjectRegistry;
import org.apache.zeppelin.display.GUI;
import org.apache.zeppelin.display.Input;
import org.apache.zeppelin.interpreter.InterpreterFactory;
import org.apache.zeppelin.interpreter.InterpreterGroup;
import org.apache.zeppelin.interpreter.InterpreterSetting;
import org.apache.zeppelin.interpreter.InterpreterSettingManager;
import org.apache.zeppelin.interpreter.remote.RemoteAngularObjectRegistry;
import org.apache.zeppelin.notebook.conf.CronJobConfiguration;
import org.apache.zeppelin.notebook.core.Paragraph;
import org.apache.zeppelin.notebook.utility.IdHashes;
import org.apache.zeppelin.user.AuthenticationInfo;
import org.apache.zeppelin.user.Credentials;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

/**
 * Represent the note of Zeppelin. All the note and its paragraph operations are done
 * via this class.
 */
//TODO(egorklimov):
// * Убрал toJson, сериализация в json должна производиться явно
// * Убрал все что связано с исполнением
// * Убрал все персонализированный мод
public class Note implements Serializable {
  private static final Logger logger = LoggerFactory.getLogger(Note.class);
  private static Gson gson = new GsonBuilder()
      .setPrettyPrinting()
      .setDateFormat("yyyy-MM-dd HH:mm:ss.SSS")
      .registerTypeAdapter(Date.class, new NotebookImportDeserializer())
      .registerTypeAdapterFactory(Input.TypeAdapterFactory)
      .create();

  private String name = "";
  private String id;
  private String defaultInterpreterGroup;

  /**
   * form and parameter settings
   * see https://github.com/apache/zeppelin/pull/2641
   */
  private final GUI settings;

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
  private final CronJobConfiguration config;

  /********************************** user permissions info *************************************/
  private final Set<String> owners;
  private final Set<String> readers;
  private final Set<String> runners;
  private final Set<String> writers;

  /********************************** transient fields ******************************************/
  private transient boolean loaded;
  private transient String path;
  private transient InterpreterFactory interpreterFactory;
  private transient InterpreterSettingManager interpreterSettingManager;
  private transient ParagraphJobListener paragraphJobListener;
  private final transient List<NoteEventListener> noteEventListeners;
  private transient Credentials credentials;


  public Note() {
    generateId();
    loaded = false;
    settings = new GUI();

    owners = new HashSet<>();
    readers = new HashSet<>();
    runners = new HashSet<>();
    writers = new HashSet<>();

    paragraphs = new ArrayList<>();
    angularObjects = new HashMap<>();
    noteEventListeners = new ArrayList<>();
    config = new CronJobConfiguration();

    setCronSupported(ZeppelinConfiguration.create());
  }

  public Note(final String path, final String defaultInterpreterGroup,
      final InterpreterFactory factory, final InterpreterSettingManager interpreterSettingManager,
      final ParagraphJobListener paragraphJobListener, final Credentials credentials,
      final List<NoteEventListener> noteEventListener) {
    setPath(path);
    this.angularObjects = new HashMap<>();
    this.defaultInterpreterGroup = defaultInterpreterGroup;
    this.interpreterFactory = factory;
    this.interpreterSettingManager = interpreterSettingManager;
    this.paragraphJobListener = paragraphJobListener;
    this.noteEventListeners = noteEventListener;
    this.credentials = credentials;
    generateId();

    loaded = false;
    settings = new GUI();

    owners = new HashSet<>();
    readers = new HashSet<>();
    runners = new HashSet<>();
    writers = new HashSet<>();

    paragraphs = new ArrayList<>();
    config = new CronJobConfiguration();

    setCronSupported(ZeppelinConfiguration.create());
  }

  public Note(NoteInfo noteInfo) {
    this.angularObjects = new HashMap<>();
    this.id = noteInfo.getId();
    setPath(noteInfo.getPath());

    loaded = false;
    settings = new GUI();

    owners = new HashSet<>();
    readers = new HashSet<>();
    runners = new HashSet<>();
    writers = new HashSet<>();

    paragraphs = new ArrayList<>();
    noteEventListeners = new ArrayList<>();
    config = new CronJobConfiguration();
  }

  public String getPath() {
    return path;
  }

  public String getParentPath() {
    int pos = path.lastIndexOf("/");
    if (pos == 0) {
      return "/";
    } else {
      return path.substring(0, pos);
    }
  }

  private String getName(final String path) {
    int pos = path.lastIndexOf("/");
    return path.substring(pos + 1);
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

  private void generateId() {
    id = IdHashes.generateId();
  }

  public boolean isLoaded() {
    return loaded;
  }

  public void setLoaded(final boolean loaded) {
    this.loaded = loaded;
  }

  private void clearUserParagraphs(final boolean isPersonalized) {
    throw new NotImplementedException("Personalized mode should be fixed");
    //    if (!isPersonalized) {
    //      for (Paragraph p : paragraphs) {
    //        p.clearUserParagraphs();
    //      }
    //    }
  }

  public String getId() {
    return id;
  }

  @VisibleForTesting
  public void setId(final String id) {
    this.id = id;
  }

  public String getName() {
    return name;
  }

  public void setPath(final String path) {
    if (!path.startsWith("/")) {
      this.path = "/" + path;
    } else {
      this.path = path;
    }
    this.name = getName(path);
  }

  public String getDefaultInterpreterGroup() {
    if (defaultInterpreterGroup == null) {
      defaultInterpreterGroup = ZeppelinConfiguration.create()
          .getString(ZeppelinConfiguration.ConfVars.ZEPPELIN_INTERPRETER_GROUP_DEFAULT);
    }
    return defaultInterpreterGroup;
  }

  public void setDefaultInterpreterGroup(final String defaultInterpreterGroup) {
    this.defaultInterpreterGroup = defaultInterpreterGroup;
  }

  public Map<String, Object> getNoteParams() {
    return settings.getParams();
  }

  public void setNoteParams(final Map<String, Object> noteParams) {
    settings.setParams(noteParams);
  }

  public Map<String, Input> getNoteForms() {
    return settings.getForms();
  }

  public void setNoteForms(final Map<String, Input> noteForms) {
    settings.setForms(noteForms);
  }

  public void setName(final String name) {
    this.name = name;
    if (this.path == null) {
      if (name.startsWith("/")) {
        this.path = name;
      } else {
        this.path = "/" + name;
      }
    } else {
      int pos = this.path.indexOf("/");
      this.path = this.path.substring(0, pos + 1) + this.name;
    }
  }

  public InterpreterFactory getInterpreterFactory() {
    return interpreterFactory;
  }

  public void setInterpreterFactory(final InterpreterFactory interpreterFactory) {
    this.interpreterFactory = interpreterFactory;
  }

  void setInterpreterSettingManager(final InterpreterSettingManager interpreterSettingManager) {
    this.interpreterSettingManager = interpreterSettingManager;
  }

  void setParagraphJobListener(final ParagraphJobListener paragraphJobListener) {
    this.paragraphJobListener = paragraphJobListener;
  }

  public Boolean isCronSupported(final ZeppelinConfiguration config) {
    if (config.isZeppelinNotebookCronEnable()) {
      config.getZeppelinNotebookCronFolders();
      if (config.getZeppelinNotebookCronFolders() == null) {
        return true;
      } else {
        for (String folder : config.getZeppelinNotebookCronFolders().split(",")) {
          folder = folder.replaceAll("\\*", "\\.*").replaceAll("\\?", "\\.");
          if (getName().matches(folder)) {
            return true;
          }
        }
      }
    }
    return false;
  }

  public void setCronSupported(final ZeppelinConfiguration config) {
    this.config.setCronEnabled(config.isZeppelinNotebookCronEnable());
  }

  public Credentials getCredentials() {
    return credentials;
  }

  public void setCredentials(Credentials credentials) {
    this.credentials = credentials;
  }

  Map<String, List<AngularObject>> getAngularObjects() {
    return angularObjects;
  }

  /**
   * Create a new paragraph and add it to the end of the note.
   */
  public Paragraph addNewParagraph(final AuthenticationInfo authenticationInfo) {
    return insertNewParagraph(paragraphs.size(), authenticationInfo);
  }

  /**
   * Clone paragraph and add it to note.
   *
   * @param srcParagraph source paragraph
   */
  void addCloneParagraph(final Paragraph srcParagraph, final AuthenticationInfo subject) {
    throw new NotImplementedException("Clone logic should be fixed");
  }

  public void fireParagraphCreateEvent(final Paragraph p) throws IOException {
    for (NoteEventListener listener : noteEventListeners) {
      listener.onParagraphCreate(p);
    }
  }

  public void fireParagraphRemoveEvent(final Paragraph p) throws IOException {
    for (NoteEventListener listener : noteEventListeners) {
      listener.onParagraphRemove(p);
    }
  }


  public void fireParagraphUpdateEvent(final Paragraph p) throws IOException {
    for (NoteEventListener listener : noteEventListeners) {
      listener.onParagraphUpdate(p);
    }
  }

  /**
   * Create a new paragraph and insert it to the note in given index.
   *
   * @param index index of paragraphs
   */
  public Paragraph insertNewParagraph(final int index, final AuthenticationInfo authenticationInfo) {
    //TODO(egorklimov): Fix this.
    Paragraph paragraph = new Paragraph("", "", authenticationInfo.getUser(), new Date(),
        new GUI());
    insertParagraph(paragraph, index);
    return paragraph;
  }

  public void addParagraph(final Paragraph paragraph) {
    insertParagraph(paragraph, paragraphs.size());
  }

  private void insertParagraph(final Paragraph paragraph, final int index) {
    synchronized (paragraphs) {
      paragraphs.add(index, paragraph);
    }
    try {
      fireParagraphCreateEvent(paragraph);
    } catch (IOException e) {
      logger.error("Failed to insert paragraph", e);
    }
  }

  /**
   * Remove paragraph by id.
   *
   * @param paragraphId ID of paragraph
   * @return a paragraph that was deleted, or <code>null</code> otherwise
   */
  public Paragraph removeParagraph(final String user, final String paragraphId) {
    removeAllAngularObjectInParagraph(user, paragraphId);
    interpreterSettingManager.removeResourcesBelongsToParagraph(getId(), paragraphId);
    synchronized (paragraphs) {
      for (Paragraph p : paragraphs) {
        //TODO(egorklimov): fix Paragraph's id

        //        if (p.getId().equals(paragraphId)) {
        //          i.remove();
        //          try {
        //            fireParagraphRemoveEvent(p);
        //          } catch (IOException e) {
        //            logger.error("Failed to remove paragraph", e);
        //          }
        //          return p;
        //        }
      }
    }
    return null;
  }

  public void clearParagraphOutputFields(final String paragraphId) {
    throw new NotImplementedException("Should be done by InterpreterResultService");
  }

  public Paragraph clearPersonalizedParagraphOutput(final String paragraphId, final String user) {
    throw new NotImplementedException("Personalized mode removed");
  }
  /**
   * Clear paragraph output by id.
   *
   * @param paragraphId ID of paragraph
   * @return Paragraph
   */
  public Paragraph clearParagraphOutput(final String paragraphId) {
    throw new NotImplementedException("Should be done by InterpreterResultService");
  }

  /**
   * Clear all paragraph output of note
   */
  public void clearAllParagraphOutput() {
    throw new NotImplementedException("Should be done by InterpreterResultService");
  }

  /**
   * Move paragraph into the new index (order from 0 ~ n-1).
   *
   * @param paragraphId ID of paragraph
   * @param index       new index
   */
  public void moveParagraph(final String paragraphId, final int index) {
    moveParagraph(paragraphId, index, false);
  }

  /**
   * Move paragraph into the new index (order from 0 ~ n-1).
   *
   * @param paragraphId                ID of paragraph
   * @param index                      new index
   * @param throwWhenIndexIsOutOfBound whether throw IndexOutOfBoundException
   *                                   when index is out of bound
   */
  public void moveParagraph(final String paragraphId, final int index,
      final boolean throwWhenIndexIsOutOfBound) {
    synchronized (paragraphs) {
      int oldIndex;
      Paragraph p = null;

      if (index < 0 || index >= paragraphs.size()) {
        if (throwWhenIndexIsOutOfBound) {
          throw new IndexOutOfBoundsException(
              "paragraph size is " + paragraphs.size() + " , index is " + index);
        } else {
          return;
        }
      }

      for (int i = 0; i < paragraphs.size(); i++) {
        //TODO(egorklimov): fix Paragraph's id

        //        if (paragraphs.get(i).getId().equals(paragraphId)) {
        //          oldIndex = i;
        //          if (oldIndex == index) {
        //            return;
        //          }
        //          p = paragraphs.remove(i);
        //        }
      }

      if (p != null) {
        paragraphs.add(index, p);
      }
    }
  }

  public boolean isLastParagraph(final String paragraphId) {
    if (!paragraphs.isEmpty()) {
      synchronized (paragraphs) {
        //TODO(egorklimov): fix Paragraph's id

        //        if (paragraphId.equals(paragraphs.get(paragraphs.size() - 1).getId())) {
        //          return true;
        //        }
      }
      return false;
    }
    //TODO(egorklimov): check logic:

    /* because empty list, cannot remove nothing right? */
    return true;
  }

  public int getParagraphCount() {
    return paragraphs.size();
  }

  public Paragraph getParagraph(final String paragraphId) {
    synchronized (paragraphs) {
      for (Paragraph p : paragraphs) {
        //TODO(egorklimov): fix Paragraph's id

        //        if (p.getId().equals(paragraphId)) {
        //          return p;
        //        }
      }
    }
    return null;
  }

  public Paragraph getParagraph(final int index) {
    return paragraphs.get(index);
  }

  public Paragraph getLastParagraph() {
    synchronized (paragraphs) {
      return paragraphs.get(paragraphs.size() - 1);
    }
  }

  public List<Map<String, String>> generateParagraphsInfo() {
    List<Map<String, String>> paragraphsInfo = new ArrayList<>();
    synchronized (paragraphs) {
      for (Paragraph p : paragraphs) {
        //TODO(egorklimov): Fix Paragraph id
        //Map<String, String> info = populateParagraphInfo(p.getId());
        //paragraphsInfo.add(info);
      }
    }
    return paragraphsInfo;
  }

  public Map<String, String> generateSingleParagraphInfo(final String paragraphId) {
    // TODO(egorklimov): необходимо либо получить ParagraphJob и получить информацию о выполнении,
    // либо выдать информацию о параграфе
    throw new NotImplementedException("Which paragraph info is needed?");
    //    synchronized (paragraphs) {
    //      for (Paragraph p : paragraphs) {
    //        if (p.getId().equals(paragraphId)) {
    //          return populateParagraphInfo(p);
    //        }
    //      }
    //      return new HashMap<>();
    //    }
  }

  private Map<String, String> populateParagraphInfo(final String paragraphId) {
    // TODO(egorklimov): необходимо либо получить ParagraphJob и получить информацию о выполнении,
    // либо выдать информацию о параграфе
    throw new NotImplementedException("Which paragraph info is needed?");
    //    Map<String, String> info = new HashMap<>();
    //    info.put("id", p.getId());
    //    info.put("status", p.getStatus().toString());
    //    if (p.getDateStarted() != null) {
    //      info.put("started", p.getDateStarted().toString());
    //    }
    //    if (p.getDateFinished() != null) {
    //      info.put("finished", p.getDateFinished().toString());
    //    }
    //    if (p.getStatus().isRunning()) {
    //      info.put("progress", String.valueOf(p.progress()));
    //    } else {
    //      info.put("progress", String.valueOf(100));
    //    }
    //return info;
  }


  public boolean isTrashed() {
    return this.path.startsWith("/" + NoteManager.TRASH_FOLDER);
  }

  public List<Paragraph> getParagraphs() {
    synchronized (paragraphs) {
      return Collections.unmodifiableList(paragraphs);
    }
  }

  private void snapshotAngularObjectRegistry(final String user) {
    angularObjects.clear();

    List<InterpreterSetting> interpreterSettings = interpreterSettingManager.getInterpreterSettings(getId());
    if (interpreterSettings == null || interpreterSettings.isEmpty()) {
      return;
    }

    for (InterpreterSetting setting : interpreterSettings) {
      InterpreterGroup intpGroup = setting.getInterpreterGroup(user, id);
      if (intpGroup != null) {
        AngularObjectRegistry registry = intpGroup.getAngularObjectRegistry();
        angularObjects.put(intpGroup.getId(), registry.getAllWithGlobal(id));
      }
    }
  }

  private void removeAllAngularObjectInParagraph(final String user, final String paragraphId) {
    // TODO(egorklimov): Почему при удалении angularObjects в параграфе удаляются все
    // angularObjects в ноуте?
    angularObjects.clear();

    List<InterpreterSetting> interpreterSettings = interpreterSettingManager.getInterpreterSettings(getId());
    if (interpreterSettings == null || interpreterSettings.isEmpty()) {
      return;
    }

    for (InterpreterSetting setting : interpreterSettings) {
      if (setting.getInterpreterGroup(user, id) == null) {
        continue;
      }
      InterpreterGroup intpGroup = setting.getInterpreterGroup(user, id);
      AngularObjectRegistry registry = intpGroup.getAngularObjectRegistry();

      if (registry instanceof RemoteAngularObjectRegistry) {
        // remove paragraph scope object
        ((RemoteAngularObjectRegistry) registry).removeAllAndNotifyRemoteProcess(id, paragraphId);

        // TODO(egorklimov): ApplicationState был убран из Paragraph

        //        // remove app scope object
        //        List<ApplicationState> appStates = getParagraph(paragraphId).getParagraph().getApps();
        //        if (appStates != null) {
        //          for (ApplicationState app : appStates) {
        //            ((RemoteAngularObjectRegistry) registry)
        //                .removeAllAndNotifyRemoteProcess(id, app.getId());
        //          }
        //        }
      } else {
        registry.removeAll(id, paragraphId);

        // TODO(egorklimov): ApplicationState был убран из Paragraph

        //        // remove app scope object
        //        List<ApplicationState> appStates = getParagraph(paragraphId).getParagraph().getApps();
        //        if (appStates != null) {
        //          for (ApplicationState app : appStates) {
        //            registry.removeAll(id, app.getId());
        //          }
        //        }
      }
    }
  }

  public CronJobConfiguration getConfig() {
    return config;
  }

  public List<NoteEventListener> getNoteEventListeners() {
    return noteEventListeners;
  }

  public void setConfig(final CronJobConfiguration config) {
    this.config.setCronEnabled(config.isCronEnabled());
    this.config.setCronExpression(config.getCronExpression());
    this.config.setReleaseResourceFlag(config.isReleaseResourceFlag());
    this.config.setCronExecutingUser(config.getCronExecutingUser());
    this.config.setCronExecutingRoles(config.getCronExecutingRoles());
  }

  public synchronized boolean isRunning() {
    return isRunning;
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
    if (config != null ? !config.equals(note.config) : note.config != null) {
      return false;
    }
    return isRunning == note.isRunning;

  }

  @Override
  public int hashCode() {
    int result = paragraphs != null ? paragraphs.hashCode() : 0;
    result = 31 * result + (id != null ? id.hashCode() : 0);
    result = 31 * result + (angularObjects != null ? angularObjects.hashCode() : 0);
    result = 31 * result + (config != null ? config.hashCode() : 0);
    return result;
  }

  @VisibleForTesting
  public static Gson getGson() {
    return gson;
  }
}
