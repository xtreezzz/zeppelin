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

package org.apache.zeppelin.repositories;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.apache.commons.lang.StringUtils;
import org.apache.zeppelin.configuration.ZeppelinConfiguration;
import org.apache.zeppelin.notebook.Note;
import org.apache.zeppelin.notebook.NoteInfo;
import org.apache.zeppelin.repository.NotebookRepo;
import org.apache.zeppelin.repository.NotebookRepoWithVersionControl;
import org.reflections.Reflections;
import org.reflections.util.ClasspathHelper;
import org.reflections.util.ConfigurationBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Component;

import javax.annotation.PostConstruct;
import java.io.File;
import java.io.IOException;
import java.lang.reflect.Modifier;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.*;
import java.util.stream.Collectors;

@Component
@Qualifier("FileSystemNoteRepository")
public class FileSystemNoteRepository implements NoteRepository{

  private static final Logger LOG = LoggerFactory.getLogger(FileSystemNoteRepository.class);

  private static final String defaultStorage = "org.apache.zeppelin.notebook.repo.GitNotebookRepo";

  private final ZeppelinConfiguration conf;
  private NotebookRepo repo;


  @Autowired
  public FileSystemNoteRepository(final ZeppelinConfiguration conf) {
    this.conf = conf;
  }

  @PostConstruct
  public void initRepository() {
    LOG.info("Start loading repository implementations");
    final File repoFolder = new File("plugins/NotebookRepo");

    final File[] directories = repoFolder.listFiles(File::isDirectory);
    if (directories == null || directories.length == 0) {
      throw new IllegalStateException("Can't load repository implementation from " + repoFolder.getAbsolutePath());
    }

    LOG.info("Found repositories folders: {}", Arrays.asList(directories));

    // load any jars in folder into separate classloader
    final List<ClassLoader> repoClassLoaders = Lists.newArrayList();
    for (final File dir : directories) {
      try {
        final List<URL> urls = Lists.newArrayList();
        for (final File file : dir.listFiles()) {
          //TODO(KOT) Critical remove all zeppelin jars!!!!!
          LOG.debug("Add file " + file.getAbsolutePath() + " to classpath of plugin: " + dir.getName());
          final URL url = file.toURI().toURL();

          urls.add(new URL("jar:" + url.toString() + "!/"));
        }

        if (urls.isEmpty()) {
          LOG.warn("Can not load plugin; folder " + dir.getName() + " is empty.");
          continue;
        }
        repoClassLoaders.add(URLClassLoader.newInstance(urls.toArray(new URL[0])));
      } catch (final Exception e) {
        LOG.error("Error while load files from {}", dir);
      }
    }

    final Map<ClassLoader, Set<Class<? extends NotebookRepo>>> repositoryLoadResults = Maps.newHashMap();
    for (final ClassLoader classLoader : repoClassLoaders) {
      final Reflections reflections = new Reflections(
              new ConfigurationBuilder()
                      .setUrls( ClasspathHelper.forPackage("org.apache.zeppelin", classLoader).stream().filter(url -> url.getProtocol().equals("jar")).collect(Collectors.toSet()))
                      .addClassLoader(classLoader)
      );
      //final Reflections reflections = new Reflections(classLoader);
      final Set<Class<? extends NotebookRepo>> repoClasses = reflections.getSubTypesOf(NotebookRepo.class);

      final Set<Class<? extends NotebookRepo>> filtered = repoClasses
              .stream()
              .filter(c -> !Modifier.isAbstract(getClass().getModifiers()) && !c.isInterface())
              .collect(Collectors.toSet());


      LOG.info("Found repositories implementations: {}", filtered);
      repositoryLoadResults.put(classLoader, filtered);
    }

    LOG.info("Found repositories implementations: {}", repositoryLoadResults);

    final String storageClassName = StringUtils.isNotEmpty(conf.getNotebookStorageClass())
            ? conf.getNotebookStorageClass().trim()
            : defaultStorage;

    LOG.info("Search repository implementation: {}", storageClassName);

    ClassLoader resultClassLoader = null;
    for (final Map.Entry<ClassLoader, Set<Class<? extends NotebookRepo>>> result : repositoryLoadResults.entrySet()) {

      if (result.getValue().stream().anyMatch(aClass -> aClass.getName().equals(storageClassName))) {
        resultClassLoader = result.getKey();
        break;
      }
    }

    if (Objects.isNull(resultClassLoader)) {
      throw new IllegalStateException("Notebook repository not found!");
    }

    try {
      repo = (NotebookRepo) resultClassLoader.loadClass(storageClassName).newInstance();
      repo.init(conf);
    } catch (final IOException | InstantiationException | IllegalAccessException | ClassNotFoundException e) {
      LOG.error("Fail to instantiate notebookrepo from class:" + storageClassName, e);
      throw new IllegalStateException("Fail to instantiate notebookrepo from class:" + storageClassName, e);
    }
  }


  public boolean isRevisionSupported() {
    return repo instanceof NotebookRepoWithVersionControl;
  }


  @Override
  public Note getNote(final String noteId) {
    try {
      final Map<String, NoteInfo> notes = repo.list();
      if (notes.containsKey(noteId)) {
        final NoteInfo noteInfo = notes.get(noteId);
        return repo.get(noteId, noteInfo.getPath());
      } else {
        return null;
      }
    } catch (Exception e) {
      throw new IllegalStateException("Note not found", e);
    }
  }

  @Override
  public List<Note> getAllNotes() {
    return new ArrayList<>();
  }

  @Override
  public List<NoteInfo> getNotesInfo() {
    try {
      return new ArrayList<>(repo.list().values());
    } catch (Exception e) {
      throw new IllegalStateException("Note not found", e);
    }
  }

  @Override
  public Note persistNote(final Note note) {
    try {
      repo.save(note);
    } catch (Exception e) {
      throw new IllegalStateException("Error while saving Note", e);
    }
    return note;
  }

  @Override
  public Note updateNote(final Note note) {
    try {
      NoteInfo repoNoteInfo = repo.list().get(note.getId());
      if (!repoNoteInfo.getPath().equals(note.getPath())) {
        repo.move(note.getId(), repoNoteInfo.getPath(), note.getPath());
      }
      repo.save(note);
    } catch (Exception e) {
      throw new IllegalStateException("Error while update Note", e);
    }
    return note;
  }

  @Override
  public boolean removeNote(final String noteId) {
    try {
      Note note = getNote(noteId);
      repo.remove(note.getId(), note.getPath());
    } catch (IOException e) {
      throw new IllegalStateException("Error while delete Note", e);
    }
    return true;
  }

}
