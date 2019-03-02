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


import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.zeppelin.conf.ZeppelinConfiguration;
import org.apache.zeppelin.notebook.conf.CronJobConfiguration;
import org.apache.zeppelin.notebook.Note;
import org.apache.zeppelin.notebook.NoteInfo;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.file.Files;
import java.util.Map;

import static org.junit.Assert.assertEquals;

public class FileSystemNotebookRepoTest {

  private ZeppelinConfiguration zConf;
  private Configuration hadoopConf;
  private FileSystem fs;
  private FileSystemNotebookRepo hdfsNotebookRepo;
  private String notebookDir;

  @Before
  public void setUp() throws IOException {
    notebookDir = Files.createTempDirectory("FileSystemNotebookRepoTest").toFile().getAbsolutePath();
    zConf = new ZeppelinConfiguration();
    System.setProperty(ZeppelinConfiguration.ConfVars.ZEPPELIN_NOTEBOOK_DIR.getVarName(), notebookDir);
    hadoopConf = new Configuration();
    fs = FileSystem.get(hadoopConf);
    hdfsNotebookRepo = new FileSystemNotebookRepo();
    hdfsNotebookRepo.init(zConf);
  }

  @After
  public void tearDown() throws IOException {
    FileUtils.deleteDirectory(new File(notebookDir));
  }

  @Test
  public void testBasics() throws IOException {
    assertEquals(0, hdfsNotebookRepo.list().size());

    // create a new note
    Note note = new Note();
    note.setPath("/title_1");

    CronJobConfiguration config = CronJobConfiguration.Builder.isCronEnabled(false).build();
    note.setConfig(config);
    hdfsNotebookRepo.save(note);
    assertEquals(1, hdfsNotebookRepo.list().size());

    // read this note from hdfs
    Note note_copy = hdfsNotebookRepo.get(note.getId(), note.getPath());
    assertEquals(note.getName(), note_copy.getName());
    assertEquals(note.getConfig(), note_copy.getConfig());

    // update this note);
    assertEquals(1, hdfsNotebookRepo.list().size());
    note_copy = hdfsNotebookRepo.get(note.getId(), note.getPath());
    assertEquals(note.getName(), note_copy.getName());
    assertEquals(note.getConfig(), note_copy.getConfig());

    // move this note
    String newPath = "/new_folder/title_1";
    hdfsNotebookRepo.move(note.getId(), note.getPath(), newPath);
    assertEquals(1, hdfsNotebookRepo.list().size());
    assertEquals("title_1", hdfsNotebookRepo.get(note.getId(), newPath).getName());

    // delete this note
    hdfsNotebookRepo.remove(note.getId(), newPath);
    assertEquals(0, hdfsNotebookRepo.list().size());

    // create another new note under folder
    note = new Note();
    note.setPath("/folder1/title_1");
    note.setConfig(config);
    hdfsNotebookRepo.save(note);
    assertEquals(1, hdfsNotebookRepo.list().size());

    hdfsNotebookRepo.move("/folder1", "/folder2/folder3");
    Map<String, NoteInfo> notesInfo = hdfsNotebookRepo.list();
    assertEquals(1, notesInfo.size());

    assertEquals("/folder2/folder3/title_1", notesInfo.get(note.getId()).getPath());

    // delete folder
    hdfsNotebookRepo.remove("/folder2");
    assertEquals(0, hdfsNotebookRepo.list().size());
  }

  @Test
  public void testComplicatedScenarios() throws IOException {
    // scenario_1: notebook_dir is not clean. There're some unrecognized dir and file under notebook_dir
    fs.mkdirs(new Path(notebookDir, "1/2"));
    OutputStream out = fs.create(new Path(notebookDir, "1/a.json"));
    out.close();

    assertEquals(0, hdfsNotebookRepo.list().size());

    // scenario_2: note_folder is existed.
    // create a new note
    Note note = new Note();
    note.setPath("/title_1");
    CronJobConfiguration config = CronJobConfiguration.Builder.isCronEnabled(false).build();
    note.setConfig(config);

    hdfsNotebookRepo.save(note);
    assertEquals(1, hdfsNotebookRepo.list().size());
  }
}
