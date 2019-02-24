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

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.InputStream;
import java.net.URI;
import java.net.URISyntaxException;
import java.security.PrivilegedExceptionAction;
import java.util.ArrayList;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RawLocalFileSystem;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.zeppelin.conf.ZeppelinConfiguration;
import org.apache.zeppelin.notebook.Note;
import org.apache.zeppelin.notebook.NoteInfo;
import org.apache.zeppelin.repo.api.NotebookRepo;
import org.apache.zeppelin.repo.api.NotebookRepoSettingsInfo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * NotebookRepos for hdfs.
 *
 */
public class FileSystemNotebookRepo implements NotebookRepo {
  private static final Logger LOGGER = LoggerFactory.getLogger(FileSystemNotebookRepo.class);

  private FileSystemStorage fs;
  private Path notebookDir;

  public FileSystemNotebookRepo() {

  }

  public void init(ZeppelinConfiguration zConf) throws IOException {
    this.fs = new FileSystemStorage(zConf, zConf.getNotebookDir());
    LOGGER.info("Creating FileSystem: " + this.fs.getFs().getClass().getName());
    this.notebookDir = this.fs.makeQualified(new Path(zConf.getNotebookDir()));
    LOGGER.info("Using folder {} to store notebook", notebookDir);
    this.fs.tryMkDir(notebookDir);
  }

  @Override
  public Map<String, NoteInfo> list() throws IOException {
    List<Path> notePaths = fs.listAll(notebookDir);
    Map<String, NoteInfo> noteInfos = new HashMap<>();
    for (Path path : notePaths) {
      try {
        NoteInfo noteInfo = new NoteInfo(getNoteId(path.getName()),
            getNotePath(notebookDir.toString(), path.toString()));
        noteInfos.put(noteInfo.getId(), noteInfo);
      } catch (IOException e) {
        LOGGER.warn("Fail to get NoteInfo for note: " + path.getName(), e);
      }
    }
    return noteInfos;
  }


  @Override
  public Note get(String noteId, String notePath) throws IOException {
    String content = this.fs.readFile(
        new Path(notebookDir, buildNoteFileName(noteId, notePath)));
    return Note.fromJson(content);
  }

  @Override
  public void save(Note note) throws IOException {
    this.fs.writeFile(note.toJson(),
        new Path(notebookDir, buildNoteFileName(note.getId(), note.getPath())),
        true);
  }

  @Override
  public void move(String noteId,
                   String notePath,
                   String newNotePath) throws IOException {
    Path src = new Path(notebookDir, buildNoteFileName(noteId, notePath));
    Path dest = new Path(notebookDir, buildNoteFileName(noteId, newNotePath));
    this.fs.move(src, dest);
  }

  @Override
  public void move(String folderPath, String newFolderPath)
      throws IOException {
    this.fs.move(new Path(notebookDir, folderPath.substring(1)),
        new Path(notebookDir, newFolderPath.substring(1)));
  }

  @Override
  public void remove(String noteId, String notePath)
      throws IOException {
    if (!this.fs.delete(new Path(notebookDir.toString(), buildNoteFileName(noteId, notePath)))) {
      LOGGER.warn("Fail to move note, noteId: " + notePath + ", notePath: " + notePath);
    }
  }

  @Override
  public void remove(String folderPath) throws IOException {
    if (!this.fs.delete(new Path(notebookDir, folderPath.substring(1)))) {
      LOGGER.warn("Fail to remove folder: " + folderPath);
    }
  }

  @Override
  public void close() {
    LOGGER.warn("close is not implemented for HdfsNotebookRepo");
  }

  @Override
  public List<NotebookRepoSettingsInfo> getSettings() {
    LOGGER.warn("getSettings is not implemented for HdfsNotebookRepo");
    return null;
  }

  @Override
  public void updateSettings(Map<String, String> settings) {
    LOGGER.warn("updateSettings is not implemented for HdfsNotebookRepo");
  }

  /**
   * Hadoop FileSystem wrapper. Support both secure and no-secure mode
   */
  private static class FileSystemStorage {

    private static Logger LOGGER = LoggerFactory.getLogger(FileSystemStorage.class);

    // only do UserGroupInformation.loginUserFromKeytab one time, otherwise you will still get
    // your ticket expired.
    static {
      if (UserGroupInformation.isSecurityEnabled()) {
        ZeppelinConfiguration zConf = ZeppelinConfiguration.create();
        String keytab = zConf.getString(
            ZeppelinConfiguration.ConfVars.ZEPPELIN_SERVER_KERBEROS_KEYTAB);
        String principal = zConf.getString(
            ZeppelinConfiguration.ConfVars.ZEPPELIN_SERVER_KERBEROS_PRINCIPAL);
        if (StringUtils.isBlank(keytab) || StringUtils.isBlank(principal)) {
          throw new RuntimeException("keytab and principal can not be empty, keytab: " + keytab
              + ", principal: " + principal);
        }
        try {
          UserGroupInformation.loginUserFromKeytab(principal, keytab);
        } catch (IOException e) {
          throw new RuntimeException("Fail to login via keytab:" + keytab +
              ", principal:" + principal, e);
        }
      }
    }

    private ZeppelinConfiguration zConf;
    private Configuration hadoopConf;
    private boolean isSecurityEnabled;
    private FileSystem fs;

    public FileSystemStorage(ZeppelinConfiguration zConf, String path) throws IOException {
      this.zConf = zConf;
      this.hadoopConf = new Configuration();
      // disable checksum for local file system. because interpreter.json may be updated by
      // non-hadoop filesystem api
      // disable caching for file:// scheme to avoid getting LocalFS which does CRC checks
      this.hadoopConf.setBoolean("fs.file.impl.disable.cache", true);
      this.hadoopConf.set("fs.file.impl", RawLocalFileSystem.class.getName());
      this.isSecurityEnabled = UserGroupInformation.isSecurityEnabled();

      try {
        this.fs = FileSystem.get(new URI(path), this.hadoopConf);
      } catch (URISyntaxException e) {
        throw new IOException(e);
      }
    }

    public FileSystem getFs() {
      return fs;
    }

    public Path makeQualified(Path path) {
      return fs.makeQualified(path);
    }

    public boolean exists(final Path path) throws IOException {
      return callHdfsOperation(new HdfsOperation<Boolean>() {

        @Override
        public Boolean call() throws IOException {
          return fs.exists(path);
        }
      });
    }

    public void tryMkDir(final Path dir) throws IOException {
      callHdfsOperation(new HdfsOperation<Void>() {
        @Override
        public Void call() throws IOException {
          if (!fs.exists(dir)) {
            fs.mkdirs(dir);
            LOGGER.info("Create dir {} in hdfs", dir.toString());
          }
          if (fs.isFile(dir)) {
            throw new IOException(dir.toString() + " is file instead of directory, please remove " +
                "it or specify another directory");
          }
          fs.mkdirs(dir);
          return null;
        }
      });
    }

    public List<Path> list(final Path path) throws IOException {
      return callHdfsOperation(new HdfsOperation<List<Path>>() {
        @Override
        public List<Path> call() throws IOException {
          List<Path> paths = new ArrayList<>();
          for (FileStatus status : fs.globStatus(path)) {
            paths.add(status.getPath());
          }
          return paths;
        }
      });
    }

    // recursive search path, (TODO zjffdu, list folder in sub folder on demand, instead of load all
    // data when zeppelin server start)
    public List<Path> listAll(final Path path) throws IOException {
      return callHdfsOperation(new HdfsOperation<List<Path>>() {
        @Override
        public List<Path> call() throws IOException {
          List<Path> paths = new ArrayList<>();
          collectNoteFiles(path, paths);
          return paths;
        }

        private void collectNoteFiles(Path folder, List<Path> noteFiles) throws IOException {
          FileStatus[] paths = fs.listStatus(folder);
          for (FileStatus path : paths) {
            if (path.isDirectory()) {
              collectNoteFiles(path.getPath(), noteFiles);
            } else {
              if (path.getPath().getName().endsWith(".zpln")) {
                noteFiles.add(path.getPath());
              } else {
                LOGGER.warn("Unknown file: " + path.getPath());
              }
            }
          }
        }
      });
    }

    public boolean delete(final Path path) throws IOException {
      return callHdfsOperation(new HdfsOperation<Boolean>() {
        @Override
        public Boolean call() throws IOException {
          return fs.delete(path, true);
        }
      });
    }

    public String readFile(final Path file) throws IOException {
      return callHdfsOperation(new HdfsOperation<String>() {
        @Override
        public String call() throws IOException {
          LOGGER.debug("Read from file: " + file);
          ByteArrayOutputStream noteBytes = new ByteArrayOutputStream();
          IOUtils.copyBytes(fs.open(file), noteBytes, hadoopConf);
          return new String(noteBytes.toString(
              zConf.getString(ZeppelinConfiguration.ConfVars.ZEPPELIN_ENCODING)));
        }
      });
    }

    public void writeFile(final String content, final Path file, boolean writeTempFileFirst)
        throws IOException {
      callHdfsOperation(new HdfsOperation<Void>() {
        @Override
        public Void call() throws IOException {
          InputStream in = new ByteArrayInputStream(content.getBytes(
              zConf.getString(ZeppelinConfiguration.ConfVars.ZEPPELIN_ENCODING)));
          Path tmpFile = new Path(file.toString() + ".tmp");
          IOUtils.copyBytes(in, fs.create(tmpFile), hadoopConf);
          fs.delete(file, true);
          fs.rename(tmpFile, file);
          return null;
        }
      });
    }

    public void move(Path src, Path dest) throws IOException {
      callHdfsOperation(() -> {
        fs.rename(src, dest);
        return null;
      });
    }

    private interface HdfsOperation<T> {
      T call() throws IOException;
    }

    public synchronized <T> T callHdfsOperation(final HdfsOperation<T> func) throws IOException {
      if (isSecurityEnabled) {
        try {
          return UserGroupInformation.getCurrentUser().doAs(new PrivilegedExceptionAction<T>() {
            @Override
            public T run() throws Exception {
              return func.call();
            }
          });
        } catch (InterruptedException e) {
          throw new IOException(e);
        }
      } else {
        return func.call();
      }
    }
  }
}
