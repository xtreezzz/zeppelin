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
package ru.tinkoff.zeppelin.interpreter.jdbc;

import com.google.common.collect.Lists;
import java.io.File;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLClassLoader;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import org.apache.commons.io.FileUtils;
import org.apache.zeppelin.DependencyResolver;
import org.apache.zeppelin.Repository;
import org.apache.zeppelin.Repository.ProxyProtocol;
import org.apache.zeppelin.interpreter.core.Interpreter;
import org.apache.zeppelin.interpreter.core.InterpreterException;
import org.apache.zeppelin.interpreter.core.InterpreterResult;
import org.apache.zeppelin.interpreter.core.InterpreterResult.Code;
import org.apache.zeppelin.interpreter.core.InterpreterResult.Message;
import org.apache.zeppelin.interpreter.core.InterpreterResult.Message.Type;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class JDBCInterpreter extends Interpreter {

  private static final Logger LOG = LoggerFactory.getLogger(JDBCInterpreter.class);

  private Connection connection;

  public JDBCInterpreter() {
    super();
  }

  @Override
  public boolean isAlive() {
    try {
      return connection != null && !connection.isClosed();
    } catch (final SQLException e) {
      return false;
    }
  }

  @Override
  public boolean isOpened() {
    try {
      return connection != null && !connection.isClosed();
    } catch (final SQLException e) {
      return false;
    }
  }

  @Override
  public void open(final Map<String, String> configuration, final String classPath)
      throws InterpreterException {
    final String className = configuration.get("driver.className");
    final String artifact = configuration.get("driver.artifact");
    final String user = configuration.get("connection.user");
    final String dbUrl = configuration.get("connection.url");
    final String password = configuration.get("connection.password");

    if (className != null
        && artifact != null
        && user != null
        && dbUrl != null
        && password != null) {
      final File driverFolder = new File(install(className, artifact));
      try {
        final List<URL> urls = Lists.newArrayList();
        for (final File file : driverFolder.listFiles()) {
          final URL url = file.toURI().toURL();

          urls.add(new URL("jar:" + url.toString() + File.separator));
        }

        final URLClassLoader classLoader = URLClassLoader.newInstance(urls.toArray(new URL[0]));
        Class.forName(className, true, classLoader);
      } catch (final MalformedURLException | ClassNotFoundException e) {
        throw new InterpreterException("SQL driver configured incorrectly");
      }

      connection = null;
      try {
        connection = DriverManager
            .getConnection(dbUrl, user, password);
      } catch (final SQLException e) {
        throw new InterpreterException("Connection Failed");
      }
    }
  }

  @Override
  public boolean isReusableForConfiguration(final Map<String, String> configuration) {
    return false;
  }

  /**
   * Called form another thread.
   *
   * @throws InterpreterException
   */
  @Override
  public void cancel() throws InterpreterException {
    if (isAlive() && isOpened()) {
      try {
        connection.abort(Runnable::run);
      } catch (final SQLException e) {
        throw new InterpreterException(e.getCause());
      }
    }
  }

  /**
   * Called form another thread.
   *
   * @throws InterpreterException
   */
  @Override
  public void close() throws InterpreterException {
    if (isAlive() && isOpened()) {
      try {
        connection.abort(Runnable::run);
        connection.close();
      } catch (final SQLException e) {
        throw new InterpreterException(e.getCause());
      }
    }
  }

  @Override
  public InterpreterResult interpretV2(final String st, final Map<String, String> noteContext,
      final Map<String, String> userContext, final Map<String, String> configuration) {
    if (isOpened() && isAlive()) {
      try (final Statement statement = connection.createStatement();
           final ResultSet resultSet = statement.executeQuery(st)) {
        return new InterpreterResult(Code.SUCCESS,
            Collections.singletonList(new Message(Type.TABLE, getResults(resultSet))));
      } catch (final SQLException e ) {
        LOG.error("Failed to execute query {}", st, e);
        return new InterpreterResult(Code.ERROR,
            Collections.singletonList(new Message(Type.TEXT, e.getMessage())));
      }
    }
    return new InterpreterResult(Code.ERROR,
        Collections.singletonList(new Message(Type.TEXT, "Interpreter is not running")));
  }

  @Override
  public FormType getFormType() throws InterpreterException {
    return null;
  }

  private boolean isInstalled(final String className, final String artifact) {
    final String version = extractArtifactVersion(artifact);
    if (!version.equals("")) {
      final File folderToStore = new File(
          String.join(File.separator, "drivers", className, version) + File.separator);
      return folderToStore.exists() && Objects.requireNonNull(folderToStore.list()).length > 0;
    }
    throw new RuntimeException("Wrong artifact passed");
  }

  private String install(final String className, final String artifact)
      throws InterpreterException {
    if (isInstalled(className, artifact)) {
      return getDirectory(className, artifact);
    }

    final String version = extractArtifactVersion(artifact);
    final File folderToStore = new File(
        String.join(File.separator, "drivers", className, version) + File.separator);
    try {
      List<Repository> repos = Collections.singletonList(new Repository(true, "central", "http://repo1.maven.org/maven2/",
          "username", "password", ProxyProtocol.HTTP, "127.0.0.1",
          8000, "proxyLogin", "proxyPass"));

      final DependencyResolver dependencyResolver = new DependencyResolver(repos);
      dependencyResolver.load(artifact, folderToStore);
      return folderToStore.getAbsolutePath();
    } catch (final Exception e) {
      LOG.error("Error while install interpreter", e);
      uninstallInterpreter(className, artifact);
      throw new InterpreterException("Wrong interpreter configuration");
    }
  }

  private void uninstallInterpreter(final String className, final String artifact) {
    final String version = extractArtifactVersion(artifact);
    final File folderToStore = new File(
        String.join(File.separator, "drivers", className, version) + File.separator);
    try {
      FileUtils.deleteDirectory(folderToStore);
    } catch (final Exception e) {
      LOG.error("Error while remove interpreter", e);
    }
  }

  private String getDirectory(final String className, final String artifact) {
    final String version = extractArtifactVersion(artifact);
    if (!version.equals("")) {
      final File folderToStore = new File(
          String.join(File.separator, "drivers", className, version) + File.separator);
      return folderToStore.getAbsolutePath();
    }
    throw new RuntimeException("Wrong artifact passed");
  }

  private String extractArtifactVersion(final String artifact) {
    final String[] coll = artifact.split(":");
    if (coll.length == 5) {
      return coll[3];
    }
    if (coll.length == 6) {
      return coll[4];
    }
    return "";
  }

  private String getResults(final ResultSet resultSet) throws SQLException {
    final ResultSetMetaData md = resultSet.getMetaData();
    final StringBuilder msg = new StringBuilder();

    for (int i = 1; i < md.getColumnCount() + 1; i++) {
      if (i > 1) {
        msg.append('\t');
      }
      if (md.getColumnLabel(i) != null && !md.getColumnLabel(i).equals("")) {
        msg.append(replaceReservedChars(md.getColumnLabel(i)));
      } else {
        msg.append(replaceReservedChars(md.getColumnName(i)));
      }
    }
    msg.append('\n');

    while (resultSet.next()) {
      for (int i = 1; i < md.getColumnCount() + 1; i++) {
        final Object resultObject;
        final String resultValue;
        resultObject = resultSet.getObject(i);
        if (resultObject == null) {
          resultValue = "null";
        } else {
          resultValue = resultSet.getString(i);
        }
        msg.append(replaceReservedChars(resultValue));
        if (i != md.getColumnCount()) {
          msg.append('\t');
        }
      }
      msg.append('\n');
    }
    return msg.toString();
  }

  /**
   * For table response replace Tab and Newline characters from the content.
   */
  private String replaceReservedChars(String str) {
    if (str == null) {
      return "";
    }
    return str.replace('\t', ' ')
        .replace('\n', ' ');
  }

}
