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
import java.io.UnsupportedEncodingException;
import java.net.URL;
import java.net.URLClassLoader;
import java.net.URLEncoder;
import java.sql.Connection;
import java.sql.Driver;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.Statement;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Properties;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.apache.zeppelin.DependencyResolver;
import org.apache.zeppelin.Repository;
import org.apache.zeppelin.interpreter.core.Interpreter;
import org.apache.zeppelin.interpreter.core.InterpreterResult;
import org.apache.zeppelin.interpreter.core.InterpreterResult.Code;
import org.apache.zeppelin.interpreter.core.InterpreterResult.Message;
import org.apache.zeppelin.interpreter.core.InterpreterResult.Message.Type;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Common JDBC Interpreter: This interpreter can be used for accessing different SQL databases.
 *
 * Before using interpreter you should configure driver, which will be installed at runtime:
 * <ul>
 *   <li>{@code driver.className} - driver class name, e.g. {@code org.postgresql.Driver}</li>
 *   <li>{@code driver.artifact} - maven driver artifact, e.g. {@code org.postgresql:postgresql:jar:42.2.5}</li>
 * </ul>
 *
 * Specify connection:
 * <ul>
 *   <li>{@code connection.user} - username for database connection</li>
 *   <li>{@code connection.url} - database url</li>
 *   <li>{@code connection.password} - password</li>
 * </ul>
 *
 * Precode and Postcode rules:
 * <ul>
 *   <li>If precode fails -> Error result from precode will be returned as total query result</li>
 *   <li>If precode succeed, postcode always will be executed</li>
 *   <li>If postcode fails -> error will be logged, and connection will be closed.</li>
 *   <li></li>
 * </ul>
 */
public class JDBCInterpreter extends Interpreter {

  private static final Logger LOGGER = LoggerFactory.getLogger(JDBCInterpreter.class);

  /**
   * Database connection.
   *
   * @see JDBCInterpreter#cancel()
   * @see JDBCInterpreter#close()
   * @see JDBCInterpreter#open(Map, String)
   * @see JDBCInterpreter#executeQuery(String, boolean)
   */
  @Nullable
  private volatile Connection connection = null;

  @Nullable
  private volatile Statement query = null;

  private static final String CONNECTION_USER_KEY = "connection.user";
  private static final String CONNECTION_URL_KEY = "connection.url";
  private static final String CONNECTION_PASSWORD_KEY = "connection.password";

  private static final String DRIVER_CLASS_NAME_KEY = "driver.className";
  private static final String DRIVER_ARTIFACT_KEY = "driver.artifact";
  private static final String DRIVER_MAVEN_REPO_KEY = "driver.maven.repository.url";

  private static final String QUERY_TIMEOUT_KEY = "query.timeout";
  private static final String QUERY_ROWLIMIT_KEY = "query.rowlimit";

  public JDBCInterpreter() {
    super();
  }

  /**
   * Checks is connection valid (useable) (may took 30 seconds).
   *
   * @return {@code true} if it is able to execute query using this instance.
   */
  @Override
  public boolean isAlive() {
    try {
      return connection != null && connection.isValid(30);
    } catch (final Exception e) {
      return false;
    }
  }

  /**
   * Checks if connection wasn't closed.
   *
   * @return {@code true} if connection wasn't closed.
   */
  @Override
  public boolean isOpened() {
    try {
      return connection != null && !connection.isClosed();
    } catch (final Exception e) {
      return false;
    }
  }

  /**
   * Installs driver if needed and opens the database connection.
   *
   * @param configuration interpreter configuration.
   * @param classPath class path.
   */
  @Override
  public void open(@Nonnull final Map<String, String> configuration, @Nonnull final String classPath) {
    if (this.configuration == null) {
      this.configuration = new HashMap<>();
    }
    this.configuration.clear();
    this.configuration.putAll(configuration);
    final String className = configuration.get(DRIVER_CLASS_NAME_KEY);
    final String artifact = configuration.get(DRIVER_ARTIFACT_KEY);
    final String user = configuration.get(CONNECTION_USER_KEY);
    final String dbUrl = configuration.get(CONNECTION_URL_KEY);
    final String password = configuration.get(CONNECTION_PASSWORD_KEY);

    if (className != null
        && artifact != null
        && user != null
        && dbUrl != null
        && password != null) {

      final String dir = installDriver(artifact);
      if (dir != null && !dir.equals("")) {
        final File driverFolder = new File(dir);
        try {
          final List<URL> urls = Lists.newArrayList();
          for (final File file : driverFolder.listFiles()) {
            final URL url = file.toURI().toURL();

            urls.add(new URL("jar:" + url.toString() + "!/"));
          }

          final URLClassLoader classLoader = URLClassLoader.newInstance(urls.toArray(new URL[0]));
          final Class driverClass = Class.forName(className, true, classLoader);
          final Driver driver = (Driver) driverClass.newInstance();

          final Properties authSettings = new Properties();
          authSettings.put("user", user);
          authSettings.put("password", password);
          connection = driver.connect(dbUrl, authSettings);
        } catch (final Exception e) {
          LOGGER.error("SQL driver configured incorrectly", e);
        }
      }
    }
  }

  @Override
  public boolean isReusableForConfiguration(@Nonnull final Map<String, String> configuration) {
    return this.configuration.equals(configuration);
  }

  /**
   * May be called form another thread.
   **/
  @Override
  public void cancel() {
    try {
      if (query != null && !query.isClosed()) {
        query.cancel();
      }
    } catch (final Exception e) {
      LOGGER.error("Failed to cancel", e);
    }
  }

  /**
   * May be called form another thread.
   */
  @Override
  public void close() {
    if (isAlive() && isOpened()) {
      try {
        if (connection != null) {
          connection.abort(Runnable::run);
        }
      } catch (final Exception e) {
        LOGGER.error("Failed to close", e);
      }
    }
  }

  /**
   * Interprets query.
   *
   * Notice that interpreter should be alive before calling interpreter. {@link JDBCInterpreter#isAlive()}
   *
   * TODO(egorklimov): check execution logic on cancel!
   * If interpreter would be canceled on precode, {@code precodeResult.code()} would be {@code Code.ERROR}
   * therefore whole interpret process will be finished.
   * If interpreter would be canceled on user query {@code queryResult.code()} would be {@code Code.ERROR},
   * but it would be returned only after postcode execution.
   * If interpreter would be canceled on postcode, {@code postcodeResult.code()} would be {@code Code.ERROR}
   * therefore connection will be closed and ??
   *
   * @param st statements to run.
   * @param noteContext Note context
   * @param userContext User context
   * @param configuration Interpreter properties
   * @return Interpreter result
   */
  @Nonnull
  @Override
  public InterpreterResult interpretV2(@Nonnull final String st,
                                       @Nonnull final Map<String, String> noteContext,
                                       @Nonnull final Map<String, String> userContext,
                                       @Nonnull final Map<String, String> configuration) {
    if (isOpened() && isAlive()) {
      final String precode = configuration.get("query.precode");
      if (precode != null && !precode.equals("")) {
        final InterpreterResult precodeResult = executeQuery(precode, false);
        if (precodeResult.code().equals(Code.ERROR)) {
          return precodeResult;
        }
      }

      final InterpreterResult queryResult = executeQuery(st, true);

      final String postcode = configuration.get("query.postcode");
      if (postcode != null && !postcode.equals("")) {
        final InterpreterResult postcodeResult = executeQuery(postcode, false);
        if (postcodeResult.code().equals(Code.ERROR)) {
          LOGGER.error("Postcode query failed: {}", postcodeResult.message());
          close();
        }
      }
      return queryResult;
    }
    return new InterpreterResult(Code.ERROR,
        Collections.singletonList(new Message(Type.TEXT, "Interpreter is not opened")));
  }

  /**
   * Util method to execute a single query.
   *
   * @param queryString - Query to execute.
   * @param processResult - Flag of result processing, if {@code true} - result
   *                        will be converted to table format, otherwise result has no message.
   * @return Result of query execution.
   */
  private InterpreterResult executeQuery(@Nonnull final String queryString, final boolean processResult) {
    ResultSet resultSet = null;
    final StringBuilder exception = new StringBuilder();
    try {
      this.query = Objects.requireNonNull(connection).createStatement();
      prepareQuery();
      resultSet = Objects.requireNonNull(this.query).executeQuery(queryString);
      if (processResult) {
        final String processedTable = getResults(resultSet);
        if (processedTable == null) {
          return new InterpreterResult(Code.ERROR,
              Collections.singletonList(new Message(Type.TEXT, "Failed to process query result")));
        }
        return new InterpreterResult(Code.SUCCESS,
            Collections.singletonList(new Message(Type.TABLE, processedTable)));
      }
      return new InterpreterResult(Code.SUCCESS);
    } catch (final Exception e) {
      exception.append("Exception during processing the query:\n")
          .append(ExceptionUtils.getStackTrace(e))
          .append("\n");
    } finally {
      try {
        if (resultSet != null) {
          resultSet.close();
        }
        if (this.query != null) {
          Objects.requireNonNull(this.query).close();
        }
      } catch (final Exception e) {
        exception.append("Exception during connection closing:\n")
            .append(ExceptionUtils.getStackTrace(e));
      }
    }
    return new InterpreterResult(Code.ERROR, Collections.singletonList(
        new Message(Type.TEXT, exception.toString())));
  }

  @Override
  public FormType getFormType() {
    return null;
  }

  /**
   * Sets query timeout and max row count.
   */
  private void prepareQuery() {
    int maxRows = Integer.parseInt(configuration.getOrDefault(QUERY_ROWLIMIT_KEY, "0"));
    if (maxRows < 0) {
      maxRows = 0;
    }
    int timeout = Integer.parseInt(configuration.getOrDefault(QUERY_TIMEOUT_KEY, "0"));
    if (timeout < 0) {
      timeout = 0;
    }
    try {
      Objects.requireNonNull(this.query).setMaxRows(maxRows);
      Objects.requireNonNull(this.query).setQueryTimeout(timeout);
    } catch (final Exception e) {
      LOGGER.error("Failed to set query limits", e);
    }
  }

  private boolean isInstalled(@Nonnull final String artifact) {
    final File folderToStore = new File(getDestinationFolder(artifact));
    return folderToStore.exists() && Objects.requireNonNull(folderToStore.list()).length > 0;
  }

  /**
   * Downloads driver by the maven artifact.
   *
   * @param artifact Driver artifact.
   * @return Absolute path to driver directory, {@code null} if installation failed.
   */
  @Nullable
  private String installDriver(@Nonnull final String artifact) {
    if (isInstalled(artifact)) {
      return getDirectory(artifact);
    }

    final File folderToStore = new File(getDestinationFolder(artifact));
    try {
      final List<Repository> repos = Collections.singletonList(
          new Repository(false, "central",
              configuration.getOrDefault(DRIVER_MAVEN_REPO_KEY, "http://repo1.maven.org/maven2/"),
              null, null, null, null,
              null, null, null));

      final DependencyResolver dependencyResolver = new DependencyResolver(repos);
      dependencyResolver.load(artifact, folderToStore);
      return folderToStore.getAbsolutePath();
    } catch (final Exception e) {
      LOGGER.error("Error while installDriver interpreter", e);
      uninstallDriver(artifact);
      return null;
    }
  }

  private void uninstallDriver(@Nonnull final String artifact) {
    try {
      final File folderToStore = new File(getDestinationFolder(artifact));
      FileUtils.deleteDirectory(folderToStore);
    } catch (final Exception e) {
      LOGGER.error("Error while remove interpreter", e);
    }
  }

  @Nonnull
  private String getDirectory(@Nonnull final String artifact) {
    final File folderToStore = new File(getDestinationFolder(artifact));
    return folderToStore.getAbsolutePath();
  }

  /**
   * Converts result set to table.
   *
   * @param resultSet - result set to convert.
   * @return converted result, {@code null} if process failed.
   */
  @Nullable
  private String getResults(final ResultSet resultSet) {
    try {
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
    } catch (final Exception e) {
      LOGGER.error("Failed to parse result", e);
      return null;
    }
  }

  private String getDestinationFolder(final String artifact) {
    try {
      return String.join(File.separator, "drivers", URLEncoder.encode(artifact, "UTF-16")) + File.separator;
    } catch (final UnsupportedEncodingException e) {
      return "";
    }
  }

  /**
   * For table response replace Tab and Newline characters from the content.
   */
  private String replaceReservedChars(final String str) {
    if (str == null) {
      return "";
    }
    return str.replace('\t', ' ')
        .replace('\n', ' ');
  }
}
