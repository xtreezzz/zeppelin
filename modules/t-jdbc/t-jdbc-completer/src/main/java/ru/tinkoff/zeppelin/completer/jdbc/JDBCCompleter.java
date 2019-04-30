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
package ru.tinkoff.zeppelin.completer.jdbc;

import com.google.common.collect.Lists;
import com.google.gson.Gson;
import net.sf.jsqlparser.JSQLParserException;
import net.sf.jsqlparser.parser.CCJSqlParserUtil;
import net.sf.jsqlparser.statement.Statement;
import net.sf.jsqlparser.statement.select.Select;
import net.sf.jsqlparser.util.TablesNamesFinder;
import org.apache.metamodel.jdbc.JdbcDataContext;
import org.apache.metamodel.schema.Column;
import org.apache.metamodel.schema.Schema;
import org.apache.metamodel.schema.Table;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import ru.tinkoff.zeppelin.commons.jdbc.JDBCInstallation;
import ru.tinkoff.zeppelin.commons.jdbc.JDBCInterpolation;
import ru.tinkoff.zeppelin.interpreter.Completer;
import ru.tinkoff.zeppelin.interpreter.InterpreterCompletion;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.io.File;
import java.lang.reflect.Constructor;
import java.net.URL;
import java.net.URLClassLoader;
import java.sql.Connection;
import java.sql.Driver;
import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * This is a completer for jdbc interpreter based on JSqlParser and Apache Metamodel.
 */
public class JDBCCompleter extends Completer {

  private static final Logger LOGGER = LoggerFactory.getLogger(JDBCCompleter.class);


  private static final String CONNECTION_USER_KEY = "connection.user";
  private static final String CONNECTION_URL_KEY = "connection.url";
  private static final String CONNECTION_PASSWORD_KEY = "connection.password";

  private static final String DRIVER_CLASS_NAME_KEY = "driver.className";
  private static final String DRIVER_ARTIFACT_KEY = "driver.artifact";
  private static final String DRIVER_MAVEN_REPO_KEY = "driver.maven.repository.url";

  private static final String QUERY_TIMEOUT_KEY = "query.timeout";
  private static final String QUERY_ROWLIMIT_KEY = "query.rowlimit";

  public JDBCCompleter() {
    super();
  }

  // Simple database meta: schema -> table -> list of columns.
  // All keys must be sorted in the natural order for effective key retrieval by prefix.
  private static NavigableMap<String, NavigableMap<String, SortedSet<String>>> database = null;


  @Override
  public boolean isAlive() {
    return database != null;
  }

  @Override
  public boolean isOpened() {
    return database != null;
  }

  /**
   * Installs driver if needed and opens the database connection.
   *
   * @param configuration interpreter configuration.
   * @param classPath     class path.
   */
  @Override
  public void open(@Nonnull final Map<String, String> configuration, @Nonnull final String classPath) {
    synchronized (JDBCCompleter.class) {

      if (database != null) {
        return;
      }
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

        final String repositpryURL = configuration.getOrDefault(
                DRIVER_MAVEN_REPO_KEY,
                "http://repo1.maven.org/maven2/"
        );
        final String dir = JDBCInstallation.installDriver(artifact, repositpryURL);
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
            final Connection connection = driver.connect(dbUrl, authSettings);

            final Class dContent = Class.forName("org.apache.metamodel.jdbc.JdbcDataContext", true, classLoader);
            final Constructor dConstructor = dContent.getConstructor(Connection.class);
            final JdbcDataContext dataContext = (JdbcDataContext) dConstructor.newInstance(connection);

            NavigableMap<String, NavigableMap<String, SortedSet<String>>> result = new TreeMap<>();
            for (final Schema s : dataContext.getSchemas()) {
              for (final Table t : s.getTables()) {
                NavigableMap<String, SortedSet<String>> tableNode = result.get(s.getName());
                if (tableNode == null) {
                  result.put(s.getName(), new TreeMap<>());
                  tableNode = result.get(s.getName());
                }
                SortedSet<String> columns = tableNode.get(t.getName());
                if (columns == null) {
                  tableNode.put(t.getName(), new TreeSet<>());
                  columns = tableNode.get(t.getName());
                }
                columns.addAll(t.getColumns().stream().map(Column::getName).collect(Collectors.toList()));
              }
            }
            dataContext.close(connection);
            connection.close();

            database = result;
          } catch (final Exception e) {
            LOGGER.error("SQL driver configured incorrectly", e);
          }
        }
      }
    }
  }

  @Override
  public boolean isReusableForConfiguration(@Nonnull final Map<String, String> configuration) {
    return true;
  }

  /**
   * May be called from another thread.
   **/
  @Override
  public void cancel() { }

  /**
   * May be called from another thread.
   */
  @Override
  public void close() { }

  @Override
  public String complete(final String st,
                         final int cursorPosition,
                         final Map<String, String> noteContext,
                         final Map<String, String> userContext,
                         final Map<String, String> configuration) {

    final Map<String, String> params = new HashMap<>();
    params.putAll(noteContext);
    params.putAll(userContext);

    final JDBCInterpolation.InterpolateResponse response = JDBCInterpolation.interpolate(st, cursorPosition, params);

    final Set<InterpreterCompletion> result = complete(response.getPayload(), response.getCursorPosition());
    return new Gson().toJson(result);
  }

  /**
   * Creates autocomplete list. Realization based on JSqlParser.
   * Whole buffer would be cut down to cursor position, then if JSqlParser could parse result statement
   * it means that completion called on grammatically corrected query, user wants to complete names that he/she started to write.
   * If parser failed to process query exception would be thrown, if it contains <S_IDENTIFIER> tag
   * it means that metadata could be
   *
   * @param buffer full text, where completion called.
   * @param pos cursor position
   * @return completion result
   */
  public Set<InterpreterCompletion> complete(@Nonnull final String buffer, final int pos) {
    final Set<InterpreterCompletion> completions = new HashSet<>();

    // if multiplie statements passed - extract which cursor is pointed to
    final String statement = getStatement(buffer, pos);

    // statement ends on cursor position - get the whole statement
    int last = buffer.lastIndexOf(";");
    if (last < pos) {
      last = buffer.length();
    }
    final List<String> tmp = Arrays.asList(buffer.substring(0, last).split(";"));
    final int skipped = tmp.subList(0, tmp.size() - 1).stream().mapToInt(String::length).sum();
    final String wholeStatement = tmp.get(tmp.size() - 1);

    // get the last word.
    String lastWord = null;
    if (buffer.charAt(pos) != ' ') {
      final List<String> words = Arrays.asList(statement.split("\\s+"));
      lastWord = words.get(words.size() - 1);
    }

    try {
      // if statement is incorrect - exception would be thrown
      final Statement parseExpression = CCJSqlParserUtil.parse(statement);

      final Select selectStatement = (Select) parseExpression;
      final TablesNamesFinder tablesNamesFinder = new TablesNamesFinder();
      final List<String> tableList = tablesNamesFinder.getTableList(selectStatement);
      // default completion list
      completions.addAll(
              Stream.of("from", "where", "select", "join", "left", "right", "inner", "outer", "on", "and", "or")
                      .map(c -> new InterpreterCompletion(c, c, "keyword", ""))
                      .collect(Collectors.toList())
      );
      completeWithMeta(lastWord, completions);
    } catch (final JSQLParserException e) {
      if (e.getCause().toString().contains("Was expecting one of:")) {
        final String errorMsg = e.getCause().toString();

        final String columnPrefix = errorMsg.substring(errorMsg.indexOf("column ") + "column ".length());
        final int errorPos = Integer.parseInt(
                columnPrefix.substring(0, columnPrefix.indexOf(".")));

        if (errorPos == pos - skipped) {
          // error on cursor.
        }

        final List<String> expected = Arrays.asList(e.getCause().toString()
                .substring(e.getCause().toString().indexOf("Was expecting one of:")).split("\n"));
        final boolean loadMeta = expected.contains("    <S_IDENTIFIER>");
        final SortedSet<String> prepared = expected.subList(1, expected.size())
                .stream()
                .map(v -> v.trim().replace("\"", "").toLowerCase())
                .filter(v -> !v.isEmpty() && !v.startsWith("<"))
                .collect(Collectors.toCollection(TreeSet::new));

        if (lastWord != null) {
          for (final String match : prepared.tailSet(lastWord)) {
            if (!match.startsWith(lastWord)) {
              break;
            }
            completions.add(new InterpreterCompletion(match, match, "keyword", ""));
          }
        } else {
          completions.addAll(
                  prepared.stream().map(c -> new InterpreterCompletion(c, c, "keyword", ""))
                          .collect(Collectors.toList()));
        }

        if (loadMeta) {
          completeWithMeta(lastWord, completions);
        }
      }
    }

    return completions;
  }

  private String getStatement(@Nonnull final String buffer, final int pos) {
    final String statement = buffer.substring(0, pos + 1);
    final List<String> statements = Arrays.asList(statement.split(";"));
    if (statements.size() == 0) {
      return buffer;
    }
    return statements.get(statements.size() - 1);
  }


  /**
   * Fills the autocomplete list with database objects (schemas, tables, columns) names.
   *
   * @param lastWord, word on which the cursor, {@code null} if cursor on space.
   * @param completions, collection to fill.
   */
  private void completeWithMeta(@Nullable final String lastWord,
                                @Nonnull final Set<InterpreterCompletion> completions) {
    if (lastWord == null) {
      // add all schemas to completion.
      completions.addAll(database.keySet()
              .stream()
              .map(s -> new InterpreterCompletion(s, s, "schema", ""))
              .collect(Collectors.toList()));
      return;
    }

    // assumption: users always write the full path to the column.
    // split word on nodes, e.g. public.bug -> ['public', 'bug'] to get schema and table names.
    final List<String> nodes = Arrays.asList(lastWord.split("\\."));
    final String lastNode = nodes.get(nodes.size() - 1);
    int nodeCnt = nodes.size();
    if (lastWord.endsWith(".")) {
      nodeCnt += 1;
    }

    if (nodeCnt == 1) {
      // user started to write schema name
      completeTailSet(database.navigableKeySet().tailSet(lastWord), lastWord, completions, "schema");
    } else if (nodeCnt == 2) {
      if (nodes.size() == 1) {
        // e.g. lastWord was 'public.', schema is written, but table name is empty
        // fill with names of tables from the corresponding schema.
        if (database.get(nodes.get(0)) != null) {
          completions.addAll(database.get(nodes.get(0)).navigableKeySet()
                  .stream()
                  .map(t -> new InterpreterCompletion(t, t, "table", ""))
                  .collect(Collectors.toList()));
        }
        // break assumption: if first node is table name.
        database.forEach((key, value) -> {
          if (value.get(nodes.get(0)) != null) {
            completions.addAll(value.get(nodes.get(0))
                    .stream()
                    .map(t -> new InterpreterCompletion(t, t, "column", ""))
                    .collect(Collectors.toList()));
          }
        });
      } else {
        // user started to write table name
        completeTailSet(database.get(nodes.get(0)).navigableKeySet(), lastNode, completions, "table");
      }
    } else if (nodeCnt == 3) {
      if (nodes.size() == 2) {
        // e.g. lastWord was 'public.bug.', schema and table is written, but column name is empty
        // fill with names of columns from the corresponding schema and table.
        completions.addAll(database.get(nodes.get(0)).get(nodes.get(1))
                .stream()
                .map(t -> new InterpreterCompletion(t, t, "column", ""))
                .collect(Collectors.toList()));

      } else {
        // user started to write column name
        completeTailSet(database.get(nodes.get(0)).get(nodes.get(1)), lastNode, completions, "column");
      }
    }
  }

  /**
   * Adds words with the same prefix to completion.
   *
   * @param tailSet, set of matched words.
   * @param prefix, searched prefix
   * @param completions collection to fill
   * @param type completion type
   */
  private void completeTailSet(@Nonnull final SortedSet<String> tailSet,
                               @Nonnull final String prefix,
                               @Nonnull final Set<InterpreterCompletion> completions,
                               @Nonnull final String type) {
    for (final String match : tailSet) {
      if (!match.startsWith(prefix)) {
        continue;
      }
      completions.add(new InterpreterCompletion(match, match, type, ""));
    }
  }
}