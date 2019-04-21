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
package org.apache.zeppelin;

import com.google.common.reflect.TypeToken;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.net.URLEncoder;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.NavigableMap;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import net.sf.jsqlparser.JSQLParserException;
import net.sf.jsqlparser.parser.CCJSqlParserUtil;
import net.sf.jsqlparser.statement.Statement;
import net.sf.jsqlparser.statement.select.Select;
import net.sf.jsqlparser.util.TablesNamesFinder;
import org.apache.metamodel.DataContext;
import org.apache.metamodel.factory.DataContextFactoryRegistryImpl;
import org.apache.metamodel.factory.DataContextPropertiesImpl;
import org.apache.metamodel.schema.Column;
import org.apache.metamodel.schema.Schema;
import org.apache.metamodel.schema.Table;

/**
 * This is a completer for jdbc interpreter based on JSqlParser and Apache Metamodel.
 */
public class JDBCCompleter {

  // Folder to store database metadata for completer.
  private static final String META_FOLDER_NAME = "jdbc-completion-meta";

  private boolean isOpened = false;

  // Simple database meta: schema -> table -> list of columns.
  // All keys must be sorted in the natural order for effective key retrieval by prefix.
  private NavigableMap<String, NavigableMap<String, SortedSet<String>>> database = new TreeMap<>();

  /**
   * On construction completer initialize the database metadata from connection or reads corresponding file.
   *
   * @param url corresponding interpreter url, see interpreter properties for details.
   * @param username corresponding interpreter username, see interpreter properties for details.
   * @param password corresponding interpreter password, see interpreter properties for details.
   * @param driverClassName corresponding interpreter driver class name, see interpreter properties for details.
   */
  public JDBCCompleter(@Nonnull final String url,
                       @Nonnull final String username,
                       @Nonnull final String password,
                       @Nonnull final String driverClassName) {
    try {
      processMetaFile(username, url);
      if (database == null || database.isEmpty()) {
        getMetaFromConnection(url, username, password, driverClassName);
      }
      isOpened = true;
    } catch (final Exception e) {
      isOpened = false;
      // log this
    }
  }

  /**
   * Constructs relative path to metadata file.
   *
   * @param username corresponding interpreter username, see interpreter properties for details.
   * @param url corresponding interpreter url, see interpreter properties for details.
   * @return constructed path.
   * @throws UnsupportedEncodingException if failed to construct path.
   */
  private String buildMetaPath(@Nonnull final String username, @Nonnull final String url)
      throws UnsupportedEncodingException {
    return String.join(
        File.separator, META_FOLDER_NAME, URLEncoder.encode(username + "_" + url, "UTF-8")) + ".json";
  }

  /**
   * Creates file for meta info or reads them if it exists.
   *
   * @param username corresponding interpreter username, see interpreter properties for details.
   * @param url corresponding interpreter url, see interpreter properties for details.
   * @throws IOException if path encoding failed or failed to create file.
   */
  private void processMetaFile(@Nonnull final String username, @Nonnull final String url) throws IOException {
    final File fileToStore = new File(buildMetaPath(username, url));
    if (fileToStore.exists()) {
      try {
        database = new Gson().fromJson(
            new String(Files.readAllBytes(Paths.get(fileToStore.getAbsolutePath()))),
            new TypeToken<TreeMap<String, TreeMap<String, TreeSet<String>>>>() {}.getType()
        );
      } catch (final IOException e) {
        //skip
      }
    } else {
      fileToStore.getParentFile().mkdirs();
      fileToStore.createNewFile();
      //log this
    }
  }

  /**
   * Gets database info from connection and saves it to the file.
   *
   * @param url corresponding interpreter url, see interpreter properties for details.
   * @param username corresponding interpreter username, see interpreter properties for details.
   * @param password corresponding interpreter password, see interpreter properties for details.
   * @param driverClassName corresponding interpreter driver class name, see interpreter properties for details.
   * @throws UnsupportedEncodingException if failed to encode path.
   */
  private void getMetaFromConnection(@Nonnull final String url,
                                     @Nonnull final String username,
                                     @Nonnull final String password,
                                     @Nonnull final String driverClassName)
      throws UnsupportedEncodingException {
    final DataContextPropertiesImpl properties = new DataContextPropertiesImpl();
    properties.put("type", "jdbc");
    properties.put("url", url);
    properties.put("username", username);
    properties.put("password", password);
    properties.put("driver-class", driverClassName);
    final DataContext dataContext = DataContextFactoryRegistryImpl.getDefaultInstance().createDataContext(properties);

    if (database == null) {
      database = new TreeMap<>();
    }
    for (final Schema s : dataContext.getSchemas()) {
      for (final Table t : s.getTables()) {
        NavigableMap<String, SortedSet<String>> tableNode = database.get(s.getName());
        if (tableNode == null) {
          database.put(s.getName(), new TreeMap<>());
          tableNode = database.get(s.getName());
        }
        SortedSet<String> columns = tableNode.get(t.getName());
        if (columns == null) {
          tableNode.put(t.getName(), new TreeSet<>());
          columns = tableNode.get(t.getName());
        }
        columns.addAll(t.getColumns().stream().map(Column::getName).collect(Collectors.toList()));
      }
    }

    saveMeta(username, url);
  }

  /**
   * Saves database meta to the file.
   * @param username corresponding interpreter username, see interpreter properties for details.
   * @param url corresponding interpreter url, see interpreter properties for details.
   * @throws UnsupportedEncodingException if failed to create path.
   */
  private void saveMeta(@Nonnull final String username, @Nonnull final String url)
      throws UnsupportedEncodingException {
    final File fileToStore = new File(buildMetaPath(username, url));
    try (final FileWriter file = new FileWriter(fileToStore.getAbsolutePath())) {
      file.write(new GsonBuilder().setPrettyPrinting().create().toJson(database));
      file.flush();
    } catch (final IOException e) {
      //skip
    }
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
    if (!isOpened) {
      return completions;
    }
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
        break;
      }
      completions.add(new InterpreterCompletion(match, match, type, ""));
    }
  }
}