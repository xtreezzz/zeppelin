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

package org.apache.zeppelin.websocket.handler;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.TreeSet;
import java.util.stream.Collectors;
import javax.annotation.Nonnull;
import net.sf.jsqlparser.JSQLParserException;
import net.sf.jsqlparser.parser.CCJSqlParserUtil;
import net.sf.jsqlparser.util.deparser.ExpressionDeParser;
import net.sf.jsqlparser.util.deparser.SelectDeParser;
import net.sf.jsqlparser.util.deparser.StatementDeParser;
import org.apache.zeppelin.realm.AuthenticationInfo;
import org.apache.zeppelin.realm.AuthorizationService;
import org.apache.zeppelin.storage.InterpreterOptionRepository;
import org.apache.zeppelin.websocket.ConnectionManager;
import org.apache.zeppelin.websocket.Operation;
import org.apache.zeppelin.websocket.SockMessage;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import org.springframework.web.socket.WebSocketSession;
import ru.tinkoff.zeppelin.core.configuration.interpreter.InterpreterOption;
import ru.tinkoff.zeppelin.core.notebook.Note;
import ru.tinkoff.zeppelin.core.notebook.Paragraph;
import ru.tinkoff.zeppelin.engine.NoteService;

@Component
public class CompletionHandler extends AbstractHandler {

  private final InterpreterOptionRepository interpreterRepo;

  @Autowired
  public CompletionHandler(final ConnectionManager connectionManager,
                           final NoteService noteService,
                           final InterpreterOptionRepository interpreterRepo) {
    super(connectionManager, noteService);
    this.interpreterRepo = interpreterRepo;
  }


  public void completion(final WebSocketSession conn, final SockMessage fromMessage) {
    final AuthenticationInfo authenticationInfo = AuthorizationService.getAuthenticationInfo();
    final List<InterpreterCompletion> completions = new ArrayList<>();

    final Note note = safeLoadNote("noteId", fromMessage, Permission.WRITER, authenticationInfo, conn);
    final Paragraph p = safeLoadParagraph("paragraphId", fromMessage, note);
    final String buf = fromMessage.getNotNull("buf");
    final int cursor = (int) Double.parseDouble(fromMessage.getNotNull("cursor").toString());

    if (cursor < 0) {
      return;
    }

    final Optional<InterpreterOption> option = interpreterRepo.getAllOptions().stream()
        .filter(o -> o.getShebang().equals(p.getShebang()))
        .findFirst();

    if (option.isPresent()) {
      if (option.get().getConfig().getGroup().equals("jdbc")) {
        completions.addAll(JDBCCompleter.complete(buf, cursor));
      } else {
        completions.add(
            new InterpreterCompletion(
                "warning",
                "",
                "keyword",
                "Completion for this interpreter is not supported yet."
            )
        );
      }
    } else {
      //LOGIC ERROR
      return;
    }
    connectionManager.broadcast(note.getId(),
        new SockMessage(Operation.COMPLETION_LIST)
            .put("id", p.getId())
            .put("completions", completions));
  }


  private static class InterpreterCompletion {

    @Nonnull
    private final String name;

    @Nonnull
    private final String value;

    @Nonnull
    private final String meta;

    @Nonnull
    private final String description;

    InterpreterCompletion(@Nonnull final String name,
        @Nonnull final String value,
        @Nonnull final String meta,
        @Nonnull final String description) {
      this.name = name;
      this.value = value;
      this.meta = meta;
      this.description = description;
    }
  }

  private static class JDBCCompleter {

    static List<InterpreterCompletion> complete(@Nonnull final String buffer, final int pos) {
      final Set<String> completions = new TreeSet<>();

      final StringBuilder myBuf = new StringBuilder();
      final SelectDeParser selectDeparser = new SelectDeParser();
      selectDeparser.setBuffer(myBuf);
      final ExpressionDeParser expressionDeParser =
          new ExpressionDeParser(selectDeparser, new StringBuilder());
      final StatementDeParser statementDeParser =
          new StatementDeParser(expressionDeParser, selectDeparser, new StringBuilder());

      try {
        String statement = buffer.substring(0, pos);
        final List<String> statements = Arrays.asList(statement.split(";"));
        statement = statements.get(statements.size() - 1);

        final net.sf.jsqlparser.statement.Statement parseExpression = CCJSqlParserUtil.parse(statement);
        parseExpression.accept(statementDeParser);
        completions.addAll(
            Arrays.asList("from", "where", "select", "join", "left", "right", "inner", "outer", "on",
                "and", "or")
        );
      } catch (final JSQLParserException e) {
        if (e.getCause().toString().contains("Was expecting one of:")) {
          List<String> expected = Arrays.asList(e.getCause().toString()
              .substring(e.getCause().toString().indexOf("Was expecting one of:")).split("\n"));
          expected = expected.subList(1, expected.size());
          for (String expectedValue : expected) {
            expectedValue =
                expectedValue.trim().replace("\"", "").toLowerCase();
            if (!expectedValue.startsWith("<") && !expectedValue.startsWith("{")) {
              completions.add(expectedValue);
            }
          }
        }
        if (completions.contains("*")) {
          completions.add("from");
        }
      }
      return completions.stream().map(c -> new InterpreterCompletion(c, c, "keyword", "")).collect(Collectors.toList());
    }
  }

}
