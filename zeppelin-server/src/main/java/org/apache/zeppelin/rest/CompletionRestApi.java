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
package org.apache.zeppelin.rest;

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
import org.apache.zeppelin.storage.InterpreterOptionRepository;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;
import ru.tinkoff.zeppelin.core.configuration.interpreter.InterpreterOption;
import ru.tinkoff.zeppelin.core.notebook.Note;
import ru.tinkoff.zeppelin.core.notebook.Paragraph;
import ru.tinkoff.zeppelin.engine.NoteService;

@RestController
@RequestMapping("/api/completion")
public class CompletionRestApi {

  private final InterpreterOptionRepository interpreterRepo;

  private final NoteService noteService;

  @Autowired
  public CompletionRestApi(final InterpreterOptionRepository interpreterRepo,
                           final NoteService noteService) {
    this.interpreterRepo = interpreterRepo;
    this.noteService = noteService;
  }

  @GetMapping(value = "/{noteId}/{paragraphId}/{buf}/{cursor}", produces = "application/json")
  public ResponseEntity completion(@PathVariable("noteId") final String noteId,
                                   @PathVariable("paragraphId") final String paragraphId,
                                   @PathVariable("buf") final String buf,
                                   @PathVariable("cursor") final String cursor) {
    final List<InterpreterCompletion> completions = new ArrayList<>();

    final Note note = noteService.getNote(noteId);
    final Optional<Paragraph> p = noteService.getParapraphs(note).stream()
        .filter(e -> e.getUuid().equals(paragraphId)).findAny();

    final int cur = (int) Double.parseDouble(cursor);

    if (p.isPresent()) {
      final Optional<InterpreterOption> option = interpreterRepo.getAllOptions().stream()
          .filter(o -> o.getShebang().equals(p.get().getShebang()))
          .findAny();

      if (option.isPresent()) {
        if (option.get().getConfig().getGroup().equals("jdbc")) {
          completions.addAll(JDBCCompleter.complete(buf, cur));
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
        return null;
      }
    } else {
      //LOGIC ERROR
      return null;
    }
    return new JsonResponse<>(HttpStatus.OK, "", completions).build();
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
      return completions.stream().map(c -> new InterpreterCompletion(c, c, "keyword", "")).collect(
          Collectors.toList());
    }
  }
}
