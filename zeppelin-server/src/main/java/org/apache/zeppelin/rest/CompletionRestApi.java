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

import com.google.common.reflect.TypeToken;
import com.google.gson.Gson;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.apache.zeppelin.InterpreterCompletion;
import org.apache.zeppelin.JDBCCompleter;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;
import ru.tinkoff.zeppelin.core.configuration.interpreter.InterpreterOption;
import ru.tinkoff.zeppelin.core.configuration.interpreter.InterpreterProperty;
import ru.tinkoff.zeppelin.core.notebook.Note;
import ru.tinkoff.zeppelin.core.notebook.Paragraph;
import ru.tinkoff.zeppelin.engine.InterpreterSettingService;
import ru.tinkoff.zeppelin.engine.NoteService;

@RestController
@RequestMapping("/api/completion")
public class CompletionRestApi {

  private final InterpreterSettingService interpreterRepo;

  private final NoteService noteService;

  @Autowired
  public CompletionRestApi(final InterpreterSettingService interpreterRepo,
                           final NoteService noteService) {
    this.interpreterRepo = interpreterRepo;
    this.noteService = noteService;
  }

  @PostMapping(value = "/{noteId}/{paragraphId}", produces = "application/json")
  public ResponseEntity completion(@PathVariable("noteId") final String noteId,
                                   @PathVariable("paragraphId") final String paragraphId,
                                   @RequestBody final String message) {
    final List<InterpreterCompletion> completions = new ArrayList<>();

    final Note note = noteService.getNote(noteId);
    final Optional<Paragraph> p = noteService.getParagraphs(note).stream()
        .filter(e -> e.getUuid().equals(paragraphId)).findAny();

    final Map<String, String> params = new Gson().fromJson(message,
        new TypeToken<HashMap<String, String>>() {}.getType());

    final String buf = params.get("buf");
    final int cur = (int) Double.parseDouble(params.get("cursor"));

    if (p.isPresent()) {
      final Optional<InterpreterOption> option = interpreterRepo.getAllOptions().stream()
          .filter(o -> o.getShebang().equals(p.get().getShebang()))
          .findAny();

      if (option.isPresent()) {
        if (option.get().getConfig().getGroup().equals("jdbc")) {
          final Map<String, InterpreterProperty> properties = option.get().getConfig().getProperties();
          final JDBCCompleter completer =
              new JDBCCompleter(
                  (String) properties.get("connection.url").getCurrentValue(),
                  (String) properties.get("connection.user").getCurrentValue(),
                  (String) properties.get("connection.password").getCurrentValue(),
                  (String) properties.get("driver.className").getCurrentValue()
              );
          completions.addAll(completer.complete(buf, cur));
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
        return new JsonResponse<>(HttpStatus.INTERNAL_SERVER_ERROR, "Interpreter not found").build();
      }
    } else {
      //LOGIC ERROR
      return new JsonResponse<>(HttpStatus.INTERNAL_SERVER_ERROR, "Paragraph not found").build();
    }
    return new JsonResponse<>(HttpStatus.OK, "", completions).build();
  }
}
