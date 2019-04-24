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

package ru.tinkoff.zeppelin.engine;

import org.springframework.stereotype.Component;
import ru.tinkoff.zeppelin.core.configuration.interpreter.InterpreterOption;
import ru.tinkoff.zeppelin.core.notebook.Note;
import ru.tinkoff.zeppelin.core.notebook.Paragraph;
import ru.tinkoff.zeppelin.engine.forms.FormsProcessor;
import ru.tinkoff.zeppelin.engine.server.AbstractRemoteProcess;
import ru.tinkoff.zeppelin.engine.server.RemoteProcessType;

@Component
public class CompletionService {

  private final InterpreterSettingService interpreterSettingService;

  public CompletionService(final InterpreterSettingService interpreterSettingService) {
    this.interpreterSettingService = interpreterSettingService;
  }

  public void complete(final Note note,
                       final Paragraph paragraph,
                       final String payload,
                       final int cursorPosition) {
    try {
      final InterpreterOption option = interpreterSettingService.getOption(paragraph.getShebang());
      final AbstractRemoteProcess process = AbstractRemoteProcess.get(paragraph.getShebang(), RemoteProcessType.INTERPRETER);
      if (process != null
              && process.getStatus() == AbstractRemoteProcess.Status.READY
              && option != null) {

        final FormsProcessor.InjectResponse response
                = FormsProcessor.injectFormValues(payload, cursorPosition, paragraph.getFormParams());

        final String ste = ";";

      } else if (process != null
              && process.getStatus() == AbstractRemoteProcess.Status.STARTING
              && option != null) {
        final String str = ";";
      }
    } catch (final Exception e) {
      //log this
    }
  }
}
