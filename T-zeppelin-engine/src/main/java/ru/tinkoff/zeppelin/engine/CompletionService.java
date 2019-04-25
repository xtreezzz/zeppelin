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

import org.apache.commons.lang3.StringUtils;
import org.springframework.context.annotation.DependsOn;
import org.springframework.stereotype.Component;
import ru.tinkoff.zeppelin.core.configuration.interpreter.InterpreterArtifactSource;
import ru.tinkoff.zeppelin.core.configuration.interpreter.InterpreterOption;
import ru.tinkoff.zeppelin.core.notebook.Note;
import ru.tinkoff.zeppelin.core.notebook.Paragraph;
import ru.tinkoff.zeppelin.engine.forms.FormsProcessor;
import ru.tinkoff.zeppelin.engine.server.AbstractRemoteProcess;
import ru.tinkoff.zeppelin.engine.server.CompleterRemoteProcess;
import ru.tinkoff.zeppelin.engine.server.RemoteProcessStarter;
import ru.tinkoff.zeppelin.engine.server.RemoteProcessType;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

@DependsOn({"configuration", "thriftBootstrap"})
@Component
public class CompletionService {

  private final InterpreterSettingService interpreterSettingService;
  private final ThriftServerBootstrap serverBootstrap;

  public CompletionService(final InterpreterSettingService interpreterSettingService,
                           final ThriftServerBootstrap serverBootstrap) {
    this.interpreterSettingService = interpreterSettingService;
    this.serverBootstrap = serverBootstrap;
  }

  public String complete(final Note note,
                       final Paragraph paragraph,
                       final String payload,
                       final int cursorPosition,
                       final String user,
                       final Set<String> roles) {
    try {
      final InterpreterOption option = interpreterSettingService.getOption(paragraph.getShebang());
      final AbstractRemoteProcess process = AbstractRemoteProcess.get(paragraph.getShebang(), RemoteProcessType.COMPLETER);
      if (process != null
              && process.getStatus() == AbstractRemoteProcess.Status.READY
              && option != null) {

        final FormsProcessor.InjectResponse response
                = FormsProcessor.injectFormValues(payload, cursorPosition, paragraph.getFormParams());

        // prepare notecontext
        final Map<String, String> noteContext = new HashMap<>();

        noteContext.put("Z_ENV_NOTE_ID", String.valueOf(paragraph.getNoteId()));
        noteContext.put("Z_ENV_PARAGRAPH_ID", String.valueOf(paragraph.getId()));

        // prepare usercontext
        final Map<String, String> userContext = new HashMap<>();
        userContext.put("Z_ENV_USER_NAME", user);
        userContext.put("Z_ENV_USER_ROLES", roles.toString());

        // prepare configuration
        final Map<String, String> configuration = new HashMap<>();
        option.getConfig()
                .getProperties()
                .forEach((p, v) -> configuration.put(p, String.valueOf(v.getCurrentValue())));

        final String result = ((CompleterRemoteProcess)process).complete(
                response.getPayload(),
                response.getCursorPosition(),
                noteContext,
                userContext,
                configuration);

        return result;

      } else if (process != null
              && process.getStatus() == AbstractRemoteProcess.Status.STARTING
              && option != null) {
        final String str = ";";
      } else {
        final String shebang = ";";

        final InterpreterArtifactSource source = option != null
                ? interpreterSettingService.getSource(option.getInterpreterName())
                : null;

        if (option == null || !option.isEnabled()
                || source == null || source.getStatus() != InterpreterArtifactSource.Status.INSTALLED) {
          return StringUtils.EMPTY;
        }

        AbstractRemoteProcess.starting(shebang, RemoteProcessType.COMPLETER);
        try {
          RemoteProcessStarter.start(
                  shebang,
                  RemoteProcessType.COMPLETER,
                  source.getPath(),
                  option.getConfig().getClassName(),
                  serverBootstrap.getServer().getRemoteServerClassPath(),
                  serverBootstrap.getServer().getAddr(),
                  serverBootstrap.getServer().getPort(),
                  option.getJvmOptions(),
                  Configuration.getInstanceMarkerPrefix());
        } catch (final Exception e) {
          AbstractRemoteProcess.remove(shebang, RemoteProcessType.COMPLETER);
        }
      }
    } catch (final Exception e) {
      //log this
    }
    return StringUtils.EMPTY;
  }
}
