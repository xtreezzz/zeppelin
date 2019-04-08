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
package ru.tinkoff.zeppelin.engine.handler;

import org.apache.zeppelin.storage.*;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;
import ru.tinkoff.zeppelin.core.configuration.interpreter.InterpreterArtifactSource;
import ru.tinkoff.zeppelin.core.configuration.interpreter.InterpreterOption;
import ru.tinkoff.zeppelin.core.notebook.Job;
import ru.tinkoff.zeppelin.core.notebook.JobBatch;
import ru.tinkoff.zeppelin.engine.server.InterpreterProcess;
import ru.tinkoff.zeppelin.engine.server.InterpreterProcessStarter;
import ru.tinkoff.zeppelin.interpreter.PredefinedInterpreterResults;

/**
 * Class for starting interpreter process
 *
 * @author Andrey Koshkin
 * @version 1.0
 * @since 1.0
 */
@Component
public class InterpreterStarterHandler extends AbstractHandler {

  public InterpreterStarterHandler(final JobBatchDAO jobBatchDAO,
                                   final JobDAO jobDAO,
                                   final JobResultDAO jobResultDAO,
                                   final JobPayloadDAO jobPayloadDAO,
                                   final NoteDAO noteDAO,
                                   final ParagraphDAO paragraphDAO,
                                   final FullParagraphDAO fullParagraphDAO) {
    super(jobBatchDAO, jobDAO, jobResultDAO, jobPayloadDAO, noteDAO, paragraphDAO, fullParagraphDAO);
  }


  @Transactional(propagation = Propagation.REQUIRES_NEW)
  public void handle(final Job job,
                     final InterpreterOption option,
                     final InterpreterArtifactSource source,
                     final String remoteServerClassPath,
                     final String thriftAddr,
                     final int thriftPort) {

    final JobBatch batch = jobBatchDAO.get(job.getBatchId());
    if (option == null) {
      setErrorResult(job, batch, PredefinedInterpreterResults.INTERPRETER_NOT_FOUND);
      return;
    }

    if (!option.isEnabled()) {
      setErrorResult(job, batch, PredefinedInterpreterResults.INTERPRETER_DISABLED);
    }

    if (source == null) {
      setErrorResult(job, batch, PredefinedInterpreterResults.INTERPRETER_NOT_FOUND);
      return;
    }

    if (source.getStatus() != InterpreterArtifactSource.Status.INSTALLED) {
      setErrorResult(job, batch, PredefinedInterpreterResults.INTERPRETER_NOT_FOUND);
      return;
    }
    InterpreterProcess.starting(job.getShebang());

    InterpreterProcessStarter.start(job.getShebang(),
            source.getPath(),
            option.getConfig().getClassName(),
            remoteServerClassPath,
            thriftAddr,
            thriftPort);
  }

}
