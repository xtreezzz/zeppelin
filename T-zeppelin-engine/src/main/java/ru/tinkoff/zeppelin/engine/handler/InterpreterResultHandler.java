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
import org.apache.zeppelin.storage.ZLog.ET;
import org.springframework.context.ApplicationContext;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;
import ru.tinkoff.zeppelin.core.notebook.Job;
import ru.tinkoff.zeppelin.core.notebook.JobBatch;
import ru.tinkoff.zeppelin.interpreter.InterpreterResult;
import ru.tinkoff.zeppelin.interpreter.PredefinedInterpreterResults;

import javax.annotation.PostConstruct;

/**
 * Class for handle intepreter results
 *
 * @author Andrey Koshkin
 * @version 1.0
 * @since 1.0
 */
@Component
public class InterpreterResultHandler extends AbstractHandler {

  private ApplicationContext applicationContext;

  private static InterpreterResultHandler instance;

  public static InterpreterResultHandler getInstance() {
    return instance;
  }

  public InterpreterResultHandler(final JobBatchDAO jobBatchDAO,
                                  final JobDAO jobDAO,
                                  final JobResultDAO jobResultDAO,
                                  final JobPayloadDAO jobPayloadDAO,
                                  final NoteDAO noteDAO,
                                  final ParagraphDAO paragraphDAO,
                                  final FullParagraphDAO fullParagraphDAO,
                                  final ApplicationContext applicationContext) {
    super(jobBatchDAO, jobDAO, jobResultDAO, jobPayloadDAO, noteDAO, paragraphDAO, fullParagraphDAO);
    this.applicationContext = applicationContext;
  }

  @PostConstruct
  private void init() {
    instance = applicationContext.getBean(InterpreterResultHandler.class);
  }

  @Transactional(propagation = Propagation.REQUIRES_NEW)
  public void handle(final String interpreterJobUUID,
                     final InterpreterResult interpreterResult) {

    Job job = null;
    // задержка на закрытие транзакций
    for (int i = 0; i < 2 * 10 * 60 ; i++ ) {
      job = jobDAO.getByInterpreterJobUUID(interpreterJobUUID);
      if (job != null) {
        break;
      }
      try {
        Thread.sleep(100);
      } catch (final Exception e) {
        // SKIp
      }
    }

    if (job == null) {
      ZLog.log(ET.JOB_NOT_FOUND, "Job not found by uuid=" + interpreterJobUUID,
          "Job not found by uuid=" + interpreterJobUUID,"Unknown");
      return;
    }

    final JobBatch batch = jobBatchDAO.get(job.getBatchId());
    ZLog.log(ET.GOT_JOB,
        String.format("Got result [batch status=%s, batch id=%s] for job: id=%s, noteId=%s, paragraphId=%s",
            batch.getStatus(), batch.getId(), job.getId(), job.getNoteId(), job.getParagraphId()),
        String.format("Got result [batch status=%s, batch id=%s] for job: id=%s, noteId=%s, paragraphId=%s",
            batch.getStatus(), batch.getId(), job.getId(), job.getNoteId(), job.getParagraphId()),
        "Unknown");

    if (batch.getStatus() == JobBatch.Status.ABORTING || batch.getStatus() == JobBatch.Status.ABORTED) {
      ZLog.log(ET.GOT_ABORTED_JOB,
          String.format("Batch status is %s", batch.getStatus()),
          String.format("Incorrect interpreter behaviour during handling job abort, process for existing job not found: job[%s]", job.toString()),
          "Unknown");
      setAbortResult(job, batch, interpreterResult);
      return;
    }

    if(interpreterResult == null) {
      ZLog.log(ET.INTERPRETER_RESULT_NOT_FOUND,
          String.format("Handler got null result, interpreterJobUUID=%s", interpreterJobUUID),
          String.format("Handler got null result, interpreterJobUUID=%s", interpreterJobUUID),
          "Unknown");

      setErrorResult(job, batch, PredefinedInterpreterResults.ERROR_WHILE_INTERPRET);
      return;
    }

    switch (interpreterResult.code()) {
      case SUCCESS:
        ZLog.log(ET.SUCCESSFUL_RESULT,
            String.format("Got successful result for job with id=%s", interpreterJobUUID),
            String.format("Got successful result for job with id=%s", interpreterJobUUID),
            "Unknown");

        setSuccessResult(job, batch, interpreterResult);
        break;
      case ABORTED:
        ZLog.log(ET.ABORTED_RESULT,
            String.format("Got aborted result for job with id=%s", interpreterJobUUID),
            String.format("Got aborted result for job with id=%s", interpreterJobUUID),
            "Unknown");
        setAbortResult(job, batch, interpreterResult);
        break;
      case ERROR:
        ZLog.log(ET.ERRORED_RESULT,
            String.format("Got errored result for job with id=%s", interpreterJobUUID),
            String.format("Got errored result for job with id=%s", interpreterJobUUID),
            "Unknown");
        setErrorResult(job, batch, interpreterResult);
        break;
    }
  }
}
