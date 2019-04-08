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
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;
import ru.tinkoff.zeppelin.core.configuration.interpreter.InterpreterOption;
import ru.tinkoff.zeppelin.core.notebook.Job;
import ru.tinkoff.zeppelin.engine.server.InterpreterProcess;
import ru.tinkoff.zeppelin.interpreter.thrift.PushResult;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Class for handle pending jobs
 *
 * @author Andrey Koshkin
 * @version 1.0
 * @since 1.0
 */
@Component
public class PendingHandler extends AbstractHandler {


  public PendingHandler(final JobBatchDAO jobBatchDAO,
                        final JobDAO jobDAO,
                        final JobResultDAO jobResultDAO,
                        final JobPayloadDAO jobPayloadDAO,
                        final NoteDAO noteDAO,
                        final ParagraphDAO paragraphDAO,
                        final FullParagraphDAO fullParagraphDAO) {
    super(jobBatchDAO, jobDAO, jobResultDAO, jobPayloadDAO, noteDAO, paragraphDAO, fullParagraphDAO);
  }

  public List<Job> loadJobs() {
    return jobDAO.loadNextPending();
  }

  @Transactional(propagation = Propagation.REQUIRES_NEW)
  public void handle(final Job job, final InterpreterProcess process, final InterpreterOption option) {

    // prepare payload
    final String payload = jobPayloadDAO.getByJobId(job.getId()).getPayload();

    // prepare notecontext
    final Map<String, String> noteContext = new HashMap<>();
    noteContext.put("noteId", String.valueOf(job.getNoteId()));
    noteContext.put("paragraphId", String.valueOf(job.getParagpaphId()));

    // prepare usercontext
    final Map<String, String> userContext = new HashMap<>();

    // prepare configuration
    final Map<String, String> configuration = new HashMap<>();
    option.getConfig()
            .getProperties()
            .forEach((p, v) -> configuration.put(p, String.valueOf(v.getCurrentValue())));

    ZLog.log(ET.JOB_READY_FOR_EXECUTION, "Job ready for execution, id=" + job.getId(),
        String.format("Job ready for execution, job[%s]", job.toString()), "Unknown");
    final PushResult result = process.push(payload, noteContext, userContext, configuration);
    if(result == null) {
      ZLog.log(ET.JOB_REQUEST_IS_EMPTY, "Push result is empty for job with id=" + job.getId(),
          String.format("Push result is empty, job[%s]", job.toString()), "Unknown");
      return;
    }

    switch (result.getStatus()) {
      case ACCEPT:
        ZLog.log(ET.JOB_ACCEPTED, String.format("Job accepted, id=%s", job.getId()),
            String.format("Job accepted, job[%s]", job.toString()), "Unknown");
        setRunningState(job, result.getInterpreterProcessUUID(), result.getInterpreterJobUUID());
        break;
      case DECLINE:
        ZLog.log(ET.JOB_DECLINED, String.format("Job declined, id=%s", job.getId()),
            String.format("Job declined, job[%s]", job.toString()), "Unknown");
        break;
      case ERROR:
        ZLog.log(ET.JOB_REQUEST_ERRORED, String.format("Job errored, id=%s", job.getId()),
            String.format("Job errored, job[%s]", job.toString()), "Unknown");
        break;
      default:
        ZLog.log(ET.JOB_UNDEFINED,
            String.format("System error, job request status undefined, id=%s, status=%s", job.getId(), result.getStatus()),
            String.format("System error, job request status undefined, job[%s], status=%s", job.toString(), result.getStatus()),
            "Unknown");
    }
  }
}
