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

import java.io.File;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;
import ru.tinkoff.zeppelin.SystemEvent.ET;
import ru.tinkoff.zeppelin.core.configuration.interpreter.ModuleConfiguration;
import ru.tinkoff.zeppelin.core.configuration.interpreter.ModuleInnerConfiguration;
import ru.tinkoff.zeppelin.core.notebook.Job;
import ru.tinkoff.zeppelin.core.notebook.Note;
import ru.tinkoff.zeppelin.engine.Configuration;
import ru.tinkoff.zeppelin.engine.server.AbstractRemoteProcess;
import ru.tinkoff.zeppelin.engine.server.InterpreterRemoteProcess;
import ru.tinkoff.zeppelin.interpreter.InterpreterResult;
import ru.tinkoff.zeppelin.interpreter.InterpreterResult.Code;
import ru.tinkoff.zeppelin.interpreter.InterpreterResult.Message;
import ru.tinkoff.zeppelin.interpreter.InterpreterResult.Message.Type;
import ru.tinkoff.zeppelin.interpreter.thrift.PushResult;
import ru.tinkoff.zeppelin.storage.FullParagraphDAO;
import ru.tinkoff.zeppelin.storage.JobBatchDAO;
import ru.tinkoff.zeppelin.storage.JobDAO;
import ru.tinkoff.zeppelin.storage.JobPayloadDAO;
import ru.tinkoff.zeppelin.storage.JobResultDAO;
import ru.tinkoff.zeppelin.storage.NoteDAO;
import ru.tinkoff.zeppelin.storage.ParagraphDAO;
import ru.tinkoff.zeppelin.storage.ZLog;

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
  public void handle(final Job job,
                     final AbstractRemoteProcess process,
                     final ModuleConfiguration config,
                     final ModuleInnerConfiguration innerConfig) {
    if (!userIsInterpreterOwner(job, config)) {
      String errorMessage = String.format(
              "User [%s] does not have access to [%s] interpreter.",
              job.getUsername(),
              config.getHumanReadableName()
      );

      ZLog.log(
              ET.ACCESS_ERROR,
              errorMessage,
              String.format("Job: interpreter access error, job[%s]", job.toString()),
              job.getUsername()
      );
      InterpreterResult interpreterResult = new InterpreterResult(
              Code.ABORTED,
              new Message(Type.TEXT, errorMessage)
      );
      setAbortResult(job, jobBatchDAO.get(job.getBatchId()), interpreterResult);
      return;
    }

    // prepare payload
    final String payload = jobPayloadDAO.getByJobId(job.getId()).getPayload();

    // prepare notecontext
    final Note note = noteDAO.get(job.getNoteId());
    final Map<String, String> noteContext = new HashMap<>();

    noteContext.put("Z_ENV_NOTE_ID", String.valueOf(job.getNoteId()));
    noteContext.put("Z_ENV_NOTE_UUID", String.valueOf(note.getUuid()));
    noteContext.put("Z_ENV_PARAGRAPH_ID", String.valueOf(job.getParagraphId()));

    noteContext.put("Z_ENV_MARKER_PREFIX", Configuration.getInstanceMarkerPrefix());

    noteContext.put("Z_ENV_NOTE_STORAGE_DIR", new File(Configuration.getNoteContextStorageDir(), note.getUuid()).getAbsolutePath());

    // prepare usercontext
    final Map<String, String> userContext = new HashMap<>();
    userContext.put("Z_ENV_USER_NAME", job.getUsername());
    userContext.put("Z_ENV_USER_ROLES", job.getRoles().toString());

    // prepare configuration

    final Map<String, String> configuration = new HashMap<>();
    innerConfig.getProperties()
            .forEach((p, v) -> configuration.put(p, String.valueOf(v.getCurrentValue())));

    ZLog.log(ET.JOB_READY_FOR_EXECUTION, "Job ready for execution, id=" + job.getId(),
            String.format("Job ready for execution, job[%s]", job.toString()), job.getUsername());
    final PushResult result = ((InterpreterRemoteProcess) process).push(payload, noteContext, userContext, configuration);
    if (result == null) {
      ZLog.log(ET.JOB_REQUEST_IS_EMPTY, "Push result is empty for job with id=" + job.getId(),
              String.format("Push result is empty, job[%s]", job.toString()), job.getUsername());
      return;
    }

    switch (result.getStatus()) {
      case ACCEPT:
        ZLog.log(ET.JOB_ACCEPTED, String.format("Job accepted, id=%s", job.getId()),
                String.format("Job accepted, job[%s]", job.toString()), job.getUsername());
        setRunningState(job, result.getInterpreterProcessUUID(), result.getInterpreterJobUUID());
        break;
      case DECLINE:
        ZLog.log(ET.JOB_DECLINED, String.format("Job declined, id=%s", job.getId()),
                String.format("Job declined, job[%s]", job.toString()), job.getUsername());
        break;
      case ERROR:
        ZLog.log(ET.JOB_REQUEST_ERRORED, String.format("Job errored, id=%s", job.getId()),
                String.format("Job errored, job[%s]", job.toString()), job.getUsername());
        break;
      default:
        ZLog.log(ET.JOB_UNDEFINED,
                String.format("System error, job request status undefined, id=%s, status=%s", job.getId(), result.getStatus()),
                String.format("System error, job request status undefined, job[%s], status=%s", job.toString(), result.getStatus()),
                job.getUsername());
    }
  }

  private boolean userIsInterpreterOwner(Job job, ModuleConfiguration option) {
    if (!option.getPermissions().isEnabled()) {
      return true;
    }
    List<String> owners = option.getPermissions().getOwners();
    if (owners.isEmpty()) {
      return true;
    }
    if (owners.contains(job.getUsername())) {
      return true;
    }
    for (String role : job.getRoles()) {
      if (owners.contains(role)) {
        return true;
      }
    }
    return false;
  }
}
