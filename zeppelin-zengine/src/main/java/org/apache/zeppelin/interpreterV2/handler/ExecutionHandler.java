package org.apache.zeppelin.interpreterV2.handler;

import java.util.List;
import org.apache.zeppelin.notebook.Note;
import org.apache.zeppelin.notebook.Paragraph;
import org.apache.zeppelin.storage.FullParagraphDAO;
import org.apache.zeppelin.storage.JobBatchDAO;
import org.apache.zeppelin.storage.JobDAO;
import org.apache.zeppelin.storage.JobPayloadDAO;
import org.apache.zeppelin.storage.JobResultDAO;
import org.apache.zeppelin.storage.NoteDAO;
import org.apache.zeppelin.storage.ParagraphDAO;
import org.apache.zeppelin.storage.ZLog;
import org.apache.zeppelin.storage.ZLog.ET;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;

@Component
public class ExecutionHandler extends AbstractHandler{

  public ExecutionHandler(final JobBatchDAO jobBatchDAO,
                          final JobDAO jobDAO,
                          final JobResultDAO jobResultDAO,
                          final JobPayloadDAO jobPayloadDAO,
                          final NoteDAO noteDAO,
                          final ParagraphDAO paragraphDAO,
                          final FullParagraphDAO fullParagraphDAO) {
    super(jobBatchDAO, jobDAO, jobResultDAO, jobPayloadDAO, noteDAO, paragraphDAO, fullParagraphDAO);
  }

  @Transactional(propagation = Propagation.REQUIRES_NEW)
  public void run(final Note note, final List<Paragraph> paragraphs, final String username, final List<String> roles) {
    ZLog.log(ET.JOB_SUBMITTED_FOR_EXECUTION,
        String.format("Note[id=%s] with paragraphs[%s] submitted for execution by user[name=%s;roles=%s]",
            note.getId(), paragraphs.toString(), username, roles.toString()),
        String.format("Batch for note[id=%s] with paragraphs[%s] published by user[name=%s;roles=%s]",
            note.getId(), paragraphs.toString(), username, roles.toString()),
        username);
    publishBatch(note, paragraphs);
  }
}
