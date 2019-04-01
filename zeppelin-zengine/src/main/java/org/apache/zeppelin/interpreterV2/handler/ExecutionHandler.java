package org.apache.zeppelin.interpreterV2.handler;

import org.apache.zeppelin.notebook.Note;
import org.apache.zeppelin.notebook.Paragraph;
import org.apache.zeppelin.storage.*;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;

import java.util.List;

@Component
public class ExecutionHandler extends AbstractHandler{

  public ExecutionHandler(final JobBatchDAO jobBatchDAO,
                          final JobDAO jobDAO,
                          final JobResultDAO jobResultDAO,
                          final JobPayloadDAO jobPayloadDAO,
                          final NotebookDAO notebookDAO) {
    super(jobBatchDAO, jobDAO, jobResultDAO, jobPayloadDAO, notebookDAO);
  }

  @Transactional(propagation = Propagation.REQUIRES_NEW)
  public void run(final Note note, final List<Paragraph> paragraphs) {
    publishBatch(note, paragraphs);
  }
}
