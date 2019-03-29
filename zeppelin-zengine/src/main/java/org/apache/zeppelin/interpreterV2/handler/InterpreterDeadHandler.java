package org.apache.zeppelin.interpreterV2.handler;

import org.apache.zeppelin.storage.JobBatchDAO;
import org.apache.zeppelin.storage.JobDAO;
import org.apache.zeppelin.storage.JobResultDAO;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;

import java.util.List;

@Component
public class InterpreterDeadHandler extends AbstractHandler {

  public InterpreterDeadHandler(final JobBatchDAO jobBatchDAO,
                                final JobDAO jobDAO,
                                final JobResultDAO jobResultDAO) {
    super(jobBatchDAO, jobDAO, jobResultDAO);
  }

  @Transactional(propagation = Propagation.REQUIRES_NEW)
  public void handle(final List<String> liveInterpreters) {
    //jobDAO.restoreState();
  }

}
