package org.apache.zeppelin;

import org.apache.zeppelin.notebook.*;
import org.apache.zeppelin.storage.JobBatchDAO;
import org.apache.zeppelin.storage.JobDAO;
import org.apache.zeppelin.storage.JobPayloadDAO;
import org.apache.zeppelin.storage.JobResultDAO;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.time.LocalDateTime;

@Component
public class NoteRunner {


    private final JobBatchDAO jobBatchDAO;
    private final JobDAO jobDAO;
    private final JobPayloadDAO jobPayloadDAO;
    private final JobResultDAO jobResultDAO;

    @Autowired
    public NoteRunner(JobBatchDAO jobBatchDAO, JobDAO jobDAO, JobPayloadDAO jobPayloadDAO, JobResultDAO jobResultDAO) {
        this.jobBatchDAO = jobBatchDAO;
        this.jobDAO = jobDAO;
        this.jobPayloadDAO = jobPayloadDAO;
        this.jobResultDAO = jobResultDAO;
    }

    public void run(final Note note) {
        final JobBatch batch = new JobBatch();
        batch.setId(0L);
        batch.setNoteId(note.getDatabaseId());
        batch.setStatus(JobBatch.Status.SAVING);
        batch.setCreatedAt(LocalDateTime.now());
        batch.setStartedAt(null);
        batch.setEndedAt(null);
        final JobBatch saved = jobBatchDAO.persist(batch);

        for (int i = 0; i < note.getParagraphs().size(); i++) {
            final Paragraph p = note.getParagraphs().get(i);

            final Job job = new Job();
            job.setId(0L);
            job.setBatchId(saved.getId());
            job.setNoteId(note.getDatabaseId());
            job.setParagpaphId(p.getDatabaseId());
            job.setIndex(i);
            job.setShebang(getShebang(p));
            job.setStatus(Job.Status.PENDING);
            job.setInterpreterProcessUUID(null);
            job.setInterpreterJobUUID(null);
            job.setCreatedAt(LocalDateTime.now());
            job.setStartedAt(null);
            job.setEndedAt(null);
            jobDAO.persist(job);

            final JobPayload jobPayload = new JobPayload();
            jobPayload.setId(0L);
            jobPayload.setJobId(job.getId());
            jobPayload.setPayload(getPayload(p));
            jobPayloadDAO.persist(jobPayload);

            p.setJobId(job.getId());
        }
        saved.setStatus(JobBatch.Status.PENDING);
        jobBatchDAO.update(saved);
    }




    private String getShebang(final Paragraph p) {
        return "%md";
    }

    private String getPayload(final Paragraph p) {
        return "PAYLOAD";
    }

}
