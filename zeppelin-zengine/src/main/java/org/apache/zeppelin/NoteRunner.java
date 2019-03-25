package org.apache.zeppelin;

import java.time.LocalDateTime;
import org.apache.zeppelin.notebook.Job;
import org.apache.zeppelin.notebook.JobBatch;
import org.apache.zeppelin.notebook.JobPayload;
import org.apache.zeppelin.notebook.Note;
import org.apache.zeppelin.notebook.Paragraph;
import org.apache.zeppelin.storage.JobBatchDAO;
import org.apache.zeppelin.storage.JobDAO;
import org.apache.zeppelin.storage.JobPayloadDAO;
import org.apache.zeppelin.storage.JobResultDAO;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

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
            job.setShebang(p.getShebang());
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
            jobPayload.setPayload(p.getText());
            jobPayloadDAO.persist(jobPayload);

            p.setJobId(job.getId());
        }
        saved.setStatus(JobBatch.Status.PENDING);
        jobBatchDAO.update(saved);
    }

    public void runParagraph(final Note note, final Paragraph p) {
        int idx = -1;
        for (int i = 0; i < note.getParagraphs().size(); ++i) {
            if (p.equals(note.getParagraphs().get(i))) {
                idx = i;
            }
        }

        if (idx != -1) {
            final JobBatch batch = new JobBatch();
            batch.setId(0L);
            batch.setNoteId(note.getDatabaseId());
            batch.setStatus(JobBatch.Status.SAVING);
            batch.setCreatedAt(LocalDateTime.now());
            batch.setStartedAt(null);
            batch.setEndedAt(null);
            final JobBatch saved = jobBatchDAO.persist(batch);

            final Job job = new Job();
            job.setId(0L);
            job.setBatchId(saved.getId());
            job.setNoteId(note.getDatabaseId());
            job.setParagpaphId(p.getDatabaseId());
            job.setIndex(idx);
            job.setShebang(p.getShebang());
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
            jobPayload.setPayload(p.getText());
            jobPayloadDAO.persist(jobPayload);

            p.setJobId(job.getId());

            saved.setStatus(JobBatch.Status.PENDING);
            jobBatchDAO.update(saved);
        } else {
            throw new IllegalArgumentException(String.format("Paragraph %s doesn't exist", p));
        }
    }
}
