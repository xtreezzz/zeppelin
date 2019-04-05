package org.apache.zeppelin.websocket.dto;

import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.zeppelin.NoteService;
import org.apache.zeppelin.notebook.Job;
import org.apache.zeppelin.notebook.JobResult;
import org.apache.zeppelin.notebook.Note;
import org.apache.zeppelin.notebook.Paragraph;
import org.apache.zeppelin.storage.JobBatchDAO;
import org.apache.zeppelin.storage.JobDAO;
import org.apache.zeppelin.storage.JobPayloadDAO;
import org.apache.zeppelin.storage.JobResultDAO;
import org.springframework.stereotype.Component;


@Component
public class NoteDTOConverter {

    private final JobBatchDAO jobBatchDAO;
    private final JobDAO jobDAO;
    private final JobPayloadDAO jobPayloadDAO;
    private final JobResultDAO jobResultDAO;
    private final NoteService noteService;


    public NoteDTOConverter(JobBatchDAO jobBatchDAO,
                            JobDAO jobDAO,
                            JobPayloadDAO jobPayloadDAO,
                            JobResultDAO jobResultDAO,
                            NoteService noteService) {
        this.jobBatchDAO = jobBatchDAO;
        this.jobDAO = jobDAO;
        this.jobPayloadDAO = jobPayloadDAO;
        this.jobResultDAO = jobResultDAO;
        this.noteService = noteService;
    }

    public NoteDTO convertNoteToDTO(final Note note) {
        final NoteDTO noteDTO = new NoteDTO();
        noteDTO.setDatabaseId(note.getId());
        noteDTO.setId(note.getUuid());
        noteDTO.setName(note.getName());
        noteDTO.setPath(note.getPath());
        noteDTO.setRevision(note.getRevision());
        noteDTO.setGuiConfiguration(note.getGuiConfiguration());
        for (final Paragraph p : noteService.getParapraphs(note)) {
            noteDTO.getParagraphs().add(convertParagraphToDTO(p));
        }
        noteDTO.setRunning(note.isRunning());
        noteDTO.setScheduler(note.getScheduler());
        noteDTO.getConfig().put("looknfeel", false);

        return noteDTO;
    }


    public ParagraphDTO convertParagraphToDTO(final Paragraph paragraph) {
        final ParagraphDTO paragraphDTO = new ParagraphDTO();
        paragraphDTO.setDatabaseId(paragraph.getId());
        paragraphDTO.setId(paragraph.getUuid());
        paragraphDTO.setTitle(paragraph.getTitle());
        paragraphDTO.setText(paragraph.getText());
        paragraphDTO.setShebang(paragraph.getShebang());
        paragraphDTO.setCreated(paragraph.getCreated());
        paragraphDTO.setUpdated(paragraph.getUpdated());
        paragraphDTO.setConfig(paragraph.getConfig());
        paragraphDTO.setSettings(paragraph.getSettings());

        if(paragraph.getJobId() != null) {
            final Job job = jobDAO.get(paragraph.getJobId());
            paragraphDTO.setStatus(job.getStatus());
            final List<JobResult> jobResults = jobResultDAO.getByJobId(job.getId());

            final InterpreterResultDTO interpreterResultDTO = new InterpreterResultDTO();
            interpreterResultDTO.setCode(job.getStatus().name());
            for (final JobResult jobResult : jobResults) {
                interpreterResultDTO.getMsg().add(new InterpreterResultDTO.Message(jobResult.getType(), jobResult.getResult()));
            }
            paragraphDTO.setResults(interpreterResultDTO);
        } else {
            paragraphDTO.setStatus(Job.Status.DONE);
        }
        return paragraphDTO;
    }

}


