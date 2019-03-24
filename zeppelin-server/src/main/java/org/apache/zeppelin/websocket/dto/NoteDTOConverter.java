package org.apache.zeppelin.websocket.dto;

import org.apache.zeppelin.notebook.Job;
import org.apache.zeppelin.notebook.JobResult;
import org.apache.zeppelin.notebook.Note;
import org.apache.zeppelin.notebook.Paragraph;
import org.apache.zeppelin.storage.JobBatchDAO;
import org.apache.zeppelin.storage.JobDAO;
import org.apache.zeppelin.storage.JobPayloadDAO;
import org.apache.zeppelin.storage.JobResultDAO;
import org.springframework.stereotype.Component;

import java.util.List;


@Component
public class NoteDTOConverter {

    private final JobBatchDAO jobBatchDAO;
    private final JobDAO jobDAO;
    private final JobPayloadDAO jobPayloadDAO;
    private final JobResultDAO jobResultDAO;


    public NoteDTOConverter(JobBatchDAO jobBatchDAO, JobDAO jobDAO, JobPayloadDAO jobPayloadDAO, JobResultDAO jobResultDAO) {
        this.jobBatchDAO = jobBatchDAO;
        this.jobDAO = jobDAO;
        this.jobPayloadDAO = jobPayloadDAO;
        this.jobResultDAO = jobResultDAO;
    }

    public NoteDTO convertNoteToDTO(final Note note) {
        final NoteDTO noteDTO = new NoteDTO();
        noteDTO.setDatabaseId(note.getDatabaseId());
        noteDTO.setId(note.getNoteId());
        noteDTO.setName(note.getName());
        noteDTO.setPath(note.getPath());
        noteDTO.setRevision(note.getRevision());
        noteDTO.setGuiConfiguration(note.getGuiConfiguration());
        for (final Paragraph p : note.getParagraphs()) {
            noteDTO.getParagraphs().add(convertParagraphToDTO(p));
        }
        noteDTO.setRunning(note.isRunning());
        noteDTO.setNoteCronConfiguration(note.getNoteCronConfiguration());
        noteDTO.getConfig().put("looknfeel", false);

        return noteDTO;
    }


    public ParagraphDTO convertParagraphToDTO(final Paragraph paragraph) {
        final ParagraphDTO paragraphDTO = new ParagraphDTO();
        paragraphDTO.setDatabaseId(paragraph.getDatabaseId());
        paragraphDTO.setId(paragraph.getId());
        paragraphDTO.setTitle(paragraph.getTitle());
        paragraphDTO.setText(paragraph.getText());
        paragraphDTO.setUser(paragraph.getUser());
        paragraphDTO.setShebang(paragraph.getShebang());
        paragraphDTO.setCreated(paragraph.getCreated());
        paragraphDTO.setUpdated(paragraph.getUpdated());
        paragraphDTO.setConfig(paragraph.getConfig());
        paragraphDTO.setSettings(paragraph.getSettings());

        if(paragraph.getJobId() != null) {
            final Job job = jobDAO.get(paragraph.getJobId());
            final List<JobResult> jobResults = jobResultDAO.getByJobId(job.getId());

            final InterpreterResultDTO interpreterResultDTO = new InterpreterResultDTO();
            interpreterResultDTO.setCode(job.getStatus().name());
            for (final JobResult jobResult : jobResults) {
                interpreterResultDTO.getMsg().add(new InterpreterResultDTO.Message(jobResult.getType(), jobResult.getResult()));
            }
            paragraphDTO.setResults(interpreterResultDTO);
        }
        return paragraphDTO;
    }

}


