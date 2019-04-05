package org.apache.zeppelin.websocket.dto;

import java.util.List;

import org.apache.zeppelin.NoteService;
import org.apache.zeppelin.externalDTO.InterpreterResultDTO;
import org.apache.zeppelin.externalDTO.NoteDTO;
import org.apache.zeppelin.externalDTO.ParagraphDTO;
import org.apache.zeppelin.notebook.Job;
import org.apache.zeppelin.notebook.JobResult;
import org.apache.zeppelin.notebook.Note;
import org.apache.zeppelin.notebook.Paragraph;
import org.apache.zeppelin.storage.*;
import org.springframework.stereotype.Component;


@Component
public class NoteDTOConverter {

    private final JobBatchDAO jobBatchDAO;
    private final JobDAO jobDAO;
    private final JobPayloadDAO jobPayloadDAO;
    private final JobResultDAO jobResultDAO;
    private final NoteService noteService;

    private final FullParagraphDAO fullParagraphDAO;

    public NoteDTOConverter(JobBatchDAO jobBatchDAO,
                            JobDAO jobDAO,
                            JobPayloadDAO jobPayloadDAO,
                            JobResultDAO jobResultDAO,
                            NoteService noteService,
                            FullParagraphDAO fullParagraphDAO) {
        this.jobBatchDAO = jobBatchDAO;
        this.jobDAO = jobDAO;
        this.jobPayloadDAO = jobPayloadDAO;
        this.jobResultDAO = jobResultDAO;
        this.noteService = noteService;
        this.fullParagraphDAO = fullParagraphDAO;
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
            noteDTO.getParagraphs().add(fullParagraphDAO.getById(p.getId()));
        }
        noteDTO.setRunning(note.isRunning());
        noteDTO.setScheduler(note.getScheduler());
        noteDTO.getConfig().put("looknfeel", false);

        return noteDTO;
    }
}


