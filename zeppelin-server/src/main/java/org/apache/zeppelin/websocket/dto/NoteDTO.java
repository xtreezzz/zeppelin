package org.apache.zeppelin.websocket.dto;

import org.apache.zeppelin.notebook.NoteCronConfiguration;
import org.apache.zeppelin.notebook.NoteRevision;
import org.apache.zeppelin.notebook.Paragraph;
import org.apache.zeppelin.notebook.display.GUI;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class NoteDTO {

    private long databaseId;
    private String id;
    private String name;
    private String path;
    private NoteRevision revision;

    /**
     * form and parameter guiConfiguration
     * see https://github.com/apache/zeppelin/pull/2641
     */
    private  GUI guiConfiguration;

    private List<ParagraphDTO> paragraphs = new ArrayList<>();

    private boolean isRunning = false;

    private NoteCronConfiguration noteCronConfiguration;

    private Map<String, Object> config = new HashMap<>();

    public long getDatabaseId() {
        return databaseId;
    }

    public void setDatabaseId(long databaseId) {
        this.databaseId = databaseId;
    }

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getPath() {
        return path;
    }

    public void setPath(String path) {
        this.path = path;
    }

    public NoteRevision getRevision() {
        return revision;
    }

    public void setRevision(NoteRevision revision) {
        this.revision = revision;
    }

    public GUI getGuiConfiguration() {
        return guiConfiguration;
    }

    public void setGuiConfiguration(GUI guiConfiguration) {
        this.guiConfiguration = guiConfiguration;
    }

    public List<ParagraphDTO> getParagraphs() {
        return paragraphs;
    }

    public void setParagraphs(List<ParagraphDTO> paragraphs) {
        this.paragraphs = paragraphs;
    }

    public boolean isRunning() {
        return isRunning;
    }

    public void setRunning(boolean running) {
        isRunning = running;
    }

    public NoteCronConfiguration getNoteCronConfiguration() {
        return noteCronConfiguration;
    }

    public void setNoteCronConfiguration(NoteCronConfiguration noteCronConfiguration) {
        this.noteCronConfiguration = noteCronConfiguration;
    }

    public Map<String, Object> getConfig() {
        return config;
    }

    public void setConfig(Map<String, Object> config) {
        this.config = config;
    }
}
