package org.apache.zeppelin.externalDTO;

import org.apache.zeppelin.notebook.display.GUI;

import java.time.LocalDateTime;
import java.util.Map;

public class ParagraphDTO {


    private long jobId;

    private long databaseId;
    private String id;
    private String title;
    private String text;
    private String user;
    private String shebang;
    private LocalDateTime created;
    private LocalDateTime updated;
    private String status;
    private int position;

    //TODO(SAN) вернул config :)
    //paragraph configs like isOpen, colWidth, etc
    private Map<String, Object> config;

    // form and parameter settings
    private GUI settings;

    private InterpreterResultDTO results;

    public long getJobId() {
        return jobId;
    }

    public void setJobId(final long jobId) {
        this.jobId = jobId;
    }

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

    public String getTitle() {
        return title;
    }

    public void setTitle(String title) {
        this.title = title;
    }

    public String getText() {
        return text;
    }

    public void setText(String text) {
        this.text = text;
    }

    public String getUser() {
        return user;
    }

    public void setUser(String user) {
        this.user = user;
    }

    public String getShebang() {
        return shebang;
    }

    public void setShebang(String shebang) {
        this.shebang = shebang;
    }

    public LocalDateTime getCreated() {
        return created;
    }

    public void setCreated(LocalDateTime created) {
        this.created = created;
    }

    public LocalDateTime getUpdated() {
        return updated;
    }

    public void setUpdated(LocalDateTime updated) {
        this.updated = updated;
    }

    public Map<String, Object> getConfig() {
        return config;
    }

    public void setConfig(Map<String, Object> config) {
        this.config = config;
    }

    public GUI getSettings() {
        return settings;
    }

    public void setSettings(GUI settings) {
        this.settings = settings;
    }

    public InterpreterResultDTO getResults() {
        return results;
    }

    public void setResults(InterpreterResultDTO results) {
        this.results = results;
    }

    public String getStatus() {
        return status;
    }

    public void setStatus(final String status) {
        this.status = status;
    }

    public int getPosition() {
        return position;
    }

    public void setPosition(final int position) {
        this.position = position;
    }
}
