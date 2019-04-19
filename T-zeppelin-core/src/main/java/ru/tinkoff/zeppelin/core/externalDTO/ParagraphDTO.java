/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package ru.tinkoff.zeppelin.core.externalDTO;

import java.time.LocalDateTime;
import java.util.Map;
import java.util.Objects;

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

    private Map<String, Object> formParams;

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

    public Map<String, Object> getFormParams() {
        return formParams;
    }

    public void setFormParams(Map<String, Object> formParams) {
        this.formParams = formParams;
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

    @Override
    public boolean equals(final Object o) {
        if (this == o) return true;
        if (!(o instanceof ParagraphDTO)) return false;
        final ParagraphDTO that = (ParagraphDTO) o;
        return jobId == that.jobId &&
                databaseId == that.databaseId &&
                position == that.position &&
                Objects.equals(id, that.id) &&
                Objects.equals(title, that.title) &&
                Objects.equals(text, that.text) &&
                Objects.equals(user, that.user) &&
                Objects.equals(shebang, that.shebang) &&
                Objects.equals(created, that.created) &&
                Objects.equals(updated, that.updated) &&
                Objects.equals(status, that.status) &&
                Objects.equals(config, that.config) &&
                Objects.equals(formParams, that.formParams) &&
                Objects.equals(results, that.results);
    }

    @Override
    public int hashCode() {
        return Objects.hash(jobId,
                databaseId,
                id,
                title,
                text,
                user,
                shebang,
                created,
                updated,
                status,
                position,
                config,
                formParams,
                results);
    }
}
