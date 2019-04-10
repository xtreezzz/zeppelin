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

import org.apache.zeppelin.notebook.display.GUI;
import ru.tinkoff.zeppelin.core.notebook.NoteRevision;
import ru.tinkoff.zeppelin.core.notebook.Scheduler;

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

    private Scheduler scheduler;

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

    public Scheduler getScheduler() {
        return scheduler;
    }

    public void setScheduler(Scheduler scheduler) {
        this.scheduler = scheduler;
    }

    public Map<String, Object> getConfig() {
        return config;
    }

    public void setConfig(Map<String, Object> config) {
        this.config = config;
    }
}