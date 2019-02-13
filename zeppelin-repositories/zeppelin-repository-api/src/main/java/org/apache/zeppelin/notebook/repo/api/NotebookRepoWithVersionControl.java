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

package org.apache.zeppelin.notebook.repo.api;

import org.apache.zeppelin.annotation.ZeppelinApi;
import org.apache.zeppelin.notebook.Note;

import java.io.IOException;
import java.util.List;

/**
 * Notebook repository (persistence layer) abstraction
 */
public interface NotebookRepoWithVersionControl extends NotebookRepo {

  /**
   * chekpoint (set revision) for notebook.
   *
   * @param noteId        Id of the note
   * @param noteName      name of the note
   * @param checkpointMsg message description of the checkpoint
   * @return Rev
   * @throws IOException
   */
  @ZeppelinApi
  Revision checkpoint(String noteId,
                      String noteName,
                      String checkpointMsg) throws IOException;

  /**
   * Get particular revision of the Notebook.
   *
   * @param noteId   Id of the note
   * @param noteName name of the note
   * @param revId    revision of the Notebook
   * @return a Notebook
   * @throws IOException
   */
  @ZeppelinApi
  Note get(String noteId, String noteName, String revId) throws IOException;

  /**
   * List of revisions of the given Notebook.
   *
   * @param noteId   id of the note
   * @param noteName name of the note
   * @return list of revisions
   */
  @ZeppelinApi
  List<Revision> revisionHistory(String noteId, String noteName) throws IOException;

  /**
   * Set note to particular revision.
   *
   * @param noteId   Id of the Notebook
   * @param noteName name of the note
   * @param revId    revision of the Notebook
   * @return a Notebook
   * @throws IOException
   */
  @ZeppelinApi
  Note setNoteRevision(String noteId,
                       String noteName,
                       String revId) throws IOException;

}
