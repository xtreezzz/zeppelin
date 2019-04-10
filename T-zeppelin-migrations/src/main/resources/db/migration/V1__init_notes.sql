--
-- Licensed to the Apache Software Foundation (ASF) under one or more
-- contributor license agreements.  See the NOTICE file distributed with
-- this work for additional information regarding copyright ownership.
-- The ASF licenses this file to You under the Apache License, Version 2.0
-- (the "License"); you may not use this file except in compliance with
-- the License.  You may obtain a copy of the License at
--
--    http://www.apache.org/licenses/LICENSE-2.0
--
-- Unless required by applicable law or agreed to in writing, software
-- distributed under the License is distributed on an "AS IS" BASIS,
-- WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
-- See the License for the specific language governing permissions and
-- limitations under the License.
--

 CREATE TABLE NOTES
(
  ID           BIGSERIAL PRIMARY KEY,
  UUID         VARCHAR(9)    NOT NULL UNIQUE,
  PATH         VARCHAR(1024) NOT NULL,
  PERMISSIONS  JSON          NOT NULL,
  GUI          JSON          NOT NULL,
  JOB_BATCH_ID BIGINT        NULL
);

CREATE TABLE REVISIONS
(
  ID      BIGSERIAL PRIMARY KEY,
  NOTE_ID BIGINT REFERENCES NOTES (ID) ON DELETE CASCADE,
  MESSAGE VARCHAR(128) NOT NULL,
  DATE    TIMESTAMP    NOT NULL
);

CREATE TABLE PARAGRAPHS
(
  ID          BIGSERIAL PRIMARY KEY,
  NOTE_ID     BIGINT REFERENCES NOTES (ID) ON DELETE CASCADE,
  UUID        VARCHAR(40)  NOT NULL,
  TITLE       VARCHAR(256) NOT NULL,
  TEXT        TEXT         NOT NULL,
  SHEBANG     VARCHAR(128),
  CREATED     TIMESTAMP    NOT NULL,
  UPDATED     TIMESTAMP    NOT NULL,
  POSITION    INTEGER      NOT NULL,
  JOB_ID      BIGINT,
  CONFIG      JSON         NOT NULL,
  GUI         JSON         NOT NULL,
  REVISION_ID BIGINT REFERENCES REVISIONS (ID)
);

CREATE TABLE JOB_BATCH
(
  ID         BIGSERIAL PRIMARY KEY,
  NOTE_ID    BIGINT      NOT NULL REFERENCES NOTES (ID) ON DELETE CASCADE,
  STATUS     VARCHAR(50) NOT NULL,
  CREATED_AT TIMESTAMP   NOT NULL,
  STARTED_AT TIMESTAMP,
  ENDED_AT   TIMESTAMP
);

ALTER TABLE NOTES
  ADD FOREIGN KEY (JOB_BATCH_ID) REFERENCES JOB_BATCH (ID);

CREATE TABLE JOB
(
  ID                       BIGSERIAL PRIMARY KEY,
  BATCH_ID                 BIGINT REFERENCES JOB_BATCH (ID) ON DELETE CASCADE,
  NOTE_ID                  BIGINT REFERENCES NOTES (ID) ON DELETE CASCADE,
  PARAGRAPH_ID             BIGINT REFERENCES PARAGRAPHS (ID) ON DELETE CASCADE,
  INDEX_NUMBER             INTEGER      NOT NULL,
  SHEBANG                  VARCHAR(100) NOT NULL,
  STATUS                   VARCHAR(50)  NOT NULL,
  INTERPRETER_PROCESS_UUID VARCHAR(200),
  INTERPRETER_JOB_UUID     VARCHAR(200),
  CREATED_AT               TIMESTAMP,
  STARTED_AT               TIMESTAMP,
  ENDED_AT                 TIMESTAMP
);

ALTER TABLE PARAGRAPHS
  ADD FOREIGN KEY (JOB_ID) REFERENCES JOB (ID);

CREATE TABLE JOB_PAYLOAD
(
  ID      BIGSERIAL PRIMARY KEY,
  JOB_ID  BIGINT REFERENCES JOB (ID) ON DELETE CASCADE,
  PAYLOAD TEXT
);


CREATE TABLE JOB_RESULT
(
  ID         BIGSERIAL PRIMARY KEY,
  JOB_ID     BIGINT REFERENCES JOB (ID) ON DELETE CASCADE,
  CREATED_AT TIMESTAMP,
  TYPE       VARCHAR(50),
  RESULT     TEXT
);

CREATE TABLE SCHEDULER
(
  ID             BIGSERIAL PRIMARY KEY,
  NOTE_ID        BIGINT UNIQUE REFERENCES NOTES (ID) ON DELETE CASCADE,
  ENABLED        VARCHAR(10)  NOT NULL DEFAULT 'FALSE',
  EXPRESSION     VARCHAR(100) NOT NULL,
  USER_NAME      VARCHAR(200) NOT NULL,
  USER_ROLES     VARCHAR(2048),
  LAST_EXECUTION TIMESTAMP    NOT NULL,
  NEXT_EXECUTION TIMESTAMP    NOT NULL
);
