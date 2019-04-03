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

-- Represents org.apache.zeppelin.LoggerType
CREATE TABLE SYSTEM_EVENT_TYPE
(
  ID    BIGSERIAL     PRIMARY KEY,
  NAME  VARCHAR(255)  NOT NULL UNIQUE
);

INSERT INTO SYSTEM_EVENT_TYPE(name)
VALUES ('UI_EVENT'), ('INTERPRETER_PROCESS_NOT_FOUND'),
       ('JOB_CANCEL_FAILED'), ('JOB_CANCEL_ACCEPTED'),
       ('JOB_CANCEL_NOT_FOUND'), ('JOB_CANCEL_ERRORED'),
       ('JOB_SUBMITTED_FOR_EXECUTION'), ('GOT_JOB'),
       ('GOT_ABORTED_JOB'), ('INTERPRETER_INSTALL'),
       ('INTERPRETER_ALREADY_INSTALLED'), ('INTERPRETER_SUCCESSFULLY_INSTALLED'),
       ('INTERPRETER_INSTALLATION_FAILED'), ('INTERPRETER_SUCCESSFULLY_UNINSTALLED'),
       ('INTERPRETER_DELETION_FAILED'), ('INTERPRETER_CONFIGURATION_REQUESTED'),
       ('INTERPRETER_CONFIGURAION_FOUND'), ('INTERPRETER_CONFIGURATION_PROCESSING_FAILED'),
       ('PROCESS_STARTED'), ('REMOTE_CONNECTION_REGISTERED'),
       ('BAD_REMOTE_CONNECTION'), ('COMPLETED_PROCESS_NOT_FOUND'),
       ('PROCESS_COMPLETED'), ('CONNECTION_FAILED'),
       ('FAILED_TO_RELEASE_CONNECTION'), ('FORCE_KILL_REQUESTED'),
       ('PUSH_FAILED_CLIENT_NOT_FOUND'), ('PUSH_FAILED'),
       ('PING_FAILED_CLIENT_NOT_FOUND'), ('PING_FAILED'),
       ('FORCE_KILL_FAILED_CLIENT_NOT_FOUND'), ('FORCE_KILL_FAILED'),
       ('INTERPRETER_EVENT_SERVER_STARTING'), ('INTERPRETER_EVENT_SERVER_START_FAILED'),
       ('INTERPRETER_EVENT_SERVER_STARTED'), ('INTERPRETER_EVENT_SERVER_STOPPED'),
       ('INTERPRETER_PROCESS_START_REQUESTED'), ('INTERPRETER_PROCESS_FINISHED'),
       ('INTERPRETER_PROCESS_FAILED'), ('JOB_READY_FOR_EXECUTION'),
       ('JOB_REQUEST_IS_EMPTY'), ('JOB_ACCEPTED'),
       ('JOB_DECLINED'), ('JOB_REQUEST_ERRORED'),
       ('JOB_UNDEFINED'), ('JOB_READY_FOR_EXECUTION_BY_SCHEDULER'),
       ('SCHEDULED_JOB_ERRORED'), ('JOB_SCHEDULED'),
       ('JOB_NOT_FOUND'), ('INTERPRETER_RESULT_NOT_FOUND'),
       ('SUCCESSFUL_RESULT'), ('ABORTED_RESULT'),
       ('ERRORED_RESULT');


-- Represents org.apache.zeppelin.SystemEvent
CREATE TABLE SYSTEM_EVENT
(
  ID            BIGSERIAL    PRIMARY KEY,
  USERNAME      VARCHAR(255) NOT NULL,
  EVENT_TYPE    BIGINT       REFERENCES SYSTEM_EVENT_TYPE (ID) ON UPDATE CASCADE,
  MESSAGE       TEXT         NOT NULL,
  DESCRIPTION   TEXT,
  ACTION_TIME   TIMESTAMP    NOT NULL
);
