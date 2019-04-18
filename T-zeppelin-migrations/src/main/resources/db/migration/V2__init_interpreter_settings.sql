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

-- Represents InterpreterArtifactSource.Status
CREATE DOMAIN INTERPRETER_ARTIFACT_SOURCE_STATUS AS VARCHAR(13) DEFAULT 'NOT_INSTALLED' NOT NULL
CHECK(VALUE IN ('NOT_INSTALLED', 'INSTALLED', 'IN_PROGRESS'));

-- Represents InterpreterArtifactSource
CREATE TABLE INTERPRETER_ARTIFACT_SOURCE
(
    id                   BIGSERIAL     PRIMARY KEY,
    interpreter_name     VARCHAR(255)  NOT NULL UNIQUE,
    artifact             VARCHAR(255)  NOT NULL,
    --TODO(egorklimov ): CHECK (artifact ~* {REGEXP})
    status               interpreter_artifact_source_status,
    "path"               VARCHAR(1024) UNIQUE,
    reinstall_on_start   BOOLEAN       NOT NULL DEFAULT FALSE
);

-- Represents BaseInterpreterConfig
CREATE TABLE BASE_INTERPRETER_CONFIG
(
    id                BIGSERIAL     PRIMARY KEY,
    "group"           VARCHAR(100)  REFERENCES INTERPRETER_ARTIFACT_SOURCE(interpreter_name) ON DELETE CASCADE ON UPDATE CASCADE,
    "name"            VARCHAR(100)  NOT NULL,
    class_name        TEXT          NOT NULL,
    properties        JSON          NOT NULL,
    editor            JSON          NOT NULL
);

-- Represents InterpreterOption.ProcessType
CREATE DOMAIN INTERPRETER_PROCESS_TYPE AS VARCHAR(10) DEFAULT 'SHARED' NOT NULL
CHECK(VALUE IN ('SHARED', 'SCOPED', 'ISOLATED'));

-- Represents InterpreterOption
CREATE TABLE INTERPRETER_OPTION
(
    id                       BIGSERIAL      PRIMARY KEY,
    shebang                  VARCHAR(100)   NOT NULL UNIQUE CHECK (shebang ~* '%([\w\.]+)(\(.*?\))?'),
    custom_interpreter_name  VARCHAR(255)   NOT NULL,
    interpreter_name         VARCHAR(255)   NOT NULL,
    per_note                 interpreter_process_type,
    per_user                 interpreter_process_type,
    jvm_options              VARCHAR(255)   NOT NULL DEFAULT '',
    concurrent_tasks         SMALLINT       NOT NULL DEFAULT 1 CHECK (concurrent_tasks > 0),
    config_id                BIGINT         REFERENCES BASE_INTERPRETER_CONFIG (id) ON DELETE CASCADE ON UPDATE CASCADE,
    remote_process           JSON           NOT NULL DEFAULT '{"host": "", "port": -1, "isEnabled": false}'::json,
    permissions              JSON           NOT NULL DEFAULT '{"isEnabled": false, "owners": []}'::json,
    is_enabled               BOOLEAN        NOT NULL DEFAULT FALSE
);

-- Represents org.apache.zeppelin.Repository
CREATE TABLE REPOSITORY
(
    id               BIGSERIAL    PRIMARY KEY,
    repository_id    VARCHAR(100) NOT NULL UNIQUE,
    snapshot         BOOLEAN      NOT NULL DEFAULT FALSE,
    url              VARCHAR(255) NOT NULL,
    --TODO(egorklimov): CHECK (url ~* '(?:(?:https?):\/\/|www\.|ftp\.)(?:\([-A-Z0-9+&@#\/%=~_|$?!:,.]*\)|[-A-Z0-9+&@#\/%=~_|$?!:,.])*(?:\([-A-Z0-9+&@#\/%=~_|$?!:,.]*\)|[A-Z0-9+&@#\/%=~_|$])'),
    username         VARCHAR(255),
    password         VARCHAR(255),
    proxy_protocol   VARCHAR      DEFAULT 'HTTP'
    CHECK (proxy_protocol IN ('HTTP', 'HTTPS')),
    proxy_host       VARCHAR(255),
    proxy_port       VARCHAR(255),
    proxy_login      VARCHAR(255),
    proxy_password   VARCHAR(255)
);
