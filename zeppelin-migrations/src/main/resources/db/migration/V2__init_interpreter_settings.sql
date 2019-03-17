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

-- Represents org.apache.zeppelin.interpreter.configuration.InterpreterArtifactSource.Status
create DOMAIN interpreter_artifact_source_status AS VARCHAR(13) DEFAULT 'not installed' NOT NULL
CHECK(VALUE IN ('not installed', 'installed', 'in progress'));

-- Represents org.apache.zeppelin.interpreter.configuration.InterpreterArtifactSource
create TABLE InterpreterArtifactSource
(
    id               BIGSERIAL     PRIMARY KEY,
    interpreter_name VARCHAR(255)  NOT NULL UNIQUE,
    artifact         VARCHAR(255)  NOT NULL,
    --TODO(egorklimov): CHECK (artifact ~* {REGEXP})
    status           interpreter_artifact_source_status,
    "path"           VARCHAR(1024) UNIQUE
);

-- Represents org.apache.zeppelin.interpreter.configuration.BaseInterpreterConfig
create TABLE BaseInterpreterConfig
(
    id              BIGSERIAL      PRIMARY KEY,
    "group"         VARCHAR(100)   REFERENCES InterpreterArtifactSource (interpreter_name) ON delete CASCADE,
    "name"          VARCHAR(100)   NOT NULL,
    class_name      TEXT           NOT NULL,
    properties      JSON           NOT NULL,
    editor          JSON           NOT NULL
);

-- Represents org.apache.zeppelin.interpreter.configuration.InterpreterOption.ProcessType
create DOMAIN interpreter_process_type AS VARCHAR(10) DEFAULT 'shared' NOT NULL
CHECK(VALUE IN ('shared', 'scoped', 'isolated'));

-- Represents org.apache.zeppelin.interpreter.configuration.InterpreterOption
create TABLE InterpreterOption
(
    id                       BIGSERIAL      PRIMARY KEY,
    shebang                  VARCHAR(100)   NOT NULL UNIQUE CHECK (shebang ~* '%([\w\.]+)(\(.*?\))?'),
    custom_interpreter_name  VARCHAR(255)   NOT NULL,
    interpreter_name         VARCHAR(255)   NOT NULL,
    per_note                 interpreter_process_type,
    per_user                 interpreter_process_type,
    jvm_options              VARCHAR(255)   NOT NULL DEFAULT '',
    concurrent_tasks         INTEGER        NOT NULL DEFAULT 1,
    config_id                INTEGER        REFERENCES BaseInterpreterConfig (id) ON delete CASCADE,
    remote_process           JSON           NOT NULL DEFAULT '{"host": "", "port": -1, "isEnabled": false}'::json,
    permissions              JSON           NOT NULL DEFAULT '{"isEnabled": false, "owners": []}'::json,
    is_enabled               BOOLEAN        NOT NULL DEFAULT FALSE
);

-- Represents org.apache.zeppelin.Repository
create TABLE repository
(
    id               BIGSERIAL    PRIMARY KEY,
    repository_id    VARCHAR(100) NOT NULL UNIQUE,
    snapshot         BOOLEAN      NOT NULL DEFAULT FALSE,
    url              VARCHAR(255) NOT NULL,
    --TODO(egorklimov): CHECK (url ~* '(?:(?:https?):\/\/|www\.|ftp\.)(?:\([-A-Z0-9+&@#\/%=~_|$?!:,.]*\)|[-A-Z0-9+&@#\/%=~_|$?!:,.])*(?:\([-A-Z0-9+&@#\/%=~_|$?!:,.]*\)|[A-Z0-9+&@#\/%=~_|$])'),
    username         VARCHAR(255) NOT NULL,
    password         VARCHAR(255) NOT NULL,
    proxy_protocol   VARCHAR      DEFAULT 'HTTP'
    CHECK (proxy_protocol IN ('HTTP', 'HTTPS')),
    proxy_host       VARCHAR(255),
    proxy_port       VARCHAR(255),
    proxy_login      VARCHAR(255),
    proxy_password   VARCHAR(255)
);
