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

-- Represents ru.tinkoff.zeppelin.core.configuration.interpreter.ModuleSource
CREATE TABLE MODULE_SOURCE
(
  ID                 BIGSERIAL PRIMARY KEY,
  NAME               VARCHAR(255) NOT NULL UNIQUE,
  TYPE               VARCHAR(255) NOT NULL,
  ARTIFACT           VARCHAR(255) NOT NULL,
  STATUS             VARCHAR(255) NOT NULL,
  PATH               VARCHAR(1024) UNIQUE,
  REINSTALL_ON_START BOOLEAN      NOT NULL DEFAULT FALSE
);

-- Represents ru.tinkoff.zeppelin.core.configuration.interpreter.ModuleInnerConfiguration
CREATE TABLE MODULE_INNER_CONFIGURATION
(
  ID         BIGSERIAL PRIMARY KEY,
  CLASS_NAME TEXT NOT NULL,
  PROPERTIES JSON NOT NULL,
  EDITOR     JSON NOT NULL
);


-- Represents InterpreterOption
CREATE TABLE MODULE_CONFIGURATION
(
  ID                  BIGSERIAL PRIMARY KEY,
  SHEBANG             VARCHAR(100)  NOT NULL UNIQUE CHECK (SHEBANG ~* '^\w+$'),
  HUMAN_READABLE_NAME VARCHAR(255)  NOT NULL CHECK (HUMAN_READABLE_NAME <> ''),
  BINDED_TO           VARCHAR(100)  NULL,
  JVM_OPTIONS         VARCHAR(1024) NOT NULL DEFAULT '',
  CONCURRENT_TASKS    SMALLINT      NOT NULL DEFAULT 1 CHECK (CONCURRENT_TASKS > 0),
  CONFIG_ID           BIGINT REFERENCES MODULE_INNER_CONFIGURATION (ID) ON DELETE CASCADE ON UPDATE CASCADE NOT NULL,
  SOURCE_ID           BIGINT REFERENCES MODULE_SOURCE (ID) NOT NULL,
  PERMISSIONS         JSON          NOT NULL DEFAULT '{"isEnabled": false, "owners": []}'::JSON,
  IS_ENABLED          BOOLEAN       NOT NULL DEFAULT FALSE
);

-- Represents org.apache.zeppelin.Repository
CREATE TABLE MODULE_REPOSITORY
(
  ID             BIGSERIAL    PRIMARY KEY,
  REPOSITORY_ID  VARCHAR(100) NOT NULL UNIQUE CHECK (REPOSITORY_ID ~* '^\w+$'),
  SNAPSHOT       BOOLEAN      NOT NULL DEFAULT FALSE,
  URL            VARCHAR(255) NOT NULL,
  USERNAME       VARCHAR(255),
  PASSWORD       VARCHAR(255),
  PROXY_PROTOCOL VARCHAR      DEFAULT 'HTTP'
    CHECK (PROXY_PROTOCOL IN ('HTTP', 'HTTPS')),
  PROXY_HOST     VARCHAR(255),
  PROXY_PORT     VARCHAR(255),
  PROXY_LOGIN    VARCHAR(255),
  PROXY_PASSWORD VARCHAR(255)
);
