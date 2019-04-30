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
  SHEBANG             VARCHAR(100)  NOT NULL UNIQUE,
  HUMAN_READABLE_NAME VARCHAR(255)  NOT NULL,
  BINDED_TO           VARCHAR(100)  NULL,
  JVM_OPTIONS         VARCHAR(1024) NOT NULL DEFAULT '',
  CONCURRENT_TASKS    SMALLINT      NOT NULL DEFAULT 1 CHECK (CONCURRENT_TASKS > 0),
  CONFIG_ID           BIGINT REFERENCES MODULE_INNER_CONFIGURATION (ID) ON DELETE CASCADE ON UPDATE CASCADE,
  SOURCE_ID           BIGINT REFERENCES MODULE_SOURCE (ID),
  PERMISSIONS         JSON          NOT NULL DEFAULT '{"isEnabled": false, "owners": []}'::JSON,
  IS_ENABLED          BOOLEAN       NOT NULL DEFAULT FALSE
);

-- Represents org.apache.zeppelin.Repository
CREATE TABLE REPOSITORY
(
  id             BIGSERIAL PRIMARY KEY,
  repository_id  VARCHAR(100) NOT NULL UNIQUE,
  snapshot       BOOLEAN      NOT NULL DEFAULT FALSE,
  url            VARCHAR(255) NOT NULL,
  --TODO(egorklimov): CHECK (url ~* '(?:(?:https?):\/\/|www\.|ftp\.)(?:\([-A-Z0-9+&@#\/%=~_|$?!:,.]*\)|[-A-Z0-9+&@#\/%=~_|$?!:,.])*(?:\([-A-Z0-9+&@#\/%=~_|$?!:,.]*\)|[A-Z0-9+&@#\/%=~_|$])'),
  username       VARCHAR(255),
  password       VARCHAR(255),
  proxy_protocol VARCHAR               DEFAULT 'HTTP'
    CHECK (proxy_protocol IN ('HTTP', 'HTTPS')),
  proxy_host     VARCHAR(255),
  proxy_port     VARCHAR(255),
  proxy_login    VARCHAR(255),
  proxy_password VARCHAR(255)
);
