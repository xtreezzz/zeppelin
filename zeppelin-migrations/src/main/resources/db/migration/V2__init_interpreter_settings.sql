CREATE TABLE InterpreterSource
(
    id               BIGSERIAL PRIMARY KEY,
    interpreter_name VARCHAR NOT NULL UNIQUE,
    artifact         VARCHAR NOT NULL UNIQUE
);

-- FIXME
-- Вынес в отдельную таблицу, т.к. нужна связь с InterpreterSource через группу.
-- По этой же причине сделал interpreter_name UNIQUE
CREATE TABLE BaseInterpreterConfig
(
    id              BIGSERIAL PRIMARY KEY,
    "group"         VARCHAR REFERENCES InterpreterSource (interpreter_name) ON DELETE CASCADE,
    "name"          VARCHAR,
    class_name      TEXT NOT NULL,
    properties      JSON NOT NULL,
    editor          JSON NOT NULL
);


CREATE DOMAIN interpreter_process_type AS VARCHAR(10) DEFAULT 'shared' NOT NULL
CHECK(VALUE IN ('shared', 'scoped', 'isolated'));

CREATE TABLE InterpreterOption
(
    id                       BIGSERIAL  PRIMARY KEY,
    shebang                  VARCHAR    NOT NULL UNIQUE CHECK (shebang ~* '%([\w\.]+)(\(.*?\))?'),
    custom_interpreter_name  TEXT       NOT NULL,
    interpreter_name         VARCHAR    NOT NULL,
    per_note                 interpreter_process_type,
    per_user                 interpreter_process_type,
    jvm_options              VARCHAR,
    concurrent_tasks         INTEGER    DEFAULT 1,
    config_id                INTEGER    REFERENCES BaseInterpreterConfig (id) ON DELETE CASCADE,
    remote_process           JSON,
    permissions              JSON
);

CREATE INDEX idx_interpreter_name
ON InterpreterOption(interpreter_name);

-- Скорее всего регексп для url можно выкинуть
CREATE TABLE repository
(
    id               BIGSERIAL  PRIMARY KEY,
    repository_id    VARCHAR    NOT NULL UNIQUE,
    snapshot         BOOLEAN    DEFAULT FALSE,
    url              TEXT
    CHECK (url ~* '(?:(?:https?):\/\/|www\.|ftp\.)(?:\([-A-Z0-9+&@#\/%=~_|$?!:,.]*\)|[-A-Z0-9+&@#\/%=~_|$?!:,.])*(?:\([-A-Z0-9+&@#\/%=~_|$?!:,.]*\)|[A-Z0-9+&@#\/%=~_|$])'),
    username         VARCHAR,
    password         VARCHAR,
    proxy_protocol   VARCHAR    NOT NULL DEFAULT 'HTTP'
    CHECK (proxy_protocol IN ('HTTP', 'HTTPS')),
    proxy_host       VARCHAR,
    proxy_port       VARCHAR,
    proxy_login      VARCHAR,
    proxy_password   VARCHAR
);
