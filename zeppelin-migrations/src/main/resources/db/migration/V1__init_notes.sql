CREATE TABLE notes
(
  id          BIGSERIAL  NOT NULL PRIMARY KEY,
  note_id     VARCHAR(9) NOT NULL UNIQUE,
  path        VARCHAR    NOT NULL,
  permissions JSON,
  gui         JSON
);

CREATE TABLE paragraphs
(
  id           BIGSERIAL NOT NULL PRIMARY KEY,
  paragraph_id VARCHAR   NOT NULL,
  note_id      VARCHAR(9) REFERENCES notes (note_id) ON DELETE CASCADE,
  title        VARCHAR,
  text         VARCHAR,
  username     VARCHAR,
  created      VARCHAR   NOT NULL,
  updated      VARCHAR   NOT NULL,
  config       JSON,
  gui          JSON,
  position     INTEGER,
  UNIQUE (paragraph_id, note_id)
);