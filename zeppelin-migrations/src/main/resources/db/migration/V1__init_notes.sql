CREATE TABLE notes
(
  id                        VARCHAR(9) NOT NULL PRIMARY KEY,
  path                      VARCHAR    NOT NULL,
  default_interpreter_group VARCHAR    NOT NULL
);

CREATE TABLE paragraphs
(
  id       VARCHAR NOT NULL PRIMARY KEY,
  note_id  VARCHAR(9) REFERENCES notes ON DELETE CASCADE,
  title    VARCHAR,
  text     VARCHAR,
  username VARCHAR,
  created  VARCHAR,
  updated  VARCHAR,
  config   VARCHAR,
  settings VARCHAR,
  position INTEGER
);