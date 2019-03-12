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
  username   VARCHAR,
  created  VARCHAR,
  updated  VARCHAR,
  settings VARCHAR
);


-- Test
insert into notes(id, path, default_interpreter_group)
values ('2E71ZWF5H', 'Super Note 3000', 'python');