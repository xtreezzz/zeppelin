create table notes
(
  id                        serial PRIMARY KEY,
  note_id                   varchar(9) NOT NULL,
  path                      varchar    NOT NULL,
  default_interpreter_group varchar    NOT NULL
);

insert into notes
values (1, '2E71ZWF3H', '/folder/note1', 'python');
insert into notes
values (2, '2E71ZWF4H', '/folder/note2', 'python');
insert into notes
values (3, '2E71ZWF5H', '/folder/note3', 'python');