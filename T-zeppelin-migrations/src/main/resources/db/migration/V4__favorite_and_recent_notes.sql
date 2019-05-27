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

CREATE TABLE FAVORITE_NOTES
(
  ID          BIGSERIAL    PRIMARY KEY,
  USER_NAME   VARCHAR(200) NOT NULL,
  NOTE_UUID   VARCHAR(9)   REFERENCES NOTES (UUID) ON DELETE CASCADE NOT NULL,
  UNIQUE (USER_NAME, NOTE_UUID)
);

CREATE TABLE RECENT_NOTES
(
  ID          BIGSERIAL    PRIMARY KEY,
  USER_NAME   VARCHAR(200) NOT NULL,
  NOTE_UUID   VARCHAR(9)   REFERENCES NOTES (UUID) ON DELETE CASCADE NOT NULL,
  VISIT_TIME  TIMESTAMP    NOT NULL DEFAULT NOW(),
  UNIQUE (USER_NAME, NOTE_UUID)
);
