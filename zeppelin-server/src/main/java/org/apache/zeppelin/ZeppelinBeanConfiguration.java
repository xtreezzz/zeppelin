/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.zeppelin;

import org.apache.zeppelin.configuration.ZeppelinConfiguration;
import org.apache.zeppelin.storage.DatabaseNoteRepository;
import org.apache.zeppelin.storage.InterpreterOptionRepository;
import org.apache.zeppelin.storage.NoteRevisionDAO;
import org.apache.zeppelin.storage.NotebookDAO;
import org.apache.zeppelin.storage.ParagraphDAO;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.task.SimpleAsyncTaskExecutor;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import org.springframework.jdbc.datasource.DriverManagerDataSource;
import org.springframework.scheduling.TaskScheduler;
import org.springframework.scheduling.concurrent.ConcurrentTaskScheduler;

import javax.sql.DataSource;
import java.util.concurrent.Executor;

@Configuration
public class ZeppelinBeanConfiguration {

  @Bean
  public ZeppelinConfiguration zeppelinConfiguration() {
    return new ZeppelinConfiguration();
  }


  @Bean
  @Autowired
  public DatabaseNoteRepository databaseNoteRepository(final NamedParameterJdbcTemplate jdbcTemplate) {
    ParagraphDAO paragraphDAO = new ParagraphDAO(jdbcTemplate);
    NoteRevisionDAO noteRevisionDAO = new NoteRevisionDAO(jdbcTemplate, paragraphDAO);
    return new DatabaseNoteRepository(new NotebookDAO(jdbcTemplate, paragraphDAO), noteRevisionDAO);
  }

  @Bean
  @Autowired
  public InterpreterOptionRepository databaseInterpreterOptionRepository(
      final NamedParameterJdbcTemplate jdbcTemplate) {
    return new InterpreterOptionRepository(jdbcTemplate);
  }

  @Bean
  DataSource dataSource(
          @Value("${spring.datasource.driverClassName}") final String driverClassName,
          @Value("${spring.datasource.url}") final String url,
          @Value("${spring.datasource.username}") final String username,
          @Value("${spring.datasource.password}") final String password) {


    //TODO(SAN) этот dataSource временный! На прод его нельзя!!!
    final DriverManagerDataSource dataSource = new DriverManagerDataSource();
    dataSource.setDriverClassName(driverClassName);
    dataSource.setUrl(url);
    dataSource.setUsername(username);
    dataSource.setPassword(password);

    return dataSource;
  }

  @Bean
  public TaskScheduler taskScheduler() {
    return new ConcurrentTaskScheduler();
  }

  @Bean
  public Executor taskExecutor() {
    return new SimpleAsyncTaskExecutor();
  }
}
