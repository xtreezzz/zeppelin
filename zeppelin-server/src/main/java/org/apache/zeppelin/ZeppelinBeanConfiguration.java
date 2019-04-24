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

import java.util.Map;
import java.util.concurrent.Executor;
import javax.sql.DataSource;
import org.apache.commons.dbcp2.BasicDataSource;
import org.apache.shiro.SecurityUtils;
import org.apache.shiro.authz.AuthorizationException;
import org.apache.shiro.authz.UnauthenticatedException;
import org.apache.shiro.cache.CacheManager;
import org.apache.shiro.cache.MemoryConstrainedCacheManager;
import org.apache.shiro.mgt.SecurityManager;
import org.apache.shiro.realm.Realm;
import org.apache.shiro.realm.activedirectory.ActiveDirectoryRealm;
import org.apache.shiro.spring.config.ShiroBeanConfiguration;
import org.apache.shiro.spring.web.config.DefaultShiroFilterChainDefinition;
import org.apache.shiro.spring.web.config.ShiroFilterChainDefinition;
import org.apache.shiro.spring.web.config.ShiroWebFilterConfiguration;
import org.apache.shiro.web.env.IniWebEnvironment;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;
import org.springframework.core.task.SimpleAsyncTaskExecutor;
import org.springframework.http.HttpStatus;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import org.springframework.scheduling.TaskScheduler;
import org.springframework.scheduling.concurrent.ConcurrentTaskScheduler;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.ResponseStatus;
import ru.tinkoff.zeppelin.engine.InterpreterSettingService;

@Configuration
@Import({ShiroBeanConfiguration.class,
        ShiroWebFilterConfiguration.class
})
public class ZeppelinBeanConfiguration {

  @Bean
  @Autowired
  public InterpreterSettingService databaseInterpreterOptionRepository(
      final NamedParameterJdbcTemplate jdbcTemplate) {
    return new InterpreterSettingService(jdbcTemplate);
  }

  @Bean
  DataSource dataSource(
          @Value("${spring.datasource.dbcp2.driver-class-name}") final String driverClassName,
          @Value("${spring.datasource.dbcp2.url}") final String url,
          @Value("${spring.datasource.dbcp2.username}") final String username,
          @Value("${spring.datasource.dbcp2.password}") final String password,
          @Value("${spring.datasource.dbcp2.initial-size}") final int initialSize,
          @Value("${spring.datasource.dbcp2.max-total}") final int maxTotal,
          @Value("${spring.datasource.dbcp2.max-idle}") final int maxIdle,
          @Value("${spring.datasource.dbcp2.min-idle}") final int minIdle
          ) {

    BasicDataSource bds = new BasicDataSource();
    bds.setDriverClassName(driverClassName);
    bds.setUrl(url);
    bds.setUsername(username);
    bds.setPassword(password);
    bds.setInitialSize(initialSize);
    bds.setMaxTotal(maxTotal);
    bds.setMaxIdle(maxIdle);
    bds.setMinIdle(minIdle);

    return bds;
  }

  @Bean
  public TaskScheduler taskScheduler() {
    return new ConcurrentTaskScheduler();
  }

  @Bean
  public Executor taskExecutor() {
    return new SimpleAsyncTaskExecutor();
  }

  @ExceptionHandler(UnauthenticatedException.class)
  @ResponseStatus(HttpStatus.UNAUTHORIZED)
  public void handleException(UnauthenticatedException e) {

  }

  @ExceptionHandler(AuthorizationException.class)
  @ResponseStatus(HttpStatus.FORBIDDEN)
  public void handleException(AuthorizationException e) {

  }

  @Bean
  public Realm realm() {
    ActiveDirectoryRealm realm = new ActiveDirectoryRealm();
    realm.setCachingEnabled(true);
    return realm;
  }

  @Bean
  public IniWebEnvironment getIniWebEnvironment() {
    final IniWebEnvironment environment = new IniWebEnvironment();
    environment.setConfigLocations(System.getProperty("shiroPath"));
    environment.init();
    return environment;
  }

  @Bean
  public ShiroFilterChainDefinition shiroFilterChainDefinition(final IniWebEnvironment environment) {

    DefaultShiroFilterChainDefinition chainDefinition = new DefaultShiroFilterChainDefinition();
    for (final Map.Entry<String, String> entry : environment.getIni().get("urls").entrySet()) {
      chainDefinition.addPathDefinition(entry.getKey(), entry.getValue());
    }
    return chainDefinition;
  }

  @Bean
  public CacheManager cacheManager() {
    return new MemoryConstrainedCacheManager();
  }

  @Bean
  public SecurityManager getSecurityManager(final IniWebEnvironment environment) {
    SecurityUtils.setSecurityManager(environment.getSecurityManager());
    return environment.getSecurityManager();
  }
}
