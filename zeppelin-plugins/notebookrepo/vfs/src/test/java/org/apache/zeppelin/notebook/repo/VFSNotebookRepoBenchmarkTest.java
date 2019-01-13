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
package org.apache.zeppelin.notebook.repo;

import java.io.File;
import java.io.IOException;
import java.util.concurrent.TimeUnit;
import org.apache.commons.io.FileUtils;
import org.apache.zeppelin.conf.ZeppelinConfiguration;
import org.apache.zeppelin.user.AuthenticationInfo;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;
import org.openjdk.jmh.results.format.ResultFormatType;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.RunnerException;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 1. Gen notebook dir: https://gist.github.com/egorklimov/5953a9f2b071a686f5cc0138c78d9425
 * 3. Specify test dir in <code>{@link MiniZeppelin#notebookDir}</code>
 * 2. Add IntelliJ IDEA JMH Plugin - https://plugins.jetbrains.com/plugin/7529-jmh-plugin
 * 3. Build Zeppelin
 * 4. Run VFSNotebookRepoBenchmarkTest in IntelliJ IDEA
 */
@State(Scope.Benchmark)
public class VFSNotebookRepoBenchmarkTest {
  private static final Logger LOGGER = LoggerFactory.getLogger(VFSNotebookRepoBenchmarkTest.class);
  private static MiniZeppelin zeppelin;

  @Setup(Level.Iteration)
  public static void startServer() throws Exception {
    LOGGER.info("\n\n===== Starting Server =====\n\n");
    zeppelin = new MiniZeppelin();
    zeppelin.start();
  }

  @Benchmark
  @BenchmarkMode(Mode.AverageTime)
  public void testImproved() throws IOException {
    if (zeppelin != null) {
      zeppelin.improvedListFolder();
    }
  }

  @Benchmark
  @BenchmarkMode(Mode.AverageTime)
  public void testDefault() throws IOException {
    if (zeppelin != null) {
      zeppelin.listFolder();
    }
  }

  @TearDown(Level.Iteration)
  public static void clean() throws IOException {
    zeppelin.clean();
  }

  public static void main(String[] args) throws RunnerException, IOException {
    File result = new File("benchmark-result.json");
    result.createNewFile();

    Options opt = new OptionsBuilder()
            .include(VFSNotebookRepoBenchmarkTest.class.getSimpleName())
            .forks(1)
            .threads(1)
            .warmupIterations(10)
            .measurementIterations(20)
            .timeUnit(TimeUnit.SECONDS)
            // stop benchmark after first error
            .shouldFailOnError(true)
            // force GC on each measurement
            .shouldDoGC(true)
            // output to JSON for using JMH Visualizer http://jmh.morethan.io/
            .resultFormat(ResultFormatType.JSON)
            .result(result.getAbsolutePath())
            .build();

    new Runner(opt).run();
  }

  private static class MiniZeppelin {
    private static final Logger LOGGER = LoggerFactory.getLogger(MiniZeppelin.class);
    private static final File notebookDir = new File("/home/user/path/gen_x_y");

    private File zeppelinHome;
    private File confDir;
    private VFSNotebookRepo repo;

    void start() throws IOException {
      zeppelinHome = new File("zeppelin-server/..");
      System.setProperty(ZeppelinConfiguration.ConfVars.ZEPPELIN_HOME.getVarName(),
              zeppelinHome.getAbsolutePath());
      confDir = new File(zeppelinHome, "conf_" + getClass().getSimpleName());
      LOGGER.info("confdir: {} has been created ({})", confDir.getAbsolutePath(), confDir.mkdirs());
      LOGGER.info("notebookDir - {}", notebookDir.getAbsolutePath());
      LOGGER.info("ZEPPELIN_HOME: " + zeppelinHome.getAbsolutePath())
      ;
      FileUtils.copyFile(
              new File(zeppelinHome, "conf/log4j.properties"),
              new File(confDir, "log4j.properties")
      );
      FileUtils.copyFile(
              new File(zeppelinHome, "conf/log4j_yarn_cluster.properties"),
              new File(confDir, "log4j_yarn_cluster.properties")
      );
      System.setProperty(
              ZeppelinConfiguration.ConfVars.ZEPPELIN_CONF_DIR.getVarName(),
              confDir.getAbsolutePath()
      );
      System.setProperty(
              ZeppelinConfiguration.ConfVars.ZEPPELIN_NOTEBOOK_DIR.getVarName(),
              notebookDir.getAbsolutePath()
      );
      System.setProperty(
              ZeppelinConfiguration.ConfVars.ZEPPELIN_INTERPRETER_CONNECT_TIMEOUT.getVarName(),
              "120000"
      );

      repo = new VFSNotebookRepo();
      repo.init(new ZeppelinConfiguration());
    }

    void listFolder() throws IOException {
      repo.list(AuthenticationInfo.ANONYMOUS);
    }

    void improvedListFolder() throws IOException {
      repo.improvedList(AuthenticationInfo.ANONYMOUS);
    }

    void clean() throws IOException {
      FileUtils.deleteDirectory(confDir);
    }
  }
}
