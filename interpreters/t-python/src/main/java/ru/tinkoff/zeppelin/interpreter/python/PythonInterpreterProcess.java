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
package ru.tinkoff.zeppelin.interpreter.python;

import jep.Jep;
import jep.JepConfig;
import jep.JepException;
import org.apache.commons.cli.*;
import sun.misc.Signal;

import java.io.*;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

public class PythonInterpreterProcess {

  public static void main(String[] args) {

    final Options options = new Options();
    final Option pyScriptPath = new Option("py_script",
            "py_script",
            true,
            "path to python script"
    );
    pyScriptPath.setRequired(true);
    options.addOption(pyScriptPath);

    final Option outputFilePath = new Option("output_file",
            "output_file",
            true,
            "path to python output"
    );
    outputFilePath.setRequired(true);
    options.addOption(outputFilePath);

    final Option paramsFilePath = new Option("params_file",
            "params_file",
            true,
            "path to python params file"
    );
    paramsFilePath.setRequired(true);
    options.addOption(paramsFilePath);

    CommandLine cmd = null;
    try {
      cmd = new DefaultParser().parse(options, args);
    } catch (ParseException e) {
      new HelpFormatter().printHelp("utility-name", options);
      System.exit(1);
    }

    final String pathToScript = cmd.getOptionValue("py_script");
    final String pathToOutput = cmd.getOptionValue("output_file");
    final String pathToParamsFile = cmd.getOptionValue("params_file");

    final JepConfig jepConfig = new JepConfig()
            .setRedirectOutputStreams(true);

    final File output = new File(pathToOutput);
    try (final FileOutputStream fis = new FileOutputStream(output, true);
         final BufferedOutputStream bos = new BufferedOutputStream(fis);
         final PrintStream ps = new PrintStream(bos)) {

      System.getenv("PYTHONPATH");
      System.setOut(ps);
      System.setErr(ps);
      Signal.handle(new Signal("TERM"), signal -> {
        ps.println("Process terminated by external signal / watchdog timeout");
        ps.close();
        System.exit(1);
      });

      try(final Jep jep = new Jep(jepConfig)) {
        jep.setInteractive(true);

        // load runtime properties from file
        Properties properties = new Properties();
        properties.load(new FileInputStream(pathToParamsFile));

        // inject values into python
        final Map<String, Object> params = new HashMap<>();
        for (String key : properties.stringPropertyNames()) {
          params.put(key, properties.get(key).toString());
          if(properties.get(key).toString().equals("ZEPPPELIN_NULL")) {
            continue;
          }
          jep.set(key, properties.get(key).toString());
        }

        // execute script
        jep.runScript(pathToScript);

        // read updated valuest from pythin process
        for (Map.Entry<String,Object> entry : params.entrySet()) {
          properties.put(entry.getKey(), jep.getValue(entry.getKey()).toString());
        }
        // store updated properties into file
        properties.store(new FileOutputStream(pathToParamsFile), null);

      } catch (final JepException je) {
        ps.println(je.getLocalizedMessage());
        throw new RuntimeException("Error");

      } catch (final Exception e) {
        e.printStackTrace(ps);
        throw new RuntimeException("Error");
      }

    } catch (final Exception e) {
      System.exit(1);
    }
    System.exit(0);
  }
}
