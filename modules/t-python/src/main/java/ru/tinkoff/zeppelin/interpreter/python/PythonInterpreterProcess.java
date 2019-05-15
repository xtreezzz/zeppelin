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

import jep.*;
import org.apache.commons.cli.*;
import sun.misc.Signal;

import java.io.*;
import java.nio.file.Files;
import java.util.*;

public class PythonInterpreterProcess {

  public static void main(final String[] args) throws JepException {

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

    final Option jepInclude = new Option("jep_include_paths",
            "jep_include_paths",
            true,
            "Sets a path of directories separated by File.pathSeparator that will be appended to the sub-intepreter's sys.path"
    );
    jepInclude.setRequired(false);
    options.addOption(jepInclude);

    final Option pythonHomePath = new Option("jep_python_home",
            "jep_python_home",
            true,
            "Python home directory path"
    );
    pythonHomePath.setRequired(false);
    options.addOption(pythonHomePath);

    final Option noteStorageDir = new Option("storage_dir",
            "storage_dir",
            true,
            "Note storage dir"
    );
    noteStorageDir.setRequired(false);
    options.addOption(noteStorageDir);

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
    final String jepIncludePaths = cmd.getOptionValue("jep_include_paths");
    final String jepPythonHome = cmd.getOptionValue("jep_python_home");
    final String noteStorage = cmd.getOptionValue("storage_dir");

    final JepConfig jepConfig = new JepConfig()
            .setRedirectOutputStreams(true)
            .addIncludePaths(jepIncludePaths.split(":"));

    if (!"".equals(jepPythonHome)) {
      PyConfig pyConfig = new PyConfig();
      pyConfig.setPythonHome(jepPythonHome);
      MainInterpreter.setInitParams(pyConfig);
    }

    final File output = new File(pathToOutput);
    try (final FileOutputStream fos = new FileOutputStream(output, true);
         final BufferedOutputStream bos = new BufferedOutputStream(fos);
         final PrintStream ps = new PrintStream(bos)) {

      System.getenv("PYTHONPATH");
      System.setOut(ps);
      System.setErr(ps);
      Signal.handle(new Signal("TERM"), signal -> {
        ps.println("Process terminated by external signal / watchdog timeout");
        ps.close();
        System.exit(1);
      });

      try (final Jep jep = new Jep(jepConfig)) {
        jep.setInteractive(true);

        // inject values into python
        // load runtime properties from file
        Properties properties = new Properties();
        properties.load(new FileInputStream(pathToParamsFile));

        // inject values into python
        final Map<String, Object> params = new HashMap<>();
        for (String key : properties.stringPropertyNames()) {
          params.put(key, properties.get(key).toString());
          if (properties.get(key).toString().equals("ZEPPELIN_NULL")) {
            continue;
          }
          jep.set(key, properties.get(key).toString());
        }

        Files.createDirectories(new File(noteStorage).toPath());

        final File noteContextFile = new File(noteStorage + "/note.context");
        if(noteContextFile.exists()) {
          FileInputStream fis = new FileInputStream(noteContextFile);
          ObjectInputStream ois = new ObjectInputStream(fis);
          final List<PythonInterpreterEnvObject> envObjects = (List<PythonInterpreterEnvObject>) ois.readObject();

          jep.eval("import pickle");
          for (final PythonInterpreterEnvObject envObject : envObjects) {
            //jep.set(envObject.getName(), Class.forName(envObject.getClassName()).cast(envObject.getPayload()));

            jep.set(envObject.getName() + "_ZZ",  new String(envObject.getPayload()));
            jep.eval(envObject.getName() + "=  pickle.loads(" + envObject.getName() + "_ZZ" + ")");
          }
        }

        // execute script
        jep.runScript(pathToScript);

        // read updated values from pythin process
        final List<PythonInterpreterEnvObject> envResult = new ArrayList<>();
        for (Map.Entry<String, Object> entry : params.entrySet()) {
          if (params.get(entry.getKey()).equals("ZEPPELIN_NULL")) {
            jep.eval("import pickle");
            final PythonInterpreterEnvObject pieo = new PythonInterpreterEnvObject(
                    entry.getKey(),
                    jep.getValue(entry.getKey()).getClass().getName(),
                    jep.getValue_bytearray("pickle.dumps(" + entry.getKey() + ")")
            );
            envResult.add(pieo);
          }
        }
        FileOutputStream fout = new FileOutputStream(noteContextFile);
        ObjectOutputStream oos = new ObjectOutputStream(fout);
        oos.writeObject(envResult);

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
