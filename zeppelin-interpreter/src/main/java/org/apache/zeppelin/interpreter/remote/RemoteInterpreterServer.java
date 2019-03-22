package org.apache.zeppelin.interpreter.remote;

import org.apache.commons.cli.*;
import org.apache.thrift.TException;
import sun.misc.Signal;

import java.util.Objects;

public class RemoteInterpreterServer {

  public static void main(String[] args) throws Exception {

    Options options = new Options();

    Option host = new Option("h", "host", true, "zeppelin thrift server host");
    host.setRequired(true);
    options.addOption(host);

    Option port = new Option("p", "port", true, "zeppelin thrift server port");
    port.setRequired(true);
    options.addOption(port);

    Option shebang = new Option("sb", "shebang", true, "interpreter shebang");
    shebang.setRequired(true);
    options.addOption(shebang);

    Option classpath = new Option("cp", "classpath", true, "interpreter classpath");
    classpath.setRequired(true);
    options.addOption(classpath);

    Option classname = new Option("cn", "classname", true, "interpreter classname");
    classname.setRequired(true);
    options.addOption(classname);


    CommandLineParser parser = new DefaultParser();
    HelpFormatter formatter = new HelpFormatter();
    CommandLine cmd = null;

    try {
      cmd = parser.parse(options, args);
    } catch (ParseException e) {
      System.out.println(e.getMessage());
      formatter.printHelp("utility-name", options);
    }

    if(Objects.isNull(cmd)) {
      System.exit(1);
    }

    String zeppelinServerHost = cmd.getOptionValue("host");
    String zeppelinServerPort = cmd.getOptionValue("port");

    String interpreterShebang = cmd.getOptionValue("shebang");
    String interpreterClasspath = cmd.getOptionValue("classpath");
    String interpreterClassname = cmd.getOptionValue("classname");


    RemoteInterpreterThread remoteInterpreterServer = new RemoteInterpreterThread(
            zeppelinServerHost,
            zeppelinServerPort,
            interpreterShebang,
            interpreterClasspath,
            interpreterClassname
    );

    // add signal handler
    Signal.handle(new Signal("TERM"), signal -> {
      try {
        remoteInterpreterServer.shutdown();
      } catch (TException e) {
        //logger.error("Error on shutdown RemoteInterpreterServer", e);
      }
    });

    remoteInterpreterServer.start();
    remoteInterpreterServer.join();
    System.exit(0);
  }

}
