package org.apache.zeppelin.interpreter.remote;

import org.apache.commons.cli.*;
import sun.misc.Signal;

import java.util.Objects;

public class RemoteInterpreterServer {

  public static void main(String[] args) throws Exception {

    final Options options = new Options();

    final Option host = new Option("h", "host", true, "zeppelin thrift server host");
    host.setRequired(true);
    options.addOption(host);

    final Option port = new Option("p", "port", true, "zeppelin thrift server port");
    port.setRequired(true);
    options.addOption(port);

    final Option shebang = new Option("sb", "shebang", true, "interpreter shebang");
    shebang.setRequired(true);
    options.addOption(shebang);

    final Option classpath = new Option("cp", "classpath", true, "interpreter classpath");
    classpath.setRequired(true);
    options.addOption(classpath);

    final Option classname = new Option("cn", "classname", true, "interpreter classname");
    classname.setRequired(true);
    options.addOption(classname);


    final CommandLineParser parser = new DefaultParser();
    final HelpFormatter formatter = new HelpFormatter();
    CommandLine cmd = null;

    try {
      cmd = parser.parse(options, args);
    } catch (ParseException e) {
      System.out.println(e.getMessage());
      formatter.printHelp("utility-name", options);
    }

    if (Objects.isNull(cmd)) {
      System.exit(1);
    }

    final String zeppelinServerHost = cmd.getOptionValue("host");
    final String zeppelinServerPort = cmd.getOptionValue("port");

    final String interpreterShebang = cmd.getOptionValue("shebang");
    final String interpreterClasspath = cmd.getOptionValue("classpath");
    final String interpreterClassname = cmd.getOptionValue("classname");


    final RemoteInterpreterThread remoteInterpreterServer = new RemoteInterpreterThread(
            zeppelinServerHost,
            zeppelinServerPort,
            interpreterShebang,
            interpreterClasspath,
            interpreterClassname
    );

    Signal.handle(new Signal("TERM"), signal -> {
      remoteInterpreterServer.shutdown();
      System.exit(0);
    });

    remoteInterpreterServer.start();
    remoteInterpreterServer.join();
    System.exit(0);
  }

}
