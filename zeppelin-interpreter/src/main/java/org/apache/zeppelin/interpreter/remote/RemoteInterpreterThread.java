package org.apache.zeppelin.interpreter.remote;

import com.google.gson.Gson;
import org.apache.thrift.TException;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.server.TThreadPoolServer;
import org.apache.thrift.transport.TServerSocket;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransport;
import org.apache.zeppelin.interpreter.core.Interpreter;
import org.apache.zeppelin.interpreter.core.InterpreterResult;
import org.apache.zeppelin.interpreter.core.PredefinedInterpreterResults;
import org.apache.zeppelin.interpreter.core.thrift.*;

import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.RejectedExecutionException;

public class RemoteInterpreterThread extends Thread implements RemoteInterpreterService.Iface {

  private final String zeppelinServerHost;
  private final String zeppelinServerPort;

  private final String interpreterShebang;
  private final String interpreterClasspath;
  private final String interpreterClassName;

  private TServerSocket serverTransport;
  private TThreadPoolServer server;
  private RemoteInterpreterEventService.Client zeppelin;

  private Class interpreterClass;

  private final ConcurrentLinkedQueue<Interpreter> cachedInstances = new ConcurrentLinkedQueue<>();
  private final ConcurrentLinkedQueue<Interpreter> workingInstances = new ConcurrentLinkedQueue<>();
  private final ExecutorService executor = Executors.newFixedThreadPool(10);
  private static final UUID processUUID = UUID.randomUUID();

  RemoteInterpreterThread(final String zeppelinServerHost,
                          final String zeppelinServerPort,
                          final String interpreterShebang,
                          final String interpreterClasspath,
                          final String interpreterClassName) {
    this.zeppelinServerHost = zeppelinServerHost;
    this.zeppelinServerPort = zeppelinServerPort;
    this.interpreterShebang = interpreterShebang;
    this.interpreterClasspath = interpreterClasspath;
    this.interpreterClassName = interpreterClassName;
  }

  @Override
  public void run() {
    try {
      interpreterClass = Class.forName(interpreterClassName);

      serverTransport = createTServerSocket();
      server = new TThreadPoolServer(
              new TThreadPoolServer
                      .Args(serverTransport)
                      .processor(new RemoteInterpreterService.Processor<>(this))
      );

      final TTransport transport = new TSocket(zeppelinServerHost, Integer.parseInt(zeppelinServerPort));
      transport.open();

      zeppelin = new RemoteInterpreterEventService.Client(new TBinaryProtocol(transport));

      new Thread(new Runnable() {
        boolean interrupted = false;

        @Override
        public void run() {
          while (!interrupted && !server.isServing()) {
            try {
              Thread.sleep(1000);
            } catch (InterruptedException e) {
              interrupted = true;
            }
          }

          if (!interrupted) {
            final RegisterInfo registerInfo = new RegisterInfo(
                    //serverTransport.getServerSocket().getInetAddress().getHostAddress(),
                    "127.0.0.1",
                    serverTransport.getServerSocket().getLocalPort(),
                    interpreterShebang,
                    processUUID.toString()
            );
            try {
              zeppelin.registerInterpreterProcess(registerInfo);
            } catch (TException e) {
              shutdown();
            }
          }
        }
      }).start();

      server.serve();
    } catch (final Exception e) {
      throw new IllegalStateException("", e);
    }
  }

  private static TServerSocket createTServerSocket() {
    int start = 1024;
    int end = 65535;
    for (int i = start; i <= end; ++i) {
      try {
        return new TServerSocket(i);
      } catch (Exception e) {
        // ignore this
      }
    }
    throw new IllegalStateException("No available port in the portRange: " + start + ":" + end);
  }


  @Override
  public PushResult push(final String st,
                         final Map<String, String> noteContext,
                         final Map<String, String> userContext,
                         final Map<String, String> configuration) {

    final Interpreter interpreter;
    try {
      if (!cachedInstances.isEmpty()) {
        final Interpreter polled = cachedInstances.poll();
        if (polled.isReusableForConfiguration(configuration) && polled.isAlive()) {
          interpreter = polled;
        } else {
          polled.close();
          interpreter = (Interpreter) (interpreterClass.newInstance());
        }
      } else {
        interpreter = (Interpreter) interpreterClass.newInstance();
      }

      final UUID uuid = UUID.randomUUID();
      synchronized (interpreter) {
        executor.submit(() -> {
          synchronized (interpreter) {
            interpreter.setSessionUUID(uuid.toString());
            workingInstances.offer(interpreter);

            InterpreterResult result;
            try {
              if (!interpreter.isOpened()) {
                interpreter.open(configuration, this.interpreterClasspath);
              }
              result = interpreter.interpretV2(st, noteContext, userContext, configuration);
            } catch (Exception e) {
              result = PredefinedInterpreterResults.ERROR_WHILE_INTERPRET;
            }
            try {
              zeppelin.handleInterpreterResult(interpreter.getSessionUUID(), new Gson().toJson(result));
            } catch (final Exception e) {
              //skip
            }
            workingInstances.remove(interpreter);
            interpreter.setSessionUUID(null);
            cachedInstances.offer(interpreter);
          }
        });
        return new PushResult(PushResultStatus.ACCEPT, uuid.toString(), processUUID.toString());
      }
    } catch (final RejectedExecutionException e) {
      return new PushResult(PushResultStatus.DECLINE, "", "");
    } catch (final Exception e) {
      return new PushResult(PushResultStatus.ERROR, "", "");
    }
  }

  @Override
  public CancelResult cancel(final String UUID) {
    try {
      for (final Interpreter interpreter : workingInstances) {
        if (interpreter.getSessionUUID().equals(UUID)) {
          interpreter.cancel();
          return new CancelResult(CancelResultStatus.ACCEPT, interpreter.getSessionUUID(), processUUID.toString());
        }
      }
      return new CancelResult(CancelResultStatus.NOT_FOUND, UUID, processUUID.toString());
    } catch (final Exception e) {
      return new CancelResult(CancelResultStatus.ERROR, UUID, processUUID.toString());
    }
  }

  @Override
  public void shutdown() {
    for (final Interpreter interpreter : workingInstances) {
      try {
        interpreter.cancel();
      } catch (final Exception e) {
        // log n skip
      }
    }
    System.exit(0);
  }


  @Override
  public PingResult ping() {
    return new PingResult(PingResultStatus.OK, processUUID.toString());
  }
}
