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
import org.apache.zeppelin.interpreter.core.thrift.*;

import java.io.File;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.ArrayList;
import java.util.List;
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

    private URLClassLoader interpreterClassloader;
    private Class interpreterClass;

    private final ConcurrentLinkedQueue<Interpreter> cachedInstances = new ConcurrentLinkedQueue<>();
    private final ConcurrentLinkedQueue<Interpreter> workingInstances = new ConcurrentLinkedQueue<>();
    private final ExecutorService executor = Executors.newFixedThreadPool(10);
    private static final UUID processUUID = UUID.randomUUID();

    public RemoteInterpreterThread(final String zeppelinServerHost,
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
            // load class for handle interpretation
            initInterpreterClass();

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
                                serverTransport.getServerSocket().getInetAddress().getHostAddress(),
                                serverTransport.getServerSocket().getLocalPort(),
                                interpreterShebang,
                                processUUID.toString()
                        );
                        try {
                            zeppelin.registerInterpreterProcess(registerInfo);
                        } catch (TException e) {
                            //logger.error("Error while registering interpreter: {}", registerInfo, e);
                            try {
                                shutdown();
                            } catch (TException e1) {
                                //logger.warn("Exception occurs while shutting down", e1);
                            }
                        }
                    }
                }
            }).start();

            server.serve();
        } catch (final Exception e) {
            throw new IllegalStateException("", e);
        }
    }

    public static TServerSocket createTServerSocket() {
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

    public void initInterpreterClass() {
        final File repoFolder = new File(interpreterClasspath);

        final File[] directories = repoFolder.listFiles();
        if (directories == null || directories.length == 0) {
            throw new IllegalStateException("Can't load implementation from " + repoFolder.getAbsolutePath());
        }

        try {
            final List<URL> urls = new ArrayList<>();
            for (final File file : directories) {
                final URL url = file.toURI().toURL();
                urls.add(new URL("jar:" + url.toString() + "!/"));
            }
            interpreterClassloader = URLClassLoader.newInstance(urls.toArray(new URL[0]));
        } catch (final Exception e) {
            // LOG.error("Error while load files from {}", dir);
        }

        try {
            interpreterClass = interpreterClassloader.loadClass(interpreterClassName);
        } catch (final ClassNotFoundException e) {
            throw new IllegalStateException("Fail to find interpreter class:" + interpreterClassName, e);
        }
    }


    @Override
    public PushResult push(final String st,
                           final Map<String, String> noteContext,
                           final Map<String, String> userContext,
                           final Map<String, String> configuration) throws TException {
        try {
            final Interpreter interpreter;
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

            executor.submit(() -> {

                interpreter.setSessionUUID(uuid.toString());
                workingInstances.offer(interpreter);

                InterpreterResult result = null;
                try {
                    if (!interpreter.isOpened()) {
                        interpreter.open(configuration, this.interpreterClasspath);
                    }
                    result = interpreter.interpretV2(st, noteContext, userContext, configuration);
                } catch (Exception e) {
                    //TODO: handle this exception
                    // result = new.......
                }
                try {
                    zeppelin.handleInterpreterResult(interpreter.getSessionUUID(), new Gson().toJson(result));
                } catch (final Exception e) {
                    //skip
                }
                workingInstances.remove(interpreter);
                interpreter.setSessionUUID(null);
                cachedInstances.offer(interpreter);
            });

            return new PushResult(PushResultStatus.ACCEPT, interpreter.getSessionUUID(), processUUID.toString());
        } catch (final RejectedExecutionException e) {
            return new PushResult(PushResultStatus.DECLINE, "", "");
        } catch (final Exception e) {
            return new PushResult(PushResultStatus.ERROR, "", "");
        }
    }

    @Override
    public CancelResult cancel(final String UUID) throws TException {
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
    public void shutdown() throws TException {
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
    public PingResult ping() throws TException {
        return new PingResult(PingResultStatus.OK, processUUID.toString());
    }
}
