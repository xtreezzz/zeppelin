package org.apache.zeppelin.interpreterV2.server;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.BiConsumer;
import org.apache.commons.exec.CommandLine;
import org.apache.commons.exec.DefaultExecutor;
import org.apache.commons.exec.ExecuteException;
import org.apache.commons.exec.ExecuteResultHandler;
import org.apache.commons.exec.ExecuteWatchdog;
import org.apache.thrift.transport.TTransportException;
import org.apache.zeppelin.interpreter.configuration.InterpreterArtifactSource;
import org.apache.zeppelin.interpreter.configuration.InterpreterOption;
import org.apache.zeppelin.interpreter.core.InterpreterResult;
import org.apache.zeppelin.interpreter.core.thrift.PingResult;
import org.apache.zeppelin.interpreter.core.thrift.RegisterInfo;
import org.apache.zeppelin.interpreter.core.thrift.RemoteInterpreterService;
import org.apache.zeppelin.storage.InterpreterOptionRepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class InterpreterProcessServerManager {

    private static final Logger LOG = LoggerFactory.getLogger(InterpreterProcessServerManager.class);

    private final InterpreterProcessServer server;

    private final InterpreterProcessServerEventHandler eventHandler;
    private final InterpreterOptionRepository interpreterOptionRepository;

    private final InterpreterInstaller interpreterInstaller = new InterpreterInstaller();
    private final String remoteServerClassPath;

    private final Map<String, InterpreterProcess> registeredInterpreters = new ConcurrentHashMap<>();

    public InterpreterProcessServerManager(final InterpreterOptionRepository interpreterOptionRepository,
                                           final BiConsumer<String, InterpreterResult> interpreterResultBiConsumer) {
        this.interpreterOptionRepository = interpreterOptionRepository;

        this.server = new InterpreterProcessServer();
        this.eventHandler = new InterpreterProcessServerEventHandler(
                this::onInterpreterProcessStarted,
                interpreterResultBiConsumer
        );

        InterpreterInstaller.uninstallInterpreter("remote-server", "org.apache.zeppelin:zeppelin-interpreter:1.0.0-T-SNAPSHOT");
        interpreterInstaller.install("remote-server", "org.apache.zeppelin:zeppelin-interpreter:1.0.0-T-SNAPSHOT", interpreterOptionRepository.getAllRepositories());
        remoteServerClassPath = InterpreterInstaller.getDirectory("remote-server", "org.apache.zeppelin:zeppelin-interpreter:1.0.0-T-SNAPSHOT");


        InterpreterInstaller.uninstallInterpreter("md", "org.apache.zeppelin:zeppelin-markdown:1.0.0-T-SNAPSHOT");
        interpreterInstaller.install("md", "org.apache.zeppelin:zeppelin-markdown:1.0.0-T-SNAPSHOT", interpreterOptionRepository.getAllRepositories());
    }


    public void startServer() throws TTransportException {
        server.start(eventHandler);
    }

    public void stopServer() {
        server.stop();
    }


    public InterpreterProcess getRemote(final String shebang, final InterpreterOption interpreterOption) {
        if (registeredInterpreters.containsKey(shebang)) {
            final InterpreterProcess interpreterProcess = registeredInterpreters.get(shebang);
            return interpreterProcess;
        } else {
            if (interpreterOption != null) {
                final InterpreterArtifactSource artifactSource = interpreterOptionRepository.getSource(interpreterOption.getInterpreterName());

                if (artifactSource == null) {
                    final InterpreterProcess interpreterProcess = new InterpreterProcess();
                    interpreterProcess.setShebang(shebang);
                    interpreterProcess.setStatus(InterpreterProcess.Status.NOT_FOUND);
                    return interpreterProcess;
                }

                if (artifactSource.getStatus().equals(InterpreterArtifactSource.Status.NOT_INSTALLED)) {
                    final InterpreterProcess interpreterProcess = new InterpreterProcess();
                    interpreterProcess.setShebang(shebang);
                    interpreterProcess.setStatus(InterpreterProcess.Status.NOT_FOUND);
                    return interpreterProcess;
                }

                if (artifactSource.getStatus().equals(InterpreterArtifactSource.Status.IN_PROGRESS)) {
                    final InterpreterProcess interpreterProcess = new InterpreterProcess();
                    interpreterProcess.setShebang(shebang);
                    interpreterProcess.setStatus(InterpreterProcess.Status.NOT_FOUND);
                    return interpreterProcess;
                }

                if (artifactSource.getStatus().equals(InterpreterArtifactSource.Status.INSTALLED)) {
                    final InterpreterProcess interpreterProcess = new InterpreterProcess();
                    interpreterProcess.setShebang(shebang);
                    interpreterProcess.setStatus(InterpreterProcess.Status.STARTING);
                    interpreterProcess.setConfig(interpreterOption);

                    startInterpreterProcess(shebang, artifactSource.getPath(), interpreterOption.getConfig().getClassName());

                    registeredInterpreters.put(shebang, interpreterProcess);
                    return interpreterProcess;
                }

                // default condition
                final InterpreterProcess interpreterProcess = new InterpreterProcess();
                interpreterProcess.setShebang(shebang);
                interpreterProcess.setStatus(InterpreterProcess.Status.NOT_FOUND);
                return interpreterProcess;

            } else {
                final InterpreterProcess interpreterProcess = new InterpreterProcess();
                interpreterProcess.setShebang(shebang);
                interpreterProcess.setStatus(InterpreterProcess.Status.NOT_FOUND);
                return interpreterProcess;
            }
        }
    }


    public void startInterpreterProcess(final String shebang, final String classpath, final String classname) {
        final String cmd = String.format("java " +
                        " -cp \"./*:%s/*\"" +
                        " org.apache.zeppelin.interpreter.remote.RemoteInterpreterServer" +
                        " -h %s" +
                        " -p %s" +
                        " -sb %s" +
                        " -cp %s" +
                        " -cn %s ",
                classpath,
                "127.0.0.1",
                server.getServerSocket().getServerSocket().getLocalPort(),
                shebang,
                classpath,
                classname
        );

        // start server process
        final CommandLine cmdLine = CommandLine.parse(cmd); //CommandLine.parse(interpreterRunner);

        final DefaultExecutor executor = new DefaultExecutor();
        executor.setWorkingDirectory(new File(remoteServerClassPath));

        final ExecuteWatchdog watchdog = new ExecuteWatchdog(ExecuteWatchdog.INFINITE_TIMEOUT);
        executor.setWatchdog(watchdog);

        final ExecuteResultHandler handler = new ExecuteResultHandler() {
            @Override
            public void onProcessComplete(final int exitValue) {
                registeredInterpreters.remove(shebang);
            }

            @Override
            public void onProcessFailed(final ExecuteException e) {
                registeredInterpreters.remove(shebang);
            }
        };

        try {
            executor.execute(cmdLine, new HashMap<>(), handler);
        } catch (final IOException e) {
            throw new RuntimeException(e);
        }
    }

    private void onInterpreterProcessStarted(final RegisterInfo registerInfo) {
        if (registeredInterpreters.containsKey(registerInfo.getShebang())) {
            final InterpreterProcess process = registeredInterpreters.get(registerInfo.getShebang());
            process.setHost(registerInfo.getHost());
            process.setPort(registerInfo.getPort());
            process.setInterpreterProcessUUID(registerInfo.getInterpreterProcessUUID());
            process.setStatus(InterpreterProcess.Status.READY);
        } else {
            // TODO: think about it
        }
    }

    public Map<String, InterpreterProcess> actualizeInterpreters() {

        for (final InterpreterProcess process : registeredInterpreters.values()) {
            if(process.getStatus() == InterpreterProcess.Status.STARTING) {
                continue;
            }
            try {
                final RemoteInterpreterService.Client connection = process.getConnection();
                final PingResult pingResult = connection.ping();
                process.releaseConnection(connection);
                switch (pingResult.status) {
                    case OK:
                        process.setStatus(InterpreterProcess.Status.READY);
                        break;
                    case KILL_ME:
                    default:
                        process.setStatus(InterpreterProcess.Status.DEAD);
                        break;
                }
            } catch (final Exception e) {
                process.setStatus(InterpreterProcess.Status.DEAD);
            }
        }
        return new HashMap<>(registeredInterpreters);
    }

    public void forceKillInterpreterProcess(final InterpreterProcess process) {
        try {
            final RemoteInterpreterService.Client connection = process.getConnection();
            connection.shutdown();
            process.releaseConnection(connection);
        } catch (final Exception e) {
            // log n skip
        }
        registeredInterpreters.remove(process.getShebang());
    }
}
