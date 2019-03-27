package org.apache.zeppelin;

import org.apache.zeppelin.interpreter.configuration.InterpreterOption;
import org.apache.zeppelin.interpreter.core.InterpreterResult;
import org.apache.zeppelin.interpreter.core.thrift.CancelResult;
import org.apache.zeppelin.interpreter.core.thrift.PushResult;
import org.apache.zeppelin.interpreter.core.thrift.RemoteInterpreterService;
import org.apache.zeppelin.interpreterV2.server.InterpreterProcess;
import org.apache.zeppelin.interpreterV2.server.InterpreterProcessServerManager;
import org.apache.zeppelin.notebook.Job;
import org.apache.zeppelin.notebook.JobBatch;
import org.apache.zeppelin.notebook.JobResult;
import org.apache.zeppelin.storage.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
import java.time.LocalDateTime;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

@Component
public class NoteExecutorService {

    private static final Logger logger = LoggerFactory.getLogger(NoteExecutorService.class);

    private final JobBatchDAO jobBatchDAO;
    private final JobDAO jobDAO;
    private final JobPayloadDAO jobPayloadDAO;
    private final JobResultDAO jobResultDAO;
    private final InterpreterOptionRepository interpreterOptionRepository;

    private final InterpreterProcessServerManager serverManager;

    private Map<String, Long> deathStatistic = new ConcurrentHashMap<>();

    public NoteExecutorService(final JobBatchDAO jobBatchDAO,
                               final JobDAO jobDAO,
                               final JobPayloadDAO jobPayloadDAO,
                               final JobResultDAO jobResultDAO,
                               final InterpreterOptionRepository interpreterOptionRepository) {

        this.jobBatchDAO = jobBatchDAO;
        this.jobDAO = jobDAO;
        this.jobPayloadDAO = jobPayloadDAO;
        this.jobResultDAO = jobResultDAO;
        this.interpreterOptionRepository = interpreterOptionRepository;
        this.serverManager = new InterpreterProcessServerManager(interpreterOptionRepository, this::onInterpreterResult);


        final ExecutorService executorService = Executors.newFixedThreadPool(3);
        executorService.execute(() -> {
            while (!Thread.interrupted()) {
                try {
                    handlePending();
                    Thread.sleep(250);
                } catch (final Exception e) {
                    e.printStackTrace();
                }
            }
        });
        executorService.execute(() -> {
            while (!Thread.interrupted()) {
                try {
                    actualizeInterpreters();
                    Thread.sleep(1000);
                } catch (final Exception e) {
                    e.printStackTrace();
                }
            }
        });
        executorService.execute(() -> {
            while (!Thread.interrupted()) {
                try {
                    handleAbort();
                    Thread.sleep(1000);
                } catch (final Exception e) {
                    e.printStackTrace();
                }
            }
        });
    }

    @PostConstruct
    public void postConstruct() throws Exception {
        serverManager.startServer();
        jobDAO.restoreState();
    }

    @PreDestroy
    public void preDestroy() {
        serverManager.stopServer();
    }


    private synchronized void handlePending() {

        final Set<String> busyInterpreters = new HashSet<>();

        final List<Job> jobs = jobDAO.loadNextPending();
        for (final Job job : jobs) {
            final JobBatch batch = jobBatchDAO.get(job.getBatchId());

            final String shebang = job.getShebang();

            if(busyInterpreters.contains(shebang)) {
                continue;
            }

            final InterpreterOption interpreterOption = interpreterOptionRepository.getOption(shebang);

            if (interpreterOption == null) {
                final InterpreterResult pseudoResult = new InterpreterResult(InterpreterResult.Code.ERROR);
                pseudoResult.add(
                        new InterpreterResult.Message(
                                InterpreterResult.Message.Type.TEXT,
                                "Interpreter nof found or not configured"
                        )
                );
                setErrorResult(job, batch, pseudoResult);

                busyInterpreters.add(job.getShebang());
                continue;
            }

            if (!interpreterOption.isEnabled()) {
                final InterpreterResult pseudoResult = new InterpreterResult(InterpreterResult.Code.ERROR);
                pseudoResult.add(
                        new InterpreterResult.Message(
                                InterpreterResult.Message.Type.TEXT,
                                "Interpreter disabled"
                        )
                );
                setErrorResult(job, batch, pseudoResult);

                busyInterpreters.add(job.getShebang());
                continue;
            }

            if(deathStatistic.getOrDefault(shebang, 0L) > 50) {
                deathStatistic.put(interpreterOption.getShebang(), 0L);

                interpreterOption.setEnabled(false);
                interpreterOptionRepository.updateOption(interpreterOption);

                busyInterpreters.add(job.getShebang());
                continue;
            }

            final InterpreterProcess remote = serverManager.getRemote(shebang, interpreterOption);
            if (remote.getStatus() == InterpreterProcess.Status.READY) {
                deathStatistic.put(remote.getShebang(), 0L);

                // prepare payload
                final String payload = jobPayloadDAO.getByJobId(job.getId()).getPayload();

                // prepare notecontext
                final Map<String, String> noteContext = new HashMap<>();
                noteContext.put("noteId", String.valueOf(job.getNoteId()));
                noteContext.put("paragraphId", String.valueOf(job.getParagpaphId()));

                // prepare usercontext
                final Map<String, String> userContext = new HashMap<>();

                // prepare configuration
                final Map<String, String> configuration = new HashMap<>();
                interpreterOption
                        .getConfig()
                        .getProperties()
                        .forEach((p, v) -> configuration.put(p, String.valueOf(v.getCurrentValue())));

                final PushResult result;
                try {
                    final RemoteInterpreterService.Client connection = remote.getConnection();
                    result = connection.push(payload, noteContext, userContext, configuration);
                    remote.releaseConnection(connection);
                    Objects.requireNonNull(result);

                } catch (final Exception e) {
                    busyInterpreters.add(job.getShebang());
                    continue;
                }

                switch (result.getStatus()) {
                    case ACCEPT:
                        job.setStatus(Job.Status.RUNNING);
                        job.setInterpreterProcessUUID(result.getInterpreterProcessUUID());
                        job.setInterpreterJobUUID(result.getInterpreterJobUUID());
                        jobDAO.update(job);

                        final JobBatch jobBatch = jobBatchDAO.get(job.getBatchId());
                        if (jobBatch.getStatus() == JobBatch.Status.PENDING) {
                            jobBatch.setStatus(JobBatch.Status.RUNNING);
                            jobBatch.setStartedAt(LocalDateTime.now());
                            jobBatchDAO.update(jobBatch);
                        }
                        break;
                    case DECLINE:
                        logger.info("");
                        busyInterpreters.add(job.getShebang());
                        break;
                    case ERROR:
                    default:
                        logger.info("");
                        busyInterpreters.add(job.getShebang());
                }
            }

            if (remote.getStatus() == InterpreterProcess.Status.NOT_FOUND) {
                final InterpreterResult pseudoResult = new InterpreterResult(InterpreterResult.Code.ERROR);
                pseudoResult.add(
                        new InterpreterResult.Message(
                                InterpreterResult.Message.Type.TEXT,
                                "Wrong configuration of interpreter. "
                        )
                );
                setErrorResult(job, batch, pseudoResult);
            }
        }
    }

    private synchronized void onInterpreterResult(final String interpreterJobUUID,
                                                  final InterpreterResult interpreterResult) {

        final Job job = jobDAO.getByInterpreterJobUUID(interpreterJobUUID);
        if (job == null) {
            return;
        }

        final JobBatch batch = jobBatchDAO.get(job.getBatchId());

        if (batch.getStatus() == JobBatch.Status.ABORTING) {
            setAbortResult(job, batch, interpreterResult);
            return;
        }

        switch (interpreterResult.code()) {
            case SUCCESS:
                setSuccessResult(job, batch, interpreterResult);
                break;
            case ABORTED:
                setAbortResult(job, batch, interpreterResult);
                break;
            case ERROR:
                setErrorResult(job, batch, interpreterResult);
                break;
        }
    }

    private void setSuccessResult(final Job job,
                                  final JobBatch batch,
                                  final InterpreterResult interpreterResult) {

        persistMessages(job, interpreterResult.message());

        job.setStatus(Job.Status.DONE);
        job.setEndedAt(LocalDateTime.now());
        job.setInterpreterJobUUID(null);
        job.setInterpreterProcessUUID(null);
        jobDAO.update(job);

        final List<Job> jobs = jobDAO.loadByBatch(job.getBatchId());
        final boolean isDone = jobs.stream().noneMatch(j -> j.getStatus() != Job.Status.DONE);
        if (isDone) {
            batch.setStatus(JobBatch.Status.DONE);
            batch.setEndedAt(LocalDateTime.now());
            jobBatchDAO.update(batch);
        }
    }

    private void setErrorResult(final Job job,
                                final JobBatch batch,
                                final InterpreterResult interpreterResult) {

        persistMessages(job, interpreterResult.message());

        job.setStatus(Job.Status.ERROR);
        job.setEndedAt(LocalDateTime.now());
        jobDAO.update(job);

        final List<Job> jobs = jobDAO.loadByBatch(job.getBatchId());
        for (final Job j : jobs) {
            if (j.getStatus() == Job.Status.PENDING) {
                j.setStatus(Job.Status.CANCELED);
                j.setStartedAt(LocalDateTime.now());
                j.setEndedAt(LocalDateTime.now());
            }
            j.setInterpreterJobUUID(null);
            j.setInterpreterProcessUUID(null);
            jobDAO.update(j);
        }
        batch.setStatus(JobBatch.Status.ERROR);
        batch.setEndedAt(LocalDateTime.now());
        jobBatchDAO.update(batch);
    }

    private void setAbortResult(final Job job,
                                final JobBatch batch,
                                final InterpreterResult interpreterResult) {

        persistMessages(job, interpreterResult.message());

        job.setStatus(Job.Status.ABORTED);
        job.setEndedAt(LocalDateTime.now());
        jobDAO.update(job);

        final List<Job> jobs = jobDAO.loadByBatch(job.getBatchId());
        for (final Job j : jobs) {
            if (j.getStatus() == Job.Status.PENDING) {
                j.setStatus(Job.Status.CANCELED);
                j.setStartedAt(LocalDateTime.now());
                j.setEndedAt(LocalDateTime.now());
            }
            j.setInterpreterJobUUID(null);
            j.setInterpreterProcessUUID(null);
            jobDAO.update(j);
        }
        batch.setStatus(JobBatch.Status.ABORTED);
        batch.setEndedAt(LocalDateTime.now());
        jobBatchDAO.update(batch);
    }

    private void persistMessages(final Job job,
                                 final List<InterpreterResult.Message> messages) {

        for (final InterpreterResult.Message message : messages) {
            final JobResult jobResult = new JobResult();
            jobResult.setJobId(job.getId());
            jobResult.setCreatedAt(LocalDateTime.now());
            jobResult.setType(message.getType().name());
            jobResult.setResult(message.getData());
            jobResultDAO.persist(jobResult);
        }
    }

    private void actualizeInterpreters() {

        final Map<String, InterpreterProcess> interpreterProcessMap = serverManager.actualizeInterpreters();
        for (final InterpreterProcess process : interpreterProcessMap.values()) {
            if (process.getStatus() == InterpreterProcess.Status.DEAD) {
                deathStatistic.putIfAbsent(process.getShebang(), 0L);
                deathStatistic.computeIfPresent(process.getShebang(), (s, l) -> ++l);

                final List<Job> jobs = jobDAO.loadJobsByInterpreterProcessUUID(process.getInterpreterProcessUUID());
                for (final Job job : jobs) {
                    job.setStatus(Job.Status.PENDING);
                    job.setInterpreterJobUUID(null);
                    job.setInterpreterProcessUUID(null);
                    jobDAO.update(job);
                }
                serverManager.forceKillInterpreterProcess(process);
            }
        }
    }

    private synchronized void handleAbort() {

        final List<Job> jobs = jobDAO.loadNextCancelling();
        for (final Job job : jobs) {
            final String shebang = job.getShebang();

            // it's dirty but fail safe
            try {
                final InterpreterOption interpreterOption = interpreterOptionRepository.getOption(shebang);
                final InterpreterProcess remote = serverManager.getRemote(shebang, interpreterOption);

                final RemoteInterpreterService.Client connection = remote.getConnection();
                final CancelResult cancelResult = connection.cancel(job.getInterpreterJobUUID());
                remote.releaseConnection(connection);
                switch (cancelResult.status) {
                    case ACCEPT:
                        job.setStatus(Job.Status.ABORTING);
                        jobDAO.update(job);
                        // do nothing
                        break;
                    case NOT_FOUND:
                        final JobBatch batch = jobBatchDAO.get(job.getBatchId());
                        final InterpreterResult pseudoResult = new InterpreterResult(InterpreterResult.Code.ABORTED);
                        pseudoResult.add(
                                new InterpreterResult.Message(
                                        InterpreterResult.Message.Type.TEXT,
                                        "Operation aborted"
                                )
                        );
                        setAbortResult(job, batch, pseudoResult);
                        break;
                    case ERROR:
                    default:
                        // TODO: понять что делать
                }
            } catch (final Exception e) {
                // log n skip
            }
        }
    }
}
