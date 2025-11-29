// DataProcessingSystem.java
import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.time.Instant;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.locks.*;
import java.util.logging.*;

/**
 * DataProcessingSystem
 * Single-file example of a multi-worker data processing system using a
 * shared queue, ReentrantLock + Condition, ExecutorService, and logging.
 */
public class DataProcessingSystem {

    // Simple Task class (expandable)
    static class Task {
        private final String id;
        private final String payload;

        public Task(String id, String payload) {
            this.id = id;
            this.payload = payload;
        }

        public String getId() { return id; }
        public String getPayload() { return payload; }

        @Override
        public String toString() {
            return "Task{id='" + id + "', payload='" + payload + "'}";
        }
    }

    /**
     * SharedTaskQueue with explicit synchronization using ReentrantLock and Condition.
     * getTask() blocks while queue empty and producer hasn't signaled completion.
     * When no more tasks will be added, setNoMoreTasks() should be called to unblock
     * waiting worker threads and allow clean shutdown.
     */
    static class SharedTaskQueue<T> {
        private final Queue<T> queue = new LinkedList<>();
        private final Lock lock = new ReentrantLock();
        private final Condition notEmpty = lock.newCondition();
        private boolean noMoreTasks = false;

        // Add a task to the queue and signal a waiting worker
        public void addTask(T task) {
            lock.lock();
            try {
                queue.add(task);
                notEmpty.signal(); // wake one waiting worker
            } finally {
                lock.unlock();
            }
        }

        /**
         * Retrieve a task, or return null if no more tasks will arrive and queue is empty.
         * This method blocks while queue is empty and noMoreTasks == false.
         */
        public T getTask() throws InterruptedException {
            lock.lock();
            try {
                while (queue.isEmpty() && !noMoreTasks) {
                    notEmpty.await();
                }
                if (queue.isEmpty() && noMoreTasks) {
                    return null; // no tasks left and producer finished
                }
                return queue.remove();
            } finally {
                lock.unlock();
            }
        }

        // Tell queue there will be no more tasks and wake all waiting workers
        public void setNoMoreTasks() {
            lock.lock();
            try {
                noMoreTasks = true;
                notEmpty.signalAll();
            } finally {
                lock.unlock();
            }
        }

        // Optional helper: check size (not used critically)
        public int size() {
            lock.lock();
            try {
                return queue.size();
            } finally {
                lock.unlock();
            }
        }
    }

    /**
     * Worker runnable that retrieves tasks from shared queue, processes them,
     * and writes results to shared results list.
     */
    static class Worker implements Runnable {
        private final int workerId;
        private final SharedTaskQueue<Task> queue;
        private final List<String> results; // thread-safe wrapper expected
        private final Logger logger;
        private final Random rnd = new Random();

        public Worker(int workerId, SharedTaskQueue<Task> queue, List<String> results, Logger logger) {
            this.workerId = workerId;
            this.queue = queue;
            this.results = results;
            this.logger = logger;
        }

        @Override
        public void run() {
            logger.info(() -> String.format("Worker-%d started at %s", workerId, Instant.now()));
            try {
                while (true) {
                    Task task;
                    try {
                        task = queue.getTask();
                    } catch (InterruptedException e) {
                        // Proper handling of interrupt: log and exit thread
                        logger.log(Level.WARNING, "Worker-" + workerId + " interrupted while waiting for task", e);
                        Thread.currentThread().interrupt();
                        break;
                    }

                    if (task == null) {
                        // null indicates queue finished and empty -> clean shutdown for this worker
                        logger.info(() -> String.format("Worker-%d: no more tasks, shutting down", workerId));
                        break;
                    }

                    // Process task with simulated work and robust exception handling
                    try {
                        String result = processTask(task);
                        // store result (results is expected to be synchronized list)
                        results.add(result);
                        logger.info(() -> String.format("Worker-%d completed %s", workerId, task.getId()));
                    } catch (Exception e) {
                        // Catch any unexpected processing error and log
                        logger.log(Level.SEVERE, "Worker-" + workerId + " error processing task " + task.getId(), e);
                        // Optionally store failure marker
                        results.add(String.format("Task %s FAILED by Worker-%d: %s", task.getId(), workerId, e.getMessage()));
                    }
                }
            } finally {
                logger.info(() -> String.format("Worker-%d terminated at %s", workerId, Instant.now()));
            }
        }

        // Simulate computation work with delay; can throw runtime exceptions if desired
        private String processTask(Task task) throws Exception {
            // Simulate variable processing time (100-600 ms)
            int sleepMs = 100 + rnd.nextInt(500);
            try {
                Thread.sleep(sleepMs);
            } catch (InterruptedException e) {
                // restore interrupt status and throw to upper layer for handling
                Thread.currentThread().interrupt();
                throw new InterruptedException("Processing interrupted");
            }

            // Simulated chance of processing error (e.g., 5% failure)
            if (rnd.nextInt(100) < 5) {
                throw new RuntimeException("Simulated processing error for " + task.getId());
            }

            // Return a formatted result
            return String.format("TaskID=%s processedBy=Worker-%d durationMs=%d payload=%s", task.getId(), workerId, sleepMs, task.getPayload());
        }
    }

    // Configure logger for console + optionally file handlers
    private static Logger setupLogger() {
        Logger logger = Logger.getLogger("DataProcessingSystem");
        logger.setUseParentHandlers(false); // we'll set our own handlers
        logger.setLevel(Level.INFO);

        // Clear existing handlers
        for (Handler h : logger.getHandlers()) {
            logger.removeHandler(h);
        }

        // Console handler
        ConsoleHandler console = new ConsoleHandler();
        console.setLevel(Level.INFO);
        console.setFormatter(new SimpleFormatter());
        logger.addHandler(console);

        // Optionally you could add a FileHandler here to persist logs.
        return logger;
    }

    public static void main(String[] args) {
        Logger logger = setupLogger();

        final int NUM_WORKERS = 4;
        final int NUM_TASKS = 30;
        final String OUTPUT_FILE = "results.txt";

        logger.info("Main: Starting DataProcessingSystem");

        // Shared queue and results list
        SharedTaskQueue<Task> sharedQueue = new SharedTaskQueue<>();
        List<String> results = Collections.synchronizedList(new ArrayList<>());

        // Create thread pool
        ExecutorService workers = Executors.newFixedThreadPool(NUM_WORKERS);

        // Start worker runnables
        for (int i = 0; i < NUM_WORKERS; i++) {
            workers.submit(new Worker(i + 1, sharedQueue, results, logger));
        }

        // Producer: add tasks (this could be another thread; here we create tasks in main)
        try {
            for (int i = 0; i < NUM_TASKS; i++) {
                Task t = new Task("T-" + (i + 1), "payload-" + (i + 1));
                sharedQueue.addTask(t);
                logger.fine(() -> "Main: added " + t);
                // Optional: simulate variable arrival time
                try {
                    Thread.sleep(20);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    logger.log(Level.WARNING, "Main interrupted while adding tasks", e);
                    break;
                }
            }
        } finally {
            // Notify queue there will be no more tasks so workers can stop after consumption
            sharedQueue.setNoMoreTasks();
        }

        // Shutdown workers and wait for completion
        workers.shutdown();
        try {
            if (!workers.awaitTermination(60, TimeUnit.SECONDS)) {
                logger.warning("Main: workers didn't finish in 60 seconds, forcing shutdown");
                workers.shutdownNow();
            }
        } catch (InterruptedException e) {
            logger.log(Level.WARNING, "Main interrupted while waiting for workers to finish", e);
            workers.shutdownNow();
            Thread.currentThread().interrupt();
        }

        // At this point all workers are done (or forcibly shutdown)
        logger.info("Main: all workers finished. Total results collected: " + results.size());

        // Write results to file, with try-catch for IOException
        try (BufferedWriter bw = new BufferedWriter(new FileWriter(OUTPUT_FILE))) {
            // We synchronize while iterating a synchronized list to avoid concurrent modification
            synchronized (results) {
                for (String r : results) {
                    bw.write(r);
                    bw.newLine();
                }
            }
            logger.info("Main: results written to " + OUTPUT_FILE);
        } catch (IOException ioe) {
            logger.log(Level.SEVERE, "Main: I/O error while writing results to file", ioe);
        }

        logger.info("Main: DataProcessingSystem finished.");
    }
}
