package semantics;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Transaction;

import java.util.Arrays;
import java.util.List;
import java.util.concurrent.*;
import java.util.concurrent.locks.LockSupport;

/**
 * @author mh
 * @since 04.07.17
 */
public class Util {

    private static final Label[] NO_LABELS = new Label[0];

    public static <T> T inTx(GraphDatabaseService db, Callable<T> callable) {
        try {
            return inTxFuture(DEFAULT, db, callable).get();
        } catch (RuntimeException e) {
            throw e;
        } catch (Exception e) {
            throw new RuntimeException("Error executing in separate transaction: "+e.getMessage(), e);
        }
    }
    public static <T> Future<T> inTxFuture(ExecutorService pool, GraphDatabaseService db, Callable<T> callable) {
        try {
            return pool.submit(() -> {
                try (Transaction tx = db.beginTx()) {
                    T result = callable.call();
                    tx.success();
                    return result;
                }
            });
        } catch (Exception e) {
            throw new RuntimeException("Error executing in separate transaction", e);
        }
    }

    public final static ExecutorService DEFAULT = createDefaultPool();

    public static ExecutorService createDefaultPool() {
        int threads = Runtime.getRuntime().availableProcessors()*2;
        int queueSize = threads * 25;
        return new ThreadPoolExecutor(threads / 2, threads, 30L, TimeUnit.SECONDS, new ArrayBlockingQueue<>(queueSize),
                new CallerBlocksPolicy());
//                new ThreadPoolExecutor.CallerRunsPolicy());
    }

    static class CallerBlocksPolicy implements RejectedExecutionHandler {
        @Override
        public void rejectedExecution(Runnable r, ThreadPoolExecutor executor) {
            if (!executor.isShutdown()) {
                // block caller for 100ns
                LockSupport.parkNanos(100);
                try {
                    // submit again
                    executor.submit(r).get();
                } catch (InterruptedException | ExecutionException e) {
                    throw new RuntimeException(e);
                }
            }
        }
    }
    public static Label[] labels(Object labelNames) {
        if (labelNames==null) return NO_LABELS;
        if (labelNames instanceof List) {
            List names = (List) labelNames;
            Label[] labels = new Label[names.size()];
            int i = 0;
            for (Object l : names) {
                if (l==null) continue;
                labels[i++] = Label.label(l.toString());
            }
            if (i <= labels.length) return Arrays.copyOf(labels,i);
            return labels;
        }
        return new Label[]{Label.label(labelNames.toString())};
    }
}
