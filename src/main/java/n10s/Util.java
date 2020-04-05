package n10s;

import com.google.common.base.Preconditions;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.RejectedExecutionHandler;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.LockSupport;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

import n10s.quadrdf.RDFQuadDirectStatementDeleter;
import n10s.quadrdf.RDFQuadDirectStatementLoader;
import org.neo4j.graphdb.*;

import javax.lang.model.SourceVersion;

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
      throw new RuntimeException("Error executing in separate transaction: " + e.getMessage(), e);
    }
  }

  public static <T> Future<T> inTxFuture(ExecutorService pool, GraphDatabaseService db,
      Callable<T> callable) {
    try {
      return pool.submit(() -> {
        try (Transaction tx = db.beginTx()) {
          T result = callable.call();
          tx.commit();
          return result;
        }
      });
    } catch (Exception e) {
      throw new RuntimeException("Error executing in separate transaction", e);
    }
  }

  public final static ExecutorService DEFAULT = createDefaultPool();

  public static ExecutorService createDefaultPool() {
    int threads = Runtime.getRuntime().availableProcessors() * 2;
    int queueSize = threads * 25;
    return new ThreadPoolExecutor(threads / 2, threads, 30L, TimeUnit.SECONDS,
        new ArrayBlockingQueue<>(queueSize),
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

  public static String labelString(List<String> labelNames) {
    return labelNames.stream().map(Util::quote).collect(Collectors.joining(":"));
  }

  public static String labelString(Node n) {
    return joinLabels(n.getLabels(), ":");
  }
  public static String joinLabels(Iterable<Label> labels, String s) {
    return StreamSupport.stream(labels.spliterator(), false).map(Label::name).collect(Collectors.joining(s));
  }
  public static List<String> labelStrings(Node n) {
    return StreamSupport.stream(n.getLabels().spliterator(),false).map(Label::name).sorted().collect(Collectors.toList());
  }

  public static String quote(String var) {
    return SourceVersion.isIdentifier(var) ? var : '`' + var + '`';
  }

  public static Label[] labels(Object labelNames) {
    if (labelNames == null) {
      return NO_LABELS;
    }
    if (labelNames instanceof List) {
      List names = (List) labelNames;
      Label[] labels = new Label[names.size()];
      int i = 0;
      for (Object l : names) {
        if (l == null) {
          continue;
        }
        labels[i++] = Label.label(l.toString());
      }
      if (i <= labels.length) {
        return Arrays.copyOf(labels, i);
      }
      return labels;
    }
    return new Label[]{Label.label(labelNames.toString())};
  }

  /**
   * @param key the resource to load
   * @return a {@link Callable} to retrieve a {@link Node}, which can be {@code null}
   * @author Emre Arkan
   *
   * Created on 02.07.2019
   * @see RDFQuadDirectStatementLoader
   * @see RDFQuadDirectStatementDeleter
   */
  static Callable<Node> loadNode(ContextResource key, GraphDatabaseService graphdb) {
    return () -> {
      Node node = null;
      Map<String, Object> params = new HashMap<>();
      String cypher = buildCypher(key.getUri(),
          key.getGraphUri(),
          params);
      Result result = graphdb.executeTransactionally(cypher, params, new ResultTransformer<Result>() {
        @Override
        public Result apply(Result result) {
          return result;
        }
      });

      if (result.hasNext()) {
        node = (Node) result.next().get("n");
        if (result.hasNext()) {
          String props =
              "{uri: " + key.getUri() +
                  (key.getGraphUri() == null ? "}" :
                      ", graphUri: " + key.getGraphUri() + "}");
          throw new IllegalStateException(
              "There are multiple matching nodes for the given properties " + props);
        }
      }
      return node;
    };
  }

  /**
   * @param uri the uri of the searched node
   * @param graphUri the graph uri of the searched node
   * @param params parameters of the query
   * @return a {@link String} of the Cypher query to be executed
   * @author Emre Arkan
   *
   * Created on 02.07.2019
   */
  private static String buildCypher(String uri, String graphUri, Map<String, Object> params) {
    Preconditions.checkNotNull(uri);
    StringBuilder cypher = new StringBuilder();
    params.put("uri", uri);
    cypher.append("MATCH (n:Resource) ");
    cypher.append("WHERE n.uri = $uri ");
    if (graphUri != null) {
      cypher.append("AND n.graphUri = $graphUri ");
      params.put("graphUri", graphUri);
    } else {
      cypher.append("AND NOT EXISTS(n.graphUri) ");
    }
    cypher.append("RETURN n");
    return cypher.toString();
  }
}
