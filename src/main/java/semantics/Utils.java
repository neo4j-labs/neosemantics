package semantics;

import apoc.Pools;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Transaction;
import org.neo4j.kernel.internal.GraphDatabaseAPI;

import java.util.concurrent.Callable;

/**
 * Created by jbarrasa on 02/06/2016.
 */
public class Utils {

    public static <T> T inTx(GraphDatabaseService db, Callable<T> callable) {
        try {
            return Pools.DEFAULT.submit(() -> {
                try (Transaction tx = db.beginTx()) {
                    T result = callable.call();
                    tx.success();
                    return result;
                }
            }).get();
        } catch (Exception e) {
            throw new RuntimeException("Error executing in separate transaction", e);
        }
    }
}
