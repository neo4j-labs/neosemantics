package semantics;

import org.junit.Test;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Result;
import org.neo4j.kernel.impl.proc.Procedures;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.test.TestGraphDatabaseFactory;

import java.util.Map;

import static org.junit.Assert.*;

/**
 * Created by jbarrasa on 21/03/2016.
 */
public class ConsistencyCheckerTest {

    @Test
    public void runConsistencyChecks() throws Exception {
        GraphDatabaseService db = new TestGraphDatabaseFactory().newImpermanentDatabase();
        ((GraphDatabaseAPI)db).getDependencyResolver().resolveDependency(Procedures.class).register(ConsistencyChecker.class);

        String dbInit = "create " +
                "(person_class:Class {\turi:\"http://neo4j.com/voc/movies#Person\", \n" +
                "\t\t\tlabel:\"Person\", \n" +
                "\t\t\tcomment:\"Individual involved in the film industry\"})," +
                "(name_dtp:DatatypeProperty {\turi:\"http://neo4j.com/voc/movies#name\", \n" +
                "\t\t\t\tlabel:\"name\",\n" +
                "\t\t\t\tcomment :\"A person's name\"}),\n" +
                "(actor_class:Class {	uri:\"http://neo4j.com/voc/movies#Actor\"," +
                "\t\t\tlabel:\"Actor\"})," +
                "(name_dtp)-[:DOMAIN]->(person_class),\n" +
                "(name_dtp)-[:DOMAIN]->(actor_class)";
        db.execute(dbInit).close();

        Result res = db.execute("CALL semantics.runConsistencyChecks()");
        assertFalse(res.hasNext());

        db.execute("CREATE (n{ name: 'An unlabeled node with a name. Inconsistency!!'}) return id(n) as created");
        //System.out.println(created);
        Result res2 = db.execute("CALL semantics.runConsistencyChecks()");
        assertTrue(res2.hasNext());
    }

}
