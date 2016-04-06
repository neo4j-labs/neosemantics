package semantics;

import org.junit.Test;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Result;
import org.neo4j.kernel.impl.proc.Procedures;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.test.TestGraphDatabaseFactory;

import static org.junit.Assert.*;

/**
 * Created by jbarrasa on 21/03/2016.
 */
public class RDFImportTest {

    @Test
    public void testDegree() throws Exception {
        GraphDatabaseService db = new TestGraphDatabaseFactory().newImpermanentDatabase();
        ((GraphDatabaseAPI)db).getDependencyResolver().resolveDependency(Procedures.class).register(ConsistencyChecker.class);
        // given Alice knowing Bob and Charlie and Dan knowing no-one
        db.execute("create " +
                "(person_class:Class {\turi:\"http://neo4j.com/voc/movies#Person\", \n" +
                "\t\t\tlabel:\"Person\", \n" +
                "\t\t\tcomment:\"Individual involved in the film industry\"})," +
                "(name_dtp:DatatypeProperty {\turi:\"http://neo4j.com/voc/movies#name\", \n" +
                "\t\t\t\tlabel:\"name\",\n" +
                "\t\t\t\tcomment :\"A person's name\"}),\n" +
                "(name_dtp)-[:DOMAIN]->(person_class)").close();

        // when retrieving the degree of the User label
        Result res = db.execute("CALL semantics.runConsistencyChecks()");

        // then we expect one result-row with min-degree 0 and max-degree 2
        assertFalse(res.hasNext());

        db.execute("CREATE ({ name: 'An unlabeled node with a name. Inconsistency!!'})");
        res = db.execute("CALL semantics.runConsistencyChecks()");
        assertTrue(res.hasNext());

//        Map<String,Object> row = res.next();
//        System.out.println(row);
//        assertEquals("User", row.get("label"));
//        // Dan has no friends
//        assertEquals(0, row.get("min"));
//        // Alice knows 2 people
//        assertEquals(2, row.get("max"));
//        // We have 4 nodes in our graph
//        assertEquals(4, row.get("count"));
//        // only one result record was produced
//        assertFalse(res.hasNext());
    }

}
