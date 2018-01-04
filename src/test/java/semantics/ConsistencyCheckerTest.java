package semantics;

import org.junit.Rule;
import org.junit.Test;
import org.neo4j.driver.v1.*;
import org.neo4j.harness.junit.Neo4jRule;

import semantics.ConsistencyChecker;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 * Created by jbarrasa on 21/03/2016.
 */
public class ConsistencyCheckerTest {

    @Rule
    public Neo4jRule neo4j = new Neo4jRule()
            .withProcedure( ConsistencyChecker.class );

    @Test
    public void runConsistencyChecks() throws Exception {

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

        try (Driver driver = GraphDatabase.driver(neo4j.boltURI(), Config.build().withEncryptionLevel(Config.EncryptionLevel.NONE).toConfig())) {

            Session session = driver.session();

            StatementResult importResults1 = session.run(dbInit);

            StatementResult res = session.run("CALL semantics.runConsistencyChecks()");
            assertFalse(res.hasNext());

            session.run("CREATE (n{ name: 'An unlabeled node with a name. Inconsistency!!'}) return id(n) as created");

            StatementResult res2 = session.run("CALL semantics.runConsistencyChecks()");
            assertTrue(res2.hasNext());
        }
    }

}
