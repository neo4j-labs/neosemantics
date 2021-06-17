package n10s.version;

import n10s.RDFProceduresTest;
import n10s.graphconfig.GraphConfigProcedures;
import n10s.inference.MicroReasoners;
import n10s.similarity.Similarities;
import org.junit.Rule;
import org.junit.Test;
import org.neo4j.driver.*;
import org.neo4j.harness.junit.rule.Neo4jRule;

import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class VersionTest {

    @Rule
    public Neo4jRule neo4j = new Neo4jRule()
            .withFunction(Version.class);


    @Test
    public void testOntoPreviewFromFileLimit()throws Exception {
        try (Driver driver = GraphDatabase.driver(neo4j.boltURI(),
                Config.builder().withoutEncryption().build()); Session session = driver.session()) {

            Result importResults
                    = session.run("RETURN n10s.version() as ver ");

            Map<String, Object> singleResult = importResults
                    .single().asMap();

            assertEquals("3.4", singleResult.get("ver"));

        }

    }


}
