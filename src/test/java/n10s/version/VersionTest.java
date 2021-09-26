package n10s.version;

import org.junit.Rule;
import org.junit.Test;
import org.neo4j.driver.*;
import org.neo4j.harness.junit.rule.Neo4jRule;

import java.util.Map;

import static org.junit.Assert.assertEquals;

public class VersionTest {

    @Rule
    public Neo4jRule neo4j = new Neo4jRule()
            .withFunction(Version.class);


    //TODO: Don't know how to test this
//    @Test
//    public void testGetVersion()throws Exception {
//        try (Driver driver = GraphDatabase.driver(neo4j.boltURI(),
//                Config.builder().withoutEncryption().build()); Session session = driver.session()) {
//
//            Result importResults
//                    = session.run("RETURN n10s.version() as ver ");
//
//            Map<String, Object> singleResult = importResults
//                    .single().asMap();
//
//            assertEquals("3.4", singleResult.get("ver"));
//
//        }
//
//    }


}
