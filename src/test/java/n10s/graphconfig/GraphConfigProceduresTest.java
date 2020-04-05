package n10s.graphconfig;

import static org.junit.Assert.assertEquals;

import org.junit.Rule;
import org.junit.Test;
import org.neo4j.driver.Config;
import org.neo4j.driver.Driver;
import org.neo4j.driver.GraphDatabase;
import org.neo4j.driver.Result;
import org.neo4j.driver.Session;
import org.neo4j.harness.junit.rule.Neo4jRule;

public class GraphConfigProceduresTest {

  @Rule
  public Neo4jRule neo4j = new Neo4jRule()
      .withProcedure(GraphConfigProcedures.class);

  @Test
  public void testInitGraphConfig() throws Exception {
    try (Driver driver = GraphDatabase.driver(neo4j.boltURI(),
        Config.builder().withoutEncryption().build())) {

      Session session = driver.session();

      Result results = session.run("CALL n10s.graphconfig.init() yield param, value "
          + " with param, value where param = 'classLabel' return param, value ");
      assertEquals(true, results.hasNext());
      assertEquals("Class", results.next().get("value").asString());

      results = session.run("CALL n10s.graphconfig.show() yield param, value "
          + " with param, value where param = 'classLabel' return param, value ");
      assertEquals(true, results.hasNext());
      assertEquals("Class", results.next().get("value").asString());

      String value = "CATEGORY";
      results = session.run("CALL n10s.graphconfig.set({classLabel: '" + value + "'}) yield param, value "
          + " with param, value where param = 'classLabel' return param, value ");
      assertEquals(true, results.hasNext());
      assertEquals(value, results.next().get("value").asString());

      results = session.run("CALL n10s.graphconfig.show() yield param, value "
          + " with param, value where param = 'classLabel' return param, value ");
      assertEquals(true, results.hasNext());
      assertEquals(value, results.next().get("value").asString());

      results = session.run("CALL n10s.graphconfig.init() yield param, value "
          + " with param, value where param = 'classLabel' return param, value ");
      assertEquals(true, results.hasNext());
      assertEquals("Class", results.next().get("value").asString());

      results = session.run("CALL n10s.graphconfig.show() yield param, value "
          + " with param, value where param = 'classLabel' return param, value ");
      assertEquals(true, results.hasNext());
      assertEquals("Class", results.next().get("value").asString());

    }
  }

}
