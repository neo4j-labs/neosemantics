package n10s.graphconfig;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import n10s.rdf.load.RDFLoadProcedures;
import org.junit.Rule;
import org.junit.Test;
import org.neo4j.driver.*;
import org.neo4j.harness.junit.rule.Neo4jRule;

public class GraphConfigProceduresTest {

  @Rule
  public Neo4jRule neo4j = new Neo4jRule()
      .withProcedure(GraphConfigProcedures.class).withProcedure(RDFLoadProcedures.class);

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
      results = session
          .run("CALL n10s.graphconfig.set({classLabel: '" + value + "'}) yield param, value "
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

      results = session.run("CALL n10s.graphconfig.init( { baseSchemaNamespace: 'http://base#' }) yield param, value "
              + " with param, value where param = 'baseSchemaNamespace' return param, value ");
      assertEquals(true, results.hasNext());
      assertEquals("http://base#", results.next().get("value").asString());

      results = session.run("CALL n10s.graphconfig.show() yield param, value "
              + " with param, value where param = 'baseSchemaNamespace' return param, value ");
      assertEquals(true, results.hasNext());
      assertEquals("http://base#", results.next().get("value").asString());

    }
  }

  @Test
  public void testModifyGraphConfigWhenDataInGraph() throws Exception {
    try (Driver driver = GraphDatabase.driver(neo4j.boltURI(),
        Config.builder().withoutEncryption().build())) {

      Session session = driver.session();

      session.run("CREATE CONSTRAINT n10s_unique_uri "
          + "ON (r:Resource) ASSERT r.uri IS UNIQUE");

      Result results = session.run("CALL n10s.graphconfig.init() yield param, value "
          + " with param, value where param = 'classLabel' return param, value ");
      assertTrue(results.hasNext());
      String rdfSnippet = "<http://indiv/a> <http://voc/pred> \"123\" .";
      results = session.run("CALL n10s.rdf.import.inline('" + rdfSnippet + "','N-Triples')");
      assertTrue(results.hasNext());
      assertEquals(1, results.next().get("triplesLoaded").asInt());

      try {
        results = session.run("CALL n10s.graphconfig.drop()");
        assertFalse(results.hasNext());
        assertTrue(false);
      } catch (Exception e) {
        assertTrue(e.getMessage().contains("n10s.graphconfig.GraphConfigProcedures$"
            + "GraphConfigException: The graph is non-empty. Config cannot be changed."));
      }


    }
  }

  @Test
  public void testForceModifyGraphConfigWhenDataInGraph() throws Exception {
    try (Driver driver = GraphDatabase.driver(neo4j.boltURI(),
            Config.builder().withoutEncryption().build())) {

      Session session = driver.session();

      session.run("CREATE CONSTRAINT n10s_unique_uri "
              + "ON (r:Resource) ASSERT r.uri IS UNIQUE");

      Result results = session.run("CALL n10s.graphconfig.init() yield param, value "
              + " with param, value where param = 'classLabel' return param, value ");
      assertTrue(results.hasNext());
      String rdfSnippet = "<http://indiv/a> <http://voc/pred> \"123\" .";
      results = session.run("CALL n10s.rdf.import.inline('" + rdfSnippet + "','N-Triples')");
      assertTrue(results.hasNext());
      assertEquals(1, results.next().get("triplesLoaded").asInt());

      try {
        results = session.run("CALL n10s.graphconfig.set({ handleMultival: 'ARRAY' })");
        assertFalse(results.hasNext());
        assertTrue(false);
      } catch (Exception e) {
        assertTrue(e.getMessage().contains("n10s.graphconfig.GraphConfigProcedures$"
                + "GraphConfigException: The graph is non-empty. Config cannot be changed."));
      }

      assertEquals("OVERWRITE", session.run("call n10s.graphconfig.show() yield param, value\n" +
              "where param = 'handleMultival' return value ").next().get("value").asString());

      results = session.run("CALL n10s.graphconfig.set({ force: true, handleMultival: 'ARRAY' })");
      assertTrue(results.hasNext());
      while(results.hasNext()){
        Record paramVal = results.next();
        if(paramVal.get("param").equals("handleMultival")){
          assertEquals("ARRAY",paramVal.get("value"));
        }
      }



    }
  }

  @Test
  public void testDropGraph() throws Exception {
    try (Driver driver = GraphDatabase.driver(neo4j.boltURI(),
        Config.builder().withoutEncryption().build())) {

      Session session = driver.session();

      Result results = session.run("CALL n10s.graphconfig.init() yield param, value "
          + " with param, value where param = 'classLabel' return param, value ");
      assertTrue(results.hasNext());
      results = session.run("CALL n10s.graphconfig.drop()");
      assertFalse(results.hasNext());

      results = session.run("MATCH (gc:_GraphConfig) return gc");
      assertFalse(results.hasNext());


    }
  }
}
