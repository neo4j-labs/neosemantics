package n10s.mapping;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.util.HashMap;
import java.util.Map;

import n10s.graphconfig.GraphConfigProcedures;
import n10s.nsprefixes.NsPrefixDefProcedures;
import n10s.rdf.load.AddProcedures;
import n10s.rdf.load.RDFLoadProcedures;
import org.junit.*;
import org.neo4j.driver.*;
import org.neo4j.driver.Record;
import org.neo4j.harness.junit.rule.Neo4jRule;

public class MappingUtilsTest {
  public static Driver driver;

  @ClassRule
  public static Neo4jRule neo4j = new Neo4jRule()
          .withProcedure(MappingUtils.class)
          .withFunction(MappingUtils.class)
          .withProcedure(NsPrefixDefProcedures.class);

  @BeforeClass
  public static void init() {
    driver = GraphDatabase.driver(neo4j.boltURI(),
            Config.builder().withoutEncryption().build());
  }

  @Before
  public void cleanDatabase() {
    driver.session().run("match (n) detach delete n").consume();
    driver.session().run("drop constraint n10s_unique_uri if exists").consume();
  }


  @Test
  public void testaddSchema() throws Exception {
    try (Session session = driver.session()) {
      assertTrue(session.run(" call n10s.nsprefixes.add(\"v1\",\"http://vocabularies.com/vocabulary1/\")").hasNext());
    }
    try (Session session = driver.session();) {

      Map<String, Object> params = new HashMap<>();
      params.put("vocElem", "http://voc.new/sch1#something");
      params.put("graphElem", "ST");

      try {
        String addSchemaCypher = "CALL n10s.mapping.add($vocElem,$graphElem)";
        Record next = session.run(addSchemaCypher, params).next();
        assertFalse(true);
      } catch(Exception e) {
        assertTrue(e.getMessage().contains("No namespace prefix defined for vocabulary http://voc.new/sch1#."));
      }
    }
  }

  @Test
  public void testAddMappingToSchema() throws Exception {
    try (Session session = driver.session();) {
      assertTrue(session.run(" call n10s.nsprefixes.add(\"sch1\",\"http://schemas.com/schema1/\")").hasNext());
    }
      Session session = driver.session();

      // when DB is empty
      assertFalse(session.run("MATCH (n) WHERE not n:_NsPrefDef RETURN n").hasNext());

      String key = "RELATIONSHIP_NAME";
      String nameInVoc = "http://schemas.com/schema1/relationshipInSchema1";

      Map<String, Object> params = new HashMap<>();
      params.put("ns","http://schemas.com/schema1/");
      params.put("graphElemName", key);
      params.put("vocElem", nameInVoc);
      String addMappingAndSchemaCypher =
          " CALL n10s.mapping.add($vocElem, $graphElemName) ";
      Record next = session.run(addMappingAndSchemaCypher, params).next();
      assertEquals(key, next.get("elemName").asString());
      assertTrue(nameInVoc.endsWith(next.get("schemaElement").asString()));
      // check mapping is linked to the schema
      String existMappingAndNs = "MATCH (mns:_MapNs { _ns: $ns } )<-[:_IN]-" +
          "(elem:_MapDef { _key : $graphElemName }) WHERE $vocElem contains elem._local"
          + " RETURN mns, elem ";
      assertTrue(session.run(existMappingAndNs, params).hasNext());

      //overwriting a mapping
      String alternativeNameInVoc = "http://schemas.com/schema1/differentRelationshipInSchema1";
      params.put("vocElem", alternativeNameInVoc);
      String addMappingToExistingSchemaCypher =
          " CALL n10s.mapping.add($vocElem,$graphElemName) ";
      next = session.run(addMappingToExistingSchemaCypher, params).next();
      assertEquals(key, next.get("elemName").asString());
      assertTrue(alternativeNameInVoc.endsWith(next.get("schemaElement").asString()));
      // check mapping is linked to the schema
      assertTrue(session.run(existMappingAndNs, params).hasNext());
      params.put("vocElem", nameInVoc);
      assertFalse(session.run(existMappingAndNs, params).hasNext());
  }

  @Test
  public void testListMappings() throws Exception {
    try (Session session = driver.session();) {
      assertTrue(session.run(" call n10s.nsprefixes.add(\"sch\",\"http://schema.org/\")").hasNext());
    }
    Session session = driver.session();

      // when DB is empty
      assertFalse(session.run("MATCH (n) WHERE not n:_NsPrefDef RETURN n").hasNext());

      assertTrue(session.run("call n10s.mapping.add(\"http://schema.org/Movie\",\"Movie\")").hasNext());
      assertTrue(session.run("call n10s.mapping.add(\"http://schema.org/name\",\"name\")").hasNext());
      assertTrue(session.run("call n10s.mapping.add(\"http://schema.org/Person\",\"Person\")").hasNext());

      assertEquals(3, session
          .run("CALL n10s.mapping.list() yield elemName RETURN count(elemName) as ct ")
          .next().get("ct").asInt());
      assertEquals(1, session.run(
          "CALL n10s.mapping.list('Person') yield elemName RETURN count(elemName) as ct ")
          .next().get("ct").asInt());
  }

  @Test
  public void testDropMappings() throws Exception {
    try (Session session = driver.session();) {
      assertTrue(session.run(" call n10s.nsprefixes.add(\"sch\",\"http://schema.org/\")").hasNext());
    }
      Session session = driver.session();

      // when DB is empty
      assertFalse(session.run("MATCH (n) WHERE not n:_NsPrefDef RETURN n").hasNext());

      assertTrue(session.run("call n10s.mapping.add(\"http://schema.org/Movie\",\"Movie\")").hasNext());
      assertTrue(session.run("call n10s.mapping.add(\"http://schema.org/Person\",\"Person\")").hasNext());
      assertTrue(session.run("call n10s.mapping.add(\"http://schema.org/name\",\"name\")").hasNext());

      assertEquals("mapping successfully deleted",
          session.run("CALL n10s.mapping.drop('Movie')").next().get("output")
              .asString());
      assertEquals(2, session
          .run("CALL n10s.mapping.list() yield elemName RETURN count(elemName) as ct ")
          .next().get("ct").asInt());
  }

  @Test
  public void testDropMappingsAndSchemas() throws Exception {
    try (Session session = driver.session();) {
      assertTrue(session.run(" call n10s.nsprefixes.add(\"sch1\",\"http://schemas.com/schema1/\")").hasNext());
    }
      Session session = driver.session();

      // when DB is empty
      assertFalse(session.run("MATCH (n) WHERE not n:_NsPrefDef RETURN n").hasNext());

      String key = "RELATIONSHIP_NAME";
      String nameInVoc = "http://schemas.com/schema1/relationshipInSchema1";

      Map<String, Object> params = new HashMap<>();
      params.put("graphElemName", key);
      params.put("vocElem", nameInVoc);
      String addMappingAndSchemaCypher =
          " CALL n10s.mapping.add($vocElem, $graphElemName) ";
      Record next = session.run(addMappingAndSchemaCypher, params).next();
      assertEquals("sch1", next.get("schemaPrefix").asString());
      assertTrue(session.run("CALL n10s.mapping.add('http://schemas.com/schema1/abc', 'ABC') ").hasNext());
      assertTrue(session.run("CALL n10s.mapping.add('http://schemas.com/schema1/def', 'DEF') ").hasNext());

      assertEquals("mapping not found",
          session.run("CALL n10s.mapping.drop('doesnotexist')").next().get("output")
              .asString());
      assertEquals(3, session
          .run("CALL n10s.mapping.list() yield elemName RETURN count(elemName) as ct ")
          .next().get("ct").asInt());
      assertEquals("mapping successfully deleted",
          session.run("CALL n10s.mapping.drop('ABC')").next()
              .get("output").asString());
      assertEquals(2, session
          .run("CALL n10s.mapping.list() yield elemName RETURN count(elemName) as ct ")
          .next().get("ct").asInt());
      assertEquals("successfully deleted schema (and mappings)",
          session.run("CALL n10s.mapping.dropAll('http://schemas.com/schema1/')").next()
              .get("output").asString());
      assertEquals(0, session
          .run("CALL n10s.mapping.list() yield elemName RETURN count(elemName) as ct ")
          .next().get("ct").asInt());
  }

  @Test
  public void testBug12931() throws Exception {
    Session session = driver.session();
      String step1 = "CALL n10s.nsprefixes.add(\"a\",\"neo4j://vocs/a/\")";
      String step2 = "CALL n10s.mapping.add(\"neo4j://vocs/a/something\",\"something\")";
      String step3 = "CALL n10s.mapping.add(\"neo4j://vocs/a/other\",\"other\")";
      String step4 = "CALL n10s.mapping.add(\"neo4j://vocs/a/something\",\"somethingElse\")";

      session.run(step1);
      session.run(step2);
      session.run(step3);
      session.run(step4);

      assertEquals(2L,session.run("call n10s.mapping.list() yield schemaElement\n" +
              "return count(*) as mappingcount").next().get("mappingcount").asLong());
      assertEquals(1L,session.run("call n10s.nsprefixes.list() yield namespace\n" +
              "return count(*) as namespacecount").next().get("namespacecount").asLong());

    }
}
