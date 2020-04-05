package n10s.mapping;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.util.HashMap;
import java.util.Map;
import org.junit.Rule;
import org.junit.Test;
import org.neo4j.driver.*;
import org.neo4j.driver.Config;
import org.neo4j.driver.Driver;
import org.neo4j.harness.junit.rule.Neo4jRule;

public class MappingUtilsTest {

  @Rule
  public Neo4jRule neo4j = new Neo4jRule()
      .withProcedure(MappingUtils.class).withFunction(MappingUtils.class);


  @Test
  public void testaddSchema() throws Exception {
    try (Driver driver = GraphDatabase.driver(neo4j.boltURI(),
        Config.builder().withoutEncryption().build())) {

      Session session = driver.session();

      //database is empty
      assertFalse(session.run("MATCH (n) RETURN n").hasNext());
      String vocUri = "http://vocabularies.com/vocabulary1/";
      String vocPrefix = "v1";
      Map<String, Object> params = new HashMap<>();
      params.put("ns", vocUri);
      params.put("prefix", vocPrefix);

      //add schema
      String addSchemaCypher = "CALL n10s.mapping.addSchema($ns,$prefix)";
      Record next = session.run(addSchemaCypher, params).next();
      assertEquals(vocPrefix, next.get("prefix").asString());
      assertEquals(vocUri, next.get("namespace").asString());

      //schema has been persisted
      String getSchemaByName = "MATCH (mns:_MapNs { _ns: $ns, _prefix : $prefix }) RETURN count(mns) as ct ";
      assertEquals(1, session.run(getSchemaByName, params).next().get("ct").asInt());

      //add same schema namespace with different prefix
      String alternativeVocPrefix = "v1alt";
      params.put("prefix", alternativeVocPrefix);
      try {
        session.run(addSchemaCypher, params).hasNext();
        //should never get here
        assertFalse(true);
      } catch (Exception e) {
        //expected
        assertTrue(e.getMessage().contains(
            "Caused by: n10s.mapping.MappingUtils$MappingDefinitionException: The schema URI or the prefix are already in use. Drop existing ones before reusing."));
      }

      //schema has not been changed in db
      assertEquals(0, session.run(getSchemaByName, params).next().get("ct").asInt());
      //old one still there
      params.put("prefix", vocPrefix);
      assertEquals(1, session.run(getSchemaByName, params).next().get("ct").asInt());

      //add an alternative schema namespace with a prefix already in use (default, no force overwrite)
      String alternativeVocUri = "http://vocabularies.com/vocabulary1alt/";
      params.put("ns", alternativeVocUri);
      try {
        session.run(addSchemaCypher, params).hasNext();
        //should never get here
        assertFalse(true);
      } catch (Exception e) {
        //expected
        assertTrue(e.getMessage().contains(
            "Caused by: n10s.mapping.MappingUtils$MappingDefinitionException: The schema URI or the prefix are already in use. Drop existing ones before reusing."));
      }

      //schema has not been changed in db
      assertEquals(0, session.run(getSchemaByName, params).next().get("ct").asInt());
      //old one still there
      params.put("ns", vocUri);
      assertEquals(1, session.run(getSchemaByName, params).next().get("ct").asInt());

    }
  }

  @Test
  public void testAddCommonSchemas() throws Exception {
    try (Driver driver = GraphDatabase.driver(neo4j.boltURI(),
            Config.builder().withoutEncryption().build())) {

      Session session = driver.session();

      //when DB is empty
      assertFalse(session.run("MATCH (n) RETURN n").hasNext());
      assertEquals(18, session.run(
          "CALL n10s.mapping.addCommonSchemas() YIELD namespace RETURN count(namespace) AS addedCount ")
          .next().get("addedCount").asInt());
      //Check that schema.org is there
      Map<String, Object> params = new HashMap<>();
      params.put("ns", "http://schema.org/");
      params.put("prefix", "sch");
      assertEquals(1, session
          .run("MATCH (mns:_MapNs { _ns: $ns, _prefix : $prefix }) RETURN count(mns) as ct ",
              params).next().get("ct").asInt());

      //if we run a second time, all schemas are in the DB so no changes
      assertEquals(0, session.run(
          "CALL n10s.mapping.addCommonSchemas() YIELD namespace RETURN count(namespace) AS addedCount ")
          .next().get("addedCount").asInt());

      //empty DB again
      session.run("MATCH (n) DETACH DELETE n");

      //and add a custom definition (prefix) for schema.org
      params.put("prefix", "myprefixforschemadotorg");
      String addSchemaCypher = "CALL n10s.mapping.addSchema($ns,$prefix)";
      session.run(addSchemaCypher, params);
      //only those not in use should be added
      assertEquals(17, session.run(
          "CALL n10s.mapping.addCommonSchemas() YIELD namespace RETURN count(namespace) AS addedCount ")
          .next().get("addedCount").asInt());
      //and the original custom definition for schema.org should still be in the DB
      assertEquals(1, session
          .run("MATCH (mns:_MapNs { _ns: $ns, _prefix : $prefix }) RETURN count(mns) as ct ",
              params).next().get("ct").asInt());
      //and the standard definition from the addcommons should not be there (it was ignored)
      params.put("prefix", "sch");
      assertEquals(0, session
          .run("MATCH (mns:_MapNs { _ns: $ns, _prefix : $prefix }) RETURN count(mns) as ct ",
              params).next().get("ct").asInt());
    }
  }

  @Test
  public void testListSchemas() throws Exception {
    try (Driver driver = GraphDatabase.driver(neo4j.boltURI(),
            Config.builder().withoutEncryption().build())) {

      Session session = driver.session();

      //when DB is empty
      assertFalse(session.run("MATCH (n) RETURN n").hasNext());
      assertFalse(session.run("CALL n10s.mapping.listSchemas()").hasNext());
      //add common schemas
      session.run("CALL n10s.mapping.addCommonSchemas()");
      Map<String, Object> params = new HashMap<>();
      params.put("searchString", "");
      String getSchemaMatchesQuery =
          "CALL n10s.mapping.listSchemas($searchString) YIELD namespace "
              + "RETURN COUNT(namespace) AS schemaCount";
      assertEquals(18,
          session.run(getSchemaMatchesQuery, params).next().get("schemaCount").asInt());
      params.put("searchString", "fibo");
      assertEquals(11,
          session.run(getSchemaMatchesQuery, params).next().get("schemaCount").asInt());
      params.put("searchString", "skos");
      assertEquals(1, session.run(getSchemaMatchesQuery, params).next().get("schemaCount").asInt());

    }
  }

  @Test
  public void testAddMappingToSchema() throws Exception {
    try (Driver driver = GraphDatabase.driver(neo4j.boltURI(),
            Config.builder().withoutEncryption().build())) {

      Session session = driver.session();

      // when DB is empty
      assertFalse(session.run("MATCH (n) RETURN n").hasNext());

      String key = "RELATIONSHIP_NAME";
      String localNameInVoc = "relationshipInSchema1";

      Map<String, Object> params = new HashMap<>();
      params.put("ns", "http://schemas.com/schema1/");
      params.put("prefix", "sch1");
      params.put("graphElemName", key);
      params.put("localVocElem", localNameInVoc);
      String addMappingAndSchemaCypher =
          " CALL n10s.mapping.addSchema($ns,$prefix) YIELD namespace AS sch " +
              " CALL n10s.mapping.addMappingToSchema($ns,$graphElemName, $localVocElem) "
              + "YIELD schemaElement, elemName RETURN schemaElement, elemName ";
      Record next = session.run(addMappingAndSchemaCypher, params).next();
      assertEquals(key, next.get("elemName").asString());
      assertEquals(localNameInVoc, next.get("schemaElement").asString());
      // check mapping is linked to the schema
      String existMappingAndNs = "MATCH (mns:_MapNs { _ns: $ns } )<-[:_IN]-" +
          "(elem:_MapDef { _key : $graphElemName, _local: $localVocElem }) RETURN mns, elem ";
      assertTrue(session.run(existMappingAndNs, params).hasNext());

      //overwriting a mapping
      String alternativeLocalNameInVoc = "differentRelationshipInSchema1";
      params.put("localVocElem", alternativeLocalNameInVoc);
      String addMappingToExistingSchemaCypher =
          " CALL n10s.mapping.listSchemas($ns) YIELD namespace AS sch " +
              " CALL n10s.mapping.addMappingToSchema($ns,$graphElemName, $localVocElem) "
              + " YIELD schemaElement, elemName RETURN schemaElement, elemName ";
      next = session.run(addMappingToExistingSchemaCypher, params).next();
      assertEquals(key, next.get("elemName").asString());
      assertEquals(alternativeLocalNameInVoc, next.get("schemaElement").asString());
      // check mapping is linked to the schema
      assertTrue(session.run(existMappingAndNs, params).hasNext());
      params.put("localVocElem", localNameInVoc);
      assertFalse(session.run(existMappingAndNs, params).hasNext());
    }
  }

  @Test
  public void testListMappings() throws Exception {
    try (Driver driver = GraphDatabase.driver(neo4j.boltURI(),
            Config.builder().withoutEncryption().build())) {

      Session session = driver.session();

      // when DB is empty
      assertFalse(session.run("MATCH (n) RETURN n").hasNext());

      String addMappingAndSchemaCypher =
          " call n10s.mapping.addSchema(\"http://schema.org/\",\"sch\") yield namespace as schema \n"
              +
              "call n10s.mapping.addMappingToSchema(\"http://schema.org/\",\"Movie\",\"Movie\") yield elemName  as mapping1\n"
              +
              "call n10s.mapping.addMappingToSchema(\"http://schema.org/\",\"Person\",\"Person\") yield elemName  as mapping2\n"
              +
              "call n10s.mapping.addMappingToSchema(\"http://schema.org/\",\"name\",\"name\") yield elemName  as mapping3\n"
              +
              "return *";
      assertTrue(session.run(addMappingAndSchemaCypher).hasNext());
      assertEquals(3, session
          .run("CALL n10s.mapping.listMappings() yield elemName RETURN count(elemName) as ct ")
          .next().get("ct").asInt());
      assertEquals(1, session.run(
          "CALL n10s.mapping.listMappings('Person') yield elemName RETURN count(elemName) as ct ")
          .next().get("ct").asInt());
      String updateMappingCypher =
          " call n10s.mapping.listSchemas('http://schema.org/') yield namespace as sch\n" +
              "call n10s.mapping.addMappingToSchema(\"http://schema.org/\",\"Movie\",\"MovieInSchemaDotOrg\") yield elemName \n"
              +
              " return * ";
      assertTrue(session.run(updateMappingCypher).hasNext());
      assertEquals(3, session
          .run("CALL n10s.mapping.listMappings() yield elemName RETURN count(elemName) as ct ")
          .next().get("ct").asInt());
    }
  }

  @Test
  public void testDropMappings() throws Exception {
    try (Driver driver = GraphDatabase.driver(neo4j.boltURI(),
            Config.builder().withoutEncryption().build())) {

      Session session = driver.session();

      // when DB is empty
      assertFalse(session.run("MATCH (n) RETURN n").hasNext());

      String addMappingAndSchemaCypher =
          " call n10s.mapping.addSchema(\"http://schema.org/\",\"sch\") yield namespace as sch\n"
              +
              "call n10s.mapping.addMappingToSchema(\"http://schema.org/\",\"Movie\",\"Movie\") yield elemName as mapping1\n"
              +
              "call n10s.mapping.addMappingToSchema(\"http://schema.org/\",\"Person\",\"Person\") yield elemName as mapping2\n"
              +
              "call n10s.mapping.addMappingToSchema(\"http://schema.org/\",\"name\",\"name\") yield elemName as mapping3\n"
              +
              "return *";
      assertTrue(session.run(addMappingAndSchemaCypher).hasNext());
      assertEquals("successfully deleted mapping",
          session.run("CALL n10s.mapping.dropMapping('Movie')").next().get("output")
              .asString());
      assertEquals(2, session
          .run("CALL n10s.mapping.listMappings() yield elemName RETURN count(elemName) as ct ")
          .next().get("ct").asInt());

    }
  }

  @Test
  public void testDropSchema() throws Exception {
    try (Driver driver = GraphDatabase.driver(neo4j.boltURI(),
            Config.builder().withoutEncryption().build())) {

      Session session = driver.session();

      // when DB is empty
      assertFalse(session.run("MATCH (n) RETURN n").hasNext());

      String addMappingAndSchemaCypher =
          " call n10s.mapping.addSchema(\"http://schema.org/\",\"sch\") yield namespace as sch\n"
              +
              "call n10s.mapping.addMappingToSchema(\"http://schema.org/\",\"Movie\",\"Movie\") yield elemName as mapping1\n"
              +
              "call n10s.mapping.addMappingToSchema(\"http://schema.org/\",\"Person\",\"Person\") yield elemName as mapping2\n"
              +
              "call n10s.mapping.addMappingToSchema(\"http://schema.org/\",\"name\",\"name\") yield elemName as mapping3\n"
              +
              "return *";
      assertTrue(session.run(addMappingAndSchemaCypher).hasNext());
      assertEquals("schema not found",
          session.run("CALL n10s.mapping.dropSchema('doesnotexist')").next().get("output")
              .asString());
      assertEquals(3, session
          .run("CALL n10s.mapping.listMappings() yield elemName RETURN count(elemName) as ct ")
          .next().get("ct").asInt());
      assertEquals("successfully deleted schema (and mappings)",
          session.run("CALL n10s.mapping.dropSchema('http://schema.org/')").next()
              .get("output").asString());
      assertEquals(0, session
          .run("CALL n10s.mapping.listMappings() yield elemName RETURN count(elemName) as ct ")
          .next().get("ct").asInt());
      assertEquals(0,
          session.run(
              "CALL n10s.mapping.listSchemas() yield namespace RETURN count(namespace) as ct ")
              .next().get("ct").asInt());

    }
  }

  @Test
  public void testBatchDropSchema() throws Exception {
    try (Driver driver = GraphDatabase.driver(neo4j.boltURI(),
            Config.builder().withoutEncryption().build())) {

      Session session = driver.session();

      // when DB is empty
      assertFalse(session.run("MATCH (n) RETURN n").hasNext());

      String addCommonSchemasCypher = " call n10s.mapping.addCommonSchemas() ";
      assertTrue(session.run(addCommonSchemasCypher).hasNext());
      assertEquals(18,
          session.run(
              "CALL n10s.mapping.listSchemas() yield namespace RETURN count(namespace) as ct ")
              .next().get("ct").asInt());
      assertEquals(2, session
          .run(
              "CALL n10s.mapping.listSchemas('schema') yield namespace RETURN count(namespace) as ct ")
          .next().get("ct").asInt());
      String batchDropSchemasCypher =
          "CALL n10s.mapping.listSchemas('schema') YIELD namespace AS schName "
              +
              "    CALL n10s.mapping.dropSchema(schName) YIELD output RETURN schName , output ";
      Result batchResult = session.run(batchDropSchemasCypher);
      assertTrue(batchResult.hasNext());
      assertEquals(16,
          session.run(
              "CALL n10s.mapping.listSchemas() yield namespace RETURN count(namespace) as ct ")
              .next().get("ct").asInt());

    }
  }

}
