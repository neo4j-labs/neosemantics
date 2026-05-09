package n10s;

import static n10s.CommonProcedures.UNIQUENESS_CONSTRAINT_STATEMENT;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.neo4j.driver.Values.NULL;

import java.net.URI;
import java.net.URISyntaxException;
import java.util.Arrays;
import java.util.List;
import n10s.experimental.ExperimentalImports;
import n10s.graphconfig.GraphConfigProcedures;
import n10s.mapping.MappingUtils;
import n10s.nsprefixes.NsPrefixDefProcedures;
import n10s.quadrdf.delete.QuadRDFDeleteProcedures;
import n10s.quadrdf.load.QuadRDFLoadProcedures;
import n10s.rdf.RDFProcedures;
import n10s.rdf.delete.RDFDeleteProcedures;
import n10s.rdf.load.RDFLoadProcedures;
import n10s.rdf.preview.RDFPreviewProcedures;
import n10s.rdf.stream.RDFStreamProcedures;
import n10s.skos.load.SKOSLoadProcedures;
import org.junit.*;
import org.neo4j.driver.*;
import org.neo4j.driver.Record;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.harness.junit.rule.Neo4jRule;

public class RDFDeleteTest {
  public static Driver driver;

  @ClassRule
  public static Neo4jRule neo4j = new Neo4jRule()
          .withProcedure(RDFLoadProcedures.class)
          .withProcedure(RDFDeleteProcedures.class)
          .withProcedure(RDFPreviewProcedures.class)
          .withProcedure(RDFStreamProcedures.class)
          .withFunction(RDFProcedures.class)
          .withProcedure(QuadRDFLoadProcedures.class)
          .withProcedure(QuadRDFDeleteProcedures.class)
          .withProcedure(MappingUtils.class)
          .withProcedure(GraphConfigProcedures.class)
          .withProcedure(NsPrefixDefProcedures.class)
          .withProcedure(ExperimentalImports.class)
          .withProcedure(SKOSLoadProcedures.class);

  @BeforeClass
  public static void init() {
    driver = GraphDatabase.driver(neo4j.boltURI(),
            Config.builder().withoutEncryption().build());
  }

  @Before
  public void cleanDatabase() {
    driver.session().run("match (n) detach delete n").consume();
    driver.session().run("drop constraint n10s_unique_uri if exists").consume();
    driver.session().run("drop index uri_index if exists").consume();
  }

  private static URI file(String path) {
    try {
      return RDFDeleteTest.class.getClassLoader().getResource(path).toURI();
    } catch (URISyntaxException e) {
      String msg = String.format("Failed to load the resource with path '%s'", path);
      throw new RuntimeException(msg, e);
    }
  }

  private void initialiseGraphDB(GraphDatabaseService db, String graphConfigParams) {
    db.executeTransactionally(UNIQUENESS_CONSTRAINT_STATEMENT);
    db.executeTransactionally("CALL n10s.graphconfig.init(" +
            (graphConfigParams != null ? graphConfigParams : "{}") + ")");
  }

  @Test
  public void testDeleteRelationshipKeepURIs() throws Exception {
    try (Session session = driver.session()) {

      initialiseGraphDB(neo4j.defaultDatabaseService(),
              "{ handleVocabUris: 'KEEP', handleRDFTypes: 'LABELS', keepCustomDataTypes: true, handleMultival: 'ARRAY'}");

      Result importResults = session.run("CALL n10s.rdf.import.fetch('" +
              RDFDeleteTest.class.getClassLoader().getResource("deleteRDF/dataset1.ttl")
                      .toURI()
              + "','Turtle', { commitSize: 500 })");

      assertEquals(15L, importResults.next().get("triplesLoaded").asLong());
      Result result = session.run("MATCH (n {uri: 'http://example.org/Resource1'}),"
              + "(m {uri: 'http://example.org/Resource2'})"
              + "OPTIONAL MATCH (n)-[r]->(m) "
              + "RETURN n.uri AS nUri, type(r) AS type, m.uri AS mUri");
      Record record = result.next();
      assertEquals("http://example.org/Predicate3", record.get("type").asString());
      assertEquals("http://example.org/Resource1", record.get("nUri").asString());
      assertEquals("http://example.org/Resource2", record.get("mUri").asString());

      Result deleteResults = session.run("CALL n10s.rdf.delete.fetch('" +
              RDFDeleteTest.class.getClassLoader().getResource("deleteRDF/dataset1Delete1.ttl")
                      .toURI()
              + "', 'Turtle', { commitSize: 500 })");

      assertEquals(1L, deleteResults.next().get("triplesDeleted").asLong());

      result = session.run("MATCH (n {uri: 'http://example.org/Resource1'}),"
              + "(m {uri: 'http://example.org/Resource2'})"
              + "OPTIONAL MATCH (n)-[r]->(m) "
              + "RETURN n.uri AS nUri, type(r) AS type, m.uri AS mUri");
      record = result.next();
      assertEquals(NULL, record.get("type"));
      assertEquals("http://example.org/Resource1", record.get("nUri").asString());
      assertEquals("http://example.org/Resource2", record.get("mUri").asString());

    }
  }

  @Test
  public void testDeleteRelationshipShortenURIs() throws Exception {
    try (Session session = driver.session()) {

      initialiseGraphDB(neo4j.defaultDatabaseService(), null);

      Result importResults = session.run("CALL n10s.rdf.import.fetch('" +
              RDFDeleteTest.class.getClassLoader().getResource("deleteRDF/dataset1.ttl")
                      .toURI()
              + "','Turtle',{ handleVocabUris: 'SHORTEN', handleRDFTypes: 'LABELS', commitSize: 500, keepCustomDataTypes: true, handleMultival: 'ARRAY'})");

      assertEquals(15L, importResults.next().get("triplesLoaded").asLong());
      Result result = session.run("MATCH (n {uri: 'http://example.org/Resource1'}),"
              + "(m {uri: 'http://example.org/Resource2'})"
              + "OPTIONAL MATCH (n)-[r]->(m) "
              + "RETURN n.uri AS nUri, type(r) AS type, m.uri AS mUri");
      Record record = result.next();
      assertEquals("ns0__Predicate3", record.get("type").asString());
      assertEquals("http://example.org/Resource1", record.get("nUri").asString());
      assertEquals("http://example.org/Resource2", record.get("mUri").asString());

      Result deleteResults = session.run("CALL n10s.rdf.delete.fetch('" +
              RDFDeleteTest.class.getClassLoader().getResource("deleteRDF/dataset1Delete1.ttl")
                      .toURI()
              + "', 'Turtle', {handleVocabUris: 'SHORTEN', handleRDFTypes: 'LABELS', commitSize: 500, keepCustomDataTypes: true})");

      assertEquals(1L, deleteResults.next().get("triplesDeleted").asLong());

      result = session.run("MATCH (n {uri: 'http://example.org/Resource1'}),"
              + "(m {uri: 'http://example.org/Resource2'})"
              + "OPTIONAL MATCH (n)-[r]->(m) "
              + "RETURN n.uri AS nUri, type(r) AS type, m.uri AS mUri");
      record = result.next();
      assertEquals(NULL, record.get("type"));
      assertEquals("http://example.org/Resource1", record.get("nUri").asString());
      assertEquals("http://example.org/Resource2", record.get("mUri").asString());

    }
  }

  @Test
  public void testDeleteRelationshipShortenURIsFromString() throws Exception {
    try (Session session = driver.session()) {

      initialiseGraphDB(neo4j.defaultDatabaseService(), null);

      String rdf = "@prefix ex: <http://example.org/> .\n"
              + "@prefix xsd: <http://www.w3.org/2001/XMLSchema#> .\n"
              + "\n"
              + "ex:Resource1\n"
              + "  a ex:TestResource ;\n"
              + "  ex:Predicate1 \"100\"^^ex:CDT ;\n"
              + "  ex:Predicate2 \"test\";\n"
              + "  ex:Predicate3 ex:Resource2 ;\n"
              + "  ex:Predicate4 \"val1\" ;\n"
              + "  ex:Predicate4 \"val2\" ;\n"
              + "  ex:Predicate4 \"val3\" ;\n"
              + "  ex:Predicate4 \"val4\" .\n"
              + "\n"
              + "ex:Resource2\n"
              + "  a ex:TestResource ;\n"
              + "  ex:Predicate1 \"test\";\n"
              + "  ex:Predicate2 ex:Resource3 ;\n"
              + "  ex:Predicate3 \"100\"^^xsd:long ;\n"
              + "  ex:Predicate3 \"200\"^^xsd:long ;\n"
              + "  ex:Predicate4 \"300.0\"^^xsd:double ;\n"
              + "  ex:Predicate4 \"400.0\"^^xsd:double .\n"
              + "\n";

      Result importResults = session.run("CALL n10s.rdf.import.inline('" +
              rdf
              + "','Turtle',{ handleVocabUris: 'SHORTEN', handleRDFTypes: 'LABELS', commitSize: 500, keepCustomDataTypes: true, handleMultival: 'ARRAY'})");

      assertEquals(15L, importResults.next().get("triplesLoaded").asLong());
      Result result = session.run("MATCH (n {uri: 'http://example.org/Resource1'}),"
              + "(m {uri: 'http://example.org/Resource2'})"
              + "OPTIONAL MATCH (n)-[r]->(m) "
              + "RETURN n.uri AS nUri, type(r) AS type, m.uri AS mUri");
      Record record = result.next();
      assertEquals("ns0__Predicate3", record.get("type").asString());
      assertEquals("http://example.org/Resource1", record.get("nUri").asString());
      assertEquals("http://example.org/Resource2", record.get("mUri").asString());

      String deleteRdf1 = "@prefix ex: <http://example.org/> .\n"
              + "\n"
              + "ex:Resource1\n"
              + "  ex:Predicate3 ex:Resource2 .\n";
      Result deleteResults = session.run("CALL n10s.rdf.delete.inline('" +
              deleteRdf1
              + "', 'Turtle', {handleVocabUris: 'SHORTEN', handleRDFTypes: 'LABELS', commitSize: 500, keepCustomDataTypes: true})");

      assertEquals(1L, deleteResults.next().get("triplesDeleted").asLong());

      result = session.run("MATCH (n {uri: 'http://example.org/Resource1'}),"
              + "(m {uri: 'http://example.org/Resource2'})"
              + "OPTIONAL MATCH (n)-[r]->(m) "
              + "RETURN n.uri AS nUri, type(r) AS type, m.uri AS mUri");
      record = result.next();
      assertEquals(NULL, record.get("type"));
      assertEquals("http://example.org/Resource1", record.get("nUri").asString());
      assertEquals("http://example.org/Resource2", record.get("mUri").asString());

    }
  }

  @Test
  public void testDeleteLiteralKeepURIs() throws Exception {
    try (Session session = driver.session()) {

      initialiseGraphDB(neo4j.defaultDatabaseService(),
              "{ handleVocabUris: 'KEEP', handleRDFTypes: 'LABELS', " +
                      "keepCustomDataTypes: true, handleMultival: 'ARRAY'}");

      Result importResults = session.run("CALL n10s.rdf.import.fetch('" +
              RDFDeleteTest.class.getClassLoader().getResource("deleteRDF/dataset1.ttl")
                      .toURI()
              + "','Turtle',{ commitSize: 500 })");

      assertEquals(15L, importResults.next().get("triplesLoaded").asLong());
      Result result = session.run("MATCH (n {uri: 'http://example.org/Resource1'})"
              + "RETURN n.`http://example.org/Predicate2` AS nP2");

      Record record = result.next();
      assertEquals(1, record.get("nP2").asList().size());
      assertTrue(record.get("nP2").asList().contains("test"));

      Result deleteResults = session.run("CALL n10s.rdf.delete.fetch('" +
              RDFDeleteTest.class.getClassLoader().getResource("deleteRDF/dataset1Delete2.ttl")
                      .toURI()
              + "', 'Turtle', {  commitSize: 500 })");

      assertEquals(1L, deleteResults.next().get("triplesDeleted").asLong());

      result = session.run("MATCH (n {uri: 'http://example.org/Resource1'})"
              + "RETURN n.`http://example.org/Predicate2` AS nP2");
      record = result.next();
      assertEquals(NULL, record.get("nP2"));

    }
  }

  @Test
  public void testDeleteLiteralShortenURIs() throws Exception {
    try (Session session = driver.session()) {

      initialiseGraphDB(neo4j.defaultDatabaseService(),
              " { handleVocabUris: 'SHORTEN', handleRDFTypes: 'LABELS', " +
                      "keepCustomDataTypes: true, handleMultival: 'ARRAY'} ");

      Result importResults = session.run("CALL n10s.rdf.import.fetch('" +
              RDFDeleteTest.class.getClassLoader().getResource("deleteRDF/dataset1.ttl")
                      .toURI()
              + "','Turtle',{ commitSize: 500 })");

      assertEquals(15L, importResults.next().get("triplesLoaded").asLong());
      Result result = session.run("MATCH (n {uri: 'http://example.org/Resource1'})"
              + "RETURN n.ns0__Predicate2 AS nP2");

      Record record = result.next();
      assertEquals(1, record.get("nP2").asList().size());
      assertTrue(record.get("nP2").asList().contains("test"));

      Result deleteResults = session.run("CALL n10s.rdf.delete.fetch('" +
              RDFDeleteTest.class.getClassLoader().getResource("deleteRDF/dataset1Delete2.ttl")
                      .toURI()
              + "', 'Turtle', { commitSize: 500 })");

      assertEquals(1L, deleteResults.next().get("triplesDeleted").asLong());

      result = session.run("MATCH (n {uri: 'http://example.org/Resource1'})"
              + "RETURN n.ns0__Predicate2 AS nP2");
      record = result.next();
      assertEquals(NULL, record.get("nP2"));

    }
  }

  @Test
  public void testDeleteLiteralShortenURIsFromString() throws Exception {
    try (Session session = driver.session()) {

      initialiseGraphDB(neo4j.defaultDatabaseService(),
              "{ handleVocabUris: 'SHORTEN', handleRDFTypes: 'LABELS', keepCustomDataTypes: true, handleMultival: 'ARRAY'}");

      String rdf = "@prefix ex: <http://example.org/> .\n"
              + "@prefix xsd: <http://www.w3.org/2001/XMLSchema#> .\n"
              + "\n"
              + "ex:Resource1\n"
              + "  a ex:TestResource ;\n"
              + "  ex:Predicate1 \"100\"^^ex:CDT ;\n"
              + "  ex:Predicate2 \"test\";\n"
              + "  ex:Predicate3 ex:Resource2 ;\n"
              + "  ex:Predicate4 \"val1\" ;\n"
              + "  ex:Predicate4 \"val2\" ;\n"
              + "  ex:Predicate4 \"val3\" ;\n"
              + "  ex:Predicate4 \"val4\" .\n"
              + "\n"
              + "ex:Resource2\n"
              + "  a ex:TestResource ;\n"
              + "  ex:Predicate1 \"test\";\n"
              + "  ex:Predicate2 ex:Resource3 ;\n"
              + "  ex:Predicate3 \"100\"^^xsd:long ;\n"
              + "  ex:Predicate3 \"200\"^^xsd:long ;\n"
              + "  ex:Predicate4 \"300.0\"^^xsd:double ;\n"
              + "  ex:Predicate4 \"400.0\"^^xsd:double .\n"
              + "\n";

      Result importResults = session.run("CALL n10s.rdf.import.inline('" +
              rdf
              + "','Turtle',{ commitSize: 500 })");

      assertEquals(15L, importResults.next().get("triplesLoaded").asLong());
      Result result = session.run("MATCH (n {uri: 'http://example.org/Resource1'})"
              + "RETURN n.ns0__Predicate2 AS nP2");

      Record record = result.next();
      assertEquals(1, record.get("nP2").asList().size());
      assertTrue(record.get("nP2").asList().contains("test"));

      String deleteRdf = "@prefix ex: <http://example.org/> .\n"
              + "\n"
              + "ex:Resource1\n"
              + "  ex:Predicate2 \"test\" .\n";

      Result deleteResults = session.run("CALL n10s.rdf.delete.inline('" +
              deleteRdf
              + "', 'Turtle', { commitSize: 500 })");

      assertEquals(1L, deleteResults.next().get("triplesDeleted").asLong());

      result = session.run("MATCH (n {uri: 'http://example.org/Resource1'})"
              + "RETURN n.ns0__Predicate2 AS nP2");
      record = result.next();
      assertEquals(NULL, record.get("nP2"));

    }
  }

  @Test
  public void testDeleteTypeFromResource() throws Exception {
    try (Session session = driver.session()) {

      initialiseGraphDB(neo4j.defaultDatabaseService(), null);

      Result importResults = session.run("CALL n10s.rdf.import.fetch('" +
              RDFDeleteTest.class.getClassLoader().getResource("deleteRDF/dataset1.ttl")
                      .toURI()
              + "','Turtle',{ handleVocabUris: 'KEEP', handleRDFTypes: 'LABELS', commitSize: 500, keepCustomDataTypes: true, handleMultival: 'ARRAY'})");

      assertEquals(15L, importResults.next().get("triplesLoaded").asLong());
      Result result = session.run("MATCH (n:Resource)"
              + "RETURN n");
      assertEquals(3, result.list().size());
      result = session.run("MATCH (n {uri: 'http://example.org/Resource2'})"
              + "RETURN labels(n) AS labels");
      Record record = result.next();
      assertEquals(2, record.get("labels").asList().size());

      Result deleteResult = session.run("CALL n10s.rdf.delete.fetch('" +
              RDFDeleteTest.class.getClassLoader().getResource("deleteRDF/dataset1Delete3.ttl")
                      .toURI()
              + "', 'Turtle', {handleVocabUris: 'KEEP', handleRDFTypes: 'LABELS', commitSize: 500, keepCustomDataTypes: true, handleMultival: 'ARRAY'})");

      assertEquals(1L, deleteResult.next().get("triplesDeleted").asLong());

      result = session.run("MATCH (n {uri: 'http://example.org/Resource2'})"
              + "RETURN labels(n) AS labels");
      record = result.next();
      assertEquals(1, record.get("labels").asList().size());

    }
  }

  @Test
  public void testDeleteAllTriplesRelatedToResource() throws Exception {
    try (Session session = driver.session()) {

      initialiseGraphDB(neo4j.defaultDatabaseService(), null);

      Result importResults = session.run("CALL n10s.rdf.import.fetch('" +
              RDFDeleteTest.class.getClassLoader().getResource("deleteRDF/dataset1.ttl")
                      .toURI()
              + "','Turtle',{ handleVocabUris: 'KEEP', handleRDFTypes: 'LABELS', commitSize: 500, keepCustomDataTypes: true, handleMultival: 'ARRAY'})");

      assertEquals(15L, importResults.next().get("triplesLoaded").asLong());
      Result result = session.run("MATCH (n:Resource)"
              + "RETURN n");
      assertEquals(3, result.list().size());

      Result deleteResult = session.run("CALL n10s.rdf.delete.fetch('" +
              RDFDeleteTest.class.getClassLoader().getResource("deleteRDF/dataset1Delete4.ttl")
                      .toURI()
              + "', 'Turtle', {handleVocabUris: 'KEEP', handleRDFTypes: 'LABELS', commitSize: 500, keepCustomDataTypes: true, handleMultival: 'ARRAY'})");

      assertEquals(8L, deleteResult.next().get("triplesDeleted").asLong());

      result = session.run("MATCH (n:Resource)"
              + "RETURN n");
      assertEquals(1, result.list().size());

    }
  }

  @Test
  public void testDeleteMultiLiteral() throws Exception {
    try (Session session = driver.session()) {

      initialiseGraphDB(neo4j.defaultDatabaseService(),
              "{handleVocabUris: 'KEEP', handleRDFTypes: 'LABELS', keepCustomDataTypes: true, " +
                      "handleMultival: 'ARRAY'}");

      Result importResults = session.run("CALL n10s.rdf.import.fetch('" +
              RDFDeleteTest.class.getClassLoader().getResource("deleteRDF/dataset1.ttl")
                      .toURI()
              + "','Turtle',{ commitSize: 500 })");

      assertEquals(15L, importResults.next().get("triplesLoaded").asLong());
      Result result = session.run("MATCH (n {uri: 'http://example.org/Resource1'}), "
              + "(m {uri: 'http://example.org/Resource2'})"
              + "RETURN n.`http://example.org/Predicate4` AS nP4, "
              + "m.`http://example.org/Predicate3` AS mP3, "
              + "m.`http://example.org/Predicate4` AS mP4");

      Record record = result.next();
      assertTrue(record.get("nP4").asList().containsAll(Arrays.asList("val1", "val2", "val3", "val4")));
      assertTrue(record.get("mP3").asList().contains(100L));
      assertTrue(record.get("mP3").asList().contains(200L));
      assertTrue(record.get("mP4").asList().contains(300.0));
      assertTrue(record.get("mP4").asList().contains(400.0));

      Result deleteResult = session.run("CALL n10s.rdf.delete.fetch('" +
              RDFDeleteTest.class.getClassLoader().getResource("deleteRDF/dataset1Delete5.ttl")
                      .toURI()
              + "', 'Turtle', { commitSize: 500 })");

      assertEquals(3L, deleteResult.next().get("triplesDeleted").asLong());

      result = session.run("MATCH (n {uri: 'http://example.org/Resource1'})"
              + "RETURN n.`http://example.org/Predicate4` AS nP4");
      record = result.next();
      assertArrayEquals(new String[]{"val2"}, record.get("nP4").asList().toArray());

      deleteResult = session.run("CALL n10s.rdf.delete.fetch('" +
              RDFDeleteTest.class.getClassLoader().getResource("deleteRDF/dataset1Delete6.ttl")
                      .toURI()
              + "', 'Turtle', { commitSize: 500 })");

      assertEquals(2L, deleteResult.next().get("triplesDeleted").asLong());

      result = session.run("MATCH (n {uri: 'http://example.org/Resource2'})"
              + "RETURN n.`http://example.org/Predicate3` AS nP3, n.`http://example.org/Predicate4` AS nP4");
      record = result.next();
      assertFalse(record.get("nP3").asList().contains(100L));
      assertFalse(record.get("nP4").asList().contains(400.0));

    }
  }

  @Test
  public void testDeleteSubjectNode() throws Exception {
    try (Session session = driver.session()) {

      initialiseGraphDB(neo4j.defaultDatabaseService(), null);

      Result importResults = session.run("CALL n10s.rdf.import.fetch('" +
              RDFDeleteTest.class.getClassLoader().getResource("deleteRDF/dataset1.ttl")
                      .toURI()
              + "','Turtle',{ handleVocabUris: 'KEEP', handleRDFTypes: 'LABELS', commitSize: 500, keepCustomDataTypes: true, handleMultival: 'ARRAY'})");

      assertEquals(15L, importResults.next().get("triplesLoaded").asLong());
      Result result = session.run("MATCH (n:Resource)"
              + "RETURN n");
      assertEquals(3, result.list().size());
      result = session.run("MATCH (n {uri: 'http://example.org/Resource3'})"
              + "RETURN n.uri");
      Record record = result.next();
      assertEquals("http://example.org/Resource3", record.get("n.uri").asString());

      Result deleteResult = session.run("CALL n10s.rdf.delete.fetch('" +
              RDFDeleteTest.class.getClassLoader().getResource("deleteRDF/dataset1Delete7.ttl")
                      .toURI()
              + "', 'Turtle', {handleVocabUris: 'KEEP', handleRDFTypes: 'LABELS', commitSize: 500, keepCustomDataTypes: true, handleMultival: 'ARRAY'})");

      assertEquals(1L, deleteResult.next().get("triplesDeleted").asLong());

      result = session.run("MATCH (n {uri: 'http://example.org/Resource3'})"
              + "RETURN n.uri");
      assertFalse(result.hasNext());

    }
  }

  @Test
  public void testRepetitiveDeletion() throws Exception {
    try (Session session = driver.session()) {

      initialiseGraphDB(neo4j.defaultDatabaseService(), "" +
              "{ handleVocabUris: 'KEEP', handleRDFTypes: 'LABELS', keepCustomDataTypes: true, handleMultival: 'ARRAY'}");

      Result importResults = session.run("CALL n10s.rdf.import.fetch('" +
              RDFDeleteTest.class.getClassLoader().getResource("deleteRDF/dataset1.ttl")
                      .toURI()
              + "','Turtle',{ commitSize: 500 })");

      assertEquals(15L, importResults.next().get("triplesLoaded").asLong());
      Result result = session.run("MATCH (n:Resource)"
              + "RETURN n");
      assertEquals(3, result.list().size());
      result = session.run("MATCH (n {uri: 'http://example.org/Resource3'})"
              + "RETURN n.uri");
      Record record = result.next();
      assertEquals("http://example.org/Resource3", record.get("n.uri").asString());

      Result deleteResult = session.run("CALL n10s.rdf.delete.fetch('" +
              RDFDeleteTest.class.getClassLoader().getResource("deleteRDF/dataset1Delete4.ttl")
                      .toURI()
              + "', 'Turtle', { commitSize: 500 })");

      assertEquals(8L, deleteResult.next().get("triplesDeleted").asLong());

      deleteResult = session.run("CALL n10s.rdf.delete.fetch('" +
              RDFDeleteTest.class.getClassLoader().getResource("deleteRDF/dataset1Delete4.ttl")
                      .toURI()
              + "', 'Turtle', { commitSize: 500 })");

      assertEquals(0L, deleteResult.next().get("triplesDeleted").asLong());

    }
  }
}
