package n10s;

import static n10s.CommonProcedures.UNIQUENESS_CONSTRAINT_STATEMENT;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.net.URI;
import java.net.URISyntaxException;
import java.time.LocalDate;
import java.time.LocalDateTime;
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

public class QuadRDFTest {
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

  final String CREATE_URI_INDEX = "CREATE INDEX uri_index FOR (n:Resource) ON (n.uri)";

  String rdfTriGSnippet = "@prefix ex: <http://www.example.org/vocabulary#> .\n"
          + "@prefix exDoc: <http://www.example.org/exampleDocument#> .\n"
          + "@prefix xsd: <http://www.w3.org/2001/XMLSchema#> .\n"
          + "\n"
          + "exDoc:G1 ex:created \"2019-06-06\"^^xsd:date .\n"
          + "exDoc:G2 ex:created \"2019-06-07T10:15:30\"^^xsd:dateTime .\n"
          + "\n"
          + "exDoc:Monica a ex:Person ;\n"
          + "             ex:friendOf exDoc:John .\n"
          + "\n"
          + "exDoc:G1 {\n"
          + "    exDoc:Monica\n"
          + "              ex:name \"Monica Murphy\" ;\n"
          + "              ex:homepage <http://www.monicamurphy.org> ;\n"
          + "              ex:email <mailto:monica@monicamurphy.org> ;\n"
          + "              ex:hasSkill ex:Management ,\n"
          + "                                  ex:Programming ;\n"
          + "              ex:knows exDoc:John . }\n"
          + "\n"
          + "exDoc:G2 {\n"
          + "    exDoc:Monica\n"
          + "              ex:city \"New York\" ;\n"
          + "              ex:country \"USA\" . }\n"
          + "\n"
          + "\n"
          + "exDoc:G3 {\n"
          + "    exDoc:John a ex:Person . }\n"
          + "\n";

  private static URI file(String path) {
    try {
      return QuadRDFTest.class.getClassLoader().getResource(path).toURI();
    } catch (URISyntaxException e) {
      String msg = String.format("Failed to load the resource with path '%s'", path);
      throw new RuntimeException(msg, e);
    }
  }

  private void initialiseGraphDBForQuads(GraphDatabaseService db, String graphConfigParams) {
    db.executeTransactionally(CREATE_URI_INDEX);
    db.executeTransactionally("CALL n10s.graphconfig.init(" +
            (graphConfigParams != null ? graphConfigParams : "{}") + ")");
  }

  @Test
  public void testImportQuadRDFTriG() throws Exception {
    try (Session session = driver.session()) {

      initialiseGraphDBForQuads(neo4j.defaultDatabaseService(),
              "{ handleVocabUris: 'KEEP', handleRDFTypes: 'LABELS', keepCustomDataTypes: true, handleMultival: 'ARRAY' }");

      Result importResults = session.run("CALL n10s.experimental.quadrdf.import.fetch('" +
              QuadRDFTest.class.getClassLoader().getResource("RDFDatasets/RDFDataset.trig")
                      .toURI()
              + "','TriG',{ commitSize: 500 })");

      assertEquals(13L, importResults.next().get("triplesLoaded").asLong());
      Result result = session
              .run("MATCH (n:Resource {uri: 'http://www.example.org/exampleDocument#Monica'})"
                      + "RETURN count(n) AS count");
      assertEquals(3, result.next().get("count").asInt());
      result = session
              .run("MATCH (n:Resource {uri: 'http://www.example.org/exampleDocument#John'})"
                      + "RETURN count(n) AS count");
      assertEquals(3, result.next().get("count").asInt());
      result = session
              .run("MATCH (n:Resource {uri: 'http://www.example.org/exampleDocument#Monica'})"
                      + "RETURN n.graphUri AS graphUri ORDER BY graphUri");
      List<Record> list = result.list();
      assertEquals("http://www.example.org/exampleDocument#G1",
              list.get(0).get("graphUri").asString());
      assertEquals("http://www.example.org/exampleDocument#G2",
              list.get(1).get("graphUri").asString());
      result = session.run("MATCH (n:Resource {uri: 'http://www.example.org/exampleDocument#G1'})"
              + "RETURN n.`http://www.example.org/vocabulary#created` AS created");
      assertEquals(LocalDate.parse("2019-06-06"),
              result.next().get("created").asList().get(0));
      result = session.run("MATCH (n:Resource {uri: 'http://www.example.org/exampleDocument#G2'})"
              + "RETURN n.`http://www.example.org/vocabulary#created` AS created");
      assertEquals(LocalDateTime.parse("2019-06-07T10:15:30"),
              result.next().get("created").asList().get(0));
      result = session.run("MATCH (n {uri: 'http://www.example.org/exampleDocument#Monica'})"
              + "WHERE n.graphUri is null "
              + "RETURN labels(n) AS labels");
      Record record = result.next();
      assertEquals("Resource",
              record.get("labels").asList().get(0));
      assertEquals("http://www.example.org/vocabulary#Person",
              record.get("labels").asList().get(1));
      result = session.run(
              "MATCH (n {uri: 'http://www.example.org/exampleDocument#John', "
                      + "graphUri: 'http://www.example.org/exampleDocument#G3'})"
                      + "RETURN labels(n) AS labels");
      record = result.next();
      assertEquals("Resource",
              record.get("labels").asList().get(0));
      assertEquals("http://www.example.org/vocabulary#Person",
              record.get("labels").asList().get(1));
      result = session
              .run(
                      "MATCH (n:Resource)"
                              + "-[:`http://www.example.org/vocabulary#friendOf`]->"
                              + "(m:Resource)"
                              + "RETURN n.graphUri is null AND m.graphUri is null AS result");
      assertTrue(result.next().get("result").asBoolean());
      result = session
              .run(
                      "MATCH (n:Resource)"
                              + "-[:`http://www.example.org/vocabulary#knows`]->"
                              + "(m:Resource)"
                              + "RETURN n.graphUri is null AND m.graphUri is null AS result");
      assertFalse(result.next().get("result").asBoolean());
    }
  }

  @Test
  public void testImportInlineQuadRDFTriG() throws Exception {
    try (Session session = driver.session()) {
      session.run("call n10s.nsprefixes.add('ns0','http://www.example.org/vocabulary#')");
    }

    try (Session session = driver.session()) {

      initialiseGraphDBForQuads(neo4j.defaultDatabaseService(),
              "{ keepCustomDataTypes: true, handleMultival: 'ARRAY' }");

      Result importResults = session.run("CALL n10s.experimental.quadrdf.import.inline('" +
              rdfTriGSnippet
              + "','TriG')");

      assertEquals(13L, importResults.next().get("triplesLoaded").asLong());
      Result result = session
              .run("MATCH (n:Resource {uri: 'http://www.example.org/exampleDocument#Monica'})"
                      + "RETURN count(n) AS count");
      assertEquals(3, result.next().get("count").asInt());
      result = session
              .run("MATCH (n:Resource {uri: 'http://www.example.org/exampleDocument#John'})"
                      + "RETURN count(n) AS count");
      assertEquals(3, result.next().get("count").asInt());
      result = session
              .run("MATCH (n:Resource {uri: 'http://www.example.org/exampleDocument#Monica'})"
                      + "RETURN n.graphUri AS graphUri ORDER BY graphUri");
      List<Record> list = result.list();
      assertEquals("http://www.example.org/exampleDocument#G1",
              list.get(0).get("graphUri").asString());
      assertEquals("http://www.example.org/exampleDocument#G2",
              list.get(1).get("graphUri").asString());
      result = session.run("MATCH (n:Resource {uri: 'http://www.example.org/exampleDocument#G1'})"
              + "RETURN n.`ns0__created` AS created");
      assertEquals(LocalDate.parse("2019-06-06"),
              result.next().get("created").asList().get(0));
      result = session.run("MATCH (n:Resource {uri: 'http://www.example.org/exampleDocument#G2'})"
              + "RETURN n.`ns0__created` AS created");
      assertEquals(LocalDateTime.parse("2019-06-07T10:15:30"),
              result.next().get("created").asList().get(0));
      result = session.run("MATCH (n {uri: 'http://www.example.org/exampleDocument#Monica'})"
              + "WHERE n.graphUri is null "
              + "RETURN labels(n) AS labels");
      Record record = result.next();
      assertEquals("Resource",
              record.get("labels").asList().get(0));
      assertEquals("ns0__Person",
              record.get("labels").asList().get(1));
      result = session.run(
              "MATCH (n {uri: 'http://www.example.org/exampleDocument#John', "
                      + "graphUri: 'http://www.example.org/exampleDocument#G3'})"
                      + "RETURN labels(n) AS labels");
      record = result.next();
      assertEquals("Resource",
              record.get("labels").asList().get(0));
      assertEquals("ns0__Person",
              record.get("labels").asList().get(1));
      result = session
              .run(
                      "MATCH (n:Resource)"
                              + "-[:`ns0__friendOf`]->"
                              + "(m:Resource)"
                              + "RETURN n.graphUri is null AND m.graphUri is null AS result");
      assertTrue(result.next().get("result").asBoolean());
      result = session
              .run(
                      "MATCH (n:Resource)"
                              + "-[:`ns0__knows`]->"
                              + "(m:Resource)"
                              + "RETURN n.graphUri is null AND m.graphUri is null AS result");
      assertFalse(result.next().get("result").asBoolean());
    }
  }

  @Test
  public void testImportQuadRDFNQuads() throws Exception {
    try (Session session = driver.session()) {

      initialiseGraphDBForQuads(neo4j.defaultDatabaseService(),
              "{ handleVocabUris: 'KEEP', handleRDFTypes: 'LABELS', keepCustomDataTypes: true, handleMultival: 'ARRAY' }");

      Result importResults = session.run("CALL n10s.experimental.quadrdf.import.fetch('" +
              QuadRDFTest.class.getClassLoader().getResource("RDFDatasets/RDFDataset.nq")
                      .toURI()
              + "','N-Quads',{ commitSize: 500 })");

      assertEquals(13L, importResults.next().get("triplesLoaded").asLong());
      Result result = session
              .run("MATCH (n:Resource {uri: 'http://www.example.org/exampleDocument#Monica'})"
                      + "RETURN count(n) AS count");
      assertEquals(3, result.next().get("count").asInt());
      result = session
              .run("MATCH (n:Resource {uri: 'http://www.example.org/exampleDocument#John'})"
                      + "RETURN count(n) AS count");
      assertEquals(3, result.next().get("count").asInt());
      result = session
              .run("MATCH (n:Resource {uri: 'http://www.example.org/exampleDocument#Monica'})"
                      + "RETURN n.graphUri AS graphUri ORDER BY graphUri");
      List<Record> list = result.list();
      assertEquals("http://www.example.org/exampleDocument#G1",
              list.get(0).get("graphUri").asString());
      assertEquals("http://www.example.org/exampleDocument#G2",
              list.get(1).get("graphUri").asString());
      result = session.run("MATCH (n:Resource {uri: 'http://www.example.org/exampleDocument#G1'})"
              + "RETURN n.`http://www.example.org/vocabulary#created` AS created");
      assertEquals(LocalDate.parse("2019-06-06"),
              result.next().get("created").asList().get(0));
      result = session.run("MATCH (n:Resource {uri: 'http://www.example.org/exampleDocument#G2'})"
              + "RETURN n.`http://www.example.org/vocabulary#created` AS created");
      assertEquals(LocalDateTime.parse("2019-06-07T10:15:30"),
              result.next().get("created").asList().get(0));
      result = session.run("MATCH (n {uri: 'http://www.example.org/exampleDocument#Monica'})"
              + "WHERE n.graphUri is null "
              + "RETURN labels(n) AS labels");
      Record record = result.next();
      assertEquals("Resource",
              record.get("labels").asList().get(0));
      assertEquals("http://www.example.org/vocabulary#Person",
              record.get("labels").asList().get(1));
      result = session.run(
              "MATCH (n {uri: 'http://www.example.org/exampleDocument#John', "
                      + "graphUri: 'http://www.example.org/exampleDocument#G3'})"
                      + "RETURN labels(n) AS labels");
      record = result.next();
      assertEquals("Resource",
              record.get("labels").asList().get(0));
      assertEquals("http://www.example.org/vocabulary#Person",
              record.get("labels").asList().get(1));
      result = session
              .run(
                      "MATCH (n:Resource)"
                              + "-[:`http://www.example.org/vocabulary#friendOf`]->"
                              + "(m:Resource)"
                              + "RETURN n.graphUri is null AND m.graphUri is null AS result");
      assertTrue(result.next().get("result").asBoolean());
      result = session
              .run(
                      "MATCH (n:Resource)"
                              + "-[:`http://www.example.org/vocabulary#knows`]->"
                              + "(m:Resource)"
                              + "RETURN n.graphUri is null AND m.graphUri is null AS result");
      assertFalse(result.next().get("result").asBoolean());
    }
  }

  @Test
  public void testDeleteQuadRDFTriG() throws Exception {
    try (Session session = driver.session()) {

      initialiseGraphDBForQuads(neo4j.defaultDatabaseService(),
              "{ handleVocabUris: 'KEEP', handleRDFTypes: 'LABELS', keepCustomDataTypes: true, handleMultival: 'ARRAY' }");

      Result importResults = session.run("CALL n10s.experimental.quadrdf.import.fetch('" +
              QuadRDFTest.class.getClassLoader().getResource("RDFDatasets/RDFDataset.trig")
                      .toURI()
              + "','TriG',{ commitSize: 500 })");

      assertEquals(13L, importResults.next().get("triplesLoaded").asLong());
      Result result = session.run("MATCH (n:Resource)"
              + "RETURN n");
      assertEquals(12, result.list().size());

      Result deleteResult = session.run("CALL n10s.experimental.quadrdf.delete.fetch('" +
              QuadRDFTest.class.getClassLoader().getResource("RDFDatasets/RDFDatasetDelete.trig")
                      .toURI()
              + "', 'TriG', { commitSize: 500 })");

      assertEquals(9L, deleteResult.next().get("triplesDeleted").asLong());

      result = session.run("MATCH (n:Resource)"
              + "RETURN n");
      assertEquals(5, result.list().size());

    }
  }

  @Test
  public void testDeleteQuadRDFNQuads() throws Exception {
    try (Session session = driver.session()) {

      initialiseGraphDBForQuads(neo4j.defaultDatabaseService(),
              " { handleVocabUris: 'KEEP', handleRDFTypes: 'LABELS', keepCustomDataTypes: true, handleMultival: 'ARRAY' } ");

      Result importResults = session.run("CALL n10s.experimental.quadrdf.import.fetch('" +
              QuadRDFTest.class.getClassLoader().getResource("RDFDatasets/RDFDataset.nq")
                      .toURI()
              + "','N-Quads',{ commitSize: 500 })");

      assertEquals(13L, importResults.next().get("triplesLoaded").asLong());
      Result result = session.run("MATCH (n:Resource)"
              + "RETURN n");
      assertEquals(12, result.list().size());

      Result deleteResult = session.run("CALL n10s.experimental.quadrdf.delete.fetch('" +
              QuadRDFTest.class.getClassLoader().getResource("RDFDatasets/RDFDatasetDelete.nq")
                      .toURI()
              + "', 'N-Quads', { commitSize: 500 })");

      assertEquals(9L, deleteResult.next().get("triplesDeleted").asLong());

      result = session.run("MATCH (n:Resource)"
              + "RETURN n");
      assertEquals(5, result.list().size());

    }
  }

  @Test
  public void testRepetitiveDeletionQuadRDF() throws Exception {
    try (Session session = driver.session()) {

      initialiseGraphDBForQuads(neo4j.defaultDatabaseService(),
              "{ handleVocabUris: 'KEEP', handleRDFTypes: 'LABELS', keepCustomDataTypes: true, handleMultival: 'ARRAY'}");

      Result importResults = session.run("CALL n10s.experimental.quadrdf.import.fetch('" +
              QuadRDFTest.class.getClassLoader().getResource("RDFDatasets/RDFDataset.trig")
                      .toURI()
              + "','TriG', { commitSize: 500 })");

      assertEquals(13L, importResults.next().get("triplesLoaded").asLong());
      Result result = session.run("MATCH (n:Resource)"
              + "RETURN n");
      assertEquals(12, result.list().size());

      Result deleteResult = session.run("CALL n10s.experimental.quadrdf.delete.fetch('" +
              QuadRDFTest.class.getClassLoader().getResource("RDFDatasets/RDFDatasetDelete.trig")
                      .toURI()
              + "', 'TriG', { commitSize: 500 })");

      assertEquals(9L, deleteResult.next().get("triplesDeleted").asLong());

      result = session.run("MATCH (n:Resource)"
              + "RETURN n");
      assertEquals(5, result.list().size());

      deleteResult = session.run("CALL n10s.experimental.quadrdf.delete.fetch('" +
              QuadRDFTest.class.getClassLoader().getResource("RDFDatasets/RDFDatasetDelete.trig")
                      .toURI()
              + "', 'TriG', { commitSize: 500 })");

      assertEquals(0L, deleteResult.next().get("triplesDeleted").asLong());

    }
  }
}
