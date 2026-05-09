package n10s;

import static n10s.CommonProcedures.UNIQUENESS_CONSTRAINT_ON_URI;
import static n10s.CommonProcedures.UNIQUENESS_CONSTRAINT_STATEMENT;
import static n10s.graphconfig.Params.PREFIX_SEPARATOR;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.neo4j.driver.Values.NULL;
import static org.neo4j.driver.Values.ofNode;

import java.net.URI;
import java.net.URISyntaxException;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.OffsetDateTime;
import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
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
import org.eclipse.rdf4j.model.vocabulary.XMLSchema;
import org.junit.*;
import org.neo4j.driver.*;
import org.neo4j.driver.Record;
import org.neo4j.driver.internal.InternalRelationship;
import org.neo4j.driver.internal.value.IntegerValue;
import org.neo4j.driver.internal.value.ListValue;
import org.neo4j.driver.types.Point;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.harness.junit.rule.Neo4jRule;

public class RDFStarTest {
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

  String rdfStarFragment = "@prefix neoind: <neo4j://individuals#> .\n"
          + "@prefix neovoc: <neo4j://vocabulary#> .\n"
          + "@prefix rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#> .\n"
          + "\n"
          + "neoind:0 a neovoc:Movie;\n"
          + "  neovoc:released \"1999\"^^<http://www.w3.org/2001/XMLSchema#long>;\n"
          + "  neovoc:tagline \"Welcome to the Real World\";\n"
          + "  neovoc:title \"The Matrix\" .\n"
          + "\n"
          + "neoind:4 a neovoc:Person;\n"
          + "  neovoc:ACTED_IN neoind:0;\n"
          + "  neovoc:born \"1961\"^^<http://www.w3.org/2001/XMLSchema#long>;\n"
          + "  neovoc:name \"Laurence Fishburne\" .\n"
          + "\n"
          + "<<neoind:4 neovoc:ACTED_IN neoind:0>> neovoc:roles \"Morpheus\" .\n"
          + "\n"
          + "neoind:16 a neovoc:Person;\n"
          + "  neovoc:ACTED_IN neoind:0;\n"
          + "  neovoc:born \"1978\"^^<http://www.w3.org/2001/XMLSchema#long>;\n"
          + "  neovoc:name \"Emil Eifrem\" .\n"
          + "\n"
          + "<<neoind:16 neovoc:ACTED_IN neoind:0>> neovoc:roles \"Emil\" .";

  String rdfStarFragmentWithTriplesAsObjects = "@prefix neoind: <neo4j://individuals#> .\n"
          + "@prefix neovoc: <neo4j://vocabulary#> .\n"
          + "@prefix rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#> .\n"
          + "\n"
          + "neoind:0 a neovoc:Movie;\n"
          + "  neovoc:released \"1999\"^^<http://www.w3.org/2001/XMLSchema#long>;\n"
          + "  neovoc:tagline \"Welcome to the Real World\";\n"
          + "  neovoc:title \"The Matrix\" .\n"
          + "\n"
          + "<<neoind:16 neovoc:ACTED_IN neoind:0>> neovoc:roles \"Emil\" ."
          + "\n"
          + "neoind:4 a neovoc:Person;\n"
          + "  neovoc:ACTED_IN neoind:0;\n"
          + "  neovoc:born \"1961\"^^<http://www.w3.org/2001/XMLSchema#long>;\n"
          + "  neovoc:name \"Laurence Fishburne\" .\n"
          + "\n"
          + "neoind:4 neovoc:SAID <<neoind:16 neovoc:ACTED_IN neoind:0>> .\n"
          + "\n"
          + "neoind:16 a neovoc:Person;\n"
          + "  neovoc:ACTED_IN neoind:0;\n"
          + "  neovoc:born \"1978\"^^<http://www.w3.org/2001/XMLSchema#long>;\n"
          + "  neovoc:name \"Emil Eifrem\" . ";

  private static URI file(String path) {
    try {
      return RDFStarTest.class.getClassLoader().getResource(path).toURI();
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
  public void testImportRDFStar() throws Exception {
    try (Session session = driver.session()) {

      initialiseGraphDB(neo4j.defaultDatabaseService(),
              "{ handleVocabUris: 'IGNORE' }");

      Result importResults
              = session.run("CALL n10s.rdf.import.fetch('" +
              RDFStarTest.class.getClassLoader().getResource("movies.ttls").toURI()
              + "','Turtle-star')"); //,{   commitSize: 1000 }

      assertEquals(1372L, importResults
              .single().get("triplesLoaded").asLong());
      Result queryResults = session.run(
              "MATCH (ee:Person { name: 'Emil Eifrem'})-[ai:ACTED_IN]->(m) "
                      + " RETURN ee.born as born, ai.roles as roles, m.title as title limit 1");
      assertTrue(queryResults.hasNext());
      Record result = queryResults.next();
      assertEquals(1978L, result.get("born").asLong());
      assertEquals("Emil", result.get("roles").asString());
      assertEquals("The Matrix", result.get("title").asString());
      assertFalse(queryResults.hasNext());
    }
  }

  @Test
  public void testImportRDFStarRelationshipIRI() throws Exception {
    // Regression test for #265: predicate IRI must be stored as 'iri' property on
    // relationships imported via RDF-star so it is not lost when handleVocabUris is IGNORE
    try (Session session = driver.session()) {
      initialiseGraphDB(neo4j.defaultDatabaseService(),
              "{ handleVocabUris: 'IGNORE', applyNeo4jNaming: true, handleRDFTypes: 'LABELS' }");

      String turtle = "@prefix ex: <https://example.com/graph/> .\n" +
              "@prefix ont: <https://example.com/ontology/> .\n" +
              "ex:item1 a ont:Product .\n" +
              "ex:item2 a ont:Product .\n" +
              "<<ex:item1 ont:upsellWith ex:item2>> ont:orderNo 1 .\n";

      Result importResult = session.run("CALL n10s.rdf.import.inline('" + turtle + "','Turtle-star')");
      assertEquals(3L, importResult.single().get("triplesLoaded").asLong());

      // With applyNeo4jNaming:true, the relationship type is UPSELLWITH (local name uppercased)
      // but the full predicate IRI must still be accessible via the 'iri' property
      Record rel = session.run(
              "MATCH ()-[r:UPSELLWITH]->() RETURN r.orderNo as orderNo, r.iri as iri")
              .single();
      assertEquals(1L, rel.get("orderNo").asLong());
      assertEquals("https://example.com/ontology/upsellWith", rel.get("iri").asString());
    }
  }

  @Test
  public void testImportRDFStarRelationshipIRINotStoredWhenKEEP() throws Exception {
    // With handleVocabUris=KEEP the relationship type name is already the full IRI,
    // so storing 'iri' as a property would be redundant — verify it is omitted
    try (Session session = driver.session()) {
      initialiseGraphDB(neo4j.defaultDatabaseService(),
              "{ handleVocabUris: 'KEEP', handleRDFTypes: 'LABELS' }");

      String turtle = "@prefix ex: <https://example.com/graph/> .\n" +
              "@prefix ont: <https://example.com/ontology/> .\n" +
              "ex:item1 a ont:Product .\n" +
              "ex:item2 a ont:Product .\n" +
              "<<ex:item1 ont:upsellWith ex:item2>> ont:orderNo 1 .\n";

      session.run("CALL n10s.rdf.import.inline('" + turtle + "','Turtle-star')");

      Record rel = session.run(
              "MATCH ()-[r]->() WHERE type(r) = 'https://example.com/ontology/upsellWith' " +
              "RETURN r.iri as iri, r.`https://example.com/ontology/orderNo` as orderNo")
              .single();
      // With KEEP, type name IS the full IRI, so 'iri' property must not be stored
      assertTrue("iri property should not be set when handleVocabUris=KEEP", rel.get("iri").isNull());
      assertEquals(1L, rel.get("orderNo").asLong());
    }
  }

  @Test
  public void testImportRDFStarWithArrayMultiVal() throws Exception {
    try (Session session = driver.session()) {

      initialiseGraphDB(neo4j.defaultDatabaseService(),
              "{ handleVocabUris: 'IGNORE' , " +
                      "handleMultival: 'ARRAY', multivalPropList: ['neo4j://vocabulary#roles']}");

      Result importResults
              = session.run("CALL n10s.rdf.import.fetch('" +
              RDFStarTest.class.getClassLoader().getResource("movies.ttls").toURI()
              + "','Turtle-star')");

      assertEquals(1372L, importResults
              .single().get("triplesLoaded").asLong());
      Result queryResults = session.run(
              "MATCH (ee:Person { name: 'Bill Paxton'})-[ai:ACTED_IN]->(m) " +
                      " WHERE m.tagline = 'Houston, we have a problem.'"
                      + " RETURN ee.born as born, ai.roles as roles, m.title as title limit 1");
      assertTrue(queryResults.hasNext());
      Record result = queryResults.next();
      assertEquals(1955L, result.get("born").asLong());
      List<Object> theRoles = result.get("roles").asList();
      assertEquals(1, theRoles.size());
      assertEquals("Fred Haise", theRoles.get(0));
      assertEquals("Apollo 13", result.get("title").asString());
      assertFalse(queryResults.hasNext());
    }
  }

  @Test
  public void testPreviewRDFStarFromSnippet() throws Exception {
    try (Session session = driver.session()) {

      initialiseGraphDB(neo4j.defaultDatabaseService(),
              "{handleVocabUris: 'IGNORE'}");
      Result importResults
              = session
              .run("CALL n10s.rdf.preview.inline('" + rdfStarFragment
                      + "','Turtle-star')");
      Map<String, Object> next = importResults
              .next().asMap();
      final List<Node> nodes = (List<Node>) next.get("nodes");
      assertEquals(3, nodes.size());
      final List<InternalRelationship> rels = (List<InternalRelationship>) next.get("relationships");
      assertEquals(2, rels.size());
      rels.forEach(r -> assertTrue(((InternalRelationship) r).hasType("ACTED_IN") &&
              (((InternalRelationship) r).asMap().get("roles").equals("Emil") ||
                      ((InternalRelationship) r).asMap().get("roles").equals("Morpheus"))));
    }
  }

  @Test
  public void testRDFStarWithTriplesAsObjectFromSnippet() throws Exception {
    try (Session session = driver.session()) {

      initialiseGraphDB(neo4j.defaultDatabaseService(),
              "{handleVocabUris: 'IGNORE'}");
      Result importResults
              = session
              .run("CALL n10s.rdf.import.inline('" + rdfStarFragmentWithTriplesAsObjects
                      + "','Turtle-star')");
      Map<String, Object> next = importResults
              .next().asMap();
      ;
      assertEquals(13L, next.get("triplesLoaded"));
      assertEquals(14L, next.get("triplesParsed"));
      assertFalse(session.run("MATCH (a)-[:SAID]->(b) RETURN a, b").hasNext());

    }
  }

  @Test
  public void testStreamRDFStarFromSnippet() throws Exception {
    try (Session session = driver.session()) {

      Result importResults
              = session
              .run("CALL n10s.rdf.stream.inline('" + rdfStarFragment
                      + "','Turtle-star')");
      while (importResults.hasNext()) {
        Map<String, Object> next = importResults
                .next().asMap();
        if (next.get("subjectSPO") != null) {
          if (next.get("subject").equals("<<neo4j://individuals#16 neo4j://vocabulary#ACTED_IN neo4j://individuals#0>>")) {
            assertEquals("neo4j://vocabulary#roles", next.get("predicate"));
            assertEquals(true, next.get("isLiteral"));
            assertEquals("Emil", next.get("object"));
            List<String> subjectSPO = (List<String>) next.get("subjectSPO");
            assertEquals("neo4j://individuals#16", subjectSPO.get(0));
            assertEquals("neo4j://vocabulary#ACTED_IN", subjectSPO.get(1));
            assertEquals("neo4j://individuals#0", subjectSPO.get(2));
          }
          if (next.get("subject").equals("<<neo4j://individuals#4 neo4j://vocabulary#ACTED_IN neo4j://individuals#0>>")) {
            assertEquals("neo4j://vocabulary#roles", next.get("predicate"));
            assertEquals(true, next.get("isLiteral"));
            assertEquals("Morpheus", next.get("object"));
            List<String> subjectSPO = (List<String>) next.get("subjectSPO");
            assertEquals("neo4j://individuals#4", subjectSPO.get(0));
            assertEquals("neo4j://vocabulary#ACTED_IN", subjectSPO.get(1));
            assertEquals("neo4j://individuals#0", subjectSPO.get(2));
          }
        } else {
          if (next.get("predicate").equals("http://www.w3.org/1999/02/22-rdf-syntax-ns#type")) {
            assertTrue(next.get("object").equals("neo4j://vocabulary#Person") ||
                    next.get("object").equals("neo4j://vocabulary#Movie"));
          }
        }
      }

    }
  }

  @Test
  public void testRDFStarMultipleAnnotationPredicates() throws Exception {
    // Two DIFFERENT annotation predicates on the same quoted triple must merge into
    // ONE relationship with BOTH properties — not two separate relationships.
    try (Session session = driver.session()) {
      initialiseGraphDB(neo4j.defaultDatabaseService(),
              "{ handleVocabUris: 'IGNORE', handleRDFTypes: 'LABELS' }");

      String turtle = "@prefix ex: <urn:ex:> .\n" +
              "ex:s1 a ex:Person .\n" +
              "ex:s2 a ex:Person .\n" +
              "<<ex:s1 ex:likes ex:s2>> ex:since 2020 .\n" +
              "<<ex:s1 ex:likes ex:s2>> ex:strength \"strong\" .\n";

      session.run("CALL n10s.rdf.import.inline('" + turtle + "','Turtle-star')");

      long relCount = session.run("MATCH ()-[r:likes]->() RETURN count(r) AS c")
              .single().get("c").asLong();
      assertEquals("Two different annotation predicates must produce exactly one relationship", 1L, relCount);

      Record r = session.run("MATCH ()-[r:likes]->() RETURN r.since AS since, r.strength AS strength")
              .single();
      assertEquals(2020L, r.get("since").asLong());
      assertEquals("strong", r.get("strength").asString());
    }
  }

  @Test
  public void testRDFStarSameAnnotationPredicateMultipleValues() throws Exception {
    // Same annotation predicate with two values: with handleMultival ARRAY both values are kept.
    try (Session session = driver.session()) {
      initialiseGraphDB(neo4j.defaultDatabaseService(),
              "{ handleVocabUris: 'IGNORE', handleRDFTypes: 'LABELS'," +
              "  handleMultival: 'ARRAY', multivalPropList: ['urn:ex:becauseOf'] }");

      String turtle = "@prefix ex: <urn:ex:> .\n" +
              "ex:s1 a ex:Person .\n" +
              "ex:s2 a ex:Person .\n" +
              "<<ex:s1 ex:likes ex:s2>> ex:becauseOf \"x\" .\n" +
              "<<ex:s1 ex:likes ex:s2>> ex:becauseOf \"y\" .\n";

      session.run("CALL n10s.rdf.import.inline('" + turtle + "','Turtle-star')");

      long relCount = session.run("MATCH ()-[r:likes]->() RETURN count(r) AS c")
              .single().get("c").asLong();
      assertEquals("Same annotation predicate with two values should produce one relationship", 1L, relCount);

      List<Object> values = session.run("MATCH ()-[r:likes]->() RETURN r.becauseOf AS v")
              .single().get("v").asList();
      assertEquals("Both annotation values must be preserved as array", 2, values.size());
      assertTrue(values.contains("x"));
      assertTrue(values.contains("y"));
    }
  }
}
