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

public class RDFIncrementalLoadTest {
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

  private String jsonLdFragment = "{\n" +
          "  \"@context\": {\n" +
          "    \"name\": \"http://xmlns.com/foaf/0.1/name\",\n" +
          "    \"knows\": \"http://xmlns.com/foaf/0.1/knows\",\n" +
          "\t\"modified\": \"http://xmlns.com/foaf/0.1/modified\"\n" +
          "  },\n" +
          "  \"@id\": \"http://me.markus-lanthaler.com/\",\n" +
          "  \"name\": \"Markus Lanthaler\",\n" +
          "  \"knows\": [\n" +
          "    {\n" +
          "      \"@id\": \"http://manu.sporny.org/about#manu\",\n" +
          "      \"name\": \"Manu Sporny\"\n" +
          "    },\n" +
          "    {\n" +
          "      \"name\": \"Dave Longley\",\n" +
          "\t  \"modified\":\n" +
          "\t    {\n" +
          "\t      \"@value\": \"2010-05-29T14:17:39+02:00\",\n" +
          "\t      \"@type\": \"http://www.w3.org/2001/XMLSchema#dateTime\"\n" +
          "\t    }\n" +
          "    }\n" +
          "  ]\n" +
          "}";

  private static URI file(String path) {
    try {
      return RDFIncrementalLoadTest.class.getClassLoader().getResource(path).toURI();
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

  private Map<String, Object> getPrePostDelta(Map<String, Object> defsPre,
                                              Map<String, Object> defsPost) {
    Map<String, Object> delta = new HashMap<>();
    defsPre.forEach((k, v) -> {
      if (!defsPost.containsKey(k) || !defsPost.get(k).equals(v)) {
        delta.put(k, v);
      }
    });
    return delta;
  }

  @Test
  public void testReificationImport() throws Exception {
    try (Session session = driver.session()) {

      initialiseGraphDB(neo4j.defaultDatabaseService(),
              "{ handleVocabUris: 'KEEP', handleRDFTypes: 'LABELS' }");

      Result importResults
              = session.run("CALL n10s.rdf.import.fetch('" +
              RDFIncrementalLoadTest.class.getClassLoader().getResource("reification.ttl")
                      .toURI()
              + "','Turtle')");
      assertEquals(25L, importResults
              .next().get("triplesLoaded").asLong());
      Result dates = session
              .run("MATCH (n:`http://www.w3.org/1999/02/22-rdf-syntax-ns#Statement`) " +
                      "\nRETURN n.`http://example.com/from` AS fromDates ORDER BY fromDates DESC");

      assertEquals(LocalDate.parse("2019-09-01"), dates.next().get("fromDates").asLocalDate());
      assertEquals(LocalDate.parse("2016-09-01"), dates.next().get("fromDates").asLocalDate());

      Result statements = session.run("MATCH (statement)\n" +
              "WHERE (statement)-[:`http://www.w3.org/1999/02/22-rdf-syntax-ns#subject`]->()\n" +
              "AND (statement)-[:`http://www.w3.org/1999/02/22-rdf-syntax-ns#predicate`]->()\n" +
              "AND (statement)-[:`http://www.w3.org/1999/02/22-rdf-syntax-ns#object`]->()\n" +
              "RETURN statement.uri AS statement ORDER BY statement");

      assertEquals("http://example.com/studyInformation1",
              statements.next().get("statement").asString());
      assertEquals("http://example.com/studyInformation2",
              statements.next().get("statement").asString());
    }
  }

  @Test
  public void testIncrementalLoadMultivaluesInArray() throws Exception {
    try (Session session = driver.session()) {

      initialiseGraphDB(neo4j.defaultDatabaseService(), "{ handleMultival: 'ARRAY' }");

      Result importResults
              = session.run("CALL n10s.rdf.import.fetch('" +
              RDFIncrementalLoadTest.class.getClassLoader().getResource("incremental/step1.ttl")
                      .toURI() + "','Turtle')");
      assertEquals(2L, importResults
              .next().get("triplesLoaded").asLong());
      importResults
              = session.run("CALL n10s.rdf.import.fetch('" +
              RDFIncrementalLoadTest.class.getClassLoader().getResource("incremental/step2.ttl")
                      .toURI() + "','Turtle')");
      assertEquals(2L, importResults
              .next().get("triplesLoaded").asLong());

      Result result = session.run("MATCH (n:ns0__Thing) " +
              "\nRETURN n.ns0__prop as multival ");

      List<String> vals = new ArrayList<>();
      vals.add("one");
      vals.add("two");
      assertEquals(vals, result.next().get("multival").asList());


    }
  }

  @Test
  public void testIncrementalLoadNamespaces() throws Exception {
    try (Session session = driver.session()) {

      initialiseGraphDB(neo4j.defaultDatabaseService(), null);

      Result importResults
              = session.run("CALL n10s.rdf.import.fetch('" +
              RDFIncrementalLoadTest.class.getClassLoader().getResource("event.json")
                      .toURI() + "','JSON-LD')");
      assertEquals(28L, importResults
              .next().get("triplesLoaded").asLong());
      Result nsDefResult = session.run("MATCH (n:_NsPrefDef) "
              + "RETURN properties(n) as defs");
      assertTrue(nsDefResult.hasNext());
      Map<String, Object> defsPre = nsDefResult.next().get("defs").asMap();
      assertFalse(nsDefResult.hasNext());
      importResults
              = session.run("CALL n10s.rdf.import.fetch('" +
              RDFIncrementalLoadTest.class.getClassLoader().getResource("fibo-fragment.rdf")
                      .toURI() + "','RDF/XML')");
      assertEquals(171L, importResults
              .next().get("triplesLoaded").asLong());
      nsDefResult = session.run("MATCH (n:_NsPrefDef) "
              + "RETURN properties(n) as defs");
      assertTrue(nsDefResult.hasNext());
      Map<String, Object> defsPost = nsDefResult.next().get("defs").asMap();
      assertFalse(nsDefResult.hasNext());
      assertTrue(getPrePostDelta(defsPre, defsPost).isEmpty());
      importResults
              = session.run("CALL n10s.rdf.import.fetch('" +
              RDFIncrementalLoadTest.class.getClassLoader().getResource("oneTriple.rdf")
                      .toURI() + "','RDF/XML')");
      assertEquals(1L, importResults
              .next().get("triplesLoaded").asLong());
      nsDefResult = session.run("MATCH (n:_NsPrefDef) "
              + "RETURN properties(n) as defs");
      assertTrue(nsDefResult.hasNext());
      Map<String, Object> defsPost2 = nsDefResult.next().get("defs").asMap();
      assertFalse(nsDefResult.hasNext());
      assertTrue(getPrePostDelta(defsPost, defsPost2).isEmpty());

    }
  }

  @Test
  public void testLoadNamespacesWithCustomPredefined() throws Exception {
    try (Session session = driver.session()) {

      initialiseGraphDB(neo4j.defaultDatabaseService(), null);

      session.run("CREATE (:_NsPrefDef {\n"
              + "  `http://www.w3.org/2000/01/rdf-schema#`: 'myschema',\n"
              + "  `http://www.w3.org/1999/02/22-rdf-syntax-ns#`: 'myrdf'})");
      Result nsDefResult = session.run("MATCH (n:_NsPrefDef) "
              + "RETURN properties(n) as defs");
      assertTrue(nsDefResult.hasNext());
      Map<String, Object> defsPre = nsDefResult.next().get("defs").asMap();
      assertFalse(nsDefResult.hasNext());
      Result importResults = session.run("CALL n10s.rdf.import.fetch('" +
              RDFIncrementalLoadTest.class.getClassLoader().getResource("fibo-fragment.rdf")
                      .toURI() + "','RDF/XML')");
      assertEquals(171L, importResults.next().get("triplesLoaded").asLong());
      nsDefResult = session.run("MATCH (n:_NsPrefDef) "
              + "RETURN properties(n) as defs");
      assertTrue(nsDefResult.hasNext());
      Map<String, Object> defsPost = nsDefResult.next().get("defs").asMap();
      assertFalse(nsDefResult.hasNext());
    }
  }

  @Test
  public void testIncrementalLoadArrayOnPreviouslyAtomicValue() throws Exception {
    try (Session session = driver.session()) {

      initialiseGraphDB(neo4j.defaultDatabaseService(), null);

      Result importResults
              = session.run("CALL n10s.rdf.import.fetch('" +
              RDFIncrementalLoadTest.class.getClassLoader().getResource("incremental/step1.ttl")
                      .toURI() + "','Turtle')");
      assertEquals(2L, importResults
              .next().get("triplesLoaded").asLong());

      try {
        Result modifyGraphConfigResult = session
                .run("CALL n10s.graphconfig.init({ handleMultival: 'ARRAY' });");
        modifyGraphConfigResult.hasNext();
        assertFalse(true);
      } catch (Exception e) {
        //expected
        assertTrue(e.getMessage().contains("The graph is non-empty. Config cannot be changed."));
      }

      session.run("MATCH (n) DETACH DELETE n ;");
      //set graph config
      session.run("CALL n10s.graphconfig.init({ handleMultival: 'ARRAY' });");

      //{ handleMultival: 'ARRAY' }
      importResults
              = session.run("CALL n10s.rdf.import.fetch('" +
              RDFIncrementalLoadTest.class.getClassLoader().getResource("incremental/step2.ttl")
                      .toURI() + "','Turtle')");
      assertEquals(2L, importResults
              .next().get("triplesLoaded").asLong());

      Result result = session.run("MATCH (n:ns0__Thing) " +
              "\nRETURN n.ns0__prop as multival ");

      List<String> vals = new ArrayList<String>();
      vals.add("two");
      assertEquals(vals, result.next().get("multival").asList());


    }
  }

  @Test
  public void testIncrementalLoadAtomicValueOnPreviouslyArray() throws Exception {
    try (Session session = driver.session()) {

      initialiseGraphDB(neo4j.defaultDatabaseService(), null);

      Result importResults
              = session.run("CALL n10s.rdf.import.fetch('" +
              RDFIncrementalLoadTest.class.getClassLoader().getResource("incremental/step1.ttl")
                      .toURI() + "','Turtle',{ handleMultival: 'ARRAY' })");
      assertEquals(2L, importResults
              .next().get("triplesLoaded").asLong());
      importResults
              = session.run("CALL n10s.rdf.import.fetch('" +
              RDFIncrementalLoadTest.class.getClassLoader().getResource("incremental/step3.ttl")
                      .toURI() + "','Turtle')");
      assertEquals(2L, importResults
              .next().get("triplesLoaded").asLong());

      Result result = session.run("MATCH (n:ns0__Thing) " +
              "\nRETURN n.ns0__prop as singleVal ");

      assertEquals(230L, result.next().get("singleVal").asLong());


    }
  }

  @Test
  public void testLargerFileManyTransactions() throws Exception {
    try (Session session = driver.session()) {

      initialiseGraphDB(neo4j.defaultDatabaseService(), null);

      Result importResults
              = session.run("CALL n10s.rdf.import.fetch('" +
              RDFIncrementalLoadTest.class.getClassLoader().getResource("100k.nt").toURI() + "','N-Triples',"
              + "{ commitSize: 1000 , predicateExclusionList: ['http://www.w3.org/2004/02/skos/core#prefLabel']})");
      assertEquals(92712L, importResults
              .next().get("triplesLoaded").asLong());
    }

  }

  @Test
  public void testTypesOnlySeparateTx() throws Exception {
    try (Session session = driver.session()) {

      initialiseGraphDB(neo4j.defaultDatabaseService(),
              "{handleMultival:'ARRAY', keepCustomDataTypes: true}");

      Result importResults
              = session.run("CALL n10s.rdf.import.fetch('" +
              RDFIncrementalLoadTest.class.getClassLoader().getResource("two-types-two-tx-no-props.ttl").toURI()
              + "','Turtle', { commitSize: 1, strictDataTypeCheck: false})");

      Record importResult = importResults.next();
      assertEquals(2L, importResult.get("triplesLoaded").asLong());
      assertEquals(2L, importResult.get("triplesParsed").asLong());

      assertEquals(1, session.run("MATCH (n:Resource) RETURN count(n) as nodeCount ").next().get("nodeCount").asInt());
      assertEquals(3, session.run("MATCH (n:Resource) RETURN size(labels(n)) as labelCount ").next().get("labelCount").asInt());


    }
  }

  @Test
  public void testSingleTransactionBaseTest() throws Exception {
    try (Session session = driver.session();) {
      initialiseGraphDB(neo4j.defaultDatabaseService(),
              " { handleVocabUris: 'IGNORE' } ");

      Transaction singleTx = session.beginTransaction();
      Result importResults1 = singleTx.run("CALL n10s.rdf.import.inline('" +
              jsonLdFragment + "','JSON-LD')");
      assertEquals(6L, importResults1.single().get("triplesLoaded").asLong());
      singleTx.rollback();
      assertEquals(false,
              session.run(
                              "MATCH (n{`http://xmlns.com/foaf/0.1/name` : 'Markus Lanthaler'}) RETURN n.uri AS uri")
                      .hasNext());

      singleTx = session.beginTransaction();
      Result importResults2 = singleTx.run("CALL n10s.rdf.import.inline('" +
              jsonLdFragment + "','JSON-LD', { singleTx: false })");
      assertEquals(6L, importResults2.single().get("triplesLoaded").asLong());
      singleTx.rollback();
      assertEquals(true,
              session.run(
                              "MATCH (n{`name` : 'Markus Lanthaler'}) RETURN n.uri AS uri")
                      .hasNext());

    }
  }


  @Test
  public void testSingleTransactionShorterStrictScenario() throws Exception {
    try (Session session = driver.session()) {
      initialiseGraphDB(neo4j.defaultDatabaseService(),
              " {keepLangTag: false, handleRDFTypes: 'LABELS', handleVocabUris: 'SHORTEN_STRICT'} ");

      String nameSpacePrefix = session.run("call n10s.nsprefixes.add('one','http://xmlns.com/foaf/0.1/')").next()
              .get("prefix").asString();

      assertEquals("one", nameSpacePrefix);

      Transaction singleTx = session.beginTransaction();
      Result importResults1 = singleTx.run("CALL n10s.rdf.import.inline('" +
              jsonLdFragment + "','JSON-LD')");
      assertEquals(6L, importResults1.single().get("triplesLoaded").asLong());
      singleTx.rollback();
      assertEquals(false,
              session.run(
                              "MATCH (n{`http://xmlns.com/foaf/0.1/` : 'Markus Lanthaler'}) RETURN n.uri AS uri")
                      .hasNext());

      singleTx = session.beginTransaction();
      Result importResults2 = singleTx.run("CALL n10s.rdf.import.inline('" +
              jsonLdFragment + "','JSON-LD', { singleTx: false })");
      assertEquals(6L, importResults2.single().get("triplesLoaded").asLong());
      singleTx.rollback();
      assertEquals(true,
              session.run(
                              "MATCH (n{`one__name` : 'Markus Lanthaler'}) RETURN n.uri AS uri")
                      .hasNext());
    }
  }

  @Test
  public void testPeriodicCommitImportsAllTriples() throws Exception {
    // Regression test for #297: buffer clearing moved from finally to success path so that
    // all triples across multiple partial commits are stored correctly.
    try (Session session = driver.session()) {
      initialiseGraphDB(neo4j.defaultDatabaseService(),
              "{ handleVocabUris: 'IGNORE', handleRDFTypes: 'LABELS' }");

      String turtle = "@prefix ex: <http://example.org/> .\n" +
              "@prefix foaf: <http://xmlns.com/foaf/0.1/> .\n" +
              "ex:alice a foaf:Person ; foaf:name \"Alice\" ; foaf:age 30 .\n" +
              "ex:bob   a foaf:Person ; foaf:name \"Bob\"   ; foaf:age 25 .\n" +
              "ex:carol a foaf:Person ; foaf:name \"Carol\" ; foaf:age 35 .\n" +
              "ex:dave  a foaf:Person ; foaf:name \"Dave\"  ; foaf:age 28 .\n" +
              "ex:alice foaf:knows ex:bob .\n" +
              "ex:bob   foaf:knows ex:carol .\n" +
              "ex:carol foaf:knows ex:dave .\n";

      // commitSize: 3 forces multiple partial commits across the 12 triples above
      Result importResults = session.run(
              "CALL n10s.rdf.import.inline($turtle,'Turtle', { commitSize: 3 })",
              Map.of("turtle", turtle));
      long loaded = importResults.single().get("triplesLoaded").asLong();
      assertTrue("All triples should be imported across partial commits, got: " + loaded,
              loaded >= 12L);

      long personCount = session.run(
              "MATCH (n:Person) RETURN count(n) AS c").single().get("c").asLong();
      assertEquals("All four Person nodes should be present", 4L, personCount);

      long knowsCount = session.run(
              "MATCH ()-[:knows]->() RETURN count(*) AS c").single().get("c").asLong();
      assertEquals("All knows relationships should be present", 3L, knowsCount);
    }
  }
}
