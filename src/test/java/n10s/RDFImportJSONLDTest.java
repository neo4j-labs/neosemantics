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

public class RDFImportJSONLDTest {
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
      return RDFImportJSONLDTest.class.getClassLoader().getResource(path).toURI();
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
  public void testImportJSONLD() throws Exception {
    try (Session session = driver.session()) {

      initialiseGraphDB(neo4j.defaultDatabaseService(),
              "{ handleVocabUris: 'KEEP', handleRDFTypes: 'LABELS' }");

      Result importResults
              = session.run("CALL n10s.rdf.import.fetch('" +
              RDFImportJSONLDTest.class.getClassLoader().getResource("mini-ld.json").toURI()
              + "','JSON-LD',"
              +
              "{ commitSize: 500, headerParams : { authorization: 'Basic bla bla bla', accept: 'rdf/xml' } })");

      assertEquals(6L, importResults
              .single().get("triplesLoaded").asLong());
      assertEquals("http://me.markus-lanthaler.com/",
              session.run(
                              "MATCH (n{`http://xmlns.com/foaf/0.1/name` : 'Markus Lanthaler'}) RETURN n.uri AS uri")
                      .next().get("uri").asString());
      assertEquals(1L,
              session.run(
                              "MATCH (n) WHERE n.`http://xmlns.com/foaf/0.1/modified` is not null RETURN count(n) AS count")
                      .next().get("count").asLong());
    }
  }

  @Test
  public void testInvalidSerialisationFormat() throws Exception {
    try (Session session = driver.session()) {

      initialiseGraphDB(neo4j.defaultDatabaseService(),
              "{ handleVocabUris: 'KEEP', handleRDFTypes: 'LABELS' }");

      try {
        Result importResults
                = session.run("CALL n10s.rdf.stream.fetch('" +
                RDFImportJSONLDTest.class.getClassLoader().getResource("mini-ld.json").toURI()
                + "','Invalid-Format')");


        importResults.single();
        assertTrue(false);
      } catch (Exception e) {
        //expected
        assertEquals("Failed to invoke procedure `n10s.rdf.stream.fetch`: Caused by: n10s.RDFImportException: Unrecognized serialization format: Invalid-Format",
                e.getMessage());
      }

      try {
        Result importResults
                = session.run("CALL n10s.rdf.preview.fetch('" +
                RDFImportJSONLDTest.class.getClassLoader().getResource("mini-ld.json").toURI()
                + "','Invalid-Format')");


        importResults.single();
        assertTrue(false);
      } catch (Exception e) {
        //expected
        assertEquals("Failed to invoke procedure `n10s.rdf.preview.fetch`: Caused by: n10s.RDFImportException: Unrecognized serialization format: Invalid-Format",
                e.getMessage());
      }


      try {
        Result importResults
                = session.run("CALL n10s.rdf.import.fetch('" +
                RDFImportJSONLDTest.class.getClassLoader().getResource("mini-ld.json").toURI()
                + "','Invalid-Format')");


        Record single = importResults.single();
        assertEquals("KO", single.get("terminationStatus").asString());
        assertEquals(0L, single.get("triplesLoaded").asLong());
        assertEquals("Unrecognized serialization format: Invalid-Format", single.get("extraInfo").asString());
      } catch (Exception e) {
        //no exceptions raised with import
        assertTrue(false);
      }

    }
  }

  @Test
  public void testImportZippedSingleFile() throws Exception {
    try (Session session = driver.session()) {

      initialiseGraphDB(neo4j.defaultDatabaseService(),
              "{ handleVocabUris: 'KEEP', handleRDFTypes: 'LABELS' }");

      Result importResults
              = session.run("CALL n10s.rdf.import.fetch('" +
              RDFImportJSONLDTest.class.getClassLoader().getResource("schema.rdf.gz").toURI()
              + "','RDF/XML',"
              +
              "{ commitSize: 500, headerParams : { authorization: 'Basic bla bla bla', accept: 'rdf/xml' } })");

      assertEquals(10774L, importResults
              .single().get("triplesLoaded").asLong());

      importResults
              = session.run("CALL n10s.rdf.import.fetch('" +
              RDFImportJSONLDTest.class.getClassLoader().getResource("schema.rdf.tgz").toURI() + "!schema.rdf"
              + "','RDF/XML',"
              +
              "{ commitSize: 500, headerParams : { authorization: 'Basic bla bla bla', accept: 'rdf/xml' } })");

      assertEquals(10774L, importResults
              .single().get("triplesLoaded").asLong());


      importResults
              = session.run("CALL n10s.rdf.import.fetch('" +
              RDFImportJSONLDTest.class.getClassLoader().getResource("schema.rdf.bz2").toURI()
              + "','RDF/XML',"
              +
              "{ commitSize: 500, headerParams : { authorization: 'Basic bla bla bla', accept: 'rdf/xml' } })");

      assertEquals(10774L, importResults
              .single().get("triplesLoaded").asLong());

      importResults
              = session.run("CALL n10s.rdf.import.fetch('" +
              RDFImportJSONLDTest.class.getClassLoader().getResource("schema.rdf.zip").toURI() + "!schema.rdf"
              + "','RDF/XML',"
              +
              "{ commitSize: 500, headerParams : { authorization: 'Basic bla bla bla', accept: 'rdf/xml' } })");

      assertEquals(10774L, importResults
              .single().get("triplesLoaded").asLong());

      importResults
              = session.run("CALL n10s.rdf.import.fetch('" +
              RDFImportJSONLDTest.class.getClassLoader().getResource("schema.rdf.zip").toURI()
              + "','RDF/XML',"
              +
              "{ commitSize: 500, headerParams : { authorization: 'Basic bla bla bla', accept: 'rdf/xml' } })");

      try {
        importResults.single();
        //should not get here
        assertTrue(false);
      } catch (Exception e) {
        assertEquals("Failed to invoke procedure `n10s.rdf.import.fetch`: Caused by: java.lang.IllegalArgumentException: Filename is required for zip files (use '!' notation)", e.getMessage());
      }

      importResults
              = session.run("CALL n10s.rdf.import.fetch('" +
              RDFImportJSONLDTest.class.getClassLoader().getResource("schema.rdf.bz2").toURI() + "!schema.rdf"
              + "','RDF/XML',"
              +
              "{ commitSize: 500, headerParams : { authorization: 'Basic bla bla bla', accept: 'rdf/xml' } })");

      try {
        importResults.single();
        //should not get here
        assertTrue(false);
      } catch (Exception e) {
        assertEquals("Failed to invoke procedure `n10s.rdf.import.fetch`: Caused by: java.lang.IllegalArgumentException: '!' notation for filenames can only be used with zip or tgz files", e.getMessage());
      }
    }
  }

  @Test
  public void testImportZippedMultiFile() throws Exception {
    try (Session session = driver.session()) {

      initialiseGraphDB(neo4j.defaultDatabaseService(),
              "{ handleVocabUris: 'KEEP', handleRDFTypes: 'LABELS' }");

      Result importResults
              = session.run("CALL n10s.rdf.import.fetch('" +
              RDFImportJSONLDTest.class.getClassLoader().getResource("rdf.tar.gz").toURI() + "!rdf/moviesontology.owl"
              + "','RDF/XML',"
              +
              "{ commitSize: 500, headerParams : { authorization: 'Basic bla bla bla', accept: 'rdf/xml' } })");

      assertEquals(60L, importResults
              .single().get("triplesLoaded").asLong());


      importResults
              = session.run("CALL n10s.rdf.import.fetch('" +
              RDFImportJSONLDTest.class.getClassLoader().getResource("rdf.zip").toURI() + "!rdf/moviesontology.owl"
              + "','RDF/XML',"
              +
              "{ commitSize: 500, headerParams : { authorization: 'Basic bla bla bla', accept: 'rdf/xml' } })");

      assertEquals(60L, importResults
              .single().get("triplesLoaded").asLong());


    }
  }

  @Test
  public void testImportJSONLDImportSnippet() throws Exception {
    try (Session session = driver.session();) {
      initialiseGraphDB(neo4j.defaultDatabaseService(),
              " { handleVocabUris: 'KEEP', handleRDFTypes: 'LABELS'} ");

      Result importResults1 = session.run("CALL n10s.rdf.import.inline('" +
              jsonLdFragment + "','JSON-LD',"
              + "{ commitSize: 500, headerParams : { authorization: 'Basic bla bla bla', accept: 'rdf/xml' } })");
      assertEquals(6L, importResults1.single().get("triplesLoaded").asLong());
      assertEquals("http://me.markus-lanthaler.com/",
              session.run(
                              "MATCH (n{`http://xmlns.com/foaf/0.1/name` : 'Markus Lanthaler'}) RETURN n.uri AS uri")
                      .next().get("uri").asString());
      assertEquals(1L,
              session.run(
                              "MATCH (n) WHERE n.`http://xmlns.com/foaf/0.1/modified` is not null RETURN count(n) AS count")
                      .next().get("count").asLong());
    }
  }

  @Test
  public void testImportDateTimeWithUTCOffset() throws Exception {
    // Regression test for #166: ISO 8601 datetimes with bare UTC offset (+00:00)
    // were falling through to string because ZonedDateTime.parse requires a zone region ID
    try (Session session = driver.session()) {
      initialiseGraphDB(neo4j.defaultDatabaseService(), "{ handleVocabUris: 'IGNORE' }");

      String turtle = "@prefix ex: <http://example.org/> .\n" +
              "@prefix xsd: <http://www.w3.org/2001/XMLSchema#> .\n" +
              "@prefix dcterms: <http://purl.org/dc/terms/> .\n" +
              "ex:item1 a ex:Example ;\n" +
              "  dcterms:modified \"2020-06-22T21:41:34.066344+00:00\"^^xsd:dateTime .\n" +
              "ex:item2 a ex:Example ;\n" +
              "  dcterms:modified \"2021-03-15T08:30:00.000000-05:00\"^^xsd:dateTime .\n";

      Result importResult = session.run("CALL n10s.rdf.import.inline('" + turtle + "','Turtle')");
      assertEquals(4L, importResult.single().get("triplesLoaded").asLong());

      // Verify UTC offset (+00:00) is stored as a temporal, not a string
      Record item1 = session.run(
              "MATCH (r:Resource {uri:'http://example.org/item1'}) RETURN r.modified as m")
              .single();
      assertFalse("datetime with +00:00 offset should not be a string",
              item1.get("m").type().name().equals("STRING"));
      assertEquals(OffsetDateTime.parse("2020-06-22T21:41:34.066344+00:00").toInstant(),
              item1.get("m").asOffsetDateTime().toInstant());

      // Verify negative offset (-05:00) is also handled correctly
      Record item2 = session.run(
              "MATCH (r:Resource {uri:'http://example.org/item2'}) RETURN r.modified as m")
              .single();
      assertFalse("datetime with -05:00 offset should not be a string",
              item2.get("m").type().name().equals("STRING"));
      assertEquals(OffsetDateTime.parse("2021-03-15T08:30:00.000000-05:00").toInstant(),
              item2.get("m").asOffsetDateTime().toInstant());
    }
  }

  @Test
  public void testImportJSONLDShortening() throws Exception {
    try (Session session = driver.session()) {

      initialiseGraphDB(neo4j.defaultDatabaseService(),
              "{ handleVocabUris: 'SHORTEN', handleRDFTypes: 'LABELS' }");

      Result importResults
              = session.run("CALL n10s.rdf.import.fetch('" +
              RDFImportJSONLDTest.class.getClassLoader().getResource("mini-ld.json").toURI()
              + "','JSON-LD',"
              +
              "{ commitSize: 10 })");
      assertEquals(6L, importResults
              .next().get("triplesLoaded").asLong());
      assertEquals("http://me.markus-lanthaler.com/",
              session.run(
                              "MATCH (n{ns0" + PREFIX_SEPARATOR + "name : 'Markus Lanthaler'}) RETURN n.uri AS uri")
                      .next().get("uri").asString());
      assertEquals(1L,
              session.run("MATCH (n) WHERE n.ns0" + PREFIX_SEPARATOR
                              + "modified is not null RETURN count(n) AS count")
                      .next().get("count").asLong());

      assertEquals("ns0",
              session.run("call n10s.nsprefixes.list() yield prefix, namespace "
                      + "with prefix, namespace where namespace = 'http://xmlns.com/foaf/0.1/' "
                      + "return prefix, namespace").next().get("prefix").asString());

      session.run("MATCH (n) DETACH DELETE n ;");
      //reset graph config
      session.run(
              "CALL n10s.graphconfig.init({ handleVocabUris: 'SHORTEN', handleRDFTypes: 'LABELS' });");

      importResults = session.run("CALL n10s.rdf.import.inline('" +
              jsonLdFragment + "','JSON-LD', { commitSize: 10 })");
      assertEquals(6L, importResults.next().get("triplesLoaded").asLong());
      assertEquals("http://me.markus-lanthaler.com/",
              session.run(
                              "MATCH (n{ns0" + PREFIX_SEPARATOR + "name : 'Markus Lanthaler'}) RETURN n.uri AS uri")
                      .next().get("uri").asString());
      assertEquals(1L,
              session.run("MATCH (n) WHERE n.ns0" + PREFIX_SEPARATOR
                              + "modified is not null RETURN count(n) AS count")
                      .next().get("count").asLong());

      assertEquals("ns0",
              session.run(
                              "call n10s.nsprefixes.list() yield prefix, namespace "
                                      + " with prefix, namespace where namespace = 'http://xmlns.com/foaf/0.1/' "
                                      + " return prefix ")
                      .next().get("prefix").asString());
    }

  }

  @Test
  public void testImportJSONLDShorteningStrict() throws Exception {
    try (Session session = driver.session();) {

      initialiseGraphDB(neo4j.defaultDatabaseService(),
              "{ handleVocabUris: 'SHORTEN_STRICT', handleRDFTypes: 'LABELS' }");
    }
    try (Session session = driver.session()) {
      Result importResults
              = session.run("CALL n10s.rdf.import.fetch('" +
              RDFImportJSONLDTest.class.getClassLoader().getResource("mini-ld.json").toURI()
              + "','JSON-LD',"
              +
              "{ commitSize: 10 })");
      Record next = importResults.next();

      assertTrue(false);

    } catch (Exception e) {
      assertEquals("Failed to invoke procedure `n10s.rdf.import.fetch`: Caused by: "
              + "n10s.utils.NamespaceWithUndefinedPrefix: No prefix has been defined for "
              + "namespace <http://xmlns.com/foaf/0.1/> and 'handleVocabUris' is set "
              + "to 'SHORTEN_STRICT'", e.getMessage());
    }
    try (Session session = driver.session()) {
      assertEquals("one",
              session.run("call n10s.nsprefixes.add('one','http://xmlns.com/foaf/0.1/')").next()
                      .get("prefix").asString());

      Result importResults
              = session.run("CALL n10s.rdf.import.fetch('" +
              RDFImportJSONLDTest.class.getClassLoader().getResource("mini-ld.json").toURI()
              + "','JSON-LD',"
              +
              "{ commitSize: 10 })");
      assertEquals(6L, importResults.next().get("triplesLoaded").asLong());
      assertEquals("http://me.markus-lanthaler.com/",
              session.run(
                              "MATCH (n{ one" + PREFIX_SEPARATOR
                                      + "name : 'Markus Lanthaler'}) RETURN n.uri AS uri")
                      .next().get("uri").asString());

      assertEquals(1L,
              session.run("MATCH (n) WHERE n.one" + PREFIX_SEPARATOR
                              + "modified is not null RETURN count(n) AS count")
                      .next().get("count").asLong());
    }

  }
}
