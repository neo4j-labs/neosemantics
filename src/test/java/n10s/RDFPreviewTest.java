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

public class RDFPreviewTest {
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

  private String turtleFragmentTypes = "@prefix show: <http://example.org/vocab/show/> .\n" +
          " show:218 show:localName \"That Seventies Show\"@en . " +
          " show:218 rdf:type show:Show . ";

  private String wrongUriTtl = "@prefix pr: <http://example.org/vocab/show/> .\n" +
          "pr:ent" +
          "      pr:P854 <https://suasprod.noc-science.at/XLCubedWeb/WebForm/ShowReport.aspx?rep=004+studierende%2f001+universit%u00e4ten%2f003+studierende+nach+universit%u00e4ten.xml&toolbar=true> ;\n"
          +
          "      pr:P813 \"2017-10-11T00:00:00Z\"^^xsd:dateTime .\n";

  private String turtleOntology = "@prefix owl: <http://www.w3.org/2002/07/owl#> .\n"
          + "@prefix rdfs: <http://www.w3.org/2000/01/rdf-schema#> .\n"
          + "\n"
          + "<http://neo4j.com/voc/movies>\n"
          + "  a owl:Ontology ;\n"
          + "  rdfs:comment \"A basic OWL ontology for Neo4j's movie database\", \"\"\"Simple ontology providing basic vocabulary and domain+range axioms\n"
          + "            for the movie database.\"\"\" ;\n"
          + "  rdfs:label \"Neo4j's Movie Ontology\" .\n"
          + "\n"
          + "<http://neo4j.com/voc/movies#Person>\n"
          + "  a owl:Class ;\n"
          + "  rdfs:label \"Person\"@en ;\n"
          + "  rdfs:comment \"Individual involved in the film industry\"@en .\n"
          + "\n"
          + "<http://neo4j.com/voc/movies#Movie>\n"
          + "  a owl:Class ;\n"
          + "  rdfs:label \"Movie\"@en ;\n"
          + "  rdfs:comment \"A film\"@en .\n"
          + "\n"
          + "<http://neo4j.com/voc/movies#name>\n"
          + "  a owl:DatatypeProperty ;\n"
          + "  rdfs:label \"name\"@en ;\n"
          + "  rdfs:comment \"A person's name\"@en ;\n"
          + "  rdfs:domain <http://neo4j.com/voc/movies#Person> .\n"
          + "\n"
          + "<http://neo4j.com/voc/movies#born>\n"
          + "  a owl:DatatypeProperty ;\n"
          + "  rdfs:label \"born\"@en ;\n"
          + "  rdfs:comment \"A person's date of birth\"@en ;\n"
          + "  rdfs:domain <http://neo4j.com/voc/movies#Person> .\n"
          + "\n"
          + "<http://neo4j.com/voc/movies#title>\n"
          + "  a owl:DatatypeProperty ;\n"
          + "  rdfs:label \"title\"@en ;\n"
          + "  rdfs:comment \"The title of a film\"@en ;\n"
          + "  rdfs:domain <http://neo4j.com/voc/movies#Movie> .\n"
          + "\n"
          + "<http://neo4j.com/voc/movies#released>\n"
          + "  a owl:DatatypeProperty ;\n"
          + "  rdfs:label \"released\"@en ;\n"
          + "  rdfs:comment \"A film's release date\"@en ;\n"
          + "  rdfs:domain <http://neo4j.com/voc/movies#Movie> .\n"
          + "\n"
          + "<http://neo4j.com/voc/movies#tagline>\n"
          + "  a owl:DatatypeProperty ;\n"
          + "  rdfs:label \"tagline\"@en ;\n"
          + "  rdfs:comment \"Tagline for a film\"@en ;\n"
          + "  rdfs:domain <http://neo4j.com/voc/movies#Movie> .\n"
          + "\n"
          + "<http://neo4j.com/voc/movies#ACTED_IN>\n"
          + "  a owl:ObjectProperty ;\n"
          + "  rdfs:label \"ACTED_IN\"@en ;\n"
          + "  rdfs:comment \"Actor had a role in film\"@en ;\n"
          + "  rdfs:domain <http://neo4j.com/voc/movies#Person> ;\n"
          + "  rdfs:range <http://neo4j.com/voc/movies#Movie> .\n"
          + "\n"
          + "<http://neo4j.com/voc/movies#DIRECTED>\n"
          + "  a owl:ObjectProperty ;\n"
          + "  rdfs:label \"DIRECTED\"@en ;\n"
          + "  rdfs:comment \"Director directed film\"@en ;\n"
          + "  rdfs:domain <http://neo4j.com/voc/movies#Person> ;\n"
          + "  rdfs:range <http://neo4j.com/voc/movies#Movie> .\n"
          + "\n"
          + "<http://neo4j.com/voc/movies#PRODUCED>\n"
          + "  a owl:ObjectProperty ;\n"
          + "  rdfs:label \"PRODUCED\"@en ;\n"
          + "  rdfs:comment \"Producer produced film\"@en ;\n"
          + "  rdfs:domain <http://neo4j.com/voc/movies#Person> ;\n"
          + "  rdfs:range <http://neo4j.com/voc/movies#Movie> .\n"
          + "\n"
          + "<http://neo4j.com/voc/movies#REVIEWED>\n"
          + "  a owl:ObjectProperty ;\n"
          + "  rdfs:label \"REVIEWED\"@en ;\n"
          + "  rdfs:comment \"Critic reviewed film\"@en ;\n"
          + "  rdfs:domain <http://neo4j.com/voc/movies#Person> ;\n"
          + "  rdfs:range <http://neo4j.com/voc/movies#Movie> .\n"
          + "\n"
          + "<http://neo4j.com/voc/movies#FOLLOWS>\n"
          + "  a owl:ObjectProperty ;\n"
          + "  rdfs:label \"FOLLOWS\"@en ;\n"
          + "  rdfs:comment \"Critic follows another critic\"@en ;\n"
          + "  rdfs:domain <http://neo4j.com/voc/movies#Person> ;\n"
          + "  rdfs:range <http://neo4j.com/voc/movies#Person> .\n"
          + "\n"
          + "<http://neo4j.com/voc/movies#WROTE>\n"
          + "  a owl:ObjectProperty ;\n"
          + "  rdfs:label \"WROTE\"@en ;\n"
          + "  rdfs:comment \"Screenwriter wrote screenplay of\"@en ;\n"
          + "  rdfs:domain <http://neo4j.com/voc/movies#Person> ;\n"
          + "  rdfs:range <http://neo4j.com/voc/movies#Movie> .";

  String turtleWithPointData =
          "@prefix wd: <http://www.wikidata.org/prop/direct/> .\n" +
                  "@prefix gs: <http://www.opengis.net/ont/geosparql#> .\n" +
                  "\n" + "<http://www.wikidata.org/entity/Q84> wd:P624 \"This is something geolocated\". " +
                  "<http://www.wikidata.org/entity/Q84> wd:P625 \"Point(-0.1275 51.507222222)\"^^gs:wktLiteral .";

  String turtleWithPointDataInMars =
          "@prefix wd: <http://www.wikidata.org/prop/direct/> .\n" +
                  "@prefix gs: <http://www.opengis.net/ont/geosparql#> .\n" +
                  "\n" + "<http://www.wikidata.org/entity/Q84> wd:P624 \"This is something geolocated in Mars\". " +
                  "<http://www.wikidata.org/entity/Q84> wd:P625 \"<http://www.wikidata.org/entity/Q111> Point(351.83 -14.47)\"^^gs:wktLiteral .";

  String turtleStarWithPointData = "@prefix neoind: <neo4j://individuals#> .\n"
          + "@prefix neovoc: <neo4j://vocabulary#> .\n"
          + "@prefix gs: <http://www.opengis.net/ont/geosparql#> .\n"
          + "@prefix rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#> .\n"
          + "\n"
          + "neoind:0 a neovoc:Movie;\n"
          + "  neovoc:released \"1999\"^^<http://www.w3.org/2001/XMLSchema#long> .\n"
          + "\n"
          + "neoind:4 a neovoc:Person;\n"
          + "  neovoc:ACTED_IN neoind:0;\n"
          + "  neovoc:born \"1961\"^^<http://www.w3.org/2001/XMLSchema#long> .\n"
          + "\n"
          + "<<neoind:4 neovoc:ACTED_IN neoind:0>> neovoc:roles \"Morpheus\" ; neovoc:where \"Point(-51.507222222 0.1275)\"^^gs:wktLiteral .\n";

  private static URI file(String path) {
    try {
      return RDFPreviewTest.class.getClassLoader().getResource(path).toURI();
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
  public void testPreviewFromSnippetPassWrongUri() throws Exception {
    try (Session session = driver.session()) {

      initialiseGraphDB(neo4j.defaultDatabaseService(),
              "{handleVocabUris: 'KEEP', handleRDFTypes: 'NODES' }");

      Result importResults
              = session
              .run("CALL n10s.rdf.preview.inline('" + wrongUriTtl
                      + "','Turtle',{ verifyUriSyntax: false})");
      Map<String, Object> next = importResults
              .next().asMap();
      final List<Node> nodes = (List<Node>) next.get("nodes");
      assertEquals(2, nodes.size());
      final List<Relationship> rels = (List<Relationship>) next.get("relationships");
      assertEquals(1, rels.size());
    }
  }

  @Test
  public void testPreviewFromSnippetFailWrongUri() throws Exception {
    try (Session session = driver.session()) {

      initialiseGraphDB(neo4j.defaultDatabaseService(),
              "{ handleVocabUris: 'KEEP', handleRDFTypes: 'NODES'}");

      try {
        Result importResults
                = session
                .run("CALL n10s.rdf.preview.inline('" + wrongUriTtl
                        + "','Turtle')");

        importResults.next();
        //we should not get here
        assertTrue(false);
      } catch (Exception e) {
        //expected
        assertTrue(e.getMessage().contains("Illegal percent encoding U+25"));
      }
    }
  }

  @Test
  public void testPreviewFromSnippet() throws Exception {
    try (Session session = driver.session()) {

      initialiseGraphDB(neo4j.defaultDatabaseService(),
              "{ handleVocabUris: 'KEEP', handleRDFTypes: 'NODES'}");
      Result importResults
              = session
              .run("CALL n10s.rdf.preview.inline('" + jsonLdFragment
                      + "','JSON-LD')");
      Map<String, Object> next = importResults
              .next().asMap();
      final List<Node> nodes = (List<Node>) next.get("nodes");
      assertEquals(3, nodes.size());
      final List<Relationship> rels = (List<Relationship>) next.get("relationships");
      assertEquals(2, rels.size());
    }
  }

  @Test
  public void testPreviewFromSnippetLimit() throws Exception {
    try (Session session = driver.session()) {

      initialiseGraphDB(neo4j.defaultDatabaseService(),
              "{ handleVocabUris: 'KEEP', handleRDFTypes: 'NODES'}");

      Map<String, Object> params = new HashMap<>();
      params.put("rdf", this.turtleOntology);

      Result importResults
              = session
              .run("CALL n10s.rdf.preview.inline($rdf,'Turtle')", params);
      Map<String, Object> next = importResults
              .next().asMap();
      List<Node> nodes = (List<Node>) next.get("nodes");
      assertEquals(18, nodes.size());
      List<Relationship> rels = (List<Relationship>) next.get("relationships");
      assertEquals(31, rels.size());

      //now  limiting it to 5 triples
      importResults
              = session
              .run("CALL n10s.rdf.preview.inline($rdf,'Turtle',  { limit: 5 })", params);
      next = importResults
              .next().asMap();
      nodes = (List<Node>) next.get("nodes");
      assertEquals(4, nodes.size());
      rels = (List<Relationship>) next.get("relationships");
      assertEquals(2, rels.size());
    }
  }

  @Test
  public void testPreviewFromFileLimit() throws Exception {
    try (Session session = driver.session()) {

      initialiseGraphDB(neo4j.defaultDatabaseService(),
              "{ handleVocabUris: 'KEEP', handleRDFTypes: 'NODES'}");

      Map<String, Object> params = new HashMap<>();
      params.put("rdf", this.turtleOntology);

      Result importResults
              = session
              .run("CALL n10s.rdf.preview.fetch('" + RDFPreviewTest.class.getClassLoader()
                      .getResource("moviesontology.owl")
                      .toURI() + "','RDF/XML')", params);
      Map<String, Object> next = importResults
              .next().asMap();
      List<Node> nodes = (List<Node>) next.get("nodes");
      assertEquals(18, nodes.size());
      List<Relationship> rels = (List<Relationship>) next.get("relationships");
      assertEquals(31, rels.size());

      //now  limiting it to 5 triples
      importResults
              = session
              .run("CALL n10s.rdf.preview.fetch(' " + RDFPreviewTest.class.getClassLoader()
                      .getResource("moviesontology.owl")
                      .toURI() + "','RDF/XML',  { limit: 5 })", params);
      next = importResults
              .next().asMap();
      nodes = (List<Node>) next.get("nodes");
      assertEquals(4, nodes.size());
      rels = (List<Relationship>) next.get("relationships");
      assertEquals(2, rels.size());
    }
  }

  @Test
  public void testLoadFromSnippetCreateNodes() throws Exception {
    try (Session session = driver.session()) {

      initialiseGraphDB(neo4j.defaultDatabaseService(),
              "{ handleVocabUris: 'KEEP', handleRDFTypes: 'NODES'}");
      Result importResults
              = session
              .run("CALL n10s.rdf.import.inline('" + turtleFragmentTypes
                      + "','Turtle')");
      Map<String, Object> next = importResults
              .next().asMap();
      assertEquals(2L, next.get("triplesLoaded"));
      Record results = session
              .run(
                      "MATCH (x:Resource)-[:`http://www.w3.org/1999/02/22-rdf-syntax-ns#type`]->(t:Resource) RETURN x, t, "
                              + "[x in labels(x) where x<>'Resource' | x ][0] as xlabel").next();
      assertEquals("http://example.org/vocab/show/218",
              results.get("x").asNode().get("uri").asString());
      assertEquals("http://example.org/vocab/show/Show",
              results.get("t").asNode().get("uri").asString());
      assertTrue(results.get("xlabel").isNull());
    }
  }

  @Test
  public void testLoadFromSnippetCreateNodesAndLabels() throws Exception {
    try (Session session = driver.session()) {

      initialiseGraphDB(neo4j.defaultDatabaseService(),
              "{ handleVocabUris: 'SHORTEN', handleRDFTypes: 'LABELS_AND_NODES'}");
      Result importResults
              = session
              .run("CALL n10s.rdf.import.inline('" + turtleFragmentTypes
                      + "','Turtle')");
      Map<String, Object> next = importResults
              .next().asMap();
      assertEquals(2L, next.get("triplesLoaded"));
      Record results = session
              .run("MATCH (x:Resource)-[:rdf__type]->(t:Resource) RETURN x, t, "
                      + "n10s.rdf.fullUriFromShortForm([x in labels(x) where x<>'Resource' | x ][0]) as xlabelAsUri")
              .next();
      assertEquals("http://example.org/vocab/show/218",
              results.get("x").asNode().get("uri").asString());
      assertEquals("http://example.org/vocab/show/Show",
              results.get("t").asNode().get("uri").asString());
      assertEquals("http://example.org/vocab/show/Show", results.get("xlabelAsUri").asString());
    }
  }

  @Test
  public void testPreviewFromSnippetLangFilter() throws Exception {
    try (Session session = driver.session()) {

      initialiseGraphDB(neo4j.defaultDatabaseService(),
              "{ handleVocabUris: 'KEEP', handleRDFTypes: 'NODES' }");

      String turtleFragment = "@prefix show: <http://example.org/vocab/show/> .\n" +
              "\n" +
              "show:218 show:localName \"That Seventies Show\"@en .                 # literal with a language tag\n"
              +
              "show:218 show:localName \"Cette Série des Années Soixante-dix\"@fr . \n" +
              "show:218 show:localName \"Cette Série des Années Septante\"@fr-be .  # literal with a region subtag";
      Result importResults
              = session
              .run("CALL n10s.rdf.preview.inline('" + turtleFragment
                      + "','Turtle',{ languageFilter: 'fr'})");
      Record next = importResults
              .next();
      assertEquals(1, next.get("nodes").size());
      assertEquals("Cette Série des Années Soixante-dix",
              next.get("nodes").asList(ofNode()).get(0).get("http://example.org/vocab/show/localName")
                      .asString());
      assertEquals(0, next.get("relationships").size());

      importResults
              = session.run("CALL n10s.rdf.preview.inline('" + turtleFragment
              + "','Turtle',{ languageFilter: 'en'})");
      assertEquals("That Seventies Show", importResults
              .next().get("nodes").asList(ofNode()).get(0)
              .get("http://example.org/vocab/show/localName").asString());

    }
  }

  @Test
  public void testPointDatatype() throws Exception {
    try (Session session = driver.session()) {

      initialiseGraphDB(neo4j.defaultDatabaseService(),
              "{handleVocabUris: 'IGNORE'}");
      Result importResults
              = session
              .run("CALL n10s.rdf.import.inline('" + turtleWithPointData
                      + "','Turtle')");
      Map<String, Object> next = importResults
              .next().asMap();
      assertEquals(2L, next.get("triplesLoaded"));
      Record record = session
              .run("MATCH (x:Resource { uri: 'http://www.wikidata.org/entity/Q84'}) return x.P624 as s, x.P625 as p")
              .next();
      assertEquals("This is something geolocated", record.get("s").asString());
      Point p = record.get("p").asPoint();
      assertEquals(-0.1275, p.x(), 0.0005);
      assertEquals(51.507222222, p.y(), 0.0005);
      assertEquals(7203, p.srid()); //cartesian


      importResults
              = session
              .run("CALL n10s.rdf.import.inline('" + turtleStarWithPointData
                      + "','Turtle-star')");
      next = importResults
              .next().asMap();
      assertEquals(7L, next.get("triplesLoaded"));
      record = session
              .run("MATCH (:Resource { uri: 'neo4j://individuals#4'})-[ai:ACTED_IN]->" +
                      "(:Resource { uri: 'neo4j://individuals#0'}) return ai.roles as s, ai.where as p")
              .next();
      assertEquals("Morpheus", record.get("s").asString());
      p = record.get("p").asPoint();
      assertEquals(-51.507222222, p.x(), 0.0005);
      assertEquals(0.1275, p.y(), 0.0005);
      assertEquals(7203, p.srid()); //cartesian
    }
  }

  @Test
  public void testPointDatatypeInMars() throws Exception {
    try (Session session = driver.session()) {

      initialiseGraphDB(neo4j.defaultDatabaseService(),
              "{handleVocabUris: 'IGNORE'}");
      Result importResults
              = session
              .run("CALL n10s.rdf.import.inline('" + turtleWithPointDataInMars
                      + "','Turtle')");
      Map<String, Object> next = importResults
              .next().asMap();
      assertEquals(2L, next.get("triplesLoaded"));
      Record record = session
              .run("MATCH (x:Resource { uri: 'http://www.wikidata.org/entity/Q84'}) return x.P624 as s, x.P625 as p")
              .next();
      assertEquals("This is something geolocated in Mars", record.get("s").asString());
      assertEquals("<http://www.wikidata.org/entity/Q111> Point(351.83 -14.47)", record.get("p").asString());
    }
  }

  @Test
  public void testPreviewFromFile() throws Exception {
    try (Session session = driver.session()) {

      initialiseGraphDB(neo4j.defaultDatabaseService(),
              "{ handleVocabUris: 'KEEP', handleRDFTypes: 'NODES' }");

      Result importResults
              = session.run("CALL n10s.rdf.preview.fetch('" +
              RDFPreviewTest.class.getClassLoader()
                      .getResource("jeu-de-donnees-des-jeux-de-donnees-open-data-paris.rdf")
                      .toURI() + "','RDF/XML')");
      Map<String, Object> next = importResults
              .next().asMap();
      final List<Node> nodes = (List<Node>) next.get("nodes");
      assertEquals(15, nodes.size());
      final List<Relationship> rels = (List<Relationship>) next.get("relationships");
      assertEquals(15, rels.size());
    }
  }

  @Test
  public void testPreviewFromBadUriFile() throws Exception {
    try (Session session = driver.session()) {

      initialiseGraphDB(neo4j.defaultDatabaseService(), null);

      Result importResults
              = session.run("CALL n10s.rdf.preview.fetch('" +
              RDFPreviewTest.class.getClassLoader()
                      .getResource("badUri.ttl")
                      .toURI()
              + "','Turtle',{ handleVocabUris: 'KEEP', handleRDFTypes: 'NODES', verifyUriSyntax: false})");
      Map<String, Object> next = importResults
              .next().asMap();
      final List<Node> nodes = (List<Node>) next.get("nodes");
      assertEquals(2, nodes.size());
      final List<Relationship> rels = (List<Relationship>) next.get("relationships");
      assertEquals(1, rels.size());
    }
  }

  @Test
  public void testPreviewFromBadUriFileFail() throws Exception {
    try (Session session = driver.session()) {

      initialiseGraphDB(neo4j.defaultDatabaseService(),
              "{ handleVocabUris: 'KEEP', handleRDFTypes: 'NODES'}");

      try {
        Result importResults
                = session.run("CALL n10s.rdf.preview.fetch('" +
                RDFPreviewTest.class.getClassLoader()
                        .getResource("badUri.ttl")
                        .toURI() + "','Turtle')");

        importResults.next();
        //we should not get here
        assertTrue(false);
      } catch (Exception e) {
        //expected
        assertTrue(e.getMessage().contains("Illegal percent encoding U+25"));
      }
    }
  }

  @Test
  public void testPreviewFromFileLangFilter() throws Exception {
    try (Session session = driver.session()) {

      initialiseGraphDB(neo4j.defaultDatabaseService(),
              "{ handleVocabUris: 'KEEP', handleRDFTypes: 'NODES', keepLangTag : false }");

      Result importResults
              = session.run("CALL n10s.rdf.preview.fetch('" +
              RDFPreviewTest.class.getClassLoader().getResource("multilang.ttl")
                      .toURI()
              + "','Turtle', { languageFilter: 'fr' })");
      Record next = importResults
              .next();

      assertEquals(1, next.get("nodes").size());
      assertEquals("Cette Série des Années Soixante-dix",
              next.get("nodes").asList(ofNode()).get(0).get("http://example.org/vocab/show/localName")
                      .asString());
      assertEquals(0, (next.get("relationships")).size());

      importResults
              = session.run("CALL n10s.rdf.preview.fetch('" +
              RDFPreviewTest.class.getClassLoader().getResource("multilang.ttl").toURI()
              + "','Turtle', { languageFilter: 'en' })");
      assertEquals("That Seventies Show", importResults
              .next().get("nodes").asList(ofNode()).get(0)
              .get("http://example.org/vocab/show/localName").asString());
    }
  }
}
