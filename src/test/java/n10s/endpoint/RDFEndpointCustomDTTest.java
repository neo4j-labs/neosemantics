package n10s.endpoint;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.type.CollectionType;
import com.fasterxml.jackson.databind.type.TypeFactory;
import com.google.common.io.Resources;
import n10s.ModelTestUtils;
import n10s.graphconfig.GraphConfigProcedures;
import n10s.mapping.MappingUtils;
import n10s.nsprefixes.NsPrefixDefProcedures;
import n10s.onto.load.OntoLoadProcedures;
import n10s.quadrdf.delete.QuadRDFDeleteProcedures;
import n10s.quadrdf.load.QuadRDFLoadProcedures;
import n10s.rdf.RDFProcedures;
import n10s.rdf.delete.RDFDeleteProcedures;
import n10s.rdf.export.RDFExportProcedures;
import n10s.rdf.load.RDFLoadProcedures;
import n10s.validation.ValidationProcedures;
import org.eclipse.rdf4j.model.vocabulary.RDF;
import org.eclipse.rdf4j.rio.RDFFormat;
import org.junit.*;
import org.neo4j.driver.Record;
import org.neo4j.driver.*;
import org.neo4j.driver.types.Node;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Result;
import org.neo4j.graphdb.Transaction;
import org.neo4j.harness.junit.rule.Neo4jRule;

import java.net.URI;
import java.net.URLEncoder;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.*;

import static n10s.CommonProcedures.UNIQUENESS_CONSTRAINT_STATEMENT;
import static n10s.graphconfig.Params.PREFIX_SEPARATOR;
import static org.junit.Assert.*;
import static org.neo4j.internal.helpers.collection.Iterators.count;

/**
 * Tests for custom data types, quad RDF, and related features.
 * Split from RDFEndpointTest.java
 */
public class RDFEndpointCustomDTTest {

  public static Driver driver;
  public static GraphDatabaseService graphDatabaseService;
  public static GraphDatabaseService tempGDBs;
  public static Driver tempDriver;

  // New HttpClient instance
  private static final HttpClient client = HttpClient.newBuilder()
          .followRedirects(HttpClient.Redirect.ALWAYS)
          .build();

  @ClassRule
  public static Neo4jRule neo4j = new Neo4jRule().withUnmanagedExtension("/rdf", RDFEndpoint.class)
          .withProcedure(RDFLoadProcedures.class)
          .withProcedure(QuadRDFLoadProcedures.class)
          .withProcedure(QuadRDFDeleteProcedures.class)
          .withFunction(RDFProcedures.class)
          .withProcedure(MappingUtils.class)
          .withProcedure(GraphConfigProcedures.class)
          .withProcedure(RDFDeleteProcedures.class)
          .withProcedure(OntoLoadProcedures.class)
          .withProcedure(NsPrefixDefProcedures.class)
          .withProcedure(ValidationProcedures.class)
          .withProcedure(RDFExportProcedures.class);

  @ClassRule
  public static Neo4jRule temp = new Neo4jRule()
          .withProcedure(RDFLoadProcedures.class)
          .withProcedure(GraphConfigProcedures.class);

  @BeforeClass
  public static void init() {
    driver = GraphDatabase.driver(neo4j.boltURI(),
            Config.builder().withoutEncryption().build());

    tempDriver = GraphDatabase.driver(temp.boltURI(),
            Config.builder().withoutEncryption().build());
  }

  @Before
  public void cleanDatabase() {
    driver.session().run("match (n) detach delete n").consume();
    driver.session().run("drop constraint n10s_unique_uri if exists").consume();
    driver.session().run("drop index uri_index if exists").consume();

    tempDriver.session().run("match (n) detach delete n").consume();
    tempDriver.session().run("drop constraint n10s_unique_uri if exists").consume();
    tempDriver.session().run("drop index uri_index if exists").consume();

    graphDatabaseService = neo4j.defaultDatabaseService();
    tempGDBs = temp.defaultDatabaseService();
  }

  private static final ObjectMapper jsonMapper = new ObjectMapper();

  private static final CollectionType collectionType = TypeFactory
      .defaultInstance().constructCollectionType(Set.class, Map.class);

  // Refactored resolveURI to construct the URL without using HTTP.GET().location()
  public static String resolveURI(java.net.URI baseUri, String path) throws java.net.URISyntaxException {
    try {
      HttpRequest request = HttpRequest.newBuilder()
              .uri(baseUri.resolve("rdf"))
              .GET()
              .build();
      HttpResponse<String> response = client.send(request, HttpResponse.BodyHandlers.ofString());
      String resolvedBase = response.uri().toString();
      if(!resolvedBase.endsWith("/")) {
        resolvedBase += "/";
      }
      return new java.net.URI(resolvedBase).resolve(path).toString();
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  @Test
  public void testNodeByUriAfterImportWithCustomDTKeepUris() throws Exception {
    try (Transaction tx = graphDatabaseService.beginTx()) {
      tx.execute(UNIQUENESS_CONSTRAINT_STATEMENT);
      tx.commit();
    }

    try (Transaction tx = graphDatabaseService.beginTx()) {
      tx.execute(
              "CALL n10s.graphconfig.init( {keepLangTag: true, handleVocabUris: 'KEEP', handleMultival: 'OVERWRITE', keepCustomDataTypes: true, typesToLabels: true} )");
      tx.execute("CALL n10s.rdf.import.fetch('" +
              RDFEndpointCustomDTTest.class.getClassLoader().getResource("customDataTypes2.ttl")
                      .toURI()
              + "','Turtle')");

      tx.commit();
    }

    HttpRequest request = HttpRequest.newBuilder()
            .uri(URI.create(resolveURI(neo4j.httpURI(), "neo4j/describe?nodeIdentifier=") + URLEncoder
                    .encode("http://example.org/Resource1", StandardCharsets.UTF_8.toString())))
            .header("Accept", "text/turtle")
            .GET()
            .build();
    HttpResponse<String> response = client.send(request, HttpResponse.BodyHandlers.ofString());

    String expected = "@prefix xsd: <http://www.w3.org/2001/XMLSchema#> .\n" +
        "<http://example.org/Resource1>  a  <http://example.org/Resource>;\n" +
        "  <http://example.org/Predicate1>  \"2008-04-17\"^^<http://www.w3.org/2001/XMLSchema#date>;\n"
        +
        "  <http://example.org/Predicate2>  \"4.75\"^^xsd:double;\n" +
        "  <http://example.org/Predicate3>  \"2\"^^xsd:long;\n" +
        "  <http://example.org/Predicate4>  true;\n" +
        "  <http://example.org/Predicate5>  \"2\"^^xsd:double;\n" +
        "  <http://example.org/Predicate6>  \"4\"^^xsd:double;\n" +
        "  <http://example.org/Predicate7>  \"52.63\"^^<http://example.org/USD>;\n" +
        "  <http://example.org/Predicate8>  \"2008-03-22T00:00:00\"^^<http://www.w3.org/2001/XMLSchema#dateTime>;\n"
        +
        "  <http://example.org/Predicate9> \"-100\"^^xsd:long.";

    assertEquals(200, response.statusCode());
    assertTrue(ModelTestUtils
            .compareModels(expected, RDFFormat.TURTLE, response.body(), RDFFormat.TURTLE));

  }

  @Test
  public void testNodeByUriAfterImportWithCustomDTShortenURIs() throws Exception {
    try (Transaction tx = graphDatabaseService.beginTx()) {
      tx.execute(UNIQUENESS_CONSTRAINT_STATEMENT);
      tx.commit();
    }
    try (Transaction tx = graphDatabaseService.beginTx()) {
      tx.execute("CALL n10s.graphconfig.init( {keepLangTag: true, "
              + " handleVocabUris: 'SHORTEN', handleMultival: 'OVERWRITE',"
              + " keepCustomDataTypes: true, typesToLabels: true} )");
      tx.execute("CALL n10s.rdf.import.fetch('" +
              RDFEndpointCustomDTTest.class.getClassLoader().getResource("customDataTypes2.ttl")
                      .toURI() + "','Turtle')");

      tx.commit();
    }
    HttpRequest request = HttpRequest.newBuilder()
            .uri(URI.create(resolveURI(neo4j.httpURI(), "neo4j/describe?nodeIdentifier=") + URLEncoder
                    .encode("http://example.org/Resource1", StandardCharsets.UTF_8.toString())))
            .header("Accept", "text/turtle")
            .GET()
            .build();
    HttpResponse<String> response = client.send(request, HttpResponse.BodyHandlers.ofString());

    String expected = "@prefix xsd: <http://www.w3.org/2001/XMLSchema#> .\n" +
        "\n" +
        "<http://example.org/Resource1>\n" +
        "                                a  <http://example.org/Resource>;\n" +
        "  <http://example.org/Predicate1>  \"2008-04-17\"^^<http://www.w3.org/2001/XMLSchema#date>;\n"
        +
        "  <http://example.org/Predicate2>  \"4.75\"^^xsd:double;\n" +
        "  <http://example.org/Predicate3>  \"2\"^^xsd:long;\n" +
        "  <http://example.org/Predicate4>  true;\n" +
        "  <http://example.org/Predicate5>  \"2\"^^xsd:double;\n" +
        "  <http://example.org/Predicate6>  \"4\"^^xsd:double;\n" +
        "  <http://example.org/Predicate7>  \"52.63\"^^<http://example.org/USD>;\n"
        +
        "  <http://example.org/Predicate8>  \"2008-03-22T00:00:00\"^^<http://www.w3.org/2001/XMLSchema#dateTime>;\n"
        +
        "  <http://example.org/Predicate9> \"-100\"^^xsd:long.";

    assertEquals(200, response.statusCode());
    assertTrue(ModelTestUtils
            .compareModels(expected, RDFFormat.TURTLE, response.body(), RDFFormat.TURTLE));

  }

  @Test
  public void testNodeByUriAfterImportWithMultiCustomDTKeepUris() throws Exception {
    try (Transaction tx = graphDatabaseService.beginTx()) {
      tx.execute(UNIQUENESS_CONSTRAINT_STATEMENT);
      tx.commit();
    }
    try (Transaction tx = graphDatabaseService.beginTx()) {
      tx.execute("CALL n10s.graphconfig.init( "
              + "{ keepLangTag: true, handleVocabUris: 'KEEP', handleMultival: 'ARRAY', "
              + "  multivalPropList: ['http://example.com/price', 'http://example.com/power'], "
              + "  keepCustomDataTypes: true, customDataTypePropList: ['http://example.com/price', 'http://example.com/color']} )");
      tx.execute("CALL n10s.rdf.import.fetch('" +
              RDFEndpointCustomDTTest.class.getClassLoader().getResource("customDataTypes.ttl")
                      .toURI()
              + "','Turtle')");

      tx.commit();
    }
    HttpRequest request = HttpRequest.newBuilder()
            .uri(URI.create(resolveURI(neo4j.httpURI(), "neo4j/describe?nodeIdentifier=") + URLEncoder
                    .encode("http://example.com/Mercedes", StandardCharsets.UTF_8.toString())))
            .header("Accept", "text/turtle")
            .GET()
            .build();
    HttpResponse<String> response = client.send(request, HttpResponse.BodyHandlers.ofString());

    String expected = "@prefix ex: <http://example.com/> .\n" +
        "@prefix xsd: <http://www.w3.org/2001/XMLSchema#> .\n" +
        "\n" +
        "ex:Mercedes \n" +
        "\trdf:type ex:Car ;\n" +
        "\tex:price \"10000\"^^ex:EUR ;\n" +
        "\tex:price \"11000\"^^ex:USD ;\n" +
        "\tex:power \"300\" ;\n" +
        "\tex:power \"223,71\" ;\n" +
        "\tex:color \"red\"^^ex:Color ;\n" +
        "\tex:class \"A-Class\"@en ;\n" +
        "\tex:released \"2019\"^^xsd:long ;\n" +
        "\tex:type \"Cabrio\" .";

    assertEquals(200, response.statusCode());
    assertTrue(ModelTestUtils
            .compareModels(expected, RDFFormat.TURTLE, response.body(), RDFFormat.TURTLE));

  }

  @Test
  public void testNodeByUriAfterImportWithMultiCustomDTShortenUris() throws Exception {
    try (Transaction tx = graphDatabaseService.beginTx()) {
      tx.execute(UNIQUENESS_CONSTRAINT_STATEMENT);
      tx.commit();
    }
    try (Transaction tx = graphDatabaseService.beginTx()) {
      tx.execute("CALL n10s.graphconfig.init( {keepLangTag: true, "
          + " handleVocabUris: 'SHORTEN', handleMultival: 'ARRAY', "
          + " multivalPropList: ['http://example.com/price', 'http://example.com/power'], "
          + " keepCustomDataTypes: true, "
          + " customDataTypePropList: ['http://example.com/price', 'http://example.com/color']} )");
      tx.execute("CALL n10s.rdf.import.fetch('" +
          RDFEndpoint.class.getClassLoader().getResource("customDataTypes.ttl")
              .toURI()
          + "','Turtle')");

      tx.commit();
    }
    HttpRequest request = HttpRequest.newBuilder()
            .uri(URI.create(resolveURI(neo4j.httpURI(), "neo4j/describe?nodeIdentifier=") + URLEncoder
                    .encode("http://example.com/Mercedes", StandardCharsets.UTF_8.toString())))
            .header("Accept", "text/turtle")
            .GET()
            .build();
    HttpResponse<String> response = client.send(request, HttpResponse.BodyHandlers.ofString());

    String expected = "@prefix ex: <http://example.com/> .\n" +
        "@prefix xsd: <http://www.w3.org/2001/XMLSchema#> .\n" +
        "\n" +
        "ex:Mercedes \n" +
        "\trdf:type ex:Car ;\n" +
        "\tex:price \"10000\"^^ex:EUR ;\n" +
        "\tex:price \"11000\"^^ex:USD ;\n" +
        "\tex:power \"300\" ;\n" +
        "\tex:power \"223,71\" ;\n" +
        "\tex:color \"red\"^^ex:Color ;\n" +
        "\tex:class \"A-Class\"@en ;\n" +
        "\tex:released \"2019\"^^xsd:long ;\n" +
        "\tex:type \"Cabrio\" .";

    assertEquals(200, response.statusCode());
    assertTrue(ModelTestUtils
            .compareModels(expected, RDFFormat.TURTLE, response.body(), RDFFormat.TURTLE));


  }

  @Test
  public void testCypherOnQuadRDFSerializeAsTriG() throws Exception {
    try (Transaction tx = graphDatabaseService.beginTx()) {
      tx.execute("CREATE INDEX uri_index FOR (r:Resource) ON (r.uri)");
      tx.commit();
    }
    try (Transaction tx = graphDatabaseService.beginTx()) {
      tx.execute("CALL n10s.graphconfig.init( { handleVocabUris: 'KEEP', "
              + " typesToLabels: true, commitSize: 500, keepCustomDataTypes: true, handleMultival: 'ARRAY'} )");
      tx.execute("CALL n10s.experimental.quadrdf.import.fetch('" +
              RDFEndpointCustomDTTest.class.getClassLoader().getResource("RDFDatasets/RDFDataset.trig")
                      .toURI()
              + "','TriG')");
      tx.commit();
    }

    Map<String, String> params = new HashMap<>();
    params.put("cypher", "MATCH (a:Resource) "
            + "OPTIONAL MATCH (a)-[r]->(b:Resource)"
            + "RETURN DISTINCT *");

    HttpRequest request = HttpRequest.newBuilder()
            .uri(URI.create(resolveURI(neo4j.httpURI(), "neo4j/cypher")))
            .header("Accept", "application/trig")
            .header("Content-Type", "application/json")
            .POST(HttpRequest.BodyPublishers.ofString(jsonMapper.writeValueAsString(params)))
            .build();
    HttpResponse<String> response = client.send(request, HttpResponse.BodyHandlers.ofString());

    String expected = Resources
            .toString(Resources.getResource("RDFDatasets/RDFDataset.trig"),
                    StandardCharsets.UTF_8);
    assertEquals(200, response.statusCode());
    assertTrue(ModelTestUtils
            .compareModels(expected, RDFFormat.TRIG, response.body(), RDFFormat.TRIG));

  }

  @Test
  public void testCypherOnQuadRDFSerializeAsNQuads() throws Exception {
    try (Transaction tx = graphDatabaseService.beginTx()) {
      tx.execute("CREATE INDEX uri_index FOR (r:Resource) ON (r.uri)");
      tx.commit();
    }
    try (Transaction tx = graphDatabaseService.beginTx()) {
      tx.execute("CALL n10s.graphconfig.init( { handleVocabUris: 'KEEP', "
              + " typesToLabels: true, commitSize: 500, keepCustomDataTypes: true, "
              + " handleMultival: 'ARRAY'} )");
      tx.execute("CALL n10s.experimental.quadrdf.import.fetch('" +
              RDFEndpointCustomDTTest.class.getClassLoader().getResource("RDFDatasets/RDFDataset.nq")
                      .toURI()
              + "','N-Quads')");
      tx.commit();
    }
    Map<String, String> params = new HashMap<>();
    params.put("cypher", "MATCH (a:Resource) "
            + "OPTIONAL MATCH (a)-[r]->(b)"
            + "RETURN DISTINCT *");

    HttpRequest request = HttpRequest.newBuilder()
            .uri(URI.create(resolveURI(neo4j.httpURI(), "neo4j/cypher")))
            .header("Accept", "application/n-quads")
            .header("Content-Type", "application/json")
            .POST(HttpRequest.BodyPublishers.ofString(jsonMapper.writeValueAsString(params)))
            .build();
    HttpResponse<String> response = client.send(request, HttpResponse.BodyHandlers.ofString());

    String expected = Resources
            .toString(Resources.getResource("RDFDatasets/RDFDataset.nq"),
                    StandardCharsets.UTF_8);
    assertEquals(200, response.statusCode());
    assertTrue(ModelTestUtils
            .compareModels(expected, RDFFormat.NQUADS, response.body(), RDFFormat.NQUADS));

  }

  @Test
  public void testNodeByUriOnQuadRDF() throws Exception {
    try (Transaction tx = graphDatabaseService.beginTx()) {
      tx.execute("CREATE INDEX uri_index FOR (r:Resource) ON (r.uri)");
      tx.commit();
    }
    try (Transaction tx = graphDatabaseService.beginTx()) {
      tx.execute(
              "CALL n10s.graphconfig.init( { handleVocabUris: 'KEEP', typesToLabels: true, keepCustomDataTypes: true, handleMultival: 'ARRAY'} )");
      tx.execute("CALL n10s.experimental.quadrdf.import.fetch('" +
              RDFEndpointCustomDTTest.class.getClassLoader().getResource("RDFDatasets/RDFDataset.trig")
                      .toURI()
              + "','TriG')");

      tx.commit();
    }

    HttpRequest request = HttpRequest.newBuilder()
            .uri(URI.create(resolveURI(neo4j.httpURI(), "neo4j/describe?nodeIdentifier=") + URLEncoder
                    .encode("http://www.example.org/exampleDocument#Monica",
                            StandardCharsets.UTF_8.toString())))
            .header("Accept", "application/trig")
            .GET()
            .build();
    HttpResponse<String> response = client.send(request, HttpResponse.BodyHandlers.ofString());

    String expected = "{\n"
            + "  <http://www.example.org/exampleDocument#Monica> a <http://www.example.org/vocabulary#Person>;\n"
            + "    <http://www.example.org/vocabulary#friendOf> <http://www.example.org/exampleDocument#John> .\n"
            + "}";

    assertEquals(200, response.statusCode());
    assertTrue(ModelTestUtils
            .compareModels(expected, RDFFormat.TRIG, response.body(), RDFFormat.TRIG));
  }

  @Test
  public void testNodeByUriWithGraphUriOnQuadRDFTrig() throws Exception {
    try (Transaction tx = graphDatabaseService.beginTx()) {
      tx.execute("CREATE INDEX uri_index FOR (r:Resource) ON (r.uri)");
      tx.commit();
    }
    try (Transaction tx = graphDatabaseService.beginTx()) {
      tx.execute(
              "CALL n10s.graphconfig.init( { handleVocabUris: 'KEEP', typesToLabels: true, keepCustomDataTypes: true, handleMultival: 'ARRAY'} )");
      tx.execute("CALL n10s.experimental.quadrdf.import.fetch('" +
              RDFEndpointCustomDTTest.class.getClassLoader().getResource("RDFDatasets/RDFDataset.trig")
                      .toURI()
              + "','TriG')");

      tx.commit();
    }

    HttpRequest request = HttpRequest.newBuilder()
            .uri(URI.create(resolveURI(neo4j.httpURI(), "neo4j/describe?nodeIdentifier=") + URLEncoder
                    .encode("http://www.example.org/exampleDocument#Monica",
                            StandardCharsets.UTF_8.toString())
                    + "&graphuri=http://www.example.org/exampleDocument%23G1"))
            .header("Accept", "application/trig")
            .GET()
            .build();
    HttpResponse<String> response = client.send(request, HttpResponse.BodyHandlers.ofString());

    String expected = "<http://www.example.org/exampleDocument#G1> {\n"
        + "  <http://www.example.org/exampleDocument#Monica> <http://www.example.org/vocabulary#name>\n"
        + "      \"Monica Murphy\";\n"
        + "    <http://www.example.org/vocabulary#homepage> <http://www.monicamurphy.org>;\n"
        + "    <http://www.example.org/vocabulary#knows> <http://www.example.org/exampleDocument#John>;\n"
        + "    <http://www.example.org/vocabulary#hasSkill> <http://www.example.org/vocabulary#Management>,\n"
        + "      <http://www.example.org/vocabulary#Programming>;\n"
        + "    <http://www.example.org/vocabulary#email> <mailto:monica@monicamurphy.org> .\n"
        + "}";

    assertEquals(200, response.statusCode());
    assertTrue(ModelTestUtils
            .compareModels(expected, RDFFormat.TRIG, response.body(), RDFFormat.TRIG));

  }

  @Test
  public void testNodeByUriWithGraphUriOnQuadRDFNQuads() throws Exception {
    try (Transaction tx = graphDatabaseService.beginTx()) {
      tx.execute("CREATE INDEX uri_index FOR (r:Resource) ON (r.uri)");
      tx.commit();
    }
    try (Transaction tx = graphDatabaseService.beginTx()) {
      tx.execute("CALL n10s.graphconfig.init({ handleVocabUris: 'KEEP', "
              + " typesToLabels: true, commitSize: 500, keepCustomDataTypes: true, handleMultival: 'ARRAY'})");
      tx.execute("CALL n10s.experimental.quadrdf.import.fetch('" +
              RDFEndpointCustomDTTest.class.getClassLoader().getResource("RDFDatasets/RDFDataset.nq")
                      .toURI()
              + "','N-Quads')");

      tx.commit();
    }

    HttpRequest request = HttpRequest.newBuilder()
            .uri(URI.create(resolveURI(neo4j.httpURI(), "neo4j/describe?nodeIdentifier=") + URLEncoder
                    .encode("http://www.example.org/exampleDocument#Monica",
                            StandardCharsets.UTF_8.toString())
                    + "&graphuri=http://www.example.org/exampleDocument%23G1"))
            .header("Accept", "application/n-quads")
            .GET()
            .build();
    HttpResponse<String> response = client.send(request, HttpResponse.BodyHandlers.ofString());
    String expected =
        "<http://www.example.org/exampleDocument#Monica> <http://www.example.org/vocabulary#name> \"Monica Murphy\" <http://www.example.org/exampleDocument#G1> .\n"
            + "<http://www.example.org/exampleDocument#Monica> <http://www.example.org/vocabulary#homepage> <http://www.monicamurphy.org> <http://www.example.org/exampleDocument#G1> .\n"
            + "<http://www.example.org/exampleDocument#Monica> <http://www.example.org/vocabulary#knows> <http://www.example.org/exampleDocument#John> <http://www.example.org/exampleDocument#G1> .\n"
            + "<http://www.example.org/exampleDocument#Monica> <http://www.example.org/vocabulary#hasSkill> <http://www.example.org/vocabulary#Management> <http://www.example.org/exampleDocument#G1> .\n"
            + "<http://www.example.org/exampleDocument#Monica> <http://www.example.org/vocabulary#hasSkill> <http://www.example.org/vocabulary#Programming> <http://www.example.org/exampleDocument#G1> .\n"
            + "<http://www.example.org/exampleDocument#Monica> <http://www.example.org/vocabulary#email> <mailto:monica@monicamurphy.org> <http://www.example.org/exampleDocument#G1> .";

    assertEquals(200, response.statusCode());
    assertTrue(ModelTestUtils
            .compareModels(expected, RDFFormat.NQUADS, response.body(), RDFFormat.NQUADS));

  }

  @Test
  public void testCypherOnQuadRDFAfterDeleteRDFBNodes() throws Exception {
    try (Transaction tx = graphDatabaseService.beginTx()) {
      tx.execute("CREATE INDEX uri_index FOR (r:Resource) ON (r.uri)");
      tx.commit();
    }
    try (Transaction tx = graphDatabaseService.beginTx()) {
      tx.execute("CALL n10s.graphconfig.init( {keepLangTag: true, handleVocabUris: 'KEEP', "
              + " handleMultival: 'ARRAY', keepCustomDataTypes: true})");
      tx.execute("CALL n10s.experimental.quadrdf.import.fetch('" +
              RDFEndpointCustomDTTest.class.getClassLoader().getResource(
                              "RDFDatasets/RDFDatasetBNodes.trig")
                      .toURI()
              + "','TriG')");
      Result res = tx.execute("CALL n10s.experimental.quadrdf.delete.fetch('" +
              RDFEndpointCustomDTTest.class.getClassLoader().getResource(
                              "RDFDatasets/RDFDatasetBNodesDelete.trig")
                      .toURI()
              + "','TriG')");
      Map map = res.next();
      assertEquals(3L, map.get("triplesDeleted"));
      assertEquals(
              "4 of the statements could not be deleted, due to containing a blank node.",
              map.get("extraInfo"));
      tx.commit();
    }

    Map<String, String> params = new HashMap<>();
    params.put("cypher", "MATCH (a:Resource) "
            + "OPTIONAL MATCH (a)-[r]->()"
            + "RETURN DISTINCT *");

    HttpRequest request = HttpRequest.newBuilder()
            .uri(URI.create(resolveURI(neo4j.httpURI(), "neo4j/cypher")))
            .header("Accept", "application/trig")
            .header("Content-Type", "application/json")
            .POST(HttpRequest.BodyPublishers.ofString(jsonMapper.writeValueAsString(params)))
            .build();
    HttpResponse<String> response = client.send(request, HttpResponse.BodyHandlers.ofString());

    String expected = Resources
            .toString(Resources.getResource("RDFDatasets/RDFDatasetBNodesPostDeletion.trig"),
                    StandardCharsets.UTF_8);
    assertEquals(200, response.statusCode());
    assertTrue(ModelTestUtils
            .compareModels(expected, RDFFormat.TRIG, response.body(), RDFFormat.TRIG));

  }


  @Test
  public void testTicket13061() throws Exception {
    //create constraint
    try (Transaction tx = graphDatabaseService.beginTx()) {
      tx.execute(UNIQUENESS_CONSTRAINT_STATEMENT);
      tx.commit();
    }
    //create graph config and import RDF
    try (Transaction tx = graphDatabaseService.beginTx()) {
      tx.execute("CALL n10s.graphconfig.init(" +
              "{ handleVocabUris: 'MAP', handleMultival: 'ARRAY', keepCustomDataTypes: true, keepLangTag: true})");
      Result importResult = tx.execute("CALL n10s.rdf.import.fetch('" +
              RDFEndpointCustomDTTest.class.getClassLoader().getResource("data13061.trig")
                      .toURI() + "','TriG',{})");

      tx.commit();
    } catch (Exception e){
      fail("exception raised on rdf.import");
    }
    //  check data is  correctly loaded
    Long id;
    try (Transaction tx = graphDatabaseService.beginTx()) {
      Result result = tx.execute("match (n:ConceptScheme) return properties(n) as n, size([(n)-[r]-()| r]) as deg");
      Map<String, Object> next = result.next();
      Map<String,Object> n = (Map<String,Object>)next.get("n");
      long[] tcVals = (long[])n.get("topConcepts");
      assertEquals(3L, tcVals.length);
      long[] expected = new long[]{0, 3, 5};
      assertTrue(Arrays.equals(tcVals, expected));
      assertEquals(1L, next.get("deg"));


      Result res
              = tx
              .execute(" CALL n10s.rdf.export.cypher(' match(n:ConceptScheme) return n ', {}) ");
      assertTrue(res.hasNext());
      while(res.hasNext()){
        Map<String, Object> triple = res.next();
        assertTrue(triple.get("subject").equals("http://data.elsevier.com/vocabulary/OmniScience"));
        List<String> expectedList = new ArrayList<String>();
        expectedList.add("0");
        expectedList.add("3");
        expectedList.add("5");
        assertTrue((triple.get("predicate").equals(RDF.TYPE.stringValue()) &&
                triple.get("object").equals("neo4j://graph.schema#ConceptScheme"))
        || (triple.get("predicate").equals("neo4j://graph.schema#topConcepts") &&
                expectedList.contains(triple.get("object"))) &&
                triple.get("isLiteral").equals(true) && triple.get("literalType").equals("http://www.w3.org/2001/XMLSchema#long"));
      }
    }

    Map<String, Object> map = new HashMap<>();
    map.put("cypher", "match(n:ConceptScheme) return n");

    HttpRequest request = HttpRequest.newBuilder()
            .uri(URI.create(resolveURI(neo4j.httpURI(), "neo4j/cypher")))
            .header("Accept", "text/plain")
            .header("Content-Type", "application/json")
            .POST(HttpRequest.BodyPublishers.ofString(jsonMapper.writeValueAsString(map)))
            .build();
    HttpResponse<String> response = client.send(request, HttpResponse.BodyHandlers.ofString());

    assertEquals(200, response.statusCode());
    String expected = "<http://data.elsevier.com/vocabulary/OmniScience> <neo4j://graph.schema#topConcepts> \"5\"^^<http://www.w3.org/2001/XMLSchema#long> .\n" +
            "<http://data.elsevier.com/vocabulary/OmniScience> <neo4j://graph.schema#topConcepts> \"3\"^^<http://www.w3.org/2001/XMLSchema#long> .\n" +
            "<http://data.elsevier.com/vocabulary/OmniScience> <neo4j://graph.schema#topConcepts> \"0\"^^<http://www.w3.org/2001/XMLSchema#long> .\n" +
            "<http://data.elsevier.com/vocabulary/OmniScience> <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <neo4j://graph.schema#ConceptScheme> .\n";
    assertTrue(ModelTestUtils
            .compareModels(expected, RDFFormat.NTRIPLES, response.body(), RDFFormat.TURTLE));
  }

}
