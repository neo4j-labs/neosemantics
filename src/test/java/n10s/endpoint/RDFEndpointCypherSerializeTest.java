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
 * Tests for serialization-focused Cypher-based RDF export endpoints (last 8 tests).
 * Split from RDFEndpointCypherTest.java
 */
public class RDFEndpointCypherSerializeTest {

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

  private String getExportedAsLPG( String uri ) {
    return "@prefix neovoc: <neo4j://graph.schema#> .\n"
            + "@prefix neoind: <neo4j://graph.individuals#> .\n"
            + "\n"
            + "<" + uri + "> neovoc:name \"the name\" .";
  }

  @Test
  public void testCypherWithBNodesSerializeAsRDFXML() throws Exception {
    try (Transaction tx = graphDatabaseService.beginTx()) {
      tx.execute("CALL n10s.graphconfig.init()");
      tx.execute("call n10s.nsprefixes.add('ns0','http://permid.org/ontology/organization/')");
      tx.execute("call n10s.nsprefixes.add('ns1','http://ont.thomsonreuters.com/mdaas/')");
      String dataInsertion =
          "CREATE (Keanu:Resource:ns0" + PREFIX_SEPARATOR + "Actor {ns1" + PREFIX_SEPARATOR
              + "name:'Keanu Reeves', ns1" + PREFIX_SEPARATOR
              + "born:1964, uri: '_:1-21523433750' })\n" +
              "CREATE (Carrie:Resource:ns0" + PREFIX_SEPARATOR + "Director {ns1"
              + PREFIX_SEPARATOR + "name:'Carrie-Anne Moss', ns1" + PREFIX_SEPARATOR
              + "born:1967, uri: 'https://permid.org/1-21523433751' })\n" +
              "CREATE (Laurence:Resource:ns0" + PREFIX_SEPARATOR + "Director {ns1"
              + PREFIX_SEPARATOR + "name:'Laurence Fishburne', ns1" + PREFIX_SEPARATOR
              + "born:1961, uri: 'https://permid.org/1-21523433752' })\n" +
              "CREATE (Hugo:Resource:ns0" + PREFIX_SEPARATOR + "Critic {ns1"
              + PREFIX_SEPARATOR + "name:'Hugo Weaving', ns1" + PREFIX_SEPARATOR
              + "born:1960, uri: 'https://permid.org/1-21523433753' })\n" +
              "CREATE (AndyW:Resource:ns0" + PREFIX_SEPARATOR + "Actor {ns1"
              + PREFIX_SEPARATOR + "name:'Andy Wachowski', ns1" + PREFIX_SEPARATOR
              + "born:1967, uri: 'https://permid.org/1-21523433754' })\n" +
              "CREATE (Keanu)-[:ns0" + PREFIX_SEPARATOR + "Likes]->(Carrie) ";
      tx.execute(dataInsertion);
      tx.commit();
    }

    Map<String, String> params = new HashMap<>();
    params.put("cypher", "MATCH (a)-[r:ns0" + PREFIX_SEPARATOR + "Likes]-(b) RETURN *");

    HttpRequest request = HttpRequest.newBuilder()
            .uri(URI.create(resolveURI(neo4j.httpURI(), "neo4j/cypher")))
            .header("Accept", "application/rdf+xml")
            .header("Content-Type", "application/json")
            .POST(HttpRequest.BodyPublishers.ofString(jsonMapper.writeValueAsString(params)))
            .build();
    HttpResponse<String> response = client.send(request, HttpResponse.BodyHandlers.ofString());

    String expected = "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n" +
        "<rdf:RDF\n" +
        "\txmlns:neovoc=\"neo4j://graph.schema#\"\n" +
        "\txmlns:rdf=\"http://www.w3.org/1999/02/22-rdf-syntax-ns#\">\n" +
        "\n" +
        "<rdf:Description rdf:about=\"https://permid.org/1-21523433751\">\n" +
        "\t<rdf:type rdf:resource=\"http://permid.org/ontology/organization/Director\"/>\n" +
        "\t<born xmlns=\"http://ont.thomsonreuters.com/mdaas/\" rdf:datatype=\"http://www.w3.org/2001/XMLSchema#long\">1967</born>\n"
        +
        "\t<name xmlns=\"http://ont.thomsonreuters.com/mdaas/\">Carrie-Anne Moss</name>\n" +
        "</rdf:Description>\n" +
        "\n" +
        "<rdf:Description rdf:about=\"_:1-21523433750\">\n" +
        "\t<Likes xmlns=\"http://permid.org/ontology/organization/\" rdf:resource=\"https://permid.org/1-21523433751\"/>\n"
        +
        "\t<rdf:type rdf:resource=\"http://permid.org/ontology/organization/Actor\"/>\n" +
        "\t<born xmlns=\"http://ont.thomsonreuters.com/mdaas/\" rdf:datatype=\"http://www.w3.org/2001/XMLSchema#long\">1964</born>\n"
        +
        "\t<name xmlns=\"http://ont.thomsonreuters.com/mdaas/\">Keanu Reeves</name>\n" +
        "\t<Likes xmlns=\"http://permid.org/ontology/organization/\" rdf:resource=\"https://permid.org/1-21523433751\"/>\n"
        +
        "</rdf:Description>\n" +
        "\n" +
        "</rdf:RDF>";

    assertEquals(200, response.statusCode());
    assertTrue(ModelTestUtils
            .compareModels(expected, RDFFormat.RDFXML, response.body(), RDFFormat.RDFXML));

  }

  @Test
  public void testcypherAfterImportWithCustomDTKeepURIsSerializeAsTurtle() throws Exception {
    try (Transaction tx = graphDatabaseService.beginTx()) {
      tx.execute(UNIQUENESS_CONSTRAINT_STATEMENT);
      tx.commit();
    }
    try (Transaction tx = graphDatabaseService.beginTx()) {
      tx.execute(
              "CALL n10s.graphconfig.init( {keepLangTag: true, handleVocabUris: 'KEEP', handleMultival: 'OVERWRITE', keepCustomDataTypes: true} )");
      tx.execute("CALL n10s.rdf.import.fetch('" +
              RDFEndpointCypherSerializeTest.class.getClassLoader().getResource("customDataTypes2.ttl")
                      .toURI()
              + "','Turtle')");

      tx.commit();
    }
    Map<String, String> params = new HashMap<>();
    params.put("cypher", "MATCH (n {uri: 'http://example.org/Resource1'})" +
            "OPTIONAL MATCH (n)-[]-(m) RETURN *");

    HttpRequest request = HttpRequest.newBuilder()
            .uri(URI.create(resolveURI(neo4j.httpURI(), "neo4j/cypher")))
            .header("Accept", "text/turtle")
            .header("Content-Type", "application/json")
            .POST(HttpRequest.BodyPublishers.ofString(jsonMapper.writeValueAsString(params)))
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
  public void testcypherDatesAndDatetimes() throws Exception {
    try (Transaction tx = graphDatabaseService.beginTx()) {
      tx.execute(UNIQUENESS_CONSTRAINT_STATEMENT);
      tx.commit();
    }
    try (Transaction tx = graphDatabaseService.beginTx()) {
      tx.execute("CALL n10s.graphconfig.init( {handleMultival: 'ARRAY'} )");
      tx.execute("CALL n10s.rdf.import.fetch('" +
          RDFEndpointCypherSerializeTest.class.getClassLoader()
              .getResource("datetime/datetime-simple-multivalued.ttl")
              .toURI()
          + "','Turtle')");

      tx.commit();
    }

    Map<String, String> params = new HashMap<>();
    params.put("cypher", "MATCH (n) RETURN *");

    HttpRequest request = HttpRequest.newBuilder()
            .uri(URI.create(resolveURI(neo4j.httpURI(), "neo4j/cypher")))
            .header("Accept", "text/turtle")
            .header("Content-Type", "application/json")
            .POST(HttpRequest.BodyPublishers.ofString(jsonMapper.writeValueAsString(params)))
            .build();
    HttpResponse<String> response = client.send(request, HttpResponse.BodyHandlers.ofString());

    String expected = "@prefix rdf:     <http://www.w3.org/1999/02/22-rdf-syntax-ns#>.\n"
        + "@prefix xsd:     <http://www.w3.org/2001/XMLSchema#>.\n"
        + "@prefix exterms: <hhttp://www.example.org/terms/>.\n"
        + "@prefix ex: <hhttp://www.example.org/indiv/>.\n"
        + "\n"
        + "ex:index.html  exterms:someDateValue  \"1999-08-16\"^^xsd:date, \"1999-08-17\"^^xsd:date, \"1999-08-18\"^^xsd:date  ;\n"
        + "               exterms:someDateTimeValues \"2012-12-31T23:57:00\"^^xsd:dateTime, \"2012-12-30T23:57:00\"^^xsd:dateTime .";

    assertEquals(200, response.statusCode());
    assertTrue(ModelTestUtils
            .compareModels(expected, RDFFormat.TURTLE, response.body(), RDFFormat.TURTLE));

  }


  @Test
  public void testcypherErrorWhereModelIsNotRDF() throws Exception {

    String cypherCreate = " CREATE (r:Resource { uri: 'neo4j://explicit_uri#123' , name: 'the name' }) ";
    try (Transaction tx = graphDatabaseService.beginTx()) {
      Result res = tx.execute(cypherCreate);
      tx.commit();
    }
    Map<String, String> params = new HashMap<>();
    params.put("cypher", "MATCH (n) RETURN * ");

    HttpRequest request = HttpRequest.newBuilder()
            .uri(URI.create(resolveURI(neo4j.httpURI(), "neo4j/cypher")))
            .header("Accept", "text/turtle")
            .header("Content-Type", "application/json")
            .POST(HttpRequest.BodyPublishers.ofString(jsonMapper.writeValueAsString(params)))
            .build();
    HttpResponse<String> response = client.send(request, HttpResponse.BodyHandlers.ofString());

    assertEquals(200, response.statusCode());

    assertTrue(ModelTestUtils
            .compareModels(getExportedAsLPG("neo4j://explicit_uri#123"), RDFFormat.TURTLE, response.body(), RDFFormat.TURTLE));

    try (Transaction tx = graphDatabaseService.beginTx()) {
      tx.execute(" MATCH (n) DETACH DELETE n ");
      tx.execute(" CALL n10s.graphconfig.init({handleVocabUris: 'IGNORE'}) ");
      Result res = tx.execute(cypherCreate);
      tx.commit();
    }

    request = HttpRequest.newBuilder()
            .uri(URI.create(resolveURI(neo4j.httpURI(), "neo4j/cypher")))
            .header("Accept", "text/turtle")
            .header("Content-Type", "application/json")
            .POST(HttpRequest.BodyPublishers.ofString(jsonMapper.writeValueAsString(params)))
            .build();
    response = client.send(request, HttpResponse.BodyHandlers.ofString());

    assertTrue(ModelTestUtils
            .compareModels(getExportedAsLPG("neo4j://explicit_uri#123"), RDFFormat.TURTLE, response.body(), RDFFormat.TURTLE));


    try (Transaction tx = graphDatabaseService.beginTx()) {
      String cypherRDFCreate = " CREATE (:Resource { uri: 'neo4j://explicit_uri#123' , voc__name: 'the name' }) ";
      tx.execute(" MATCH (n) DETACH DELETE n ");
      tx.execute(" CALL n10s.graphconfig.init() ");
      tx.execute("call n10s.nsprefixes.add('voc','neo4j://myvoc#')");
      tx.execute(cypherRDFCreate);
      tx.commit();
    }

    request = HttpRequest.newBuilder()
            .uri(URI.create(resolveURI(neo4j.httpURI(), "neo4j/cypher")))
            .header("Accept", "text/turtle")
            .header("Content-Type", "application/json")
            .POST(HttpRequest.BodyPublishers.ofString(jsonMapper.writeValueAsString(params)))
            .build();
    response = client.send(request, HttpResponse.BodyHandlers.ofString());

    String exportedAsRDF = "@prefix rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#> .\n"
        + "@prefix neovoc: <neo4j://myvoc#> .\n"
        + "\n"
        + "\n"
        + "<neo4j://explicit_uri#123> neovoc:name \"the name\" .";

    assertTrue(ModelTestUtils
            .compareModels(exportedAsRDF, RDFFormat.TURTLE, response.body(), RDFFormat.TURTLE));

  }

  @Test
  public void testcypherAfterImportWithCustomDTShortenURIsSerializeAsTurtle()
          throws Exception {
    try (Transaction tx = graphDatabaseService.beginTx()) {
      tx.execute(UNIQUENESS_CONSTRAINT_STATEMENT);
      tx.commit();
    }
    try (Transaction tx = graphDatabaseService.beginTx()) {
      tx.execute("CALL n10s.graphconfig.init( {keepLangTag: true, "
              + " handleVocabUris: 'SHORTEN', handleMultival: 'OVERWRITE', "
              + " keepCustomDataTypes: true } )");
      tx.execute("CALL n10s.rdf.import.fetch('" +
              RDFEndpointCypherSerializeTest.class.getClassLoader().getResource("customDataTypes2.ttl")
                      .toURI() + "','Turtle')");

      tx.commit();
    }
    Map<String, String> params = new HashMap<>();
    params.put("cypher", "MATCH (n {uri: 'http://example.org/Resource1'})" +
            "OPTIONAL MATCH (n)-[]-(m) RETURN *");

    HttpRequest request = HttpRequest.newBuilder()
            .uri(URI.create(resolveURI(neo4j.httpURI(), "neo4j/cypher")))
            .header("Accept", "text/turtle")
            .header("Content-Type", "application/json")
            .POST(HttpRequest.BodyPublishers.ofString(jsonMapper.writeValueAsString(params)))
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
  public void testcypherAfterImportWithMultiCustomDTKeepURIsSerializeAsTurtle()
          throws Exception {

    try (Transaction tx = graphDatabaseService.beginTx()) {
      tx.execute(UNIQUENESS_CONSTRAINT_STATEMENT);
      tx.commit();
    }
    try (Transaction tx = graphDatabaseService.beginTx()) {
      tx.execute("CALL n10s.graphconfig.init( "
              + "{keepLangTag: true, handleVocabUris: 'KEEP', handleMultival: 'ARRAY', "
              + " multivalPropList: ['http://example.com/price', 'http://example.com/power', 'http://example.com/class'], "
              + "keepCustomDataTypes: true, customDataTypePropList: ['http://example.com/price', 'http://example.com/color']} )");
      tx.execute("CALL n10s.rdf.import.fetch('" +
              RDFEndpointCypherSerializeTest.class.getClassLoader().getResource("customDataTypes.ttl")
                      .toURI()
              + "','Turtle')");

      tx.commit();
    }

    Map<String, String> params = new HashMap<>();
    params.put("cypher", "MATCH (a:`http://example.com/Car`) RETURN *");

    HttpRequest request = HttpRequest.newBuilder()
            .uri(URI.create(resolveURI(neo4j.httpURI(), "neo4j/cypher")))
            .header("Accept", "text/turtle")
            .header("Content-Type", "application/json")
            .POST(HttpRequest.BodyPublishers.ofString(jsonMapper.writeValueAsString(params)))
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
        "\tex:class \"A-Klasse\"@de ;\n" +
        "\tex:class \"A-Class\"@en ;\n" +
        "\tex:released \"2019\"^^xsd:long ;\n" +
        "\tex:type \"Cabrio\" .";

    assertEquals(200, response.statusCode());
    assertTrue(ModelTestUtils
            .compareModels(expected, RDFFormat.TURTLE, response.body(), RDFFormat.TURTLE));

  }

  @Test
  public void testcypherAfterImportWithMultiCustomDTShortenURIsSerializeAsTurtle()
          throws Exception {
    try (Transaction tx = graphDatabaseService.beginTx()) {
      tx.execute(UNIQUENESS_CONSTRAINT_STATEMENT);
      tx.commit();
    }
    try (Transaction tx = graphDatabaseService.beginTx()) {
      tx.execute(
              "CALL n10s.graphconfig.init( {keepLangTag: true, handleVocabUris: 'SHORTEN', handleMultival: 'ARRAY', "
                      + " multivalPropList: ['http://example.com/price', 'http://example.com/power', 'http://example.com/class'], "
                      + " keepCustomDataTypes: true, customDataTypePropList: ['http://example.com/price', 'http://example.com/color']} )");
      tx.execute("CALL n10s.rdf.import.fetch('" +
              RDFEndpointCypherSerializeTest.class.getClassLoader().getResource("customDataTypes.ttl")
                      .toURI()
              + "','Turtle')");

      tx.commit();
    }

    Map<String, String> params = new HashMap<>();
    params.put("cypher", "MATCH (a:ns0__Car) RETURN *");

    HttpRequest request = HttpRequest.newBuilder()
            .uri(URI.create(resolveURI(neo4j.httpURI(), "neo4j/cypher")))
            .header("Accept", "text/turtle")
            .header("Content-Type", "application/json")
            .POST(HttpRequest.BodyPublishers.ofString(jsonMapper.writeValueAsString(params)))
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
        "\tex:class \"A-Klasse\"@de ;\n" +
        "\tex:class \"A-Class\"@en ;\n" +
        "\tex:released \"2019\"^^xsd:long ;\n" +
        "\tex:type \"Cabrio\" .";

    assertEquals(200, response.statusCode());
    assertTrue(ModelTestUtils
            .compareModels(expected, RDFFormat.TURTLE, response.body(), RDFFormat.TURTLE));

  }

  @Test
  public void testcypherAfterDeleteRDFBNodes() throws Exception {
    try (Transaction tx = graphDatabaseService.beginTx()) {
      tx.execute(UNIQUENESS_CONSTRAINT_STATEMENT);

      tx.commit();
    }
    try (Transaction tx = graphDatabaseService.beginTx()) {
      tx.execute("CALL n10s.graphconfig.init( "
              + " { keepLangTag: true, handleVocabUris: 'KEEP', handleMultival: 'ARRAY', keepCustomDataTypes: true} )");
      tx.execute("CALL n10s.rdf.import.fetch('" +
              RDFEndpointCypherSerializeTest.class.getClassLoader().getResource("deleteRDF/bNodes.ttl")
                      .toURI()
              + "','Turtle')");
      Result res = tx.execute("CALL n10s.rdf.delete.fetch('" +
              RDFEndpointCypherSerializeTest.class.getClassLoader().getResource("deleteRDF/bNodesDeletion.ttl")
                      .toURI()
              + "','Turtle')");
      Map map = res.next();
      assertEquals(1L, map.get("triplesDeleted"));
      assertEquals(
              "8 of the statements could not be deleted, due to use of blank nodes.",
              map.get("extraInfo"));
      tx.commit();
    }
    String aliceUri = null;
    String addrUri = null;

    try (Transaction tx = graphDatabaseService.beginTx()) {
      Map<String, Object> next = tx.execute("MATCH (c:Resource { `http://example.org/fullName`: ['Alice Carol']} )-[:`http://example.org/hasAddress`]->(x) return c.uri as aliceUri, x.uri as addrUri").next();
      aliceUri = (String)next.get("aliceUri");
      addrUri = (String)next.get("addrUri");
    }

    Map<String, String> params = new HashMap<>();
    params.put("cypher", "MATCH p = ()<-[:`http://example.org/homePage`]-(:Resource)-[:`http://example.org/hasAddress`]->() return p");

    HttpRequest request = HttpRequest.newBuilder()
            .uri(URI.create(resolveURI(neo4j.httpURI(), "neo4j/cypher")))
            .header("Accept", "text/turtle")
            .header("Content-Type", "application/json")
            .POST(HttpRequest.BodyPublishers.ofString(jsonMapper.writeValueAsString(params)))
            .build();
    HttpResponse<String> response = client.send(request, HttpResponse.BodyHandlers.ofString());

    String expected = "@prefix rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#> .\n" +
            "\n" +
            "<" + addrUri + "> a <http://example.org/Address>;\n" +
            "  <http://example.org/addressLocality> \"London\";\n" +
            "  <http://example.org/postalCode> \"A1A1A1\";\n" +
            "  <http://example.org/streetAddress> \"123 Main St.\" .\n" +
            "\n" +
            "<" + aliceUri + "> <http://example.org/fullName>\n" +
            "    \"Alice Carol\";\n" +
            "  <http://example.org/hasAddress> <" + addrUri + ">;\n" +
            "  <http://example.org/homePage> <http://example.net/alice-carol> .";

    assertEquals(200, response.statusCode());
    assertTrue(ModelTestUtils
            .compareModels(expected, RDFFormat.TURTLE, response.body(), RDFFormat.TURTLE));

  }

}
