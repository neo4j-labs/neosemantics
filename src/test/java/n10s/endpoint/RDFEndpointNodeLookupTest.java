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
 * Tests for node lookup by ID, URI, label/property, and ping endpoint.
 * Split from RDFEndpointTest.java
 */
public class RDFEndpointNodeLookupTest {

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
  private String emptyJsonLd = "{\n"
      + "  \"@context\" : {\n"
      + "    \"n4sch\" : \"neo4j://graph.schema#\",\n"
      + "    \"n4ind\" : \"neo4j://graph.individuals#\"\n"
      + "  }\n"
      + "}";

  // Refactored resolveURI to construct the URL without using HTTP.GET().location()
  public static String resolveURI(java.net.URI baseUri, String path) throws java.net.URISyntaxException {
    // In newer Neo4j harness, the unmanaged extension is mounted at "/rdf"
    // and we can construct the URI directly.
    // neo4j.httpURI() returns something like http://localhost:1234
    // We need http://localhost:1234/rdf/neo4j/describe...

    // If we want to be robust and follow redirects like the old utility:
    try {
      HttpRequest request = HttpRequest.newBuilder()
              .uri(baseUri.resolve("rdf"))
              .GET()
              .build();
      // We don't really need to execute this if we trust the structure,
      // but if the extension redirects:
      HttpResponse<String> response = client.send(request, HttpResponse.BodyHandlers.ofString());
      // If 3xx, the client follows it automatically.
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
  public void testGetNodeById() throws Exception {
    try (Transaction tx = graphDatabaseService.beginTx()) {

      String ontoCreation = "MERGE (p:Category {catName: ' Person'})\n" +
          "MERGE (a:Category {catName: 'Actor'})\n" +
          "MERGE (d:Category {catName: 'Director'})\n" +
          "MERGE (c:Category {catName: 'Critic'})\n" +
          "CREATE (a)-[:SCO]->(p)\n" +
          "CREATE (d)-[:SCO]->(p)\n" +
          "CREATE (c)-[:SCO]->(p)\n" +
          "RETURN *";
      tx.execute(ontoCreation);
      String dataInsertion = "CREATE (Keanu:Actor {name:'Keanu Reeves', born:1964})\n" +
          "CREATE (Carrie:Director {name:'Carrie-Anne Moss', born:1967})\n" +
          "CREATE (Laurence:Director {name:'Laurence Fishburne', born:1961})\n" +
          "CREATE (Hugo:Critic {name:'Hugo Weaving', born:1960})\n" +
          "CREATE (AndyW:Actor {name:'Andy Wachowski', born:1967})\n" +
          "CREATE (Hugo)-[:WORKS_WITH]->(AndyW)\n" +
          "CREATE (Hugo)<-[:FRIEND_OF]-(Carrie)";
      tx.execute(dataInsertion);
      tx.commit();
    }

    // When
    Long id;
    try (Transaction tx = graphDatabaseService.beginTx()) {
      Result result = tx.execute("MATCH (n:Critic) RETURN id(n) AS id ");
      id = (Long) result.next().get("id");
      assertNotNull(id);
    }

    Map<String,String> nameToId = new HashMap<>();
    try (Transaction tx = graphDatabaseService.beginTx()) {
      tx.execute("match (n) return n.name as name, id(n) as id")
              .stream()
              .forEach(r -> nameToId.put((String) r.get("name"), String.format("#%s", (Long)r.get("id")) ));
    }

    // When
    HttpRequest request = HttpRequest.newBuilder()
            .uri(URI.create(resolveURI(neo4j.httpURI(), "neo4j/describe?nodeIdentifier=" + id.toString())))
            .header("Accept", "application/ld+json")
            .GET()
            .build();
    HttpResponse<String> response = client.send(request, HttpResponse.BodyHandlers.ofString());

    String expected = String.format("[ {\n"
        + "  \"@id\" : \"neo4j://graph.individuals%1$s\",\n"
        + "  \"neo4j://graph.schema#FRIEND_OF\" : [ {\n"
        + "    \"@id\" : \"neo4j://graph.individuals%2$s\"\n"
        + "  } ]\n"
        + "}, {\n"
        + "  \"@id\" : \"neo4j://graph.individuals%2$s\",\n"
        + "  \"@type\" : [ \"neo4j://graph.schema#Critic\" ],\n"
        + "  \"neo4j://graph.schema#WORKS_WITH\" : [ {\n"
        + "    \"@id\" : \"neo4j://graph.individuals%3$s\"\n"
        + "  } ],\n"
        + "  \"neo4j://graph.schema#born\" : [ {\n"
        + "    \"@type\" : \"http://www.w3.org/2001/XMLSchema#long\",\n"
        + "    \"@value\" : \"1960\"\n"
        + "  } ],\n"
        + "  \"neo4j://graph.schema#name\" : [ {\n"
        + "    \"@value\" : \"Hugo Weaving\"\n"
        + "  } ]\n"
        + "} ]",
            nameToId.get("Carrie-Anne Moss"),
            nameToId.get("Hugo Weaving"),
            nameToId.get("Andy Wachowski")
    );
    assertEquals(200, response.statusCode());
    assertTrue(ModelTestUtils
        .compareModels(expected, RDFFormat.JSONLD, response.body(), RDFFormat.JSONLD));

  }

  @Test
  public void testGetNodeByIdFromRDFizedLPG() throws Exception {
    try (Transaction tx = graphDatabaseService.beginTx()) {

      String configCreation = "CALL n10s.graphconfig.init({handleVocabUris:'IGNORE'}) ";

      tx.execute(configCreation);

      String dataInsertion = "CREATE (Keanu:Actor:Resource { uri: 'http://neo4j.com/movies/Keanu', name:'Keanu Reeves', born:1964})\n" +
              "CREATE (Carrie:Director:Resource {uri: 'http://neo4j.com/movies/Carrie', name:'Carrie-Anne Moss', born:1967})\n" +
              "CREATE (Laurence:Director:Resource {uri: 'http://neo4j.com/movies/Laurence', name:'Laurence Fishburne', born:1961})\n" +
              "CREATE (Hugo:Critic:Resource {uri: 'http://neo4j.com/movies/Hugo', name:'Hugo Weaving', born:1960})\n" +
              "CREATE (AndyW:Actor:Resource {uri: 'http://neo4j.com/movies/Andy', name:'Andy Wachowski', born:1967})\n" +
              "CREATE (Hugo)-[:WORKS_WITH { from: 1999 } ]->(AndyW)\n" +
              "CREATE (Hugo)<-[:FRIEND_OF]-(Carrie)";
      tx.execute(dataInsertion);
      tx.commit();

    }

    // When
    HttpRequest request = HttpRequest.newBuilder()
            .uri(URI.create(resolveURI(neo4j.httpURI(), "neo4j/describe?nodeIdentifier=http%3A%2F%2Fneo4j.com%2Fmovies%2FKeanu")))
            .header("Accept", "application/ld+json")
            .GET()
            .build();
    HttpResponse<String> response = client.send(request, HttpResponse.BodyHandlers.ofString());

    String expected = "{\n" +
            "  \"@id\" : \"http://neo4j.com/movies/Keanu\",\n" +
            "  \"@type\" : \"neovoc:Actor\",\n" +
            "  \"neovoc:born\" : {\n" +
            "    \"@type\" : \"http://www.w3.org/2001/XMLSchema#long\",\n" +
            "    \"@value\" : \"1964\"\n" +
            "  },\n" +
            "  \"neovoc:name\" : \"Keanu Reeves\",\n" +
            "  \"@context\" : {\n" +
            "    \"rdf\" : \"http://www.w3.org/1999/02/22-rdf-syntax-ns#\",\n" +
            "    \"neovoc\" : \"neo4j://graph.schema#\" " +
            "  }\n" +
            "}";
    assertEquals(200, response.statusCode());
    assertTrue(ModelTestUtils
            .compareModels(expected, RDFFormat.JSONLD, response.body(), RDFFormat.JSONLD));

    // When
    request = HttpRequest.newBuilder()
            .uri(URI.create(resolveURI(neo4j.httpURI(), "neo4j/describe?nodeIdentifier=http%3A%2F%2Fneo4j.com%2Fmovies%2FHugo")))
            .header("Accept", "text/x-turtlestar")
            .GET()
            .build();
    response = client.send(request, HttpResponse.BodyHandlers.ofString());

    expected = "@prefix rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#> .\n" +
            "@prefix neovoc: <neo4j://graph.schema#> .\n" +
            "@prefix neoind: <neo4j://graph.individuals#> .\n" +
            "\n" +
            "<http://neo4j.com/movies/Hugo> a neovoc:Critic;\n" +
            "  neovoc:WORKS_WITH <http://neo4j.com/movies/Andy>;\n" +
            "  neovoc:name \"Hugo Weaving\";\n" +
            "  neovoc:born \"1960\"^^<http://www.w3.org/2001/XMLSchema#long> .\n" +
            "\n" +
            "<<<http://neo4j.com/movies/Hugo> neovoc:WORKS_WITH <http://neo4j.com/movies/Andy>>>\n" +
            "  neovoc:from \"1999\"^^<http://www.w3.org/2001/XMLSchema#long> .\n" +
            "\n" +
            "<http://neo4j.com/movies/Carrie> neovoc:FRIEND_OF <http://neo4j.com/movies/Hugo> .\n";


    assertEquals(200, response.statusCode());
    assertTrue(ModelTestUtils
            .compareModels(expected, RDFFormat.TURTLESTAR, response.body(), RDFFormat.TURTLESTAR));

  }

  @Test
  public void testGetNodeByIdRDFStar() throws Exception {
    try (Transaction tx = graphDatabaseService.beginTx()) {

      String dataInsertion = "CREATE (Keanu:Actor {name:'Keanu Reeves', born:1964})\n" +
          "CREATE (Carrie:Director {name:'Carrie-Anne Moss', born:1967})\n" +
          "CREATE (Laurence:Director {name:'Laurence Fishburne', born:1961})\n" +
          "CREATE (Hugo:Critic {name:'Hugo Weaving', born:1960})\n" +
          "CREATE (AndyW:Actor {name:'Andy Wachowski', born:1967})\n" +
          "CREATE (Hugo)-[:WORKS_WITH { hoursADay: 8 } ]->(AndyW)\n" +
          "CREATE (Hugo)<-[:FRIEND_OF  { since: 'the early days' }]-(Carrie)";
      tx.execute(dataInsertion);
      tx.commit();

    }
    Long id1;
    Long id4;
    Long id3;
    try (Transaction tx = graphDatabaseService.beginTx()) {
      Result result = tx.execute("MATCH (n3:Critic), (n4:Actor {name:'Andy Wachowski'}), (n1:Director {name:'Carrie-Anne Moss'}) " +
              "return id(n1) as id1, id(n4) as id4, id(n3) as id3");
      Map<String, Object> next = result.next();
      id1 = (Long) next.get("id1");
      id4 = (Long) next.get("id4");
      id3 = (Long) next.get("id3");
    }

    // When
    HttpRequest request = HttpRequest.newBuilder()
            .uri(URI.create(resolveURI(neo4j.httpURI(), "neo4j/describe?nodeIdentifier=") + id3.toString()))
            .header("Accept", "text/x-turtlestar")
            .GET()
            .build();
    HttpResponse<String> response = client.send(request, HttpResponse.BodyHandlers.ofString());

    String expected = String.format( "@prefix neoind: <neo4j://graph.individuals#> .\n"
        + "@prefix neovoc: <neo4j://graph.schema#> .\n"
        + "@prefix rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#> .\n"
        + "\n"
        + "neoind:%2$s a neovoc:Critic;\n"
        + "  neovoc:WORKS_WITH neoind:%3$s;\n"
        + "  neovoc:born \"1960\"^^<http://www.w3.org/2001/XMLSchema#long>;\n"
        + "  neovoc:name \"Hugo Weaving\" .\n"
        + "\n"
        + "<<neoind:%1$s neovoc:FRIEND_OF neoind:%2$s>> neovoc:since \"the early days\" .\n"
        + "\n"
        + "<<neoind:%2$s neovoc:WORKS_WITH neoind:%3$s>> neovoc:hoursADay \"8\"^^<http://www.w3.org/2001/XMLSchema#long> .\n"
        + "\n"
        + "neoind:%1$s neovoc:FRIEND_OF neoind:%2$s .", id1, id3, id4);
    assertEquals(200, response.statusCode());
    assertTrue(ModelTestUtils
            .compareModels(expected, RDFFormat.TURTLESTAR, response.body(), RDFFormat.TURTLESTAR));

  }

  @Test
  public void testFindNodeByLabelAndProperty() throws Exception {

    try (Transaction tx = graphDatabaseService.beginTx()) {
      String ontoCreation = "MERGE (p:Category {catName: ' Person'})\n" +
          "MERGE (a:Category {catName: 'Actor'})\n" +
          "MERGE (d:Category {catName: 'Director'})\n" +
          "MERGE (c:Category {catName: 'Critic'})\n" +
          "CREATE (a)-[:SCO]->(p)\n" +
          "CREATE (d)-[:SCO]->(p)\n" +
          "CREATE (c)-[:SCO]->(p)\n" +
          "RETURN *";
      tx.execute(ontoCreation);
      String dataInsertion = "CREATE (Keanu:Actor {name:'Keanu Reeves', born:1964})\n" +
          "CREATE (Carrie:Director {name:'Carrie-Anne Moss', born:1967})\n" +
          "CREATE (Laurence:Director {name:'Laurence Fishburne', born:1961})\n" +
          "CREATE (Hugo:Critic {name:'Hugo Weaving', born:1960})\n" +
          "CREATE (AndyW:Actor {name:'Andy Wachowski', born:1964})\n" +
          "CREATE (Hugo)-[:WORKS_WITH]->(AndyW)\n" +
          "CREATE (Hugo)<-[:FRIEND_OF]-(Carrie)";
      tx.execute(dataInsertion);
      tx.commit();
    }
    Long id = null;
    try (Transaction tx = graphDatabaseService.beginTx()) {
      Result result = tx.execute("MATCH (n:Director {name:'Laurence Fishburne'}) RETURN id(n) AS id ");
      id = (Long) result.next().get("id");
      assertNotNull(id);
    }
    // When
    HttpRequest request = HttpRequest.newBuilder()
            .uri(URI.create(resolveURI(neo4j.httpURI(), "neo4j/describe/find/Director/born/1961?valType=INTEGER")))
            .header("Accept", "application/ld+json")
            .GET()
            .build();
    HttpResponse<String> response = client.send(request, HttpResponse.BodyHandlers.ofString());
//        resolveURI(neo4j.httpURI(), "neo4j/describe?nodeIdentifier=find/Director/born/1961?valType=INTEGER"));

    String expected = String.format("[ {\n"
        + "  \"@id\" : \"neo4j://graph.individuals#%s\",\n"
        + "  \"@type\" : [ \"neo4j://graph.schema#Director\" ],\n"
        + "  \"neo4j://graph.schema#born\" : [ {\n"
        + "    \"@type\" : \"http://www.w3.org/2001/XMLSchema#long\",\n"
        + "    \"@value\" : \"1961\"\n"
        + "  } ],\n"
        + "  \"neo4j://graph.schema#name\" : [ {\n"
        + "    \"@value\" : \"Laurence Fishburne\"\n"
        + "  } ]\n"
        + "} ]", id.toString());

    assertEquals(200, response.statusCode());
    assertTrue(ModelTestUtils
            .compareModels(expected, RDFFormat.JSONLD, response.body(), RDFFormat.JSONLD));

    // When
    request = HttpRequest.newBuilder()
            .uri(URI.create(resolveURI(neo4j.httpURI(), "neo4j/describe/find/Director/name/Laurence%20Fishburne")))
            .header("Accept", "application/ld+json")
            .GET()
            .build();
    response = client.send(request, HttpResponse.BodyHandlers.ofString());

    expected = String.format("[ {\n"
        + "  \"@id\" : \"neo4j://graph.individuals#%s\",\n"
        + "  \"@type\" : [ \"neo4j://graph.schema#Director\" ],\n"
        + "  \"neo4j://graph.schema#born\" : [ {\n"
        + "    \"@type\" : \"http://www.w3.org/2001/XMLSchema#long\",\n"
        + "    \"@value\" : \"1961\"\n"
        + "  } ],\n"
        + "  \"neo4j://graph.schema#name\" : [ {\n"
        + "    \"@value\" : \"Laurence Fishburne\"\n"
        + "  } ]\n"
        + "} ]", id.toString());
    assertEquals(200, response.statusCode());
    assertTrue(ModelTestUtils
            .compareModels(expected, RDFFormat.JSONLD, response.body(), RDFFormat.JSONLD));

    // When
    request = HttpRequest.newBuilder()
            .uri(URI.create(resolveURI(neo4j.httpURI(), "neo4j/describe/find/Actor/born/1964?valType=INTEGER")))
            .header("Accept", "application/ld+json")
            .GET()
            .build();
    response = client.send(request, HttpResponse.BodyHandlers.ofString());

    Map<String,String> nameToId = new HashMap<>();
    try (Transaction tx = graphDatabaseService.beginTx()) {
      tx.execute("match (n) return n.name as name, id(n) as id")
              .stream()
              .forEach(r -> nameToId.put((String) r.get("name"), String.format("#%s", (Long)r.get("id")) ));
    }

    expected = String.format(
  "[ {\n"
        + "  \"@id\" : \"neo4j://graph.individuals%1$s\",\n"
        + "  \"@type\" : [ \"neo4j://graph.schema#Actor\" ],\n"
        + "  \"neo4j://graph.schema#born\" : [ {\n"
        + "    \"@type\" : \"http://www.w3.org/2001/XMLSchema#long\",\n"
        + "    \"@value\" : \"1964\"\n"
        + "  } ],\n"
        + "  \"neo4j://graph.schema#name\" : [ {\n"
        + "    \"@value\" : \"Keanu Reeves\"\n"
        + "  } ]\n"
        + "}, {\n"
        + "  \"@id\" : \"neo4j://graph.individuals%2$s\",\n"
        + "  \"neo4j://graph.schema#WORKS_WITH\" : [ {\n"
        + "    \"@id\" : \"neo4j://graph.individuals%3$s\"\n"
        + "  } ]\n"
        + "}, {\n"
        + "  \"@id\" : \"neo4j://graph.individuals%3$s\",\n"
        + "  \"@type\" : [ \"neo4j://graph.schema#Actor\" ],\n"
        + "  \"neo4j://graph.schema#born\" : [ {\n"
        + "    \"@type\" : \"http://www.w3.org/2001/XMLSchema#long\",\n"
        + "    \"@value\" : \"1964\"\n"
        + "  } ],\n"
        + "  \"neo4j://graph.schema#name\" : [ {\n"
        + "    \"@value\" : \"Andy Wachowski\"\n"
        + "  } ]\n"
        + "} ]",
          nameToId.get("Keanu Reeves"),   //0
          nameToId.get("Hugo Weaving"),   //1
          nameToId.get("Andy Wachowski") //2

    );
    assertEquals(200, response.statusCode());
    assertTrue(ModelTestUtils
            .compareModels(expected, RDFFormat.JSONLD, response.body(), RDFFormat.JSONLD));
  }

  @Test
  public void testFindNodeByLabelAndPropertyNotFoundOrInvalid() throws Exception {
    HttpRequest request = HttpRequest.newBuilder()
            .uri(URI.create(resolveURI(neo4j.httpURI(), "neo4j/describe/find/WrongLabel/wrongProperty/someValue")))
            .header("Accept", "application/ld+json")
            .GET()
            .build();
    HttpResponse<String> response = client.send(request, HttpResponse.BodyHandlers.ofString());

    // TODO - document it??
    assertEquals(emptyJsonLd, response.body());
    assertEquals(200, response.statusCode());

    request = HttpRequest.newBuilder()
            .uri(URI.create(resolveURI(neo4j.httpURI(), "neo4j/describe/find/Something")))
            .header("Accept", "application/ld+json")
            .GET()
            .build();
    response = client.send(request, HttpResponse.BodyHandlers.ofString());

    assertEquals("{\"errors\":[{\"code\":\"Neo.ClientError.Request.Invalid\",\"message\":\"Not Found\"}]}", response.body());
    assertEquals(404, response.statusCode());
  }

  @Test
  public void testGetNodeByUriOrIdNotFoundOrInvalid() throws Exception {

    HttpRequest request = HttpRequest.newBuilder()
            .uri(URI.create(resolveURI(neo4j.httpURI(), "neo4j/describe?nodeIdentifier=9999999")))
            .header("Accept", "text/n3")
            .GET()
            .build();
    HttpResponse<String> response = client.send(request, HttpResponse.BodyHandlers.ofString());
    assertEquals(200, response.statusCode());
    assertEquals("@prefix n4sch: <neo4j://graph.schema#> .\n" +
            "@prefix n4ind: <neo4j://graph.individuals#> .\n", response.body());

    request = HttpRequest.newBuilder()
            .uri(URI.create(resolveURI(neo4j.httpURI(), "neo4j/describe?nodeIdentifier=9999999")))
            .header("Accept", "application/rdf+xml")
            .GET()
            .build();
    response = client.send(request, HttpResponse.BodyHandlers.ofString());
    assertEquals(200, response.statusCode());
    assertEquals("<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n" +
            "<rdf:RDF\n" +
            "\txmlns:n4sch=\"neo4j://graph.schema#\"\n" +
            "\txmlns:n4ind=\"neo4j://graph.individuals#\"\n" +
            "\txmlns:rdf=\"http://www.w3.org/1999/02/22-rdf-syntax-ns#\">\n" +
            "\n" +
            "</rdf:RDF>", response.body());

    request = HttpRequest.newBuilder()
            .uri(URI.create(resolveURI(neo4j.httpURI(), "neo4j/describe?nodeIdentifier=adb")))
            .header("Accept", "application/ld+json")
            .GET()
            .build();
    response = client.send(request, HttpResponse.BodyHandlers.ofString());

    assertEquals("[ ]", response.body());
    assertEquals(200, response.statusCode());

    try (Transaction tx = graphDatabaseService.beginTx()) {
      tx.execute("CALL n10s.graphconfig.init()");
      tx.commit();
    }

    request = HttpRequest.newBuilder()
            .uri(URI.create(resolveURI(neo4j.httpURI(), "neo4j/describe?nodeIdentifier=9999999")))
            .header("Accept", "text/n3")
            .GET()
            .build();
    response = client.send(request, HttpResponse.BodyHandlers.ofString());
    assertEquals(200, response.statusCode());
    assertEquals("", response.body());

    request = HttpRequest.newBuilder()
            .uri(URI.create(resolveURI(neo4j.httpURI(), "neo4j/describe?nodeIdentifier=9999999")))
            .header("Accept", "application/rdf+xml")
            .GET()
            .build();
    response = client.send(request, HttpResponse.BodyHandlers.ofString());
    assertEquals(200, response.statusCode());
    assertEquals("<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n" +
            "<rdf:RDF\n" +
            "\txmlns:rdf=\"http://www.w3.org/1999/02/22-rdf-syntax-ns#\">\n" +
            "\n" +
            "</rdf:RDF>", response.body());

  }

  @Test
  public void testPing() throws Exception {
    HttpRequest request = HttpRequest.newBuilder()
            .uri(URI.create(resolveURI(neo4j.httpURI(), "ping")))
            .GET()
            .build();
    HttpResponse<String> response = client.send(request, HttpResponse.BodyHandlers.ofString());

    assertEquals("{\"ping\":\"here!\"}", response.body());
    assertEquals(200, response.statusCode());

  }

  @Test
  public void testNodeByUri() throws Exception {
    try (Transaction tx = graphDatabaseService.beginTx()) {
      tx.execute("CALL n10s.graphconfig.init()");
      tx.execute("call n10s.nsprefixes.add('ns1','http://ont.thomsonreuters.com/mdaas/')");
      tx.execute("call n10s.nsprefixes.add('ns0','http://permid.org/ontology/organization/')");
      String dataInsertion =
          "CREATE (Keanu:Resource:ns0" + PREFIX_SEPARATOR + "Actor {ns1" + PREFIX_SEPARATOR
              + "name:'Keanu Reeves', ns1" + PREFIX_SEPARATOR
              + "born:1964, uri: 'https://permid.org/1-21523433750' })\n" +
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
              "CREATE (Keanu)-[:ns0" + PREFIX_SEPARATOR + "Likes]->(Carrie) \n" +
              "CREATE (Keanu)<-[:ns0" + PREFIX_SEPARATOR + "FriendOf]-(Hugo) ";
      tx.execute(dataInsertion);
      tx.commit();
    }

    try (Transaction tx = graphDatabaseService.beginTx()) {
      Result result = tx
              .execute("MATCH (n:ns0" + PREFIX_SEPARATOR + "Critic) RETURN n.uri AS uri ");
      //assertEquals( 1, count( result ) );

      assertEquals("https://permid.org/1-21523433753", result.next().get("uri"));
    }
    HttpRequest request = HttpRequest.newBuilder()
            .uri(URI.create(resolveURI(neo4j.httpURI(), "neo4j/describe?nodeIdentifier=") + URLEncoder
                    .encode("https://permid.org/1-21523433750", StandardCharsets.UTF_8.toString())))
            .header("Accept", "text/turtle")
            .GET()
            .build();
    HttpResponse<String> response = client.send(request, HttpResponse.BodyHandlers.ofString());

    String expected = "@prefix neovoc: <neo4j://graph.schema#> .\n" +
        "<https://permid.org/1-21523433750> a <http://permid.org/ontology/organization/Actor>;\n"
        + " <http://ont.thomsonreuters.com/mdaas/born> \"1964\"^^<http://www.w3.org/2001/XMLSchema#long>;\n"
        + " <http://ont.thomsonreuters.com/mdaas/name> \"Keanu Reeves\";\n"
        + " <http://permid.org/ontology/organization/Likes> <https://permid.org/1-21523433751> .\n"
        + " <https://permid.org/1-21523433753> <http://permid.org/ontology/organization/FriendOf>\n"
        + " <https://permid.org/1-21523433750> .\n";
    assertEquals(200, response.statusCode());
    assertTrue(ModelTestUtils
            .compareModels(expected, RDFFormat.TURTLE, response.body(), RDFFormat.TURTLE));

  }

  @Test
  public void testNodeByUriAfterImport() throws Exception {
    try (Transaction tx = graphDatabaseService.beginTx()) {
      tx.execute(UNIQUENESS_CONSTRAINT_STATEMENT);
      tx.commit();
    }
    try (Transaction tx = graphDatabaseService.beginTx()) {
      tx.execute("CALL n10s.graphconfig.init({})");
      tx.execute("CALL n10s.rdf.import.fetch('" +
          RDFEndpointNodeLookupTest.class.getClassLoader().getResource("fibo-fragment.rdf")
              .toURI() + "','RDF/XML')");

      tx.commit();
    }

    HttpRequest request = HttpRequest.newBuilder()
            .uri(URI.create(resolveURI(neo4j.httpURI(), "neo4j/describe?nodeIdentifier=") + URLEncoder.encode(
                    "https://spec.edmcouncil.org/fibo/ontology/BE/Corporations/Corporations/BoardAgreement",
                    StandardCharsets.UTF_8.toString())
                    + "&excludeContext=true"))
            .header("Accept", "application/rdf+xml")
            .GET()
            .build();
    HttpResponse<String> response = client.send(request, HttpResponse.BodyHandlers.ofString());

    String expected = "<?xml version=\"1.0\" encoding=\"UTF-8\"?>" +
        "<rdf:RDF\txmlns:neovoc=\"neo4j://graph.schema#\"" +
        "\txmlns:rdf=\"http://www.w3.org/1999/02/22-rdf-syntax-ns#\">" +
        "<rdf:Description rdf:about=\"https://spec.edmcouncil.org/fibo/ontology/BE/Corporations/Corporations/BoardAgreement\">"
        +
        "\t<rdf:type rdf:resource=\"http://www.w3.org/2002/07/owl#Class\"/>" +
        "\t<definition xmlns=\"http://www.w3.org/2004/02/skos/core#\">a formal, legally binding agreement between members of the Board of Directors of the organization</definition>"
        +
        "\t<label xmlns=\"http://www.w3.org/2000/01/rdf-schema#\">board agreement</label>" +
        "</rdf:Description></rdf:RDF>";

    assertEquals(200, response.statusCode());
    assertTrue(ModelTestUtils
            .compareModels(expected, RDFFormat.RDFXML, response.body(), RDFFormat.RDFXML));

    //uris need to be urlencoded. Normally not a problem but beware of hash signs!!
    request = HttpRequest.newBuilder()
            .uri(URI.create(resolveURI(neo4j.httpURI(), "neo4j/describe?nodeIdentifier=")
                    + URLEncoder.encode("http://www.w3.org/2004/02/skos/core#TestyMcTestFace", "UTF-8")))
            .header("Accept", "text/plain")
            .GET()
            .build();
    response = client.send(request, HttpResponse.BodyHandlers.ofString());

    expected = "<https://spec.edmcouncil.org/fibo/ontology/BE/Corporations/Corporations/> <http://www.omg.org/techprocess/ab/SpecificationMetadata/linkToResourceAddedForTestingPurposesByJB> <http://www.w3.org/2004/02/skos/core#TestyMcTestFace> .";
    assertTrue(ModelTestUtils
            .compareModels(expected, RDFFormat.NTRIPLES, response.body(), RDFFormat.NTRIPLES));
    assertEquals(200, response.statusCode());
  }


  @Test
  public void testNodeByUriMissingNamespaceDefinition() throws Exception {
    try (Transaction tx = graphDatabaseService.beginTx()) {
      tx.execute(UNIQUENESS_CONSTRAINT_STATEMENT);
      tx.commit();
    }

    try (Transaction tx = graphDatabaseService.beginTx()) {
      tx.execute("CALL n10s.graphconfig.init({})");
      //set a prefix that we can remove afterwards
      tx.execute(
          "call n10s.nsprefixes.add('fiboanno','https://spec.edmcouncil.org/fibo/ontology/FND/Utilities/AnnotationVocabulary/')");
      //add dct namespace prefix, as it's not part of the predefined ones.
      tx.execute("call n10s.nsprefixes.add(\"dct\",\"http://purl.org/dc/terms/\")").next();
      tx.commit();
    }

    try (Transaction tx = graphDatabaseService.beginTx()) {

      Map<String, Object> importResult = tx.execute("CALL n10s.rdf.import.fetch('" +
          RDFEndpointNodeLookupTest.class.getClassLoader().getResource("fibo-fragment.rdf")
              .toURI() + "','RDF/XML',{})").next();
      Map<String, Object> nsFromImportResults = (Map<String, Object>) importResult
          .get("namespaces");
      assertTrue(nsFromImportResults.size() == 7);

      Map<String, Object> nspd = (Map<String, Object>) tx
          .execute("match (n:_NsPrefDef) return properties(n) as p").next()
          .get("p");
      assertTrue(nspd.containsKey("fiboanno"));
      assertTrue(nspd.get("fiboanno")
          .equals("https://spec.edmcouncil.org/fibo/ontology/FND/Utilities/AnnotationVocabulary/"));
      assertTrue(nspd.containsKey("dct"));
      assertTrue(nspd.get("dct").equals("http://purl.org/dc/terms/"));
      assertTrue(nspd.containsKey("owl"));
      assertTrue(nspd.get("owl").equals("http://www.w3.org/2002/07/owl#"));

      //we try (and fail) to remove the namespace
      tx.execute("call n10s.nsprefixes.remove('fiboanno')");
      tx.execute("call n10s.nsprefixes.list()").next();
      assertTrue(false);
      tx.commit();
    } catch (Exception e) {
      //expected
      assertTrue(true);
    }

    try (Transaction tx = graphDatabaseService.beginTx()) {
      //now we force delete it from the node
      tx.execute("match (n:_NsPrefDef) remove n.fiboanno ");
      tx.commit();
    }

    HttpRequest request = HttpRequest.newBuilder()
            .uri(URI.create(resolveURI(neo4j.httpURI(), "neo4j/describe?nodeIdentifier=") + URLEncoder
                    .encode("https://spec.edmcouncil.org/fibo/ontology/BE/Corporations/Corporations/",
                            StandardCharsets.UTF_8.toString())))
            .header("Accept", "application/rdf+xml")
            .GET()
            .build();
    HttpResponse<String> response = client.send(request, HttpResponse.BodyHandlers.ofString());

    assertEquals(200, response.statusCode());
    String expected = "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n"
        + "<rdf:RDF\n"
        + "\txmlns:rdf=\"http://www.w3.org/1999/02/22-rdf-syntax-ns#\">\n"
        + "<!-- RDF Serialization ERROR: Prefix fiboanno in use but not defined in the '_NsPrefDef' node -->\n"
        + "\n"
        + "</rdf:RDF>";
    assertTrue(ModelTestUtils
            .compareModels(expected, RDFFormat.RDFXML, response.body(), RDFFormat.RDFXML));

    assertTrue(response.body().contains("RDF Serialization ERROR: Prefix fiboanno "
        + "in use but not in the namespace prefix definition"));

  }

  @Test
  public void testNodeByUriAfterImportWithMultilang() throws Exception {
    try (Transaction tx = graphDatabaseService.beginTx()) {
      tx.execute(UNIQUENESS_CONSTRAINT_STATEMENT);
      tx.commit();
    }
    try (Transaction tx = graphDatabaseService.beginTx()) {
      tx.execute("CALL n10s.graphconfig.init( { keepLangTag : true, handleMultival: 'ARRAY'} )");
      tx.execute("CALL n10s.rdf.import.fetch('" +
              RDFEndpointNodeLookupTest.class.getClassLoader().getResource("multilang.ttl")
                      .toURI() + "','Turtle')");

      tx.commit();
    }

    HttpRequest request = HttpRequest.newBuilder()
            .uri(URI.create(resolveURI(neo4j.httpURI(), "neo4j/describe?nodeIdentifier=") + URLEncoder
                    .encode("http://example.org/vocab/show/218", StandardCharsets.UTF_8.toString())))
            .header("Accept", "text/turtle")
            .GET()
            .build();
    HttpResponse<String> response = client.send(request, HttpResponse.BodyHandlers.ofString());

    String expected = "@prefix show: <http://example.org/vocab/show/> .\n" +
            "show:218 show:localName \"That Seventies Show\"@en .                 # literal with a language tag\n"
            +
            "show:218 show:localName 'Cette Série des Années Soixante-dix'@fr . # literal delimited by single quote\n"
            +
            "show:218 show:localName \"Cette Série des Années Septante\"@fr-be .  # literal with a region subtag";

    assertEquals(200, response.statusCode());
    assertTrue(ModelTestUtils
            .compareModels(expected, RDFFormat.TURTLE, response.body(), RDFFormat.TURTLE));

  }

}
