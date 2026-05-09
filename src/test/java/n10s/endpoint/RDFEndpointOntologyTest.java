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
 * Tests for the /onto (ontology) endpoint.
 * Split from RDFEndpointTest.java
 */
public class RDFEndpointOntologyTest {

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
  public void testontoOnLPG() throws Exception {
    try (Transaction tx = graphDatabaseService.beginTx()) {
      String dataInsertion =
          "CREATE (kean:Actor:Resource {name:'Keanu Reeves', born:1964})\n" +
              "CREATE (mtrx:Movie:Resource {title:'The Matrix', released:2001})\n" +
              "CREATE (dir:Director:Resource {name:'Laurence Fishburne', born:1961})\n" +
              "CREATE (cri:Critic:Resource {name:'Hugo Weaving', born:1960})\n" +
              "CREATE (kean)-[:ACTED_IN]->(mtrx)\n" +
              "CREATE (dir)-[:DIRECTED]->(mtrx)\n" +
              "CREATE (cri)-[:RATED]->(mtrx)\n" +
              "RETURN *";
      tx.execute(dataInsertion);
      tx.commit();
    }

    try (Transaction tx = graphDatabaseService.beginTx()) {
      Result result = tx.execute("MATCH (n:Critic) RETURN id(n) AS id ");
      assertEquals(1, count(result));
    }
    HttpRequest request = HttpRequest.newBuilder()
            .uri(URI.create(resolveURI(neo4j.httpURI(), "neo4j/onto")))
            .header("Accept", "text/plain")
            .GET()
            .build();
    HttpResponse<String> response = client.send(request, HttpResponse.BodyHandlers.ofString());

    String expected =
        "<neo4j://graph.schema#title> <http://www.w3.org/2000/01/rdf-schema#domain> <neo4j://graph.schema#Movie> .\n" +
                "<neo4j://graph.schema#ACTED_IN> <http://www.w3.org/2000/01/rdf-schema#label> \"ACTED_IN\" .\n" +
                "<neo4j://graph.schema#Movie> <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://www.w3.org/2002/07/owl#Class> .\n" +
                "<neo4j://graph.schema#born> <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://www.w3.org/2002/07/owl#DatatypeProperty> .\n" +
                "<neo4j://graph.schema#Critic> <http://www.w3.org/2000/01/rdf-schema#label> \"Critic\" .\n" +
                "<neo4j://graph.schema#born> <http://www.w3.org/2000/01/rdf-schema#domain> <neo4j://graph.schema#Critic> .\n" +
                "<neo4j://graph.schema#released> <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://www.w3.org/2002/07/owl#DatatypeProperty> .\n" +
                "<neo4j://graph.schema#ACTED_IN> <http://www.w3.org/2000/01/rdf-schema#domain> <neo4j://graph.schema#Actor> .\n" +
                "<neo4j://graph.schema#DIRECTED> <http://www.w3.org/2000/01/rdf-schema#range> <neo4j://graph.schema#Movie> .\n" +
                "<neo4j://graph.schema#Critic> <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://www.w3.org/2002/07/owl#Class> .\n" +
                "<neo4j://graph.schema#released> <http://www.w3.org/2000/01/rdf-schema#domain> <neo4j://graph.schema#Movie> .\n" +
                "<neo4j://graph.schema#title> <http://www.w3.org/2000/01/rdf-schema#label> \"title\" .\n" +
                "<neo4j://graph.schema#title> <http://www.w3.org/2000/01/rdf-schema#range> <http://www.w3.org/2001/XMLSchema#string> .\n" +
                "<neo4j://graph.schema#name> <http://www.w3.org/2000/01/rdf-schema#domain> <neo4j://graph.schema#Director> .\n" +
                "<neo4j://graph.schema#released> <http://www.w3.org/2000/01/rdf-schema#label> \"released\" .\n" +
                "<neo4j://graph.schema#Director> <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://www.w3.org/2002/07/owl#Class> .\n" +
                "<neo4j://graph.schema#title> <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://www.w3.org/2002/07/owl#DatatypeProperty> .\n" +
                "<neo4j://graph.schema#released> <http://www.w3.org/2000/01/rdf-schema#range> <http://www.w3.org/2001/XMLSchema#integer> .\n" +
                "<neo4j://graph.schema#Actor> <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://www.w3.org/2002/07/owl#Class> .\n" +
                "<neo4j://graph.schema#name> <http://www.w3.org/2000/01/rdf-schema#domain> <neo4j://graph.schema#Actor> .\n" +
                "<neo4j://graph.schema#Movie> <http://www.w3.org/2000/01/rdf-schema#label> \"Movie\" .\n" +
                "<neo4j://graph.schema#RATED> <http://www.w3.org/2000/01/rdf-schema#domain> <neo4j://graph.schema#Critic> .\n" +
                "<neo4j://graph.schema#DIRECTED> <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://www.w3.org/2002/07/owl#ObjectProperty> .\n" +
                "<neo4j://graph.schema#DIRECTED> <http://www.w3.org/2000/01/rdf-schema#domain> <neo4j://graph.schema#Director> .\n" +
                "<neo4j://graph.schema#name> <http://www.w3.org/2000/01/rdf-schema#domain> <neo4j://graph.schema#Critic> .\n" +
                "<neo4j://graph.schema#ACTED_IN> <http://www.w3.org/2000/01/rdf-schema#range> <neo4j://graph.schema#Movie> .\n" +
                "<neo4j://graph.schema#Actor> <http://www.w3.org/2000/01/rdf-schema#label> \"Actor\" .\n" +
                "<neo4j://graph.schema#RATED> <http://www.w3.org/2000/01/rdf-schema#label> \"RATED\" .\n" +
                "<neo4j://graph.schema#name> <http://www.w3.org/2000/01/rdf-schema#label> \"name\" .\n" +
                "<neo4j://graph.schema#name> <http://www.w3.org/2000/01/rdf-schema#range> <http://www.w3.org/2001/XMLSchema#string> .\n" +
                "<neo4j://graph.schema#born> <http://www.w3.org/2000/01/rdf-schema#domain> <neo4j://graph.schema#Director> .\n" +
                "<neo4j://graph.schema#born> <http://www.w3.org/2000/01/rdf-schema#label> \"born\" .\n" +
                "<neo4j://graph.schema#RATED> <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://www.w3.org/2002/07/owl#ObjectProperty> .\n" +
                "<neo4j://graph.schema#DIRECTED> <http://www.w3.org/2000/01/rdf-schema#label> \"DIRECTED\" .\n" +
                "<neo4j://graph.schema#ACTED_IN> <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://www.w3.org/2002/07/owl#ObjectProperty> .\n" +
                "<neo4j://graph.schema#name> <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://www.w3.org/2002/07/owl#DatatypeProperty> .\n" +
                "<neo4j://graph.schema#RATED> <http://www.w3.org/2000/01/rdf-schema#range> <neo4j://graph.schema#Movie> .\n" +
                "<neo4j://graph.schema#born> <http://www.w3.org/2000/01/rdf-schema#domain> <neo4j://graph.schema#Actor> .\n" +
                "<neo4j://graph.schema#born> <http://www.w3.org/2000/01/rdf-schema#range> <http://www.w3.org/2001/XMLSchema#integer> .\n" +
                "<neo4j://graph.schema#Director> <http://www.w3.org/2000/01/rdf-schema#label> \"Director\" .";
    assertEquals(200, response.statusCode());
    assertTrue(ModelTestUtils
            .compareModels(expected, RDFFormat.NTRIPLES, response.body(), RDFFormat.NTRIPLES));

  }

  @Test
  public void testontoOnLPGWithPropertyLessNode() throws Exception {
    try (Transaction tx = graphDatabaseService.beginTx()) {
      String dataInsertion =
              " CREATE (:PropertyLessThing) " ;
      tx.execute(dataInsertion);
      tx.commit();
    }

    try (Transaction tx = graphDatabaseService.beginTx()) {
      Result result = tx.execute("MATCH (n:PropertyLessThing) RETURN id(n) AS id ");
      assertEquals(1, count(result));
    }

    HttpRequest request = HttpRequest.newBuilder()
            .uri(URI.create(resolveURI(neo4j.httpURI(), "neo4j/onto")))
            .header("Accept", "text/plain")
            .GET()
            .build();
    HttpResponse<String> response = client.send(request, HttpResponse.BodyHandlers.ofString());

    String expected =
            "<neo4j://graph.schema#PropertyLessThing> <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://www.w3.org/2002/07/owl#Class> .\n" +
                    "<neo4j://graph.schema#PropertyLessThing> <http://www.w3.org/2000/01/rdf-schema#label> \"PropertyLessThing\" .\n" ;
    assertEquals(200, response.statusCode());
    assertTrue(ModelTestUtils
            .compareModels(expected, RDFFormat.NTRIPLES, response.body(), RDFFormat.NTRIPLES));

  }

  @Test
  public void testontoOnRDF() throws Exception {

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

    HttpRequest request = HttpRequest.newBuilder()
            .uri(URI.create(resolveURI(neo4j.httpURI(), "neo4j/onto")))
            .header("Accept", "text/plain")
            .GET()
            .build();
    HttpResponse<String> response = client.send(request, HttpResponse.BodyHandlers.ofString());

    String expected =
        "<http://permid.org/ontology/organization/Director> <http://www.w3.org/2000/01/rdf-schema#label> \"Director\" .\n" +
                "<http://permid.org/ontology/organization/FriendOf> <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://www.w3.org/2002/07/owl#ObjectProperty> .\n" +
                "<http://ont.thomsonreuters.com/mdaas/name> <http://www.w3.org/2000/01/rdf-schema#range> <http://www.w3.org/2001/XMLSchema#string> .\n" +
                "<http://permid.org/ontology/organization/Actor> <http://www.w3.org/2000/01/rdf-schema#label> \"Actor\" .\n" +
                "<http://ont.thomsonreuters.com/mdaas/born> <http://www.w3.org/2000/01/rdf-schema#domain> <http://permid.org/ontology/organization/Director> .\n" +
                "<http://ont.thomsonreuters.com/mdaas/name> <http://www.w3.org/2000/01/rdf-schema#domain> <http://permid.org/ontology/organization/Actor> .\n" +
                "<http://permid.org/ontology/organization/FriendOf> <http://www.w3.org/2000/01/rdf-schema#domain> <http://permid.org/ontology/organization/Critic> .\n" +
                "<http://ont.thomsonreuters.com/mdaas/born> <http://www.w3.org/2000/01/rdf-schema#range> <http://www.w3.org/2001/XMLSchema#integer> .\n" +
                "<http://ont.thomsonreuters.com/mdaas/born> <http://www.w3.org/2000/01/rdf-schema#domain> <http://permid.org/ontology/organization/Critic> .\n" +
                "<http://ont.thomsonreuters.com/mdaas/name> <http://www.w3.org/2000/01/rdf-schema#domain> <http://permid.org/ontology/organization/Director> .\n" +
                "<http://ont.thomsonreuters.com/mdaas/name> <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://www.w3.org/2002/07/owl#DatatypeProperty> .\n" +
                "<http://ont.thomsonreuters.com/mdaas/name> <http://www.w3.org/2000/01/rdf-schema#label> \"name\" .\n" +
                "<http://permid.org/ontology/organization/FriendOf> <http://www.w3.org/2000/01/rdf-schema#range> <http://permid.org/ontology/organization/Actor> .\n" +
                "<http://ont.thomsonreuters.com/mdaas/born> <http://www.w3.org/2000/01/rdf-schema#label> \"born\" .\n" +
                "<http://permid.org/ontology/organization/Critic> <http://www.w3.org/2000/01/rdf-schema#label> \"Critic\" .\n" +
                "<http://ont.thomsonreuters.com/mdaas/born> <http://www.w3.org/2000/01/rdf-schema#domain> <http://permid.org/ontology/organization/Actor> .\n" +
                "<http://permid.org/ontology/organization/Critic> <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://www.w3.org/2002/07/owl#Class> .\n" +
                "<http://permid.org/ontology/organization/Likes> <http://www.w3.org/2000/01/rdf-schema#range> <http://permid.org/ontology/organization/Director> .\n" +
                "<http://ont.thomsonreuters.com/mdaas/name> <http://www.w3.org/2000/01/rdf-schema#domain> <http://permid.org/ontology/organization/Critic> .\n" +
                "<http://ont.thomsonreuters.com/mdaas/born> <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://www.w3.org/2002/07/owl#DatatypeProperty> .\n" +
                "<http://permid.org/ontology/organization/Likes> <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://www.w3.org/2002/07/owl#ObjectProperty> .\n" +
                "<http://permid.org/ontology/organization/Actor> <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://www.w3.org/2002/07/owl#Class> .\n" +
                "<http://permid.org/ontology/organization/Likes> <http://www.w3.org/2000/01/rdf-schema#domain> <http://permid.org/ontology/organization/Actor> .\n" +
                "<http://permid.org/ontology/organization/FriendOf> <http://www.w3.org/2000/01/rdf-schema#label> \"FriendOf\" .\n" +
                "<http://permid.org/ontology/organization/Director> <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://www.w3.org/2002/07/owl#Class> .\n" +
                "<http://permid.org/ontology/organization/Likes> <http://www.w3.org/2000/01/rdf-schema#label> \"Likes\" .\n";
    assertEquals(200, response.statusCode());
    assertTrue(ModelTestUtils
            .compareModels(expected, RDFFormat.NTRIPLES, response.body(), RDFFormat.NTRIPLES));

  }

  @Test
  public void testontoOnRDFWithPropertyLessNodes() throws Exception {

    try (Transaction tx = graphDatabaseService.beginTx()) {
      tx.execute("CALL n10s.graphconfig.init()");
      tx.execute("call n10s.nsprefixes.add('ns0','http://permid.org/ontology/organization/')");

      String dataInsertion =
              "CREATE (Keanu:Resource:ns0" + PREFIX_SEPARATOR + "PropertyLessThing )\n" +
                      "CREATE (Carrie:Resource:ns0" + PREFIX_SEPARATOR + "Person { uri: 'https://permid.org/1-21523433751' })" ;
      tx.execute(dataInsertion);
      tx.commit();
    }

    HttpRequest request = HttpRequest.newBuilder()
            .uri(URI.create(resolveURI(neo4j.httpURI(), "neo4j/onto")))
            .header("Accept", "text/plain")
            .GET()
            .build();
    HttpResponse<String> response = client.send(request, HttpResponse.BodyHandlers.ofString());

    String expected =
            "<http://permid.org/ontology/organization/Person> <http://www.w3.org/2000/01/rdf-schema#label> \"Person\" .\n" +
            "<http://permid.org/ontology/organization/Person> <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://www.w3.org/2002/07/owl#Class> .\n" +
            "<http://permid.org/ontology/organization/PropertyLessThing> <http://www.w3.org/2000/01/rdf-schema#label> \"PropertyLessThing\" .\n" +
            "<http://permid.org/ontology/organization/PropertyLessThing> <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://www.w3.org/2002/07/owl#Class> .\n" ;

    assertEquals(200, response.statusCode());
    assertTrue(ModelTestUtils
            .compareModels(expected, RDFFormat.NTRIPLES, response.body(), RDFFormat.NTRIPLES));

  }

}
