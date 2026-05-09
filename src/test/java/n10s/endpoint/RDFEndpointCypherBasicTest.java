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
 * Tests for basic Cypher-based RDF export endpoints (first 9 tests).
 * Split from RDFEndpointCypherTest.java
 */
public class RDFEndpointCypherBasicTest {

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
  public void testCypherCgnt() throws Exception {

    try (Transaction tx = graphDatabaseService.beginTx()) {
      tx.execute("UNWIND RANGE(1,5,1) as id\n" +
              "CREATE(cable:Cable{id: id, name: \"cable_\"+id, createdAt: datetime(\"2017-04-05T12:34:00+02:00\")})\n" +
              "WITH cable\n" +
              "UNWIND RANGE(1, 2, 1) as cableRoutingPointId\n" +
              "WITH cable, cableRoutingPointId\n" +
              "CREATE (routingPoint:CableRoutingPoint{id: cableRoutingPointId, " +
              "location: point({x: -0.1275 , y: 51.507222222}), typeCodes: ['A', 'B', 'C'], " +
              "inspectionDates: [datetime(\"2018-04-05T12:34:00+02:00\"), " +
              "datetime(\"2019-04-05T12:34:00+02:00\"), datetime(\"2020-04-05T12:34:00+02:00\")]})\n" +
              "CREATE (cable)-[:HAS_ROUTING_POINT{createdAt: datetime(\"2018-04-05T12:34:00+02:00\"), " +
              "labels: [\"foo\", \"bar\"]}]->(routingPoint);");
      tx.commit();
    }

    String cableid;
    String crpid;
    try (Transaction tx = graphDatabaseService.beginTx()) {
      Result result = tx.execute("match (c:Cable{ id: 4 })--(crp:CableRoutingPoint{id: 2 }) " +
              " return id(c) as cableid, id(crp) as crpid ");
      Map<String, Object> record = result.next();
      cableid = record.get("cableid").toString();
      crpid = record.get("crpid").toString();
    }

    Map<String, Object> map = new HashMap<>();
    map.put("cypher", "MATCH s = (:Cable{id: 4 })--(:CableRoutingPoint{id: 2 }) RETURN s");
    map.put("format", "Turtle-star");

    HttpRequest request = HttpRequest.newBuilder()
            .uri(URI.create(resolveURI(neo4j.httpURI(), "neo4j/cypher")))
            .header("Accept", "text/plain")
            .header("Content-Type", "application/json")
            .POST(HttpRequest.BodyPublishers.ofString(jsonMapper.writeValueAsString(map)))
            .build();
    HttpResponse<String> response = client.send(request, HttpResponse.BodyHandlers.ofString());

    String expected = "@prefix n4sch: <neo4j://graph.schema#> .\n" +
            "@prefix n4ind: <neo4j://graph.individuals#> .\n" +
            "\n" +
            "n4ind:" + cableid + " a n4sch:Cable;\n" +
            "  n4sch:name \"cable_4\";\n" +
            "  n4sch:createdAt \"2017-04-05T12:34:00+02:00\"^^<http://www.w3.org/2001/XMLSchema#dateTime>;\n" +
            "  n4sch:id \"4\"^^<http://www.w3.org/2001/XMLSchema#long>;\n" +
            "  n4sch:HAS_ROUTING_POINT n4ind:" + crpid + " .\n" +
            "\n" +
            "n4ind:" + crpid + " a n4sch:CableRoutingPoint;\n" +
            "  n4sch:inspectionDates \"2019-04-05T12:34:00+02:00\"^^<http://www.w3.org/2001/XMLSchema#dateTime>,\n" +
            "    \"2020-04-05T12:34:00+02:00\"^^<http://www.w3.org/2001/XMLSchema#dateTime>, \"2018-04-05T12:34:00+02:00\"^^<http://www.w3.org/2001/XMLSchema#dateTime>;\n" +
            "  n4sch:id \"2\"^^<http://www.w3.org/2001/XMLSchema#long>;\n" +
            "  n4sch:typeCodes \"A\", \"B\", \"C\";\n" +
            "  n4sch:location \"Point(-0.1275 51.507222222)\"^^<http://www.opengis.net/ont/geosparql#wktLiteral> .\n" +
            "<<n4ind:" + cableid + " n4sch:HAS_ROUTING_POINT n4ind:" + crpid + ">> n4sch:labels \"bar\", \"foo\";\n" +
            "  n4sch:createdAt \"2018-04-05T12:34:00+02:00\"^^<http://www.w3.org/2001/XMLSchema#dateTime> .\n" +
            "\n" ;

    assertEquals(200, response.statusCode());
    assertTrue(ModelTestUtils
            .compareModels(expected, RDFFormat.TURTLESTAR, response.body(), RDFFormat.TURTLESTAR));

  }

  @Test
  public void testCypherReturnsList() throws Exception {

    try (Transaction tx = graphDatabaseService.beginTx()) {
      tx.execute("call n10s.nsprefixes.add('sch','http://schema.org/')");
      tx.commit();
    }

    try (Transaction tx = graphDatabaseService.beginTx()) {

        String dataInsertion = "CREATE (Keanu:Actor {uri:'neo4j://person#1', name:'Keanu Reeves', born:1964})\n" +
            "CREATE (Carrie:Director {uri:'neo4j://person#2', name:'Carrie-Anne Moss', born:1967})\n" +
            "CREATE (Laurence:Director {uri:'neo4j://person#3', name:'Laurence Fishburne', born:1961})\n" +
            "CREATE (Hugo:Critic {uri:'neo4j://person#4', name:'Hugo Weaving', born:1960})\n" +
            "CREATE (AndyW:Actor {uri:'neo4j://person#5', name:'Andy Wachowski', born:1967})\n" +
            "CREATE (Hugo)-[:WORKS_WITH { hoursADay: 8 } ]->(AndyW)\n" +
            "CREATE (Hugo)<-[:FRIEND_OF  { since: 'the early days' }]-(Carrie)";
        tx.execute(dataInsertion);

        tx.execute("call n10s.mapping.add(\"http://schema.org/something\",\"STH\")");
        tx.commit();

    }


    Map<String, Object> map = new HashMap<>();
    map.put("cypher", "MATCH (n)  RETURN collect(n) as col");

    HttpRequest request = HttpRequest.newBuilder()
            .uri(URI.create(resolveURI(neo4j.httpURI(), "neo4j/cypher")))
            .header("Accept", "text/plain")
            .header("Content-Type", "application/json")
            .POST(HttpRequest.BodyPublishers.ofString(jsonMapper.writeValueAsString(map)))
            .build();
    HttpResponse<String> response = client.send(request, HttpResponse.BodyHandlers.ofString());

    String expected = "<neo4j://person#1> <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <neo4j://graph.schema#Actor> .\n"
        + "<neo4j://person#5> <neo4j://graph.schema#born> \"1967\"^^<http://www.w3.org/2001/XMLSchema#long> .\n"
        + "<neo4j://person#5> <neo4j://graph.schema#name> \"Andy Wachowski\" .\n"
        + "<neo4j://person#4> <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <neo4j://graph.schema#Critic> .\n"
        + "<neo4j://person#5> <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <neo4j://graph.schema#Actor> .\n"
        + "<neo4j://person#2> <neo4j://graph.schema#born> \"1967\"^^<http://www.w3.org/2001/XMLSchema#long> .\n"
        + "<neo4j://person#3> <neo4j://graph.schema#born> \"1961\"^^<http://www.w3.org/2001/XMLSchema#long> .\n"
        + "<neo4j://person#4> <neo4j://graph.schema#born> \"1960\"^^<http://www.w3.org/2001/XMLSchema#long> .\n"
        + "<neo4j://person#1> <neo4j://graph.schema#born> \"1964\"^^<http://www.w3.org/2001/XMLSchema#long> .\n"
        + "<neo4j://person#1> <neo4j://graph.schema#name> \"Keanu Reeves\" .\n"
        + "<neo4j://person#3> <neo4j://graph.schema#name> \"Laurence Fishburne\" .\n"
        + "<neo4j://person#2> <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <neo4j://graph.schema#Director> .\n"
        + "<neo4j://person#3> <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <neo4j://graph.schema#Director> .\n"
        + "<neo4j://person#4> <neo4j://graph.schema#name> \"Hugo Weaving\" .\n"
        + "<neo4j://person#2> <neo4j://graph.schema#name> \"Carrie-Anne Moss\" .\n";
    assertEquals(200, response.statusCode());

    String responseString = response.body();

    assertTrue(ModelTestUtils
        .compareModels(expected, RDFFormat.TURTLE, responseString, RDFFormat.TURTLE));

  }

  @Test
  public void testPrefixwithHyphen() throws Exception {

    //first import onto
    try (Transaction tx = graphDatabaseService.beginTx()) {
      tx.execute(UNIQUENESS_CONSTRAINT_STATEMENT);
      tx.commit();
    }
    try (Transaction tx = graphDatabaseService.beginTx()) {
      tx.execute("CALL n10s.graphconfig.init()");
      tx.execute("CALL n10s.nsprefixes.add(\"my-prefix\", \"http://www.example.com/example#\")");

      tx.commit();
    }

    String xmlrdf = "<rdf:RDF xmlns=\"http://www.example.com/example#\"\n" +
            "     xml:base=\"http://www.example.com/example\"\n" +
            "     xmlns:owl=\"http://www.w3.org/2002/07/owl#\"\n" +
            "     xmlns:rdf=\"http://www.w3.org/1999/02/22-rdf-syntax-ns#\"\n" +
            "     xmlns:xml=\"http://www.w3.org/XML/1998/namespace\"\n" +
            "     xmlns:xsd=\"http://www.w3.org/2001/XMLSchema#\"\n" +
            "     xmlns:rdfs=\"http://www.w3.org/2000/01/rdf-schema#\"\n" +
            "     xmlns:example=\"http://www.example.com/example#\">\n" +
            "    <owl:Ontology rdf:about=\"http://www.example.com/example\"/>\n" +
            "    \n" +
            "    <owl:ObjectProperty rdf:about=\"http://www.example.com/example#requires\"/>\n" +
            "\n" +
            "    <owl:Class rdf:about=\"http://www.example.com/example#Enitity1\"/>\n" +
            "\n" +
            "    <owl:Class rdf:about=\"http://www.example.com/example#Entity2\"/>\n" +
            "\n" +
            "    <owl:NamedIndividual rdf:about=\"http://www.example.com/example#Enitity1Individual\">\n" +
            "        <rdf:type rdf:resource=\"http://www.example.com/example#Enitity1\"/>\n" +
            "        <requiresProp>12345</requiresProp>" +
            "        <requires rdf:resource=\"http://www.example.com/example#Entity2Individual\"/>\n" +
            "    </owl:NamedIndividual>\n" +
            "\n" +
            "    <owl:NamedIndividual rdf:about=\"http://www.example.com/example#Entity2Individual\">\n" +
            "        <rdf:type rdf:resource=\"http://www.example.com/example#Entity2\"/>\n" +
            "    </owl:NamedIndividual>\n" +
            "</rdf:RDF>";

    try (Transaction tx = graphDatabaseService.beginTx()) {
      tx.execute("CALL n10s.rdf.import.inline('" + xmlrdf + "','RDF/XML')");
      tx.commit();
    }

    HttpRequest request = HttpRequest.newBuilder()
            .uri(URI.create(resolveURI(neo4j.httpURI(), "neo4j/describe?nodeIdentifier=" + URLEncoder.encode("http://www.example.com/example#Enitity1Individual", StandardCharsets.UTF_8) + "&format=RDF/XML")))
            .header("Accept", "text/plain")
            .GET()
            .build();
    HttpResponse<String> response = client.send(request, HttpResponse.BodyHandlers.ofString());

    String expected =
            "<http://www.example.com/example#Enitity1Individual> <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://www.example.com/example#Enitity1> .\n" +
            "<http://www.example.com/example#Enitity1Individual> <http://www.example.com/example#requires> <http://www.example.com/example#Entity2Individual> .\n" +
            "<http://www.example.com/example#Enitity1Individual> <http://www.example.com/example#requiresProp> \"12345\" ." +
            "<http://www.example.com/example#Enitity1Individual> <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://www.w3.org/2002/07/owl#NamedIndividual> .";
    assertEquals(200, response.statusCode());
    System.out.println(response.body());
    assertTrue(ModelTestUtils
            .compareModels(expected, RDFFormat.TURTLE, response.body(), RDFFormat.RDFXML));

  }


  @Test
  public void testCypherOnMovieDBReturnsList() throws Exception {

    try (Transaction tx = graphDatabaseService.beginTx()) {
      tx.execute("call n10s.nsprefixes.add('sch','http://schema.org/')");
      tx.commit();
    }

    try (Transaction tx = graphDatabaseService.beginTx()) {

      tx.execute(Files.readString(Paths.get(
          RDFEndpointCypherBasicTest.class.getClassLoader().getResource("movies.cypher").getPath())));

      tx.commit();
    }
    //ADD mapppings and nsprefixes
    try (Transaction tx = graphDatabaseService.beginTx()) {

      tx.execute("call n10s.mapping.add(\"http://schema.org/when\",\"released\")");
      tx.commit();
    }

    try (Transaction tx = graphDatabaseService.beginTx()) {
      Result execute = tx.execute("call n10s.validation.shacl.import.fetch('" +
          RDFEndpointCypherBasicTest.class.getClassLoader().getResource("shacl/person2-shacl.ttl")
              .toURI() + "','Turtle')");
      assertTrue(execute.hasNext());
      Map<String, Object> next = execute.next();
      tx.commit();
    }

    Map<String,String> nameToId = new HashMap<>();
    try (Transaction tx = graphDatabaseService.beginTx()) {
      tx.execute("match (n) return case n:Movie when true then n.title else n.name end as name, id(n) as id")
              .stream()
              .forEach(r -> nameToId.put((String) r.get("name"), String.format("#%s", (Long)r.get("id")) ));
    }

    Map<String, Object> map = new HashMap<>();
    map.put("cypher", "MATCH (n:Movie { title: \"That Thing You Do\"})--(x) "
        + "RETURN collect(distinct n) + collect(distinct x)");

    HttpRequest request = HttpRequest.newBuilder()
            .uri(URI.create(resolveURI(neo4j.httpURI(), "neo4j/cypher")))
            .header("Accept", "text/plain")
            .header("Content-Type", "application/json")
            .POST(HttpRequest.BodyPublishers.ofString(jsonMapper.writeValueAsString(map)))
            .build();
    HttpResponse<String> response = client.send(request, HttpResponse.BodyHandlers.ofString());

    String expected = String.format(
          "<neo4j://graph.individuals%4$s> <neo4j://graph.schema#title> \"That Thing You Do\" .\n"
        + "<neo4j://graph.individuals%1$s> <neo4j://graph.schema#name> \"Charlize Theron\" .\n"
        + "<neo4j://graph.individuals%2$s> <neo4j://graph.schema#born> \"1977\"^^<http://www.w3.org/2001/XMLSchema#long> .\n"
        + "<neo4j://graph.individuals%3$s> <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <neo4j://graph.schema#Person> .\n"
        + "<neo4j://graph.individuals%4$s> <http://schema.org/when> \"1996\"^^<http://www.w3.org/2001/XMLSchema#long> .\n"
        + "<neo4j://graph.individuals%3$s> <neo4j://graph.schema#name> \"Tom Hanks\" .\n"
        + "<neo4j://graph.individuals%2$s> <neo4j://graph.schema#name> \"Liv Tyler\" .\n"
        + "<neo4j://graph.individuals%2$s> <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <neo4j://graph.schema#Person> .\n"
        + "<neo4j://graph.individuals%1$s> <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <neo4j://graph.schema#Person> .\n"
        + "<neo4j://graph.individuals%3$s> <neo4j://graph.schema#born> \"1956\"^^<http://www.w3.org/2001/XMLSchema#long> .\n"
        + "<neo4j://graph.individuals%4$s> <neo4j://graph.schema#tagline> \"In every life there comes a time when that thing you dream becomes that thing you do\" .\n"
        + "<neo4j://graph.individuals%4$s> <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <neo4j://graph.schema#Movie> .\n"
        + "<neo4j://graph.individuals%1$s> <neo4j://graph.schema#born> \"1975\"^^<http://www.w3.org/2001/XMLSchema#long> .",
            nameToId.get("Charlize Theron"),
            nameToId.get("Liv Tyler"),
            nameToId.get("Tom Hanks"),
            nameToId.get("That Thing You Do")
            );

    assertEquals(200, response.statusCode());
    assertTrue(ModelTestUtils
        .compareModels(expected, RDFFormat.TURTLE, response.body(), RDFFormat.TURTLE));

  }

  @Test
  public void testCypherReturnsListOnRDFGraph() throws Exception {
    try (Transaction tx = graphDatabaseService.beginTx()) {
      tx.execute(UNIQUENESS_CONSTRAINT_STATEMENT);
      tx.commit();
    }
    try (Transaction tx = graphDatabaseService.beginTx()) {
      tx.execute("CALL n10s.graphconfig.init()");
      tx.execute("call n10s.nsprefixes.add(\"tst\",\"http://tst.voc/\")");
      tx.commit();
    }
    try (Transaction tx = graphDatabaseService.beginTx()) {
      Result execute = tx.execute("CALL n10s.rdf.import.fetch('" +
          RDFEndpointCypherBasicTest.class.getClassLoader().getResource("multival.ttl")
              .toURI() + "','Turtle',{})");
      Map<String, Object> next = execute.next();
      assertEquals(9L, next.get("triplesLoaded"));

      tx.commit();
    }

    Map<String, Object> map = new HashMap<>();
    map.put("cypher", "MATCH (n) RETURN collect(n) as col");

    HttpRequest request = HttpRequest.newBuilder()
            .uri(URI.create(resolveURI(neo4j.httpURI(), "neo4j/cypher")))
            .header("Accept", "text/plain")
            .header("Content-Type", "application/json")
            .POST(HttpRequest.BodyPublishers.ofString(jsonMapper.writeValueAsString(map)))
            .build();
    HttpResponse<String> response = client.send(request, HttpResponse.BodyHandlers.ofString());

    String expectedNoNonResources = "<http://example.org/vocab/show/218> <http://example.org/vocab/show/producer> \"Joanna Smith\" .\n"
        + "<http://example.org/vocab/show/218> <http://example.org/vocab/show/localName> \"Cette Série des Années Septante\" .\n"
        + "<http://example.org/vocab/show/218> <http://example.org/vocab/show/showId> \"218\"^^<http://www.w3.org/2001/XMLSchema#long> .\n"
        + "<http://example.org/vocab/show/218> <http://example.org/vocab/show/availableInLang> \"ES\" .";
    assertEquals(200, response.statusCode());
    assertTrue(ModelTestUtils
            .compareModels(expectedNoNonResources, RDFFormat.TURTLE, response.body(), RDFFormat.TURTLE));

  }

  @Test
  public void testCypherOnLPG() throws Exception {

    try (Transaction tx = graphDatabaseService.beginTx()) {
      String ontoCreation = "MERGE (p:Category {catName: 'Person'})\n" +
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
          "CREATE (AndyW:Actor {name:'Andy Wachowski', born:1967})";
      tx.execute(dataInsertion);
      tx.commit();
    }

    Map<String,String> nameToId = new HashMap<>();
    try (Transaction tx = graphDatabaseService.beginTx()) {
      tx.execute("match (n:Category) return n.catName as name, id(n) as id")
              .stream()
              .forEach(r -> nameToId.put((String) r.get("name"), String.format("#%s", (Long)r.get("id")) ));
    }

    try (Transaction tx = graphDatabaseService.beginTx()) {

      Result result = tx.execute("MATCH (n:Critic) RETURN id(n) AS id ");
      assertEquals(1, count(result));
    }

    Map<String, Object> map = new HashMap<>();
    map.put("cypher", "MATCH (n:Category)--(:Category) RETURN distinct n LIMIT 4");

    HttpRequest request = HttpRequest.newBuilder()
            .uri(URI.create(resolveURI(neo4j.httpURI(), "neo4j/cypher")))
            .header("Accept", "text/plain")
            .header("Content-Type", "application/json")
            .POST(HttpRequest.BodyPublishers.ofString(jsonMapper.writeValueAsString(map)))
            .build();
    HttpResponse<String> response = client.send(request, HttpResponse.BodyHandlers.ofString());

    String expected =
        String.format(
              "<neo4j://graph.individuals%1$s> <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <neo4j://graph.schema#Category> .\n"
            + "<neo4j://graph.individuals%1$s> <neo4j://graph.schema#catName> \"Critic\" .\n"
            + "<neo4j://graph.individuals%2$s> <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <neo4j://graph.schema#Category> .\n"
            + "<neo4j://graph.individuals%2$s> <neo4j://graph.schema#catName> \"Person\" .\n"
            + "<neo4j://graph.individuals%3$s> <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <neo4j://graph.schema#Category> .\n"
            + "<neo4j://graph.individuals%3$s> <neo4j://graph.schema#catName> \"Director\" .\n"
            + "<neo4j://graph.individuals%4$s> <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <neo4j://graph.schema#Category> .\n"
            + "<neo4j://graph.individuals%4$s> <neo4j://graph.schema#catName> \"Actor\" .\n",
                nameToId.get("Critic"),
                nameToId.get("Person"),
                nameToId.get("Director"),
                nameToId.get("Actor")
        );

    assertEquals(200, response.statusCode());
    assertTrue(ModelTestUtils
            .compareModels(expected, RDFFormat.NTRIPLES, response.body(), RDFFormat.NTRIPLES));

    // request passing serialisation format as request param
    map.put("format", "RDF/XML");
    request = HttpRequest.newBuilder()
            .uri(URI.create(resolveURI(neo4j.httpURI(), "neo4j/cypher")))
            .header("Accept", "text/plain")
            .header("Content-Type", "application/json")
            .POST(HttpRequest.BodyPublishers.ofString(jsonMapper.writeValueAsString(map)))
            .build();
    response = client.send(request, HttpResponse.BodyHandlers.ofString());

    assertEquals(200, response.statusCode());
    assertTrue(ModelTestUtils
            .compareModels(expected, RDFFormat.NTRIPLES, response.body(), RDFFormat.RDFXML));

    map.put("mappedElemsOnly", "true");
    map.remove("format");
    request = HttpRequest.newBuilder()
            .uri(URI.create(resolveURI(neo4j.httpURI(), "neo4j/cypher")))
            .header("Accept", "text/plain")
            .header("Content-Type", "application/json")
            .POST(HttpRequest.BodyPublishers.ofString(jsonMapper.writeValueAsString(map)))
            .build();
    response = client.send(request, HttpResponse.BodyHandlers.ofString());

    assertEquals(200, response.statusCode());
    assertEquals("", response.body());

  }

  @Test
  public void testCypherOnLPGMappingsAndQueryParams() throws Exception {

    try (Transaction tx = graphDatabaseService.beginTx()) {
      tx.execute("call n10s.nsprefixes.add('sch','http://schema.org/')");
      tx.commit();
    }

    try (Transaction tx = graphDatabaseService.beginTx()) {

      String dataInsertion = "CREATE (Keanu:Actor {uri:'neo4j://graph.individuals#1', name:'Keanu Reeves', born:1964})\n" +
          "CREATE (Carrie:Director {uri:'neo4j://graph.individuals#2', name:'Carrie-Anne Moss', born:1967})\n" +
          "CREATE (Laurence:Director {uri:'neo4j://graph.individuals#3', name:'Laurence Fishburne', born:1961})\n" +
          "CREATE (Hugo:Critic {uri:'neo4j://graph.individuals#4', name:'Hugo Weaving', born:1960})\n" +
          "CREATE (AndyW:Actor {uri:'neo4j://graph.individuals#5', name:'Andy Wachowski', born:1967}) "
          + "CREATE (Keanu)-[:ACTED_IN]->(:Movie {uri:'neo4j://graph.individuals#6', title: 'The Matrix'})";
      tx.execute(dataInsertion);

      tx.execute("CALL n10s.mapping.add('http://schema.org/Person','Actor')");
      tx.execute("CALL n10s.mapping.add('http://schema.org/familyName','name')");
      tx.execute("CALL n10s.mapping.add('http://schema.org/inMovie','ACTED_IN')");
      tx.execute("CALL n10s.mapping.add('http://schema.org/dob','born')");
      tx.commit();
    }

    try (Transaction tx = graphDatabaseService.beginTx()) {
      Result result = tx.execute(" MATCH (n:Actor) RETURN id(n) AS id ");
      assertEquals(2, count(result));

      result = tx.execute(" MATCH (n:_MapDef) RETURN id(n) AS id ");
      assertEquals(4, count(result));
    }

    Map<String, Object> map = new HashMap<>();
    map.put("cypher", "MATCH (n:Actor { name : $actorName })-[r]-(m) RETURN n, r, m ");
    Map<String, Object> cypherParams = new HashMap<String, Object>();
    cypherParams.put("actorName", "Keanu Reeves");
    map.put("cypherParams", cypherParams);

    HttpRequest request = HttpRequest.newBuilder()
            .uri(URI.create(resolveURI(neo4j.httpURI(), "neo4j/cypher")))
            .header("Accept", "text/plain")
            .header("Content-Type", "application/json")
            .POST(HttpRequest.BodyPublishers.ofString(jsonMapper.writeValueAsString(map)))
            .build();
    HttpResponse<String> response = client.send(request, HttpResponse.BodyHandlers.ofString());

    String expected =
        "<neo4j://graph.individuals#1> <http://schema.org/dob> \"1964\"^^<http://www.w3.org/2001/XMLSchema#long> .\n"
            + "<neo4j://graph.individuals#6> <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <neo4j://graph.schema#Movie> .\n"
            + "<neo4j://graph.individuals#1> <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://schema.org/Person> .\n"
            + "<neo4j://graph.individuals#6> <neo4j://graph.schema#title> \"The Matrix\" .\n"
            + "<neo4j://graph.individuals#1> <http://schema.org/inMovie> <neo4j://graph.individuals#6> .\n"
            + "<neo4j://graph.individuals#1> <http://schema.org/familyName> \"Keanu Reeves\" .";

    assertEquals(200, response.statusCode());
    assertTrue(ModelTestUtils
            .compareModels(expected, RDFFormat.NTRIPLES, response.body(), RDFFormat.NTRIPLES));

    map.put("mappedElemsOnly", "true");
    request = HttpRequest.newBuilder()
            .uri(URI.create(resolveURI(neo4j.httpURI(), "neo4j/cypher")))
            .header("Accept", "text/plain")
            .header("Content-Type", "application/json")
            .POST(HttpRequest.BodyPublishers.ofString(jsonMapper.writeValueAsString(map)))
            .build();
    response = client.send(request, HttpResponse.BodyHandlers.ofString());

    String expectedOnlyMapped =
        "<neo4j://graph.individuals#1> <http://schema.org/inMovie> <neo4j://graph.individuals#6> .\n"
            + "<neo4j://graph.individuals#1> <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://schema.org/Person> .\n"
            + "<neo4j://graph.individuals#1> <http://schema.org/dob> \"1964\"^^<http://www.w3.org/2001/XMLSchema#long> .\n"
            + "<neo4j://graph.individuals#1> <http://schema.org/familyName> \"Keanu Reeves\" .\n";

    assertEquals(200, response.statusCode());
    assertTrue(ModelTestUtils
            .compareModels(expectedOnlyMapped, RDFFormat.NTRIPLES, response.body(),
                    RDFFormat.NTRIPLES));


  }

  @Test
  public void testCypherWithUrisSerializeAsJsonLd() throws Exception {
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
              "CREATE (Keanu)-[:ns0" + PREFIX_SEPARATOR + "Likes]->(Carrie) ";
      tx.execute(dataInsertion);
      tx.commit();
    }

    Map<String, String> params = new HashMap<>();
    params.put("cypher", "MATCH (n:Resource) RETURN n LIMIT 1");

    HttpRequest request = HttpRequest.newBuilder()
            .uri(URI.create(resolveURI(neo4j.httpURI(), "neo4j/cypher")))
            .header("Accept", "application/ld+json")
            .header("Content-Type", "application/json")
            .POST(HttpRequest.BodyPublishers.ofString(jsonMapper.writeValueAsString(params)))
            .build();
    HttpResponse<String> response = client.send(request, HttpResponse.BodyHandlers.ofString());

    String expected = "[ {\n" +
        "  \"@id\" : \"https://permid.org/1-21523433750\",\n" +
        "  \"@type\" : [ \"http://permid.org/ontology/organization/Actor\" ],\n" +
        "  \"http://ont.thomsonreuters.com/mdaas/born\" : [ {\n" +
        "    \"@type\" : \"http://www.w3.org/2001/XMLSchema#long\",\n" +
        "    \"@value\" : \"1964\"\n" +
        "  } ],\n" +
        "  \"http://ont.thomsonreuters.com/mdaas/name\" : [ {\n" +
        "    \"@value\" : \"Keanu Reeves\"\n" +
        "  } ]\n" +
        "} ]";

    assertEquals(200, response.statusCode());
    assertTrue(ModelTestUtils
            .compareModels(expected, RDFFormat.JSONLD, response.body(), RDFFormat.JSONLD));

  }

  @Test
  public void testOneNodeCypherWithUrisSerializeAsJsonLd() throws Exception {

    try (Transaction tx = graphDatabaseService.beginTx()) {
      tx.execute("CALL n10s.graphconfig.init()");
      tx.execute("CALL n10s.nsprefixes.add('ns1', 'http://ont.thomsonreuters.com/mdaas/')");
      tx.execute("CALL n10s.nsprefixes.add('ns0', 'http://permid.org/ontology/organization/')");
      String dataInsertion =
              "CREATE (Keanu:Resource:ns0" + PREFIX_SEPARATOR + "Actor {ns1" + PREFIX_SEPARATOR
                      + "name:'Keanu Reeves', ns1" + PREFIX_SEPARATOR
                      + "born:1964, uri: 'https://permid.org/1-21523433750' }) ";
      tx.execute(dataInsertion);
      tx.commit();
    }

    Map<String, String> params = new HashMap<>();
    params.put("cypher", "MATCH (n) RETURN n ");

    HttpRequest request = HttpRequest.newBuilder()
            .uri(URI.create(resolveURI(neo4j.httpURI(), "neo4j/cypher")))
            .header("Accept", "application/ld+json")
            .header("Content-Type", "application/json")
            .POST(HttpRequest.BodyPublishers.ofString(jsonMapper.writeValueAsString(params)))
            .build();
    HttpResponse<String> response = client.send(request, HttpResponse.BodyHandlers.ofString());

    String expected = "[ {\n" +
        "  \"@id\" : \"https://permid.org/1-21523433750\",\n" +
        "  \"@type\" : [ \"http://permid.org/ontology/organization/Actor\" ],\n" +
        "  \"http://ont.thomsonreuters.com/mdaas/born\" : [ {\n" +
        "    \"@type\" : \"http://www.w3.org/2001/XMLSchema#long\",\n" +
        "    \"@value\" : \"1964\"\n" +
        "  } ],\n" +
        "  \"http://ont.thomsonreuters.com/mdaas/name\" : [ {\n" +
        "    \"@value\" : \"Keanu Reeves\"\n" +
        "  } ]\n" +
        "} ]";

    assertEquals(200, response.statusCode());
    assertTrue(ModelTestUtils
            .compareModels(expected, RDFFormat.JSONLD, response.body(), RDFFormat.JSONLD));

  }

}
