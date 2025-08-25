package n10s.endpoint;

import static n10s.CommonProcedures.UNIQUENESS_CONSTRAINT_ON_URI;
import static n10s.CommonProcedures.UNIQUENESS_CONSTRAINT_STATEMENT;
import static n10s.graphconfig.Params.PREFIX_SEPARATOR;
import static org.junit.Assert.*;
import static org.neo4j.internal.helpers.collection.Iterators.count;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.type.CollectionType;
import com.fasterxml.jackson.databind.type.TypeFactory;
import com.google.common.io.Resources;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.text.MessageFormat;
import java.util.*;

import n10s.ModelTestUtils;
import n10s.graphconfig.GraphConfigProcedures;
import n10s.mapping.MappingUtils;
import n10s.nsprefixes.NsPrefixDefProcedures;
import n10s.onto.load.OntoLoadProcedures;
import n10s.quadrdf.delete.QuadRDFDeleteProcedures;
import n10s.quadrdf.load.QuadRDFLoadProcedures;
import n10s.rdf.RDFProcedures;
import n10s.rdf.aggregate.CollectTriples;
import n10s.rdf.delete.RDFDeleteProcedures;
import n10s.rdf.export.ExportProcessor;
import n10s.rdf.export.RDFExportProcedures;
import n10s.rdf.load.RDFLoadProcedures;
import n10s.rdf.stream.RDFStreamProcedures;
import n10s.validation.ValidationProcedures;
import org.eclipse.rdf4j.model.vocabulary.RDF;
import org.eclipse.rdf4j.query.algebra.Str;
import org.eclipse.rdf4j.rio.RDFFormat;
import org.junit.*;
import org.neo4j.driver.Config;
import org.neo4j.driver.Driver;
import org.neo4j.driver.GraphDatabase;
import org.neo4j.driver.Record;
import org.neo4j.driver.Session;
import org.neo4j.driver.types.Node;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Result;
import org.neo4j.graphdb.Transaction;
import org.neo4j.harness.junit.rule.Neo4jRule;
import org.neo4j.test.server.HTTP;
import org.neo4j.test.server.HTTP.Response;

/**
 * Created by jbarrasa on 14/09/2016.
 */
public class RDFEndpointTest {

  public static Driver driver;
  public static GraphDatabaseService graphDatabaseService;
  public static GraphDatabaseService tempGDBs;
  public static Driver tempDriver;

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
    HTTP.Response response = HTTP.withHeaders("Accept", "application/ld+json").GET(
        resolveURI(neo4j.httpURI(), "neo4j/describe?nodeIdentifier=" + id.toString()));

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
    assertEquals(200, response.status());
    assertTrue(ModelTestUtils
        .compareModels(expected, RDFFormat.JSONLD, response.rawContent(), RDFFormat.JSONLD));

  }

//  @Test
//  public void testGetNodeByUriOnLPGGraph() throws Exception {
//    final GraphDatabaseService graphDatabaseService = neo4j.defaultDatabaseService();
//    try (Transaction tx = graphDatabaseService.beginTx()) {
//
//      String dataInsertion = "create (:Person:Resource { name: \"JB\", uri: \"http://neo4j.paradise.icij.dataset/ind/JB\"})" +
//              "-[:KNOWS]->(:Person:Resource { name: \"CC\", uri: \"http://neo4j.paradise.icij.dataset/ind/CC\"})";
//      tx.execute(dataInsertion);
//      tx.commit();
//
//    }
//
//    // When
//    HTTP.Response response = HTTP.withHeaders("Accept", "application/ld+json").GET(
//            resolveURI(neo4j.httpURI(), "neo4j/describe/")
//                    + URLEncoder.encode("http://neo4j.paradise.icij.dataset/ind/JB",StandardCharsets.UTF_8));
//
//    String expected = "";
//    assertEquals(200, response.status());
//    System.out.println(response.rawContent());
//    assertTrue(ModelTestUtils
//            .compareModels(expected, RDFFormat.JSONLD, response.rawContent(), RDFFormat.JSONLD));
//
//  }

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
    HTTP.Response response = HTTP.withHeaders("Accept", "application/ld+json").GET(
            resolveURI(neo4j.httpURI(), "neo4j/describe?nodeIdentifier=http%3A%2F%2Fneo4j.com%2Fmovies%2FKeanu"));

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
    assertEquals(200, response.status());
    assertTrue(ModelTestUtils
            .compareModels(expected, RDFFormat.JSONLD, response.rawContent(), RDFFormat.JSONLD));

    // When
    response = HTTP.withHeaders("Accept", "text/x-turtlestar").GET(
            resolveURI(neo4j.httpURI(), "neo4j/describe?nodeIdentifier=http%3A%2F%2Fneo4j.com%2Fmovies%2FHugo"));

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


    assertEquals(200, response.status());
    assertTrue(ModelTestUtils
            .compareModels(expected, RDFFormat.TURTLESTAR, response.rawContent(), RDFFormat.TURTLESTAR));

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

    Response response = HTTP.withHeaders("Accept", "text/plain").POST(
            resolveURI(neo4j.httpURI(), "neo4j/cypher"), map);

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

    assertEquals(200, response.status());
    assertTrue(ModelTestUtils
            .compareModels(expected, RDFFormat.TURTLESTAR, response.rawContent(), RDFFormat.TURTLESTAR));

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

    Response response = HTTP.withHeaders("Accept", "text/plain").POST(
        resolveURI(neo4j.httpURI(), "neo4j/cypher"), map);

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
    assertEquals(200, response.status());

    String responseString = response.rawContent();

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

    Response response = HTTP.withHeaders("Accept", "text/plain").GET(
            resolveURI(neo4j.httpURI(), "neo4j/describe?nodeIdentifier=" + URLEncoder.encode("http://www.example.com/example#Enitity1Individual", StandardCharsets.UTF_8) + "&format=RDF/XML"));

    String expected =
            "<http://www.example.com/example#Enitity1Individual> <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://www.example.com/example#Enitity1> .\n" +
            "<http://www.example.com/example#Enitity1Individual> <http://www.example.com/example#requires> <http://www.example.com/example#Entity2Individual> .\n" +
            "<http://www.example.com/example#Enitity1Individual> <http://www.example.com/example#requiresProp> \"12345\" ." +
            "<http://www.example.com/example#Enitity1Individual> <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://www.w3.org/2002/07/owl#NamedIndividual> .";
    assertEquals(200, response.status());
    System.out.println(response.rawContent());
    assertTrue(ModelTestUtils
            .compareModels(expected, RDFFormat.TURTLE, response.rawContent(), RDFFormat.RDFXML));

  }


  @Test
  public void testCypherOnMovieDBReturnsList() throws Exception {

    try (Transaction tx = graphDatabaseService.beginTx()) {
      tx.execute("call n10s.nsprefixes.add('sch','http://schema.org/')");
      tx.commit();
    }

    try (Transaction tx = graphDatabaseService.beginTx()) {

      tx.execute(Files.readString(Paths.get(
          RDFEndpointTest.class.getClassLoader().getResource("movies.cypher").getPath())));

      tx.commit();
    }
    //ADD mapppings and nsprefixes
    try (Transaction tx = graphDatabaseService.beginTx()) {

      tx.execute("call n10s.mapping.add(\"http://schema.org/when\",\"released\")");
      tx.commit();
    }

    try (Transaction tx = graphDatabaseService.beginTx()) {
      Result execute = tx.execute("call n10s.validation.shacl.import.fetch('" +
          RDFEndpointTest.class.getClassLoader().getResource("shacl/person2-shacl.ttl")
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

    Response response = HTTP.withHeaders("Accept", "text/plain").POST(
        resolveURI(neo4j.httpURI(), "neo4j/cypher"), map);

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

    assertEquals(200, response.status());
    assertTrue(ModelTestUtils
        .compareModels(expected, RDFFormat.TURTLE, response.rawContent(), RDFFormat.TURTLE));

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
          RDFEndpointTest.class.getClassLoader().getResource("multival.ttl")
              .toURI() + "','Turtle',{})");
      Map<String, Object> next = execute.next();
      assertEquals(9L, next.get("triplesLoaded"));

      tx.commit();
    }

    Map<String, Object> map = new HashMap<>();
    map.put("cypher", "MATCH (n) RETURN collect(n) as col");

    Response response = HTTP.withHeaders("Accept", "text/plain").POST(
        resolveURI(neo4j.httpURI(), "neo4j/cypher"), map);

    String expectedNoNonResources = "<http://example.org/vocab/show/218> <http://example.org/vocab/show/producer> \"Joanna Smith\" .\n"
        + "<http://example.org/vocab/show/218> <http://example.org/vocab/show/localName> \"Cette Série des Années Septante\" .\n"
        + "<http://example.org/vocab/show/218> <http://example.org/vocab/show/showId> \"218\"^^<http://www.w3.org/2001/XMLSchema#long> .\n"
        + "<http://example.org/vocab/show/218> <http://example.org/vocab/show/availableInLang> \"ES\" .";
    assertEquals(200, response.status());
    assertTrue(ModelTestUtils
        .compareModels(expectedNoNonResources, RDFFormat.TURTLE, response.rawContent(), RDFFormat.TURTLE));

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
    HTTP.Response response = HTTP.withHeaders("Accept", "text/x-turtlestar").GET(
        resolveURI(neo4j.httpURI(), "neo4j/describe?nodeIdentifier=")
            + id3.toString());

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
    assertEquals(200, response.status());
    assertTrue(ModelTestUtils
        .compareModels(expected, RDFFormat.TURTLESTAR, response.rawContent(), RDFFormat.TURTLESTAR));

  }

  @Test
  public void ImportGetNodeById() throws Exception {

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
    long id;
    try (Transaction tx = graphDatabaseService.beginTx()) {

      Result result = tx.execute("MATCH (n:Critic) RETURN id(n) AS id ");
      id = (Long) result.next().get("id");
      assertNotNull(id);
    }

    Session session = tempDriver.session();
    session.run(UNIQUENESS_CONSTRAINT_STATEMENT);
    session.run("CALL n10s.graphconfig.init( { handleVocabUris: 'IGNORE', typesToLabels: true } )");
    org.neo4j.driver.Result importResults
        = session.run("CALL n10s.rdf.import.fetch('" +
        resolveURI(neo4j.httpURI(), "neo4j/describe?nodeIdentifier=")
        + id +
        "','Turtle')");

      Map<String, Object> singleResult = importResults
          .single().asMap();

      assertEquals(5L, singleResult.get("triplesLoaded"));
      org.neo4j.driver.Result postImport = session.run("MATCH (n:Resource:Critic) RETURN n");
      Node criticPostImport = postImport.next().get("n").asNode();

      try (Transaction tx = graphDatabaseService.beginTx()) {

        Result result = tx.execute("MATCH (n:Critic) WHERE NOT n:Resource "
            + " RETURN n.born AS born, n.name AS name,  id(n) AS id");
        Map<String, Object> criticPreImport = result.next();
        assertEquals(criticPreImport.get("name"), criticPostImport.get("name").asString());
        assertEquals(criticPreImport.get("born"), criticPostImport.get("born").asLong());
        assertNull(criticPreImport.get("uri"));
        assertEquals("neo4j://graph.individuals#" + criticPreImport.get("id"),
            criticPostImport.get("uri").asString());
      }
  }


  @Test
  public void ImportGetNodeByIdOnImportedOnto() throws Exception {

    //first import onto
    try (Transaction tx = graphDatabaseService.beginTx()) {
      tx.execute(UNIQUENESS_CONSTRAINT_STATEMENT);
      tx.commit();
    }
    try (Transaction tx = graphDatabaseService.beginTx()) {
      tx.execute("CALL n10s.graphconfig.init({ handleVocabUris: \"IGNORE\" })");
      tx.execute("CALL n10s.onto.import.fetch('" +
          RDFEndpointTest.class.getClassLoader().getResource("onto1.owl")
              .toURI() + "','RDF/XML',{})");

      tx.commit();
    }
    //  check data is  correctly loaded
    try (Transaction tx = graphDatabaseService.beginTx()) {
      Result result = tx.execute("match (n:Class " +
          "{ uri: \"http://n4j.com/tst1/ontologies/2017/4/Cyber_EA_Smart_City#RF_signal_strength\"})"
          +
          "-[r]-(o) " +
          "return n.name as name, id(n) as id, type(r) as reltype, o.uri as otheruri");
      Map<String, Object> next = result.next();
      assertEquals("RF_signal_strength", next.get("name"));
      assertEquals("SCO", next.get("reltype"));
      assertEquals("http://n4j.com/tst1/ontologies/2017/4/Cyber_EA_Smart_City#Vehicle_Key",
          next.get("otheruri"));
    }

    // then export elements and check the output is right
    HTTP.Response response = HTTP.withHeaders("Accept", "text/turtle").GET(
        resolveURI(neo4j.httpURI(), "neo4j/describe?nodeIdentifier=") +
                "http%3A%2F%2Fn4j.com%2Ftst1%2Fontologies%2F2017%2F4%2FCyber_EA_Smart_City%23RF_signal_strength");

    String expected = "@prefix n4sch: <neo4j://graph.schema#> .\n" +
            "\n" +
            "<http://n4j.com/tst1/ontologies/2017/4/Cyber_EA_Smart_City#RF_signal_strength> a n4sch:Class;\n" +
            "  n4sch:SCO <http://n4j.com/tst1/ontologies/2017/4/Cyber_EA_Smart_City#Vehicle_Key>;\n" +
            "  n4sch:name \"RF_signal_strength\" .";

    assertEquals(200, response.status());
    assertTrue(ModelTestUtils
        .compareModels(expected, RDFFormat.TURTLE, response.rawContent(), RDFFormat.TURTLE));


  }

  @Test
  public void ImportGetNodeByUriOnImportedOntoShorten() throws Exception {

    //first import onto
    try (Transaction tx = graphDatabaseService.beginTx()) {
      tx.execute(UNIQUENESS_CONSTRAINT_STATEMENT);
      tx.commit();
    }
    try (Transaction tx = graphDatabaseService.beginTx()) {
      tx.execute("CALL n10s.graphconfig.init()");
      tx.execute("CALL n10s.onto.import.fetch('" +
              RDFEndpointTest.class.getClassLoader().getResource("onto1.owl")
                      .toURI() + "','RDF/XML',{})");

      tx.commit();
    }
    //  check data is  correctly loaded
    Long id;
    try (Transaction tx = graphDatabaseService.beginTx()) {
      Result result = tx.execute("match (n:n4sch__Class " +
              "{ uri: \"http://n4j.com/tst1/ontologies/2017/4/Cyber_EA_Smart_City#RF_signal_strength\"})"
              +
              "-[r]-(o) " +
              "return n.n4sch__name as name, id(n) as id, type(r) as reltype, o.uri as otheruri");
      Map<String, Object> next = result.next();
      assertEquals("RF_signal_strength", next.get("name"));
      assertEquals("n4sch__SCO", next.get("reltype"));
      assertEquals("http://n4j.com/tst1/ontologies/2017/4/Cyber_EA_Smart_City#Vehicle_Key",
              next.get("otheruri"));

      id = (Long) next.get("id");
    }

    try (Transaction tx = graphDatabaseService.beginTx()) {
      tx.execute("MATCH (gc:_GraphConfig ) DELETE gc; ");
      tx.commit();
    }
    // then export elements and check the output is right
    HTTP.Response response = HTTP.withHeaders("Accept", "text/turtle").GET(
            resolveURI(neo4j.httpURI(), "neo4j/describe?nodeIdentifier=" + id));

    String expected = "@prefix n4sch: <neo4j://graph.schema#> .\n" +
            "@prefix n4ind: <neo4j://graph.individuals#> .\n" +
            "\n" +
            "<http://n4j.com/tst1/ontologies/2017/4/Cyber_EA_Smart_City#RF_signal_strength> a n4sch:n4sch__Class;\n" +
            "  n4sch:n4sch__SCO <http://n4j.com/tst1/ontologies/2017/4/Cyber_EA_Smart_City#Vehicle_Key>;\n" +
            "  n4sch:n4sch__name \"RF_signal_strength\" .";

    assertEquals(200, response.status());
    assertTrue(ModelTestUtils
            .compareModels(expected, RDFFormat.TURTLE, response.rawContent(), RDFFormat.TURTLE));


    response = HTTP.withHeaders("Accept", "text/turtle").GET(
            resolveURI(neo4j.httpURI(), "neo4j/describe?nodeIdentifier=") +
                    URLEncoder.encode("http://n4j.com/tst1/ontologies/2017/4/Cyber_EA_Smart_City#RF_signal_strength",
                            "UTF-8"));
    assertEquals(200, response.status());
    assertTrue(ModelTestUtils
            .compareModels(expected, RDFFormat.TURTLE, response.rawContent(), RDFFormat.TURTLE));
  }

  @Test
  public void ImportGetNodeByUriOnImportedOntoIgnore() throws Exception {

    //first import onto
    try (Transaction tx = graphDatabaseService.beginTx()) {
      tx.execute(UNIQUENESS_CONSTRAINT_STATEMENT);
      tx.commit();
    }
    try (Transaction tx = graphDatabaseService.beginTx()) {
      tx.execute("CALL n10s.graphconfig.init({ handleVocabUris: \"IGNORE\" })");
      tx.execute("CALL n10s.onto.import.fetch('" +
              RDFEndpointTest.class.getClassLoader().getResource("onto1.owl")
                      .toURI() + "','RDF/XML',{})");

      tx.commit();
    }
    //  check data is  correctly loaded
    Long id;
    try (Transaction tx = graphDatabaseService.beginTx()) {
      Result result = tx.execute("match (n:Class " +
              "{ uri: \"http://n4j.com/tst1/ontologies/2017/4/Cyber_EA_Smart_City#RF_signal_strength\"})"
              +
              "-[r]-(o) " +
              "return n.name as name, id(n) as id, type(r) as reltype");
      Map<String, Object> next = result.next();
      assertEquals("RF_signal_strength", next.get("name"));
      assertEquals("SCO", next.get("reltype"));

      id = (Long) next.get("id");
    }

    // then export elements and check the output is right
    HTTP.Response response = HTTP.withHeaders("Accept", "text/turtle").GET(
            resolveURI(neo4j.httpURI(), "neo4j/describe?nodeIdentifier=" + id));

    String expected = "@prefix rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#> .\n" +
            "@prefix neovoc: <neo4j://graph.schema#> .\n" +
            "@prefix neoind: <neo4j://graph.individuals#> .\n" +
            "<http://n4j.com/tst1/ontologies/2017/4/Cyber_EA_Smart_City#RF_signal_strength> a neovoc:Class;\n"
            +
            "  neovoc:SCO <http://n4j.com/tst1/ontologies/2017/4/Cyber_EA_Smart_City#Vehicle_Key> ;\n" +
            "  neovoc:name \"RF_signal_strength\" .\n";

    assertEquals(200, response.status());
    assertTrue(ModelTestUtils
            .compareModels(expected, RDFFormat.TURTLE, response.rawContent(), RDFFormat.TURTLE));
    
    response = HTTP.withHeaders("Accept", "text/turtle").GET(
            resolveURI(neo4j.httpURI(), "neo4j/describe?nodeIdentifier=") +
                    URLEncoder.encode("http://n4j.com/tst1/ontologies/2017/4/Cyber_EA_Smart_City#RF_signal_strength",
                            StandardCharsets.UTF_8)
    );

    assertEquals(200, response.status());
    assertTrue(ModelTestUtils
            .compareModels(expected, RDFFormat.TURTLE, response.rawContent(), RDFFormat.TURTLE));

  }


  @Test
  public void ImportGetCypher() throws Exception {

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

    Map<String, Object> preImport;
    try (Transaction tx = graphDatabaseService.beginTx()) {
      Result result = tx.execute(
          "MATCH (n:Critic) RETURN n.born AS born, n.name AS name , n.uri as uri, id(n) as id");
      preImport = result.next();
      assertEquals(1960L, preImport.get("born"));
      assertNotNull(preImport.get("id"));
      assertEquals("Hugo Weaving", preImport.get("name"));
      //no uri pre-import
      assertNull(preImport.get("uri"));
    }

    Session session = tempDriver.session();
    session.run(UNIQUENESS_CONSTRAINT_STATEMENT);
    session.run("CALL n10s.graphconfig.init( { handleVocabUris: 'IGNORE' })");
    org.neo4j.driver.Result importResults
        = session.run("CALL n10s.rdf.import.fetch('" +
        resolveURI(neo4j.httpURI(), "neo4j/cypher") +
        "','Turtle',{ headerParams: { Accept: \"text/turtle\"},"
        + "payload: '{ \"cypher\": \"MATCH (x:Critic) RETURN x \"}'})");

      Map<String, Object> singleResult = importResults
          .single().asMap();

      assertEquals(3L, singleResult.get("triplesLoaded"));
      org.neo4j.driver.Result postImport = session.run("MATCH (n:Resource:Critic) RETURN n");
      Record next = postImport.next();
      Node criticPostImport = next.get("n").asNode();
      assertEquals(preImport.get("name"), criticPostImport.get("name").asString());
      assertEquals(preImport.get("born"), criticPostImport.get("born").asLong());
      assertEquals("neo4j://graph.individuals#" + preImport.get("id"),
          criticPostImport.get("uri").asString());
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
    HTTP.Response response = HTTP.withHeaders("Accept", "application/ld+json").GET(
        resolveURI(neo4j.httpURI(), "neo4j/describe/find/Director/born/1961?valType=INTEGER"));
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

    assertEquals(200, response.status());
    assertTrue(ModelTestUtils
        .compareModels(expected, RDFFormat.JSONLD, response.rawContent(), RDFFormat.JSONLD));

    // When
    response = HTTP.withHeaders("Accept", "application/ld+json").GET(
        resolveURI(neo4j.httpURI(), "neo4j/describe/find/Director/name/Laurence%20Fishburne"));

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
    assertEquals(200, response.status());
    assertTrue(ModelTestUtils
        .compareModels(expected, RDFFormat.JSONLD, response.rawContent(), RDFFormat.JSONLD));

    // When
    response = HTTP.withHeaders("Accept", "application/ld+json").GET(
        resolveURI(neo4j.httpURI(), "neo4j/describe/find/Actor/born/1964?valType=INTEGER"));

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
    assertEquals(200, response.status());
    assertTrue(ModelTestUtils
        .compareModels(expected, RDFFormat.JSONLD, response.rawContent(), RDFFormat.JSONLD));
  }

  @Test
  public void testFindNodeByLabelAndPropertyNotFoundOrInvalid() throws Exception {
    HTTP.Response response = HTTP.withHeaders("Accept", "application/ld+json").GET(
        resolveURI(neo4j.httpURI(), "neo4j/describe/find/WrongLabel/wrongProperty/someValue"));

    // TODO - document it??
    assertEquals(emptyJsonLd, response.rawContent());
    assertEquals(200, response.status());

    response = HTTP.withHeaders("Accept", "application/ld+json").GET(
        resolveURI(neo4j.httpURI(), "neo4j/describe/find/Something"));

    assertEquals("", response.rawContent());
    assertEquals(404, response.status());
  }

  @Test
  public void testGetNodeByUriOrIdNotFoundOrInvalid() throws Exception {

    HTTP.Response response = HTTP.withHeaders("Accept", "text/n3").GET(
            resolveURI(neo4j.httpURI(), "neo4j/describe?nodeIdentifier=9999999"));
    assertEquals(200, response.status());
    assertEquals("@prefix n4sch: <neo4j://graph.schema#> .\n" +
            "@prefix n4ind: <neo4j://graph.individuals#> .\n", response.rawContent());

    response = HTTP.withHeaders("Accept", "application/rdf+xml").GET(
            resolveURI(neo4j.httpURI(), "neo4j/describe?nodeIdentifier=9999999"));
    assertEquals(200, response.status());
    assertEquals("<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n" +
            "<rdf:RDF\n" +
            "\txmlns:n4sch=\"neo4j://graph.schema#\"\n" +
            "\txmlns:n4ind=\"neo4j://graph.individuals#\"\n" +
            "\txmlns:rdf=\"http://www.w3.org/1999/02/22-rdf-syntax-ns#\">\n" +
            "\n" +
            "</rdf:RDF>", response.rawContent());

    response = HTTP.withHeaders("Accept", "application/ld+json").GET(
            resolveURI(neo4j.httpURI(), "neo4j/describe?nodeIdentifier=adb"));

    assertEquals("[ ]", response.rawContent());
    assertEquals(200, response.status());

    try (Transaction tx = graphDatabaseService.beginTx()) {
      tx.execute("CALL n10s.graphconfig.init()");
      tx.commit();
    }

    response = HTTP.withHeaders("Accept", "text/n3").GET(
            resolveURI(neo4j.httpURI(), "neo4j/describe?nodeIdentifier=9999999"));
    assertEquals(200, response.status());
    assertEquals("", response.rawContent());

    response = HTTP.withHeaders("Accept", "application/rdf+xml").GET(
        resolveURI(neo4j.httpURI(), "neo4j/describe?nodeIdentifier=9999999"));
    assertEquals(200, response.status());
    assertEquals("<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n" +
            "<rdf:RDF\n" +
            "\txmlns:rdf=\"http://www.w3.org/1999/02/22-rdf-syntax-ns#\">\n" +
            "\n" +
            "</rdf:RDF>", response.rawContent());

  }

  @Test
  public void testPing() throws Exception {
    HTTP.Response response = HTTP.GET(
        resolveURI(neo4j.httpURI(), "ping"));

    assertEquals("{\"ping\":\"here!\"}", response.rawContent());
    assertEquals(200, response.status());

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

    HTTP.Response response = HTTP.withHeaders("Accept", "text/plain").POST(
        resolveURI(neo4j.httpURI(), "neo4j/cypher"), map);

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

    assertEquals(200, response.status());
    assertTrue(ModelTestUtils
        .compareModels(expected, RDFFormat.NTRIPLES, response.rawContent(), RDFFormat.NTRIPLES));

    // request passing serialisation format as request param
    map.put("format", "RDF/XML");
    response = HTTP.withHeaders("Accept", "text/plain").POST(
        resolveURI(neo4j.httpURI(), "neo4j/cypher"), map);

    assertEquals(200, response.status());
    assertTrue(ModelTestUtils
        .compareModels(expected, RDFFormat.NTRIPLES, response.rawContent(), RDFFormat.RDFXML));

    map.put("mappedElemsOnly", "true");
    map.remove("format");
    response = HTTP.withHeaders("Accept", "text/plain").POST(
        resolveURI(neo4j.httpURI(), "neo4j/cypher"), map);

    assertEquals(200, response.status());
    assertEquals("", response.rawContent());

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

    HTTP.Response response = HTTP.withHeaders("Accept", "text/plain").POST(
        resolveURI(neo4j.httpURI(), "neo4j/cypher"), map);

    String expected =
        "<neo4j://graph.individuals#1> <http://schema.org/dob> \"1964\"^^<http://www.w3.org/2001/XMLSchema#long> .\n"
            + "<neo4j://graph.individuals#6> <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <neo4j://graph.schema#Movie> .\n"
            + "<neo4j://graph.individuals#1> <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://schema.org/Person> .\n"
            + "<neo4j://graph.individuals#6> <neo4j://graph.schema#title> \"The Matrix\" .\n"
            + "<neo4j://graph.individuals#1> <http://schema.org/inMovie> <neo4j://graph.individuals#6> .\n"
            + "<neo4j://graph.individuals#1> <http://schema.org/familyName> \"Keanu Reeves\" .";

    assertEquals(200, response.status());
    assertTrue(ModelTestUtils
        .compareModels(expected, RDFFormat.NTRIPLES, response.rawContent(), RDFFormat.NTRIPLES));

    map.put("mappedElemsOnly", "true");
    response = HTTP.withHeaders("Accept", "text/plain").POST(
        resolveURI(neo4j.httpURI(), "neo4j/cypher"), map);

    String expectedOnlyMapped =
        "<neo4j://graph.individuals#1> <http://schema.org/inMovie> <neo4j://graph.individuals#6> .\n"
            + "<neo4j://graph.individuals#1> <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://schema.org/Person> .\n"
            + "<neo4j://graph.individuals#1> <http://schema.org/dob> \"1964\"^^<http://www.w3.org/2001/XMLSchema#long> .\n"
            + "<neo4j://graph.individuals#1> <http://schema.org/familyName> \"Keanu Reeves\" .\n";

    assertEquals(200, response.status());
    assertTrue(ModelTestUtils
        .compareModels(expectedOnlyMapped, RDFFormat.NTRIPLES, response.rawContent(),
            RDFFormat.NTRIPLES));


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
    HTTP.Response response = HTTP.withHeaders("Accept", "text/plain").GET(
        resolveURI(neo4j.httpURI(), "neo4j/onto"));

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
    assertEquals(200, response.status());
    assertTrue(ModelTestUtils
        .compareModels(expected, RDFFormat.NTRIPLES, response.rawContent(), RDFFormat.NTRIPLES));

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

    HTTP.Response response = HTTP.withHeaders("Accept", "text/plain").GET(
            resolveURI(neo4j.httpURI(), "neo4j/onto"));

    String expected =
            "<neo4j://graph.schema#PropertyLessThing> <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://www.w3.org/2002/07/owl#Class> .\n" +
                    "<neo4j://graph.schema#PropertyLessThing> <http://www.w3.org/2000/01/rdf-schema#label> \"PropertyLessThing\" .\n" ;
    assertEquals(200, response.status());
    assertTrue(ModelTestUtils
            .compareModels(expected, RDFFormat.NTRIPLES, response.rawContent(), RDFFormat.NTRIPLES));

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

    HTTP.Response response = HTTP.withHeaders("Accept", "text/plain").GET(
        resolveURI(neo4j.httpURI(), "neo4j/onto"));

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
    assertEquals(200, response.status());
    assertTrue(ModelTestUtils
        .compareModels(expected, RDFFormat.NTRIPLES, response.rawContent(), RDFFormat.NTRIPLES));

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

    HTTP.Response response = HTTP.withHeaders("Accept", "text/plain").GET(
            resolveURI(neo4j.httpURI(), "neo4j/onto"));

    String expected =
            "<http://permid.org/ontology/organization/Person> <http://www.w3.org/2000/01/rdf-schema#label> \"Person\" .\n" +
            "<http://permid.org/ontology/organization/Person> <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://www.w3.org/2002/07/owl#Class> .\n" +
            "<http://permid.org/ontology/organization/PropertyLessThing> <http://www.w3.org/2000/01/rdf-schema#label> \"PropertyLessThing\" .\n" +
            "<http://permid.org/ontology/organization/PropertyLessThing> <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://www.w3.org/2002/07/owl#Class> .\n" ;

    assertEquals(200, response.status());
    assertTrue(ModelTestUtils
            .compareModels(expected, RDFFormat.NTRIPLES, response.rawContent(), RDFFormat.NTRIPLES));

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
    HTTP.Response response = HTTP.withHeaders("Accept", "text/turtle").GET(
        resolveURI(neo4j.httpURI(), "neo4j/describe?nodeIdentifier=") + URLEncoder
            .encode("https://permid.org/1-21523433750", StandardCharsets.UTF_8.toString()));
    String expected = "@prefix neovoc: <neo4j://graph.schema#> .\n" +
        "<https://permid.org/1-21523433750> a <http://permid.org/ontology/organization/Actor>;\n"
        + " <http://ont.thomsonreuters.com/mdaas/born> \"1964\"^^<http://www.w3.org/2001/XMLSchema#long>;\n"
        + " <http://ont.thomsonreuters.com/mdaas/name> \"Keanu Reeves\";\n"
        + " <http://permid.org/ontology/organization/Likes> <https://permid.org/1-21523433751> .\n"
        + " <https://permid.org/1-21523433753> <http://permid.org/ontology/organization/FriendOf>\n"
        + " <https://permid.org/1-21523433750> .\n";
    assertEquals(200, response.status());
    assertTrue(ModelTestUtils
        .compareModels(expected, RDFFormat.TURTLE, response.rawContent(), RDFFormat.TURTLE));

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
          RDFEndpointTest.class.getClassLoader().getResource("fibo-fragment.rdf")
              .toURI() + "','RDF/XML')");

      tx.commit();
    }

    HTTP.Response response = HTTP.withHeaders("Accept", "application/rdf+xml").GET(
        resolveURI(neo4j.httpURI(), "neo4j/describe?nodeIdentifier=") + URLEncoder.encode(
            "https://spec.edmcouncil.org/fibo/ontology/BE/Corporations/Corporations/BoardAgreement",
            StandardCharsets.UTF_8.toString())
            + "&excludeContext=true");

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

    assertEquals(200, response.status());
    assertTrue(ModelTestUtils
        .compareModels(expected, RDFFormat.RDFXML, response.rawContent(), RDFFormat.RDFXML));

    //uris need to be urlencoded. Normally not a problem but beware of hash signs!!
    response = HTTP.withHeaders("Accept", "text/plain").GET(
        resolveURI(neo4j.httpURI(), "neo4j/describe?nodeIdentifier=")
            + URLEncoder.encode("http://www.w3.org/2004/02/skos/core#TestyMcTestFace", "UTF-8")
    );

    expected = "<https://spec.edmcouncil.org/fibo/ontology/BE/Corporations/Corporations/> <http://www.omg.org/techprocess/ab/SpecificationMetadata/linkToResourceAddedForTestingPurposesByJB> <http://www.w3.org/2004/02/skos/core#TestyMcTestFace> .";
    assertTrue(ModelTestUtils
        .compareModels(expected, RDFFormat.NTRIPLES, response.rawContent(), RDFFormat.NTRIPLES));
    assertEquals(200, response.status());
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
          RDFEndpointTest.class.getClassLoader().getResource("fibo-fragment.rdf")
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

    HTTP.Response response = HTTP.withHeaders("Accept", "application/rdf+xml").GET(
        resolveURI(neo4j.httpURI(), "neo4j/describe?nodeIdentifier=") + URLEncoder
            .encode("https://spec.edmcouncil.org/fibo/ontology/BE/Corporations/Corporations/",
                StandardCharsets.UTF_8.toString()));

    assertEquals(200, response.status());
    String expected = "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n"
        + "<rdf:RDF\n"
        + "\txmlns:rdf=\"http://www.w3.org/1999/02/22-rdf-syntax-ns#\">\n"
        + "<!-- RDF Serialization ERROR: Prefix fiboanno in use but not defined in the '_NsPrefDef' node -->\n"
        + "\n"
        + "</rdf:RDF>";
    assertTrue(ModelTestUtils
        .compareModels(expected, RDFFormat.RDFXML, response.rawContent(), RDFFormat.RDFXML));

    assertTrue(response.rawContent().contains("RDF Serialization ERROR: Prefix fiboanno "
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
          RDFEndpointTest.class.getClassLoader().getResource("multilang.ttl")
              .toURI() + "','Turtle')");

      tx.commit();
    }

    HTTP.Response response = HTTP.withHeaders("Accept", "text/turtle").GET(
        resolveURI(neo4j.httpURI(), "neo4j/describe?nodeIdentifier=") + URLEncoder
            .encode("http://example.org/vocab/show/218", StandardCharsets.UTF_8.toString()));

    String expected = "@prefix show: <http://example.org/vocab/show/> .\n" +
        "show:218 show:localName \"That Seventies Show\"@en .                 # literal with a language tag\n"
        +
        "show:218 show:localName 'Cette Série des Années Soixante-dix'@fr . # literal delimited by single quote\n"
        +
        "show:218 show:localName \"Cette Série des Années Septante\"@fr-be .  # literal with a region subtag";

    assertEquals(200, response.status());
    assertTrue(ModelTestUtils
        .compareModels(expected, RDFFormat.TURTLE, response.rawContent(), RDFFormat.TURTLE));

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

    HTTP.Response response = HTTP.withHeaders("Accept", "application/ld+json").POST(
        resolveURI(neo4j.httpURI(), "neo4j/cypher"),
        params);

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

    assertEquals(200, response.status());
    assertTrue(ModelTestUtils
        .compareModels(expected, RDFFormat.JSONLD, response.rawContent(), RDFFormat.JSONLD));

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

    HTTP.Response response = HTTP.withHeaders("Accept", "application/ld+json").POST(
        resolveURI(neo4j.httpURI(), "neo4j/cypher"),
        params);

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

    assertEquals(200, response.status());
    assertTrue(ModelTestUtils
        .compareModels(expected, RDFFormat.JSONLD, response.rawContent(), RDFFormat.JSONLD));

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

    HTTP.Response response = HTTP.withHeaders("Accept", "application/rdf+xml").POST(
        resolveURI(neo4j.httpURI(), "neo4j/cypher"),
        params);

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

    assertEquals(200, response.status());
    assertTrue(ModelTestUtils
        .compareModels(expected, RDFFormat.RDFXML, response.rawContent(), RDFFormat.RDFXML));

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
          RDFEndpointTest.class.getClassLoader().getResource("customDataTypes2.ttl")
              .toURI()
          + "','Turtle')");

      tx.commit();
    }

    HTTP.Response response = HTTP.withHeaders("Accept", "text/turtle").GET(
        resolveURI(neo4j.httpURI(), "neo4j/describe?nodeIdentifier=") + URLEncoder
            .encode("http://example.org/Resource1", StandardCharsets.UTF_8.toString()));

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

    assertEquals(200, response.status());
    assertTrue(ModelTestUtils
        .compareModels(expected, RDFFormat.TURTLE, response.rawContent(), RDFFormat.TURTLE));

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
          RDFEndpointTest.class.getClassLoader().getResource("customDataTypes2.ttl")
              .toURI() + "','Turtle')");

      tx.commit();
    }
    HTTP.Response response = HTTP.withHeaders("Accept", "text/turtle").GET(
        resolveURI(neo4j.httpURI(), "neo4j/describe?nodeIdentifier=") + URLEncoder
            .encode("http://example.org/Resource1", StandardCharsets.UTF_8.toString()));

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

    assertEquals(200, response.status());
    assertTrue(ModelTestUtils
        .compareModels(expected, RDFFormat.TURTLE, response.rawContent(), RDFFormat.TURTLE));

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
          RDFEndpointTest.class.getClassLoader().getResource("customDataTypes.ttl")
              .toURI()
          + "','Turtle')");

      tx.commit();
    }
    HTTP.Response response = HTTP.withHeaders("Accept", "text/turtle").GET(
        resolveURI(neo4j.httpURI(), "neo4j/describe?nodeIdentifier=") + URLEncoder
            .encode("http://example.com/Mercedes", StandardCharsets.UTF_8.toString()));

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

    assertEquals(200, response.status());
    assertTrue(ModelTestUtils
        .compareModels(expected, RDFFormat.TURTLE, response.rawContent(), RDFFormat.TURTLE));

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
    HTTP.Response response = HTTP.withHeaders("Accept", "text/turtle").GET(
        resolveURI(neo4j.httpURI(), "neo4j/describe?nodeIdentifier=") + URLEncoder
            .encode("http://example.com/Mercedes", StandardCharsets.UTF_8.toString()));

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

    assertEquals(200, response.status());
    assertTrue(ModelTestUtils
        .compareModels(expected, RDFFormat.TURTLE, response.rawContent(), RDFFormat.TURTLE));


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
          RDFEndpointTest.class.getClassLoader().getResource("customDataTypes2.ttl")
              .toURI()
          + "','Turtle')");

      tx.commit();
    }
    Map<String, String> params = new HashMap<>();
    params.put("cypher", "MATCH (n {uri: 'http://example.org/Resource1'})" +
        "OPTIONAL MATCH (n)-[]-(m) RETURN *");

    HTTP.Response response = HTTP.withHeaders("Accept", "text/turtle").POST(
        resolveURI(neo4j.httpURI(), "neo4j/cypher"),
        params);

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

    assertEquals(200, response.status());
    assertTrue(ModelTestUtils
        .compareModels(expected, RDFFormat.TURTLE, response.rawContent(), RDFFormat.TURTLE));


  }

  public static String resolveURI(java.net.URI baseUri, String path) throws java.net.URISyntaxException {
    String location = HTTP.GET(baseUri.resolve("rdf").toString()).location();
    if (location.startsWith("http")) {
      return new java.net.URI(location).resolve(path).toString();
    } else {
      return baseUri.resolve(location).resolve(path).toString();
    }
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
          RDFEndpointTest.class.getClassLoader()
              .getResource("datetime/datetime-simple-multivalued.ttl")
              .toURI()
          + "','Turtle')");

      tx.commit();
    }

    Map<String, String> params = new HashMap<>();
    params.put("cypher", "MATCH (n) RETURN *");

    HTTP.Response response = HTTP.withHeaders("Accept", "text/turtle").POST(resolveURI(neo4j.httpURI(), "neo4j/cypher"), params);

    String expected = "@prefix rdf:     <http://www.w3.org/1999/02/22-rdf-syntax-ns#>.\n"
        + "@prefix xsd:     <http://www.w3.org/2001/XMLSchema#>.\n"
        + "@prefix exterms: <hhttp://www.example.org/terms/>.\n"
        + "@prefix ex: <hhttp://www.example.org/indiv/>.\n"
        + "\n"
        + "ex:index.html  exterms:someDateValue  \"1999-08-16\"^^xsd:date, \"1999-08-17\"^^xsd:date, \"1999-08-18\"^^xsd:date  ;\n"
        + "               exterms:someDateTimeValues \"2012-12-31T23:57:00\"^^xsd:dateTime, \"2012-12-30T23:57:00\"^^xsd:dateTime .";

    assertEquals(200, response.status());
    assertTrue(ModelTestUtils
        .compareModels(expected, RDFFormat.TURTLE, response.rawContent(), RDFFormat.TURTLE));

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

    HTTP.Response response = HTTP.withHeaders("Accept", "text/turtle").POST(
        resolveURI(neo4j.httpURI(), "neo4j/cypher"),
        params);

    assertEquals(200, response.status());

    assertTrue(ModelTestUtils
        .compareModels(getExportedAsLPG("neo4j://explicit_uri#123"), RDFFormat.TURTLE, response.rawContent(), RDFFormat.TURTLE));

    try (Transaction tx = graphDatabaseService.beginTx()) {
      tx.execute(" MATCH (n) DETACH DELETE n ");
      tx.execute(" CALL n10s.graphconfig.init({handleVocabUris: 'IGNORE'}) ");
      Result res = tx.execute(cypherCreate);
      tx.commit();
    }

    response = HTTP.withHeaders("Accept", "text/turtle").POST(
        resolveURI(neo4j.httpURI(), "neo4j/cypher"),
        params);

    assertTrue(ModelTestUtils
        .compareModels(getExportedAsLPG("neo4j://explicit_uri#123"), RDFFormat.TURTLE, response.rawContent(), RDFFormat.TURTLE));


    try (Transaction tx = graphDatabaseService.beginTx()) {
      String cypherRDFCreate = " CREATE (:Resource { uri: 'neo4j://explicit_uri#123' , voc__name: 'the name' }) ";
      tx.execute(" MATCH (n) DETACH DELETE n ");
      tx.execute(" CALL n10s.graphconfig.init() ");
      tx.execute("call n10s.nsprefixes.add('voc','neo4j://myvoc#')");
      tx.execute(cypherRDFCreate);
      tx.commit();
    }

    response = HTTP.withHeaders("Accept", "text/turtle").POST(
        resolveURI(neo4j.httpURI(), "neo4j/cypher"),
        params);

    String exportedAsRDF = "@prefix rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#> .\n"
        + "@prefix neovoc: <neo4j://myvoc#> .\n"
        + "\n"
        + "\n"
        + "<neo4j://explicit_uri#123> neovoc:name \"the name\" .";

    assertTrue(ModelTestUtils
        .compareModels(exportedAsRDF, RDFFormat.TURTLE, response.rawContent(), RDFFormat.TURTLE));

  }

  private String getExportedAsLPG( String uri ) {
    return "@prefix neovoc: <neo4j://graph.schema#> .\n"
        + "@prefix neoind: <neo4j://graph.individuals#> .\n"
        + "\n"
        + "<" + uri + "> neovoc:name \"the name\" .";
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
          RDFEndpointTest.class.getClassLoader().getResource("customDataTypes2.ttl")
              .toURI() + "','Turtle')");

      tx.commit();
    }
    Map<String, String> params = new HashMap<>();
    params.put("cypher", "MATCH (n {uri: 'http://example.org/Resource1'})" +
        "OPTIONAL MATCH (n)-[]-(m) RETURN *");

    HTTP.Response response = HTTP.withHeaders("Accept", "text/turtle").POST(
        resolveURI(neo4j.httpURI(), "neo4j/cypher"),
        params);

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

    assertEquals(200, response.status());
    assertTrue(ModelTestUtils
        .compareModels(expected, RDFFormat.TURTLE, response.rawContent(), RDFFormat.TURTLE));

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
          RDFEndpointTest.class.getClassLoader().getResource("customDataTypes.ttl")
              .toURI()
          + "','Turtle')");

      tx.commit();
    }

    Map<String, String> params = new HashMap<>();
    params.put("cypher", "MATCH (a:`http://example.com/Car`) RETURN *");

    HTTP.Response response = HTTP.withHeaders("Accept", "text/turtle").POST(
        resolveURI(neo4j.httpURI(), "neo4j/cypher"),
        params);

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

    assertEquals(200, response.status());
    assertTrue(ModelTestUtils
        .compareModels(expected, RDFFormat.TURTLE, response.rawContent(), RDFFormat.TURTLE));

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
          RDFEndpointTest.class.getClassLoader().getResource("customDataTypes.ttl")
              .toURI()
          + "','Turtle')");

      tx.commit();
    }

    Map<String, String> params = new HashMap<>();
    params.put("cypher", "MATCH (a:ns0__Car) RETURN *");

    HTTP.Response response = HTTP.withHeaders("Accept", "text/turtle").POST(
        resolveURI(neo4j.httpURI(), "neo4j/cypher"),
        params);

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

    assertEquals(200, response.status());
    assertTrue(ModelTestUtils
        .compareModels(expected, RDFFormat.TURTLE, response.rawContent(), RDFFormat.TURTLE));

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
          RDFEndpointTest.class.getClassLoader().getResource("deleteRDF/bNodes.ttl")
              .toURI()
          + "','Turtle')");
      Result res = tx.execute("CALL n10s.rdf.delete.fetch('" +
          RDFEndpointTest.class.getClassLoader().getResource("deleteRDF/bNodesDeletion.ttl")
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

    HTTP.Response response = HTTP.
        withHeaders("Accept", "text/turtle")
        .POST(
            resolveURI(neo4j.httpURI(), "neo4j/cypher"),
            params);

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

    assertEquals(200, response.status());
    assertTrue(ModelTestUtils
        .compareModels(expected, RDFFormat.TURTLE, response.rawContent(), RDFFormat.TURTLE));

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
          RDFEndpointTest.class.getClassLoader().getResource("RDFDatasets/RDFDataset.trig")
              .toURI()
          + "','TriG')");
      tx.commit();
    }

    Map<String, String> params = new HashMap<>();
    params.put("cypher", "MATCH (a:Resource) "
        + "OPTIONAL MATCH (a)-[r]->(b:Resource)"
        + "RETURN DISTINCT *");

    HTTP.Response response = HTTP.
        withHeaders("Accept", "application/trig")
        .POST(
            resolveURI(neo4j.httpURI(), "neo4j/cypher"),
            params);

    String expected = Resources
        .toString(Resources.getResource("RDFDatasets/RDFDataset.trig"),
            StandardCharsets.UTF_8);
    assertEquals(200, response.status());
    assertTrue(ModelTestUtils
        .compareModels(expected, RDFFormat.TRIG, response.rawContent(), RDFFormat.TRIG));

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
          RDFEndpointTest.class.getClassLoader().getResource("RDFDatasets/RDFDataset.nq")
              .toURI()
          + "','N-Quads')");
      tx.commit();
    }
    Map<String, String> params = new HashMap<>();
    params.put("cypher", "MATCH (a:Resource) "
        + "OPTIONAL MATCH (a)-[r]->(b)"
        + "RETURN DISTINCT *");

    HTTP.Response response = HTTP.
        withHeaders("Accept", "application/n-quads")
        .POST(
            resolveURI(neo4j.httpURI(), "neo4j/cypher"),
            params);

    String expected = Resources
        .toString(Resources.getResource("RDFDatasets/RDFDataset.nq"),
            StandardCharsets.UTF_8);
    assertEquals(200, response.status());
    assertTrue(ModelTestUtils
        .compareModels(expected, RDFFormat.NQUADS, response.rawContent(), RDFFormat.NQUADS));

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
          RDFEndpointTest.class.getClassLoader().getResource("RDFDatasets/RDFDataset.trig")
              .toURI()
          + "','TriG')");

      tx.commit();
    }

    HTTP.Response response = HTTP.withHeaders("Accept", "application/trig").GET(
        resolveURI(neo4j.httpURI(), "neo4j/describe?nodeIdentifier=") + URLEncoder
            .encode("http://www.example.org/exampleDocument#Monica",
                StandardCharsets.UTF_8.toString()));

    String expected = "{\n"
        + "  <http://www.example.org/exampleDocument#Monica> a <http://www.example.org/vocabulary#Person>;\n"
        + "    <http://www.example.org/vocabulary#friendOf> <http://www.example.org/exampleDocument#John> .\n"
        + "}";

    assertEquals(200, response.status());
    assertTrue(ModelTestUtils
        .compareModels(expected, RDFFormat.TRIG, response.rawContent(), RDFFormat.TRIG));
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
          RDFEndpointTest.class.getClassLoader().getResource("RDFDatasets/RDFDataset.trig")
              .toURI()
          + "','TriG')");

      tx.commit();
    }

    HTTP.Response response = HTTP.withHeaders("Accept", "application/trig").GET(
        resolveURI(neo4j.httpURI(), "neo4j/describe?nodeIdentifier=") + URLEncoder
            .encode("http://www.example.org/exampleDocument#Monica",
                StandardCharsets.UTF_8.toString())
            + "&graphuri=http://www.example.org/exampleDocument%23G1");

    String expected = "<http://www.example.org/exampleDocument#G1> {\n"
        + "  <http://www.example.org/exampleDocument#Monica> <http://www.example.org/vocabulary#name>\n"
        + "      \"Monica Murphy\";\n"
        + "    <http://www.example.org/vocabulary#homepage> <http://www.monicamurphy.org>;\n"
        + "    <http://www.example.org/vocabulary#knows> <http://www.example.org/exampleDocument#John>;\n"
        + "    <http://www.example.org/vocabulary#hasSkill> <http://www.example.org/vocabulary#Management>,\n"
        + "      <http://www.example.org/vocabulary#Programming>;\n"
        + "    <http://www.example.org/vocabulary#email> <mailto:monica@monicamurphy.org> .\n"
        + "}";

    assertEquals(200, response.status());
    assertTrue(ModelTestUtils
        .compareModels(expected, RDFFormat.TRIG, response.rawContent(), RDFFormat.TRIG));

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
          RDFEndpointTest.class.getClassLoader().getResource("RDFDatasets/RDFDataset.nq")
              .toURI()
          + "','N-Quads')");

      tx.commit();
    }

    HTTP.Response response = HTTP.withHeaders("Accept", "application/n-quads").GET(
        resolveURI(neo4j.httpURI(), "neo4j/describe?nodeIdentifier=") + URLEncoder
            .encode("http://www.example.org/exampleDocument#Monica",
                StandardCharsets.UTF_8.toString())
            + "&graphuri=http://www.example.org/exampleDocument%23G1");
    String expected =
        "<http://www.example.org/exampleDocument#Monica> <http://www.example.org/vocabulary#name> \"Monica Murphy\" <http://www.example.org/exampleDocument#G1> .\n"
            + "<http://www.example.org/exampleDocument#Monica> <http://www.example.org/vocabulary#homepage> <http://www.monicamurphy.org> <http://www.example.org/exampleDocument#G1> .\n"
            + "<http://www.example.org/exampleDocument#Monica> <http://www.example.org/vocabulary#knows> <http://www.example.org/exampleDocument#John> <http://www.example.org/exampleDocument#G1> .\n"
            + "<http://www.example.org/exampleDocument#Monica> <http://www.example.org/vocabulary#hasSkill> <http://www.example.org/vocabulary#Management> <http://www.example.org/exampleDocument#G1> .\n"
            + "<http://www.example.org/exampleDocument#Monica> <http://www.example.org/vocabulary#hasSkill> <http://www.example.org/vocabulary#Programming> <http://www.example.org/exampleDocument#G1> .\n"
            + "<http://www.example.org/exampleDocument#Monica> <http://www.example.org/vocabulary#email> <mailto:monica@monicamurphy.org> <http://www.example.org/exampleDocument#G1> .";

    assertEquals(200, response.status());
    assertTrue(ModelTestUtils
        .compareModels(expected, RDFFormat.NQUADS, response.rawContent(), RDFFormat.NQUADS));

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
          RDFEndpointTest.class.getClassLoader().getResource(
              "RDFDatasets/RDFDatasetBNodes.trig")
              .toURI()
          + "','TriG')");
      Result res = tx.execute("CALL n10s.experimental.quadrdf.delete.fetch('" +
          RDFEndpointTest.class.getClassLoader().getResource(
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

    HTTP.Response response = HTTP.
        withHeaders("Accept", "application/trig")
        .POST(
            resolveURI(neo4j.httpURI(), "neo4j/cypher"),
            params);

    String expected = Resources
        .toString(Resources.getResource("RDFDatasets/RDFDatasetBNodesPostDeletion.trig"),
            StandardCharsets.UTF_8);
    assertEquals(200, response.status());
    assertTrue(ModelTestUtils
        .compareModels(expected, RDFFormat.TRIG, response.rawContent(), RDFFormat.TRIG));

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
              RDFEndpointTest.class.getClassLoader().getResource("data13061.trig")
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

    Response response = HTTP.withHeaders("Accept", "text/plain").POST(
            resolveURI(neo4j.httpURI(), "neo4j/cypher"), map);

    assertEquals(200, response.status());
    String expected = "<http://data.elsevier.com/vocabulary/OmniScience> <neo4j://graph.schema#topConcepts> \"5\"^^<http://www.w3.org/2001/XMLSchema#long> .\n" +
            "<http://data.elsevier.com/vocabulary/OmniScience> <neo4j://graph.schema#topConcepts> \"3\"^^<http://www.w3.org/2001/XMLSchema#long> .\n" +
            "<http://data.elsevier.com/vocabulary/OmniScience> <neo4j://graph.schema#topConcepts> \"0\"^^<http://www.w3.org/2001/XMLSchema#long> .\n" +
            "<http://data.elsevier.com/vocabulary/OmniScience> <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <neo4j://graph.schema#ConceptScheme> .\n";
    assertTrue(ModelTestUtils
            .compareModels(expected, RDFFormat.NTRIPLES, response.rawContent(), RDFFormat.TURTLE));
  }

}
