package semantics.extension;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.type.CollectionType;
import com.fasterxml.jackson.databind.type.TypeFactory;
import org.eclipse.rdf4j.rio.RDFFormat;
import org.junit.Rule;
import org.junit.Test;
import org.neo4j.driver.*;
import org.neo4j.driver.types.Node;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Result;
import org.neo4j.graphdb.Transaction;
import org.neo4j.harness.junit.rule.Neo4jRule;
import org.neo4j.test.server.HTTP;
import semantics.graphconfig.GraphConfigProcedures;
import semantics.ModelTestUtils;
import semantics.RDFImport;
import semantics.mapping.MappingUtils;
import static org.neo4j.internal.helpers.collection.Iterators.count;
import com.google.common.io.Resources;

import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import semantics.nsprefixes.NsPrefixDefProcedures;

import static org.junit.Assert.*;
import static semantics.graphconfig.Params.PREFIX_SEPARATOR;

/**
 * Created by jbarrasa on 14/09/2016.
 */
public class RDFEndpointTest {

  @Rule
  public Neo4jRule neo4j = new Neo4jRule().withUnmanagedExtension("/rdf", RDFEndpoint.class)
      .withProcedure(RDFImport.class).withFunction(RDFImport.class)
      .withProcedure(MappingUtils.class).withProcedure(GraphConfigProcedures.class)
      .withProcedure(NsPrefixDefProcedures.class);

  @Rule
  public Neo4jRule temp = new Neo4jRule().withProcedure(RDFImport.class).withProcedure(GraphConfigProcedures.class);

  private static final ObjectMapper jsonMapper = new ObjectMapper();

  private static final CollectionType collectionType = TypeFactory
      .defaultInstance().constructCollectionType(Set.class, Map.class);
  private String emptyJsonLd = "{\n"
      + "  \"@context\" : {\n"
      + "    \"rdf\" : \"http://www.w3.org/1999/02/22-rdf-syntax-ns#\",\n"
      + "    \"neovoc\" : \"neo4j://vocabulary#\",\n"
      + "    \"neoind\" : \"neo4j://individuals#\"\n"
      + "  }\n"
      + "}";

  @Test
  public void testGetNodeById() throws Exception {
    // Given
    final GraphDatabaseService graphDatabaseService = neo4j.defaultDatabaseService();
    try(Transaction tx =  graphDatabaseService.beginTx()) {

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
    try(Transaction tx =  graphDatabaseService.beginTx()) {
      Result result = tx.execute("MATCH (n:Critic) RETURN id(n) AS id ");
      id = (Long) result.next().get("id");
      assertEquals(Long.valueOf(7), id);
    }

      // When
      HTTP.Response response = HTTP.withHeaders("Accept", "application/ld+json").GET(
          HTTP.GET(neo4j.httpURI().resolve("rdf").toString()).location() + "n10s/neo4j/describe/"
              + id.toString());

      String expected = "[ {\n"
          + "  \"@id\" : \"neo4j://individuals#5\",\n"
          + "  \"neo4j://vocabulary#FRIEND_OF\" : [ {\n"
          + "    \"@id\" : \"neo4j://individuals#7\"\n"
          + "  } ]\n"
          + "}, {\n"
          + "  \"@id\" : \"neo4j://individuals#7\",\n"
          + "  \"@type\" : [ \"neo4j://vocabulary#Critic\" ],\n"
          + "  \"neo4j://vocabulary#WORKS_WITH\" : [ {\n"
          + "    \"@id\" : \"neo4j://individuals#8\"\n"
          + "  } ],\n"
          + "  \"neo4j://vocabulary#born\" : [ {\n"
          + "    \"@type\" : \"http://www.w3.org/2001/XMLSchema#long\",\n"
          + "    \"@value\" : \"1960\"\n"
          + "  } ],\n"
          + "  \"neo4j://vocabulary#name\" : [ {\n"
          + "    \"@value\" : \"Hugo Weaving\"\n"
          + "  } ]\n"
          + "} ]";
      assertEquals(200, response.status());
      assertTrue(ModelTestUtils
          .compareModels(expected, RDFFormat.JSONLD, response.rawContent(), RDFFormat.JSONLD));

    }

  @Test
  public void ImportGetNodeById() throws Exception {
    // Given
    final GraphDatabaseService graphDatabaseService = neo4j.defaultDatabaseService();

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
      id = (Long)result.next().get("id");
      assertEquals(7, id);
    }

      try (Driver driver = GraphDatabase.driver(temp.boltURI(),
              Config.builder().withoutEncryption().build()); Session session = driver.session()) {
        session.run("CREATE CONSTRAINT n10s_unique_uri "
         + "ON (r:Resource) ASSERT r.uri IS UNIQUE");
        session.run("CALL semantics.setGraphConfig( { handleVocabUris: 'IGNORE', typesToLabels: true } )");
        org.neo4j.driver.Result importResults
            = session.run("CALL semantics.importRDF('" +
            HTTP.GET(neo4j.httpURI().resolve("rdf").toString()).location() + "n10s/neo4j/describe/"
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
          assertEquals("neo4j://individuals#" + criticPreImport.get("id") ,criticPostImport.get("uri").asString());
        }
      }
  }


    @Test
    public void ImportGetNodeByIdOnImportedOnto() throws Exception {
        // Given
        final GraphDatabaseService graphDatabaseService = neo4j.defaultDatabaseService();

        //first import onto
        try (Transaction tx = graphDatabaseService.beginTx()) {
            tx.execute("CREATE CONSTRAINT n10s_unique_uri "
         + "ON (r:Resource) ASSERT r.uri IS UNIQUE");
            tx.commit();
        }
        try (Transaction tx = graphDatabaseService.beginTx()) {
            tx.execute("CALL semantics.setGraphConfig({})");
            tx.execute("CALL semantics.importOntology('" +
                    RDFEndpointTest.class.getClassLoader().getResource("onto1.owl")
                            .toURI() + "','RDF/XML',{})");

            tx.commit();
        }
        //  check data is  correctly loaded
        Long  id;
        try (Transaction tx = graphDatabaseService.beginTx()) {
            Result result = tx.execute("match (n:Class " +
                    "{ uri: \"http://n4j.com/tst1/ontologies/2017/4/Cyber_EA_Smart_City#RF_signal_strength\"})" +
                    "-[r]-(o) " +
                    "return n.name as name, id(n) as id, type(r) as reltype, o.uri as otheruri");
            Map<String, Object> next = result.next();
            assertEquals("RF_signal_strength", next.get("name"));
            assertEquals("SCO", next.get("reltype"));
            assertEquals("http://n4j.com/tst1/ontologies/2017/4/Cyber_EA_Smart_City#Vehicle_Key", next.get("otheruri"));

            id = (Long)next.get("id");
        }

        try (Transaction tx = graphDatabaseService.beginTx()) {
          tx.execute("MATCH (gc:_GraphConfig ) DELETE gc; ");
          tx.commit();
        }
        // then export elements and check the output is right
        HTTP.Response response = HTTP.withHeaders("Accept", "text/turtle").GET(
                HTTP.GET(neo4j.httpURI().resolve("rdf").toString()).location()
                        + "n10s/neo4j/describe/" + id);

        String expected = "@prefix rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#> .\n" +
                "@prefix neovoc: <neo4j://vocabulary#> .\n" +
                "@prefix neoind: <neo4j://individuals#> .\n" +
                "neovoc:RF_signal_strength a <http://www.w3.org/2000/01/rdf-schema#Class>, neovoc:Resource;\n" +
                "  <http://www.w3.org/2000/01/rdf-schema#subClassOf> neovoc:Vehicle_Key;\n" +
                "  <neo4j://neo4j.org/rdfs/1#name> \"RF_signal_strength\";\n" +
                "  neovoc:uri \"http://n4j.com/tst1/ontologies/2017/4/Cyber_EA_Smart_City#RF_signal_strength\" .\n";

        assertEquals(200, response.status());
        assertTrue(ModelTestUtils
                .compareModels(expected, RDFFormat.TURTLE, response.rawContent(), RDFFormat.TURTLE));


    }


  @Test
  public void ImportGetCypher() throws Exception {

    final GraphDatabaseService graphDatabaseService = neo4j.defaultDatabaseService();
    final GraphDatabaseService tempGDBs = temp.defaultDatabaseService();

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
      Result result = tx.execute("MATCH (n:Critic) RETURN n.born AS born, n.name AS name , n.uri as uri, id(n) as id");
      preImport = result.next();
      assertEquals(1960L, preImport.get("born"));
      assertEquals(7L, preImport.get("id"));
      assertEquals("Hugo Weaving", preImport.get("name"));
      //no uri pre-import
      assertNull(preImport.get("uri"));
    }

    try (Driver driver = GraphDatabase.driver(temp.boltURI(),
              Config.builder().withoutEncryption().build()); Session session = driver.session()) {
        session.run("CREATE CONSTRAINT n10s_unique_uri "
         + "ON (r:Resource) ASSERT r.uri IS UNIQUE");
        session.run("CALL semantics.setGraphConfig( { handleVocabUris: 'IGNORE' })");
        org.neo4j.driver.Result importResults
            = session.run("CALL semantics.importRDF('" +
            HTTP.GET(neo4j.httpURI().resolve("rdf").toString()).location() + "n10s/neo4j/cypher" +
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
        assertEquals("neo4j://individuals#" + preImport.get("id") ,criticPostImport.get("uri").asString());
      }

    }

  @Test
  public void testFindNodeByLabelAndProperty() throws Exception {

    final GraphDatabaseService graphDatabaseService = neo4j.defaultDatabaseService();

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

    try (Transaction tx = graphDatabaseService.beginTx()) {
      Result result = tx.execute("MATCH (n:Critic) RETURN id(n) AS id ");
      assertEquals(7L, result.next().get("id"));
    }
      // When
      HTTP.Response response = HTTP.withHeaders("Accept", "application/ld+json").GET(
          HTTP.GET(neo4j.httpURI().resolve("rdf").toString()).location()
              + "n10s/neo4j/describe/find/Director/born/1961?valType=INTEGER");

      String expected = "[ {\n"
          + "  \"@id\" : \"neo4j://individuals#6\",\n"
          + "  \"@type\" : [ \"neo4j://vocabulary#Director\" ],\n"
          + "  \"neo4j://vocabulary#born\" : [ {\n"
          + "    \"@type\" : \"http://www.w3.org/2001/XMLSchema#long\",\n"
          + "    \"@value\" : \"1961\"\n"
          + "  } ],\n"
          + "  \"neo4j://vocabulary#name\" : [ {\n"
          + "    \"@value\" : \"Laurence Fishburne\"\n"
          + "  } ]\n"
          + "} ]";

      assertEquals(200, response.status());
      assertTrue(ModelTestUtils
          .compareModels(expected, RDFFormat.JSONLD, response.rawContent(), RDFFormat.JSONLD));

      // When
      response = HTTP.withHeaders("Accept", "application/ld+json").GET(
          HTTP.GET(neo4j.httpURI().resolve("rdf").toString()).location()
              + "n10s/neo4j/describe/find/Director/name/Laurence%20Fishburne");

      expected = "[ {\n"
          + "  \"@id\" : \"neo4j://individuals#6\",\n"
          + "  \"@type\" : [ \"neo4j://vocabulary#Director\" ],\n"
          + "  \"neo4j://vocabulary#born\" : [ {\n"
          + "    \"@type\" : \"http://www.w3.org/2001/XMLSchema#long\",\n"
          + "    \"@value\" : \"1961\"\n"
          + "  } ],\n"
          + "  \"neo4j://vocabulary#name\" : [ {\n"
          + "    \"@value\" : \"Laurence Fishburne\"\n"
          + "  } ]\n"
          + "} ]";
      assertEquals(200, response.status());
      assertTrue(ModelTestUtils
          .compareModels(expected, RDFFormat.JSONLD, response.rawContent(), RDFFormat.JSONLD));

      // When
      response = HTTP.withHeaders("Accept", "application/ld+json").GET(
          HTTP.GET(neo4j.httpURI().resolve("rdf").toString()).location()
              + "n10s/neo4j/describe/find/Actor/born/1964?valType=INTEGER");

      expected = "[ {\n"
          + "  \"@id\" : \"neo4j://individuals#4\",\n"
          + "  \"@type\" : [ \"neo4j://vocabulary#Actor\" ],\n"
          + "  \"neo4j://vocabulary#born\" : [ {\n"
          + "    \"@type\" : \"http://www.w3.org/2001/XMLSchema#long\",\n"
          + "    \"@value\" : \"1964\"\n"
          + "  } ],\n"
          + "  \"neo4j://vocabulary#name\" : [ {\n"
          + "    \"@value\" : \"Keanu Reeves\"\n"
          + "  } ]\n"
          + "}, {\n"
          + "  \"@id\" : \"neo4j://individuals#7\",\n"
          + "  \"neo4j://vocabulary#WORKS_WITH\" : [ {\n"
          + "    \"@id\" : \"neo4j://individuals#8\"\n"
          + "  } ]\n"
          + "}, {\n"
          + "  \"@id\" : \"neo4j://individuals#8\",\n"
          + "  \"@type\" : [ \"neo4j://vocabulary#Actor\" ],\n"
          + "  \"neo4j://vocabulary#born\" : [ {\n"
          + "    \"@type\" : \"http://www.w3.org/2001/XMLSchema#long\",\n"
          + "    \"@value\" : \"1964\"\n"
          + "  } ],\n"
          + "  \"neo4j://vocabulary#name\" : [ {\n"
          + "    \"@value\" : \"Andy Wachowski\"\n"
          + "  } ]\n"
          + "} ]";
      assertEquals(200, response.status());
      assertTrue(ModelTestUtils
          .compareModels(expected, RDFFormat.JSONLD, response.rawContent(), RDFFormat.JSONLD));
    }


  @Test
  public void testGetNodeByIdNotFoundOrInvalid() throws Exception {
    // Given

      HTTP.Response response = HTTP.withHeaders("Accept", "application/ld+json").GET(
          HTTP.GET(neo4j.httpURI().resolve("rdf").toString()).location()
              + "n10s/neo4j/describe/9999999");

      assertEquals("[ ]", response.rawContent());
      assertEquals(200, response.status());

      //TODO: Non Long param for ID (would be a good idea to be consistent with previous case?...)
      response = HTTP.withHeaders("Accept", "application/ld+json").GET(
          HTTP.GET(neo4j.httpURI().resolve("rdf").toString()).location()
              + "n10s/neo4j/describe/adb");

      assertEquals("[ ]", response.rawContent());
      assertEquals(200, response.status());
  }

  @Test
  public void testFindNodeByLabelAndPropertyNotFoundOrInvalid() throws Exception {
      HTTP.Response response = HTTP.withHeaders("Accept", "application/ld+json").GET(
          HTTP.GET(neo4j.httpURI().resolve("rdf").toString()).location()
              + "n10s/neo4j/describe/find/WrongLabel/wrongProperty/someValue");

      assertEquals(emptyJsonLd, response.rawContent());
      assertEquals(200, response.status());

      response = HTTP.withHeaders("Accept", "application/ld+json").GET(
          HTTP.GET(neo4j.httpURI().resolve("rdf").toString()).location()
              + "n10s/neo4j/describe/find/Something");

      assertEquals("", response.rawContent());
      assertEquals(404, response.status());

  }

  @Test
  public void testGetNodeByUriNotFoundOrInvalid() throws Exception {

    try (Transaction tx = neo4j.defaultDatabaseService().beginTx()) {
      tx.execute("CALL semantics.setGraphConfig()");
      tx.commit();
    }
    HTTP.Response response = HTTP.withHeaders("Accept", "application/ld+json").GET(
        HTTP.GET(neo4j.httpURI().resolve("rdf").toString()).location()
            + "n10s/neo4j/describe/9999999");
    assertEquals(emptyJsonLd, response.rawContent());
    assertEquals(200, response.status());

  }

  @Test
  public void testPing() throws Exception {
    // Given
      HTTP.Response response = HTTP.GET(
          HTTP.GET(neo4j.httpURI().resolve("rdf").toString()).location() + "n10s/ping");

      assertEquals("{\"ping\":\"here!\"}", response.rawContent());
      assertEquals(200, response.status());

  }

  @Test
  public void testCypherOnLPG() throws Exception {

      final GraphDatabaseService graphDatabaseService = neo4j.defaultDatabaseService();

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

      try (Transaction tx = graphDatabaseService.beginTx()) {

          Result result = tx.execute("MATCH (n:Critic) RETURN id(n) AS id ");
          assertEquals(1, count(result));
      }

      Map<String, Object> map = new HashMap<>();
      map.put("cypher", "MATCH (n:Category)--(m:Category) RETURN n,m LIMIT 4");

      HTTP.Response response = HTTP.withHeaders("Accept", "text/plain").POST(
          HTTP.GET(neo4j.httpURI().resolve("rdf").toString()).location() + "n10s/neo4j/cypher", map);

      String expected =
          "<neo4j://individuals#3> <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <neo4j://vocabulary#Category> .\n"
              + "<neo4j://individuals#3> <neo4j://vocabulary#catName> \"Critic\" .\n"
              + "<neo4j://individuals#0> <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <neo4j://vocabulary#Category> .\n"
              + "<neo4j://individuals#0> <neo4j://vocabulary#catName> \"Person\" .\n"
              + "<neo4j://individuals#2> <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <neo4j://vocabulary#Category> .\n"
              + "<neo4j://individuals#2> <neo4j://vocabulary#catName> \"Director\" .\n"
              + "<neo4j://individuals#1> <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <neo4j://vocabulary#Category> .\n"
              + "<neo4j://individuals#1> <neo4j://vocabulary#catName> \"Actor\" .\n";

      assertEquals(200, response.status());
      assertTrue(ModelTestUtils
          .compareModels(expected, RDFFormat.NTRIPLES, response.rawContent(), RDFFormat.NTRIPLES));

      // request passing serialisation format as request param
      map.put("format", "RDF/XML");
      response = HTTP.withHeaders("Accept", "text/plain").POST(
          HTTP.GET(neo4j.httpURI().resolve("rdf").toString()).location() + "n10s/neo4j/cypher", map);

      assertEquals(200, response.status());
      assertTrue(ModelTestUtils
          .compareModels(expected, RDFFormat.NTRIPLES, response.rawContent(), RDFFormat.RDFXML));

      map.put("mappedElemsOnly", "true");
      map.remove("format");
      response = HTTP.withHeaders("Accept", "text/plain").POST(
          HTTP.GET(neo4j.httpURI().resolve("rdf").toString()).location() + "n10s/neo4j/cypher", map);

      assertEquals(200, response.status());
      assertEquals("", response.rawContent());

  }

  @Test
  public void testCypherOnLPGMappingsAndQueryParams() throws Exception {

      final GraphDatabaseService graphDatabaseService = neo4j.defaultDatabaseService();
          try (Transaction tx = graphDatabaseService.beginTx()) {

            String dataInsertion = "CREATE (Keanu:Actor {name:'Keanu Reeves', born:1964})\n" +
                "CREATE (Carrie:Director {name:'Carrie-Anne Moss', born:1967})\n" +
                "CREATE (Laurence:Director {name:'Laurence Fishburne', born:1961})\n" +
                "CREATE (Hugo:Critic {name:'Hugo Weaving', born:1960})\n" +
                "CREATE (AndyW:Actor {name:'Andy Wachowski', born:1967}) "
                + "CREATE (Keanu)-[:ACTED_IN]->(:Movie {title: 'The Matrix'})";
            tx.execute(dataInsertion);

            String mappingCreation =
                "CALL semantics.mapping.addSchema('http://schema.org/','sch') YIELD namespace  "
                    + "CALL semantics.mapping.addMappingToSchema('http://schema.org/','Actor','Person') YIELD elemName AS en1  "
                    + "CALL semantics.mapping.addMappingToSchema('http://schema.org/','born','dob') YIELD elemName AS en2 "
                    + "CALL semantics.mapping.addMappingToSchema('http://schema.org/','name','familyName') YIELD elemName AS en3 "
                    + "CALL semantics.mapping.addMappingToSchema('http://schema.org/','ACTED_IN','inMovie') YIELD elemName AS en4 "
                    + "RETURN 'OK'";
            tx.execute(mappingCreation);
            tx.commit();
          }


      try(Transaction tx =  graphDatabaseService.beginTx()) {
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
          HTTP.GET(neo4j.httpURI().resolve("rdf").toString()).location() + "n10s/neo4j/cypher", map);

      String expected =
          "<neo4j://individuals#5> <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <neo4j://vocabulary#Movie> .\n"
              + "<neo4j://individuals#5> <neo4j://vocabulary#title> \"The Matrix\" .\n"
              + "<neo4j://individuals#0> <http://schema.org/inMovie> <neo4j://individuals#5> .\n"
              + "<neo4j://individuals#0> <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://schema.org/Person> .\n"
              + "<neo4j://individuals#0> <http://schema.org/dob> \"1964\"^^<http://www.w3.org/2001/XMLSchema#long> .\n"
              + "<neo4j://individuals#0> <http://schema.org/familyName> \"Keanu Reeves\" .\n";

      assertEquals(200, response.status());
      assertTrue(ModelTestUtils
          .compareModels(expected, RDFFormat.NTRIPLES, response.rawContent(), RDFFormat.NTRIPLES));

      map.put("mappedElemsOnly", "true");
      response = HTTP.withHeaders("Accept", "text/plain").POST(
          HTTP.GET(neo4j.httpURI().resolve("rdf").toString()).location() + "n10s/neo4j/cypher", map);

      String expectedOnlyMapped =
          "<neo4j://individuals#0> <http://schema.org/inMovie> <neo4j://individuals#5> .\n"
              + "<neo4j://individuals#0> <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://schema.org/Person> .\n"
              + "<neo4j://individuals#0> <http://schema.org/dob> \"1964\"^^<http://www.w3.org/2001/XMLSchema#long> .\n"
              + "<neo4j://individuals#0> <http://schema.org/familyName> \"Keanu Reeves\" .\n";

      assertEquals(200, response.status());
      assertTrue(ModelTestUtils
          .compareModels(expectedOnlyMapped, RDFFormat.NTRIPLES, response.rawContent(),
              RDFFormat.NTRIPLES));


  }

  @Test
  public void testontoOnLPG() throws Exception {
      final GraphDatabaseService graphDatabaseService = neo4j.defaultDatabaseService();
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
          HTTP.GET(neo4j.httpURI().resolve("rdf").toString()).location() + "n10s/neo4j/onto");

      String expected =
          "<neo4j://vocabulary#Movie> <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://www.w3.org/2002/07/owl#Class> .\n"
              + "<neo4j://vocabulary#Movie> <http://www.w3.org/2000/01/rdf-schema#label> \"Movie\" .\n"
              + "<neo4j://vocabulary#Actor> <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://www.w3.org/2002/07/owl#Class> .\n"
              + "<neo4j://vocabulary#Actor> <http://www.w3.org/2000/01/rdf-schema#label> \"Actor\" .\n"
              + "<neo4j://vocabulary#Director> <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://www.w3.org/2002/07/owl#Class> .\n"
              + "<neo4j://vocabulary#Director> <http://www.w3.org/2000/01/rdf-schema#label> \"Director\" .\n"
              + "<neo4j://vocabulary#Critic> <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://www.w3.org/2002/07/owl#Class> .\n"
              + "<neo4j://vocabulary#Critic> <http://www.w3.org/2000/01/rdf-schema#label> \"Critic\" .\n"
              + "<neo4j://vocabulary#ACTED_IN> <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://www.w3.org/2002/07/owl#ObjectProperty> .\n"
              + "<neo4j://vocabulary#ACTED_IN> <http://www.w3.org/2000/01/rdf-schema#label> \"ACTED_IN\" .\n"
              + "<neo4j://vocabulary#ACTED_IN> <http://www.w3.org/2000/01/rdf-schema#domain> <neo4j://vocabulary#Actor> .\n"
              + "<neo4j://vocabulary#ACTED_IN> <http://www.w3.org/2000/01/rdf-schema#range> <neo4j://vocabulary#Movie> .\n"
              + "<neo4j://vocabulary#RATED> <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://www.w3.org/2002/07/owl#ObjectProperty> .\n"
              + "<neo4j://vocabulary#RATED> <http://www.w3.org/2000/01/rdf-schema#label> \"RATED\" .\n"
              + "<neo4j://vocabulary#RATED> <http://www.w3.org/2000/01/rdf-schema#domain> <neo4j://vocabulary#Critic> .\n"
              + "<neo4j://vocabulary#RATED> <http://www.w3.org/2000/01/rdf-schema#range> <neo4j://vocabulary#Movie> .\n"
              + "<neo4j://vocabulary#DIRECTED> <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://www.w3.org/2002/07/owl#ObjectProperty> .\n"
              + "<neo4j://vocabulary#DIRECTED> <http://www.w3.org/2000/01/rdf-schema#label> \"DIRECTED\" .\n"
              + "<neo4j://vocabulary#DIRECTED> <http://www.w3.org/2000/01/rdf-schema#domain> <neo4j://vocabulary#Director> .\n"
              + "<neo4j://vocabulary#DIRECTED> <http://www.w3.org/2000/01/rdf-schema#range> <neo4j://vocabulary#Movie> .\n";
      assertEquals(200, response.status());
      assertTrue(ModelTestUtils
          .compareModels(expected, RDFFormat.NTRIPLES, response.rawContent(), RDFFormat.NTRIPLES));

  }

  @Test
  public void testontoOnRDF() throws Exception {
      final GraphDatabaseService graphDatabaseService = neo4j.defaultDatabaseService();

    try (Transaction tx = graphDatabaseService.beginTx()) {
            tx.execute("CALL semantics.setGraphConfig()");
            tx.execute("call semantics.nsprefixes.add('ns1','http://ont.thomsonreuters.com/mdaas/')");
            tx.execute("call semantics.nsprefixes.add('ns0','http://permid.org/ontology/organization/')");

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
          HTTP.GET(neo4j.httpURI().resolve("rdf").toString()).location() + "n10s/neo4j/onto");

      String expected =
          "<http://permid.org/ontology/organization/Director> <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://www.w3.org/2002/07/owl#Class> .\n"
              +
              "<http://permid.org/ontology/organization/Director> <http://www.w3.org/2000/01/rdf-schema#label> \"Director\" .\n"
              +
              "<http://permid.org/ontology/organization/Actor> <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://www.w3.org/2002/07/owl#Class> .\n"
              +
              "<http://permid.org/ontology/organization/Actor> <http://www.w3.org/2000/01/rdf-schema#label> \"Actor\" .\n"
              +
              "<http://permid.org/ontology/organization/Critic> <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://www.w3.org/2002/07/owl#Class> .\n"
              +
              "<http://permid.org/ontology/organization/Critic> <http://www.w3.org/2000/01/rdf-schema#label> \"Critic\" .\n"
              +
              "<http://permid.org/ontology/organization/Likes> <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://www.w3.org/2002/07/owl#ObjectProperty> .\n"
              +
              "<http://permid.org/ontology/organization/Likes> <http://www.w3.org/2000/01/rdf-schema#label> \"Likes\" .\n"
              +
              "<http://permid.org/ontology/organization/Likes> <http://www.w3.org/2000/01/rdf-schema#range> <http://permid.org/ontology/organization/Director> .\n"
              +
              "<http://permid.org/ontology/organization/Likes> <http://www.w3.org/2000/01/rdf-schema#domain> <http://permid.org/ontology/organization/Actor> .\n"
              +
              "<http://permid.org/ontology/organization/FriendOf> <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://www.w3.org/2002/07/owl#ObjectProperty> .\n"
              +
              "<http://permid.org/ontology/organization/FriendOf> <http://www.w3.org/2000/01/rdf-schema#label> \"FriendOf\" .\n"
              +
              "<http://permid.org/ontology/organization/FriendOf> <http://www.w3.org/2000/01/rdf-schema#domain> <http://permid.org/ontology/organization/Critic> .\n"
              +
              "<http://permid.org/ontology/organization/FriendOf> <http://www.w3.org/2000/01/rdf-schema#range> <http://permid.org/ontology/organization/Actor> .";
      assertEquals(200, response.status());
      assertTrue(ModelTestUtils
          .compareModels(expected, RDFFormat.NTRIPLES, response.rawContent(), RDFFormat.NTRIPLES));

  }


  @Test
  public void testNodeByUri() throws Exception {
      final GraphDatabaseService graphDatabaseService = neo4j.defaultDatabaseService();
    try (Transaction tx = graphDatabaseService.beginTx()) {
            tx.execute("CALL semantics.setGraphConfig()");
            tx.execute("call semantics.nsprefixes.add('ns1','http://ont.thomsonreuters.com/mdaas/')");
            tx.execute("call semantics.nsprefixes.add('ns0','http://permid.org/ontology/organization/')");
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
          Result result = tx.execute("MATCH (n:ns0" + PREFIX_SEPARATOR + "Critic) RETURN n.uri AS uri ");
          //assertEquals( 1, count( result ) );

          assertEquals("https://permid.org/1-21523433753",result.next().get("uri"));
      }
      HTTP.Response response = HTTP.withHeaders("Accept", "text/turtle").GET(
          HTTP.GET(neo4j.httpURI().resolve("rdf").toString()).location()
              + "n10s/neo4j/describe/" + URLEncoder
              .encode("https://permid.org/1-21523433750", StandardCharsets.UTF_8.toString()));
      String expected = "@prefix neovoc: <neo4j://vocabulary#> .\n" +
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
      final GraphDatabaseService graphDatabaseService = neo4j.defaultDatabaseService();
      try (Transaction tx = graphDatabaseService.beginTx()) {
            tx.execute("CREATE CONSTRAINT n10s_unique_uri "
         + "ON (r:Resource) ASSERT r.uri IS UNIQUE");
            tx.commit();
      }
    try (Transaction tx = graphDatabaseService.beginTx()) {
      tx.execute("CALL semantics.setGraphConfig({})");
      tx.execute("CALL semantics.importRDF('" +
                RDFEndpointTest.class.getClassLoader().getResource("fibo-fragment.rdf")
                    .toURI() + "','RDF/XML')");

            tx.commit();
          }


      HTTP.Response response = HTTP.withHeaders("Accept", "application/rdf+xml").GET(
          HTTP.GET(neo4j.httpURI().resolve("rdf").toString()).location()
              + "n10s/neo4j/describe/" + URLEncoder.encode(
              "https://spec.edmcouncil.org/fibo/ontology/BE/Corporations/Corporations/BoardAgreement",
              StandardCharsets.UTF_8.toString())
              + "?excludeContext=true");

      String expected = "<?xml version=\"1.0\" encoding=\"UTF-8\"?>" +
          "<rdf:RDF\txmlns:neovoc=\"neo4j://vocabulary#\"" +
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
          HTTP.GET(neo4j.httpURI().resolve("rdf").toString()).location() + "n10s/neo4j/describe/"
              + URLEncoder.encode("http://www.w3.org/2004/02/skos/core#TestyMcTestFace", "UTF-8")
      );

      expected = "<https://spec.edmcouncil.org/fibo/ontology/BE/Corporations/Corporations/> <http://www.omg.org/techprocess/ab/SpecificationMetadata/linkToResourceAddedForTestingPurposesByJB> <http://www.w3.org/2004/02/skos/core#TestyMcTestFace> .";
      assertTrue(ModelTestUtils
          .compareModels(expected, RDFFormat.NTRIPLES, response.rawContent(), RDFFormat.NTRIPLES));
      assertEquals(200, response.status());
  }


  @Test
  public void testNodeByUriMissingNamespaceDefinition() throws Exception {

      final GraphDatabaseService graphDatabaseService = neo4j.defaultDatabaseService();
      try (Transaction tx = graphDatabaseService.beginTx()) {
            tx.execute("CREATE CONSTRAINT n10s_unique_uri "
         + "ON (r:Resource) ASSERT r.uri IS UNIQUE");
            tx.commit();
          }

          try (Transaction tx = graphDatabaseService.beginTx()) {
            tx.execute("CALL semantics.setGraphConfig({})");
            //set a prefix that we can remove afterwards
            tx.execute(
                "call semantics.nsprefixes.add('fiboanno','https://spec.edmcouncil.org/fibo/ontology/FND/Utilities/AnnotationVocabulary/')");

            tx.execute("CALL semantics.importRDF('" +
                RDFEndpointTest.class.getClassLoader().getResource("fibo-fragment.rdf")
                    .toURI() + "','RDF/XML',{})");
            tx.commit();
          }

          try (Transaction tx = graphDatabaseService.beginTx()){
            //we try (and fail) to remove the namespace
            tx.execute("call semantics.nsprefixes.remove('fiboanno')");
            tx.execute("call semantics.nsprefixes.list()").next();
            assertTrue(false);
            tx.commit();
          } catch (Exception e) {
            //expected
            assertTrue(true);
          }

          try (Transaction tx = graphDatabaseService.beginTx()) {
            //now we force delete it from the node
            tx.execute("match (n:NamespacePrefixDefinition) remove n.fiboanno ");
            tx.commit();
          }

      HTTP.Response response = HTTP.withHeaders("Accept", "application/rdf+xml").GET(
          HTTP.GET(neo4j.httpURI().resolve("rdf").toString()).location()
              + "n10s/neo4j/describe/" + URLEncoder
              .encode("https://spec.edmcouncil.org/fibo/ontology/BE/Corporations/Corporations/",
                  StandardCharsets.UTF_8.toString()));

      assertEquals(200, response.status());
      String expected = "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n"
          + "<rdf:RDF\n"
          + "\txmlns:rdf=\"http://www.w3.org/1999/02/22-rdf-syntax-ns#\">\n"
          + "<!-- RDF Serialization ERROR: Prefix fiboanno in use but not defined in the 'NamespacePrefixDefinition' node -->\n"
          + "\n"
          + "</rdf:RDF>";
      assertTrue(ModelTestUtils
          .compareModels(expected, RDFFormat.RDFXML, response.rawContent(), RDFFormat.RDFXML));

      assertTrue(response.rawContent().contains("RDF Serialization ERROR: Prefix fiboanno in use "
          + "but not defined in the 'NamespacePrefixDefinition' node"));

  }

  @Test
  public void testNodeByUriAfterImportWithMultilang() throws Exception {
      final GraphDatabaseService graphDatabaseService = neo4j.defaultDatabaseService();
    try (Transaction tx = graphDatabaseService.beginTx()) {
            tx.execute("CREATE CONSTRAINT n10s_unique_uri "
         + "ON (r:Resource) ASSERT r.uri IS UNIQUE");
            tx.commit();
    }
    try (Transaction tx = graphDatabaseService.beginTx()) {
      tx.execute("CALL semantics.setGraphConfig( { keepLangTag : true, handleMultival: 'ARRAY'} )");
      tx.execute("CALL semantics.importRDF('" +
                RDFEndpointTest.class.getClassLoader().getResource("multilang.ttl")
                    .toURI() + "','Turtle')");

            tx.commit();
          }

      HTTP.Response response = HTTP.withHeaders("Accept", "text/turtle").GET(
          HTTP.GET(neo4j.httpURI().resolve("rdf").toString()).location()
              + "n10s/neo4j/describe/" + URLEncoder
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
      final GraphDatabaseService graphDatabaseService = neo4j.defaultDatabaseService();
    try (Transaction tx = graphDatabaseService.beginTx()) {
            tx.execute("CALL semantics.setGraphConfig()");
            tx.execute("call semantics.nsprefixes.add('ns1','http://ont.thomsonreuters.com/mdaas/')");
            tx.execute("call semantics.nsprefixes.add('ns0','http://permid.org/ontology/organization/')");


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
          HTTP.GET(neo4j.httpURI().resolve("rdf").toString()).location() + "n10s/neo4j/cypher", params);

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
      final GraphDatabaseService graphDatabaseService = neo4j.defaultDatabaseService();

    try (Transaction tx = graphDatabaseService.beginTx()) {
      tx.execute("CALL semantics.setGraphConfig()");
      tx.execute("CALL semantics.nsprefixes.add('ns1', 'http://ont.thomsonreuters.com/mdaas/')");
      tx.execute("CALL semantics.nsprefixes.add('ns0', 'http://permid.org/ontology/organization/')");
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
          HTTP.GET(neo4j.httpURI().resolve("rdf").toString()).location() + "n10s/neo4j/cypher", params);

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
    final GraphDatabaseService graphDatabaseService = neo4j.defaultDatabaseService();
    try (Transaction tx = graphDatabaseService.beginTx()) {
            tx.execute("CALL semantics.setGraphConfig()");
            tx.execute("call semantics.nsprefixes.add('ns0','http://permid.org/ontology/organization/')");
            tx.execute("call semantics.nsprefixes.add('ns1','http://ont.thomsonreuters.com/mdaas/')");
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
          HTTP.GET(neo4j.httpURI().resolve("rdf").toString()).location() + "n10s/neo4j/cypher", params);

      String expected = "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n" +
          "<rdf:RDF\n" +
          "\txmlns:neovoc=\"neo4j://vocabulary#\"\n" +
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
      final GraphDatabaseService graphDatabaseService = neo4j.defaultDatabaseService();
      try (Transaction tx = graphDatabaseService.beginTx()) {
            tx.execute("CREATE CONSTRAINT n10s_unique_uri "
                + "ON (r:Resource) ASSERT r.uri IS UNIQUE");
            tx.commit();
      }

    try (Transaction tx = graphDatabaseService.beginTx()) {
          tx.execute("CALL semantics.setGraphConfig( {keepLangTag: true, handleVocabUris: 'KEEP', handleMultival: 'OVERWRITE', keepCustomDataTypes: true, typesToLabels: true} )");
          tx.execute("CALL semantics.importRDF('" +
                RDFEndpointTest.class.getClassLoader().getResource("customDataTypes2.ttl")
                    .toURI()
                + "','Turtle')");

          tx.commit();
          }

      HTTP.Response response = HTTP.withHeaders("Accept", "text/turtle").GET(
          HTTP.GET(neo4j.httpURI().resolve("rdf").toString()).location()
              + "n10s/neo4j/describe/" + URLEncoder
              .encode("http://example.org/Resource1", StandardCharsets.UTF_8.toString()));

      String expected = "@prefix xsd: <http://www.w3.org/2001/XMLSchema#> .\n" +
          "<http://example.org/Resource1>  a  <http://example.org/Resource>;\n" +
          "  <http://example.org/Predicate1>  \"2008-04-17\"^^<http://www.w3.org/2001/XMLSchema#date>;\n" +
          "  <http://example.org/Predicate2>  \"4.75\"^^xsd:double;\n" +
          "  <http://example.org/Predicate3>  \"2\"^^xsd:long;\n" +
          "  <http://example.org/Predicate4>  true;\n" +
          "  <http://example.org/Predicate5>  \"2\"^^xsd:double;\n" +
          "  <http://example.org/Predicate6>  \"4\"^^xsd:double;\n" +
          "  <http://example.org/Predicate7>  \"52.63\"^^<http://example.org/USD>;\n" +
          "  <http://example.org/Predicate8>  \"2008-03-22T00:00:00\"^^<http://www.w3.org/2001/XMLSchema#dateTime>;\n" +
          "  <http://example.org/Predicate9> \"-100\"^^xsd:long.";

      assertEquals(200, response.status());
      assertTrue(ModelTestUtils
          .compareModels(expected, RDFFormat.TURTLE, response.rawContent(), RDFFormat.TURTLE));

  }

  @Test
  public void testNodeByUriAfterImportWithCustomDTShortenURIs() throws Exception {
      final GraphDatabaseService graphDatabaseService = neo4j.defaultDatabaseService();
    try (Transaction tx = graphDatabaseService.beginTx()) {
            tx.execute("CREATE CONSTRAINT n10s_unique_uri "
         + "ON (r:Resource) ASSERT r.uri IS UNIQUE");
            tx.commit();
    }
    try (Transaction tx = graphDatabaseService.beginTx()) {
      tx.execute("CALL semantics.setGraphConfig( {keepLangTag: true, "
          + " handleVocabUris: 'SHORTEN', handleMultival: 'OVERWRITE',"
          + " keepCustomDataTypes: true, typesToLabels: true} )");
      tx.execute("CALL semantics.importRDF('" +
                RDFEndpointTest.class.getClassLoader().getResource("customDataTypes2.ttl")
                    .toURI() + "','Turtle')");

            tx.commit();
          }
    HTTP.Response response = HTTP.withHeaders("Accept", "text/turtle").GET(
          HTTP.GET(neo4j.httpURI().resolve("rdf").toString()).location()
              + "n10s/neo4j/describe/" + URLEncoder
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
      final GraphDatabaseService graphDatabaseService = neo4j.defaultDatabaseService();
      try (Transaction tx = graphDatabaseService.beginTx()) {
            tx.execute("CREATE CONSTRAINT n10s_unique_uri "
         + "ON (r:Resource) ASSERT r.uri IS UNIQUE");
            tx.commit();
      }
    try (Transaction tx = graphDatabaseService.beginTx()) {
      tx.execute("CALL semantics.setGraphConfig( "
          + "{ keepLangTag: true, handleVocabUris: 'KEEP', handleMultival: 'ARRAY', "
          + "  multivalPropList: ['http://example.com/price', 'http://example.com/power'], "
          + "  keepCustomDataTypes: true, customDataTypePropList: ['http://example.com/price', 'http://example.com/color']} )");
      tx.execute("CALL semantics.importRDF('" +
                RDFEndpointTest.class.getClassLoader().getResource("customDataTypes.ttl")
                    .toURI()
                + "','Turtle')");

            tx.commit();
          }
      HTTP.Response response = HTTP.withHeaders("Accept", "text/turtle").GET(
          HTTP.GET(neo4j.httpURI().resolve("rdf").toString()).location()
              + "n10s/neo4j/describe/" + URLEncoder
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
      final GraphDatabaseService graphDatabaseService = neo4j.defaultDatabaseService();
      try (Transaction tx = graphDatabaseService.beginTx()) {
            tx.execute("CREATE CONSTRAINT n10s_unique_uri "
         + "ON (r:Resource) ASSERT r.uri IS UNIQUE");
            tx.commit();
      }
    try (Transaction tx = graphDatabaseService.beginTx()) {
      tx.execute("CALL semantics.setGraphConfig( {keepLangTag: true, "
          + " handleVocabUris: 'SHORTEN', handleMultival: 'ARRAY', "
          + " multivalPropList: ['http://example.com/price', 'http://example.com/power'], "
          + " keepCustomDataTypes: true, "
          + " customDataTypePropList: ['http://example.com/price', 'http://example.com/color']} )");
      tx.execute("CALL semantics.importRDF('" +
                RDFEndpoint.class.getClassLoader().getResource("customDataTypes.ttl")
                    .toURI()
                + "','Turtle')");

            tx.commit();
          }
      HTTP.Response response = HTTP.withHeaders("Accept", "text/turtle").GET(
          HTTP.GET(neo4j.httpURI().resolve("rdf").toString()).location()
              + "n10s/neo4j/describe/" + URLEncoder
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
      final GraphDatabaseService graphDatabaseService = neo4j.defaultDatabaseService();
      try (Transaction tx = graphDatabaseService.beginTx()) {
            tx.execute("CREATE CONSTRAINT n10s_unique_uri "
         + "ON (r:Resource) ASSERT r.uri IS UNIQUE");
            tx.commit();
      }
    try (Transaction tx = graphDatabaseService.beginTx()) {
      tx.execute("CALL semantics.setGraphConfig( {keepLangTag: true, handleVocabUris: 'KEEP', handleMultival: 'OVERWRITE', keepCustomDataTypes: true} )");
      tx.execute("CALL semantics.importRDF('" +
                RDFEndpointTest.class.getClassLoader().getResource("customDataTypes2.ttl")
                    .toURI()
                + "','Turtle')");

            tx.commit();
          }
      Map<String, String> params = new HashMap<>();
      params.put("cypher", "MATCH (n {uri: 'http://example.org/Resource1'})" +
          "OPTIONAL MATCH (n)-[]-(m) RETURN *");

      HTTP.Response response = HTTP.withHeaders("Accept", "text/turtle").POST(
          HTTP.GET(neo4j.httpURI().resolve("rdf").toString()).location() + "n10s/neo4j/cypher", params);

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
  public void testcypherDatesAndDatetimes() throws Exception {
      final GraphDatabaseService graphDatabaseService = neo4j.defaultDatabaseService();
      try (Transaction tx = graphDatabaseService.beginTx()) {
            tx.execute("CREATE CONSTRAINT n10s_unique_uri "
         + "ON (r:Resource) ASSERT r.uri IS UNIQUE");
            tx.commit();
      }
    try (Transaction tx = graphDatabaseService.beginTx()) {
      tx.execute("CALL semantics.setGraphConfig( {handleMultival: 'ARRAY'} )");
      tx.execute("CALL semantics.importRDF('" +
                RDFEndpointTest.class.getClassLoader()
                    .getResource("datetime/datetime-simple-multivalued.ttl")
                    .toURI()
                + "','Turtle')");

            tx.commit();
          }

      Map<String, String> params = new HashMap<>();
      params.put("cypher", "MATCH (n) RETURN *");

      HTTP.Response response = HTTP.withHeaders("Accept", "text/turtle").POST(
          HTTP.GET(neo4j.httpURI().resolve("rdf").toString()).location() + "n10s/neo4j/cypher", params);

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
      final GraphDatabaseService graphDatabaseService = neo4j.defaultDatabaseService();

    String cypherCreate = " CREATE (:Resource { uri: 'neo4j://ind#123' , name: 'the name' }) ";
    try (Transaction tx = graphDatabaseService.beginTx()) {
            Result res = tx.execute(cypherCreate);
            tx.commit();
          }
      Map<String, String> params = new HashMap<>();
      params.put("cypher", "MATCH (n) RETURN *");

      HTTP.Response response = HTTP.withHeaders("Accept", "text/turtle").POST(
          HTTP.GET(neo4j.httpURI().resolve("rdf").toString()).location() + "n10s/neo4j/cypher", params);

      assertEquals(200, response.status());

      String exportedAsLPG = "@prefix rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#> .\n"
          + "@prefix neovoc: <neo4j://vocabulary#> .\n"
          + "@prefix neoind: <neo4j://individuals#> .\n"
          + "\n"
          + "\n"
          + "neoind:0 a neovoc:Resource;\n"
          + "  neovoc:name \"the name\";\n"
          + "  neovoc:uri \"neo4j://ind#123\" .";

      assertTrue(ModelTestUtils.compareModels(exportedAsLPG, RDFFormat.TURTLE, response.rawContent(), RDFFormat.TURTLE));

      try (Transaction tx = graphDatabaseService.beginTx()) {
        tx.execute(" MATCH (n) DETACH DELETE n ");
        tx.execute(" CALL semantics.setGraphConfig({handleVocabUris: 'IGNORE'}) ");
        tx.execute(cypherCreate );
        tx.commit();
      }

      response = HTTP.withHeaders("Accept", "text/turtle").POST(
          HTTP.GET(neo4j.httpURI().resolve("rdf").toString()).location() + "n10s/neo4j/cypher", params);

    String exportedAsRDF = "@prefix rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#> .\n"
        + "@prefix neovoc: <neo4j://vocabulary#> .\n"
        + "@prefix neoind: <neo4j://individuals#> .\n"
        + "\n"
        + "\n"
        + "<neo4j://ind#123> neovoc:name \"the name\" .";

    assertTrue(ModelTestUtils.compareModels(exportedAsRDF, RDFFormat.TURTLE, response.rawContent(), RDFFormat.TURTLE));

  }


  @Test
  public void testcypherAfterImportWithCustomDTShortenURIsSerializeAsTurtle()
      throws Exception {
      final GraphDatabaseService graphDatabaseService = neo4j.defaultDatabaseService();
      try (Transaction tx = graphDatabaseService.beginTx()) {
            tx.execute("CREATE CONSTRAINT n10s_unique_uri "
         + "ON (r:Resource) ASSERT r.uri IS UNIQUE");
            tx.commit();
      }
    try (Transaction tx = graphDatabaseService.beginTx()) {
      tx.execute("CALL semantics.setGraphConfig( {keepLangTag: true, "
          + " handleVocabUris: 'SHORTEN', handleMultival: 'OVERWRITE', "
          + " keepCustomDataTypes: true } )");
      tx.execute("CALL semantics.importRDF('" +
                RDFEndpointTest.class.getClassLoader().getResource("customDataTypes2.ttl")
                    .toURI() + "','Turtle')");

            tx.commit();
          }
      Map<String, String> params = new HashMap<>();
      params.put("cypher", "MATCH (n {uri: 'http://example.org/Resource1'})" +
          "OPTIONAL MATCH (n)-[]-(m) RETURN *");

      HTTP.Response response = HTTP.withHeaders("Accept", "text/turtle").POST(
          HTTP.GET(neo4j.httpURI().resolve("rdf").toString()).location() + "n10s/neo4j/cypher", params);

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
      final GraphDatabaseService graphDatabaseService = neo4j.defaultDatabaseService();

      try (Transaction tx = graphDatabaseService.beginTx()) {
            tx.execute("CREATE CONSTRAINT n10s_unique_uri "
         + "ON (r:Resource) ASSERT r.uri IS UNIQUE");
            tx.commit();
      }
    try (Transaction tx = graphDatabaseService.beginTx()) {
      tx.execute("CALL semantics.setGraphConfig( "
          + "{keepLangTag: true, handleVocabUris: 'KEEP', handleMultival: 'ARRAY', "
          + " multivalPropList: ['http://example.com/price', 'http://example.com/power', 'http://example.com/class'], "
          + "keepCustomDataTypes: true, customDataTypePropList: ['http://example.com/price', 'http://example.com/color']} )");
      tx.execute("CALL semantics.importRDF('" +
                RDFEndpointTest.class.getClassLoader().getResource("customDataTypes.ttl")
                    .toURI()
                + "','Turtle')");

            tx.commit();
          }

      Map<String, String> params = new HashMap<>();
      params.put("cypher", "MATCH (a:`http://example.com/Car`) RETURN *");

      HTTP.Response response = HTTP.withHeaders("Accept", "text/turtle").POST(
          HTTP.GET(neo4j.httpURI().resolve("rdf").toString()).location() + "n10s/neo4j/cypher", params);

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
      final GraphDatabaseService graphDatabaseService = neo4j.defaultDatabaseService();
      try (Transaction tx = graphDatabaseService.beginTx()) {
            tx.execute("CREATE CONSTRAINT n10s_unique_uri "
         + "ON (r:Resource) ASSERT r.uri IS UNIQUE");
            tx.commit();
      }
    try (Transaction tx = graphDatabaseService.beginTx()) {
      tx.execute("CALL semantics.setGraphConfig( {keepLangTag: true, handleVocabUris: 'SHORTEN', handleMultival: 'ARRAY', "
          + " multivalPropList: ['http://example.com/price', 'http://example.com/power', 'http://example.com/class'], "
          + " keepCustomDataTypes: true, customDataTypePropList: ['http://example.com/price', 'http://example.com/color']} )");
      tx.execute("CALL semantics.importRDF('" +
                RDFEndpointTest.class.getClassLoader().getResource("customDataTypes.ttl")
                    .toURI()
                + "','Turtle')");

            tx.commit();
          }

      Map<String, String> params = new HashMap<>();
      params.put("cypher", "MATCH (a:ns0__Car) RETURN *");

      HTTP.Response response = HTTP.withHeaders("Accept", "text/turtle").POST(
          HTTP.GET(neo4j.httpURI().resolve("rdf").toString()).location() + "n10s/neo4j/cypher", params);

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
      final GraphDatabaseService graphDatabaseService = neo4j.defaultDatabaseService();
      try (Transaction tx = graphDatabaseService.beginTx()) {
            tx.execute("CREATE CONSTRAINT n10s_unique_uri "
         + "ON (r:Resource) ASSERT r.uri IS UNIQUE");

        tx.commit();
      }
    try (Transaction tx = graphDatabaseService.beginTx()) {
      tx.execute("CALL semantics.setGraphConfig( "
          + " { keepLangTag: true, handleVocabUris: 'KEEP', handleMultival: 'ARRAY', keepCustomDataTypes: true} )");
      tx.execute("CALL semantics.importRDF('" +
                RDFEndpointTest.class.getClassLoader().getResource("deleteRDF/bNodes.ttl")
                    .toURI()
                + "','Turtle')");
            Result res = tx.execute("CALL semantics.deleteRDF('" +
                RDFEndpointTest.class.getClassLoader().getResource("deleteRDF/bNodesDeletion.ttl")
                    .toURI()
                + "','Turtle')");
            Map map = res.next();
            assertEquals(1L, map.get("triplesDeleted"));
            assertEquals(
                "8 of the statements could not be deleted, due to containing a blank node.",
                map.get("extraInfo"));
            tx.commit();
          }
      Map<String, String> params = new HashMap<>();
      params.put("cypher", "MATCH (a:Resource) "
          + "OPTIONAL MATCH (a)-[r]->()"
          + "RETURN DISTINCT *");

      HTTP.Response response = HTTP.
          withHeaders("Accept", "text/turtle")
          .POST(
              HTTP.GET(neo4j.httpURI().resolve("rdf").toString()).location() + "n10s/neo4j/cypher",
              params);

      String expected = Resources
          .toString(Resources.getResource("deleteRDF/bNodesPostDeletion.ttl"),
              StandardCharsets.UTF_8);
      assertEquals(200, response.status());
      assertTrue(ModelTestUtils
          .compareModels(expected, RDFFormat.TURTLE, response.rawContent(), RDFFormat.TURTLE));

  }

  @Test
  public void testCypherOnQuadRDFSerializeAsTriG() throws Exception {
      final GraphDatabaseService graphDatabaseService = neo4j.defaultDatabaseService();
      try (Transaction tx = graphDatabaseService.beginTx()) {
            tx.execute("CREATE INDEX ON :Resource(uri)");
        tx.commit();
      }
    try (Transaction tx = graphDatabaseService.beginTx()) {
      tx.execute("CALL semantics.setGraphConfig( { handleVocabUris: 'KEEP', "
          + " typesToLabels: true, commitSize: 500, keepCustomDataTypes: true, handleMultival: 'ARRAY'} )");
      tx.execute("CALL semantics.importQuadRDF('" +
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
              HTTP.GET(neo4j.httpURI().resolve("rdf").toString()).location() + "n10s/neo4j/cypher",
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
      final GraphDatabaseService graphDatabaseService = neo4j.defaultDatabaseService();
      try (Transaction tx = graphDatabaseService.beginTx()) {
            tx.execute("CREATE INDEX ON :Resource(uri)");
            tx.commit();
      }
    try (Transaction tx = graphDatabaseService.beginTx()) {
      tx.execute("CALL semantics.setGraphConfig( { handleVocabUris: 'KEEP', "
          + " typesToLabels: true, commitSize: 500, keepCustomDataTypes: true, "
          + " handleMultival: 'ARRAY'} )");
      tx.execute("CALL semantics.importQuadRDF('" +
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
              HTTP.GET(neo4j.httpURI().resolve("rdf").toString()).location() + "n10s/neo4j/cypher",
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
      final GraphDatabaseService graphDatabaseService = neo4j.defaultDatabaseService();
      try (Transaction tx = graphDatabaseService.beginTx()) {
            tx.execute("CREATE INDEX ON :Resource(uri)");
            tx.commit();
      }
    try (Transaction tx = graphDatabaseService.beginTx()) {
      tx.execute("CALL semantics.setGraphConfig( { handleVocabUris: 'KEEP', typesToLabels: true, keepCustomDataTypes: true, handleMultival: 'ARRAY'} )");
      tx.execute("CALL semantics.importQuadRDF('" +
                RDFEndpointTest.class.getClassLoader().getResource("RDFDatasets/RDFDataset.trig")
                    .toURI()
                + "','TriG')");

            tx.commit();
          }

      HTTP.Response response = HTTP.withHeaders("Accept", "application/trig").GET(
          HTTP.GET(neo4j.httpURI().resolve("rdf").toString()).location()
              + "n10s/neo4j/describe/" + URLEncoder
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
      final GraphDatabaseService graphDatabaseService = neo4j.defaultDatabaseService();
      try (Transaction tx = graphDatabaseService.beginTx()) {
            tx.execute("CREATE INDEX ON :Resource(uri)");
            tx.commit();
      }
    try (Transaction tx = graphDatabaseService.beginTx()) {
      tx.execute("CALL semantics.setGraphConfig( { handleVocabUris: 'KEEP', typesToLabels: true, keepCustomDataTypes: true, handleMultival: 'ARRAY'} )");
      tx.execute("CALL semantics.importQuadRDF('" +
                RDFEndpointTest.class.getClassLoader().getResource("RDFDatasets/RDFDataset.trig")
                    .toURI()
                + "','TriG')");

            tx.commit();
          }

      HTTP.Response response = HTTP.withHeaders("Accept", "application/trig").GET(
          HTTP.GET(neo4j.httpURI().resolve("rdf").toString()).location()
              + "n10s/neo4j/describe/" + URLEncoder
              .encode("http://www.example.org/exampleDocument#Monica",
                  StandardCharsets.UTF_8.toString())
              + "?graphuri=http://www.example.org/exampleDocument%23G1");

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
      final GraphDatabaseService graphDatabaseService = neo4j.defaultDatabaseService();
      try (Transaction tx = graphDatabaseService.beginTx()) {
            tx.execute("CREATE INDEX ON :Resource(uri)");
            tx.commit();
      }
    try (Transaction tx = graphDatabaseService.beginTx()) {
      tx.execute("CALL semantics.setGraphConfig({ handleVocabUris: 'KEEP', "
          + " typesToLabels: true, commitSize: 500, keepCustomDataTypes: true, handleMultival: 'ARRAY'})");
      tx.execute("CALL semantics.importQuadRDF('" +
                RDFEndpointTest.class.getClassLoader().getResource("RDFDatasets/RDFDataset.nq")
                    .toURI()
                + "','N-Quads')");

            tx.commit();
          }

      HTTP.Response response = HTTP.withHeaders("Accept", "application/n-quads").GET(
          HTTP.GET(neo4j.httpURI().resolve("rdf").toString()).location()
              + "n10s/neo4j/describe/" + URLEncoder
              .encode("http://www.example.org/exampleDocument#Monica",
                  StandardCharsets.UTF_8.toString())
              + "?graphuri=http://www.example.org/exampleDocument%23G1");
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
    // Given
      final GraphDatabaseService graphDatabaseService = neo4j.defaultDatabaseService();
      try (Transaction tx = graphDatabaseService.beginTx()) {
            tx.execute("CREATE INDEX ON :Resource(uri)");
            tx.commit();
      }
    try (Transaction tx = graphDatabaseService.beginTx()) {
      tx.execute("CALL semantics.setGraphConfig( {keepLangTag: true, handleVocabUris: 'KEEP', "
          + " handleMultival: 'ARRAY', keepCustomDataTypes: true})");
      tx.execute("CALL semantics.importQuadRDF('" +
                RDFEndpointTest.class.getClassLoader().getResource(
                    "RDFDatasets/RDFDatasetBNodes.trig")
                    .toURI()
                + "','TriG')");
            Result res = tx.execute("CALL semantics.deleteQuadRDF('" +
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
              HTTP.GET(neo4j.httpURI().resolve("rdf").toString()).location() + "n10s/neo4j/cypher",
              params);

      String expected = Resources
          .toString(Resources.getResource("RDFDatasets/RDFDatasetBNodesPostDeletion.trig"),
              StandardCharsets.UTF_8);
      assertEquals(200, response.status());
      assertTrue(ModelTestUtils
          .compareModels(expected, RDFFormat.TRIG, response.rawContent(), RDFFormat.TRIG));

  }

}
