package n10s;

import static n10s.graphconfig.Params.BASE_INDIV_NS;
import static n10s.graphconfig.Params.DEFAULT_BASE_SCH_NS;
import static org.junit.Assert.*;


import n10s.endpoint.RDFEndpointTest;
import n10s.graphconfig.GraphConfigProcedures;
import n10s.mapping.MappingUtils;
import n10s.nsprefixes.NsPrefixDefProcedures;
import n10s.rdf.export.RDFExportProcedures;
import n10s.rdf.load.RDFLoadProcedures;
import org.eclipse.rdf4j.model.IRI;
import org.eclipse.rdf4j.model.Literal;
import org.eclipse.rdf4j.model.Statement;
import org.eclipse.rdf4j.model.ValueFactory;
import org.eclipse.rdf4j.model.impl.SimpleValueFactory;
import org.eclipse.rdf4j.model.vocabulary.FOAF;
import org.eclipse.rdf4j.model.vocabulary.RDF;
import org.eclipse.rdf4j.model.vocabulary.XSD;
import org.eclipse.rdf4j.rio.RDFFormat;
import org.junit.Rule;
import org.junit.Test;
import org.neo4j.driver.*;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.harness.junit.rule.Neo4jRule;
import org.neo4j.driver.Record;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

public class RDFExportTest {

  @Rule
  public Neo4jRule neo4j = new Neo4jRule()
      .withProcedure(RDFExportProcedures.class)
      .withProcedure(MappingUtils.class)
      .withProcedure(RDFLoadProcedures.class)
      .withProcedure(GraphConfigProcedures.class)
      .withProcedure(NsPrefixDefProcedures.class);


  @Test
  public void testExportFromCypherOnLPG() throws Exception {
    try (Driver driver = GraphDatabase.driver(neo4j.boltURI(),
        Config.builder().withoutEncryption().build()); Session session = driver.session()) {

      session
          .run(
              "CREATE (n:Node { a: 1, b: 'hello' })-[:CONNECTED_TO]->(:Node {  a:2, b2:'bye@en'})");

      Result res
          = session
          .run(" CALL n10s.rdf.export.cypher(' MATCH path = (n)-[r]->(m) RETURN path ', {}) ");
      assertTrue(res.hasNext());

      final ValueFactory vf = SimpleValueFactory.getInstance();
      Set<Statement> expectedStatememts = new HashSet<>(Arrays.asList(
              vf.createStatement(vf.createIRI(BASE_INDIV_NS + "0"), RDF.TYPE, vf.createIRI(DEFAULT_BASE_SCH_NS + "Node")),
              vf.createStatement(vf.createIRI(BASE_INDIV_NS + "0"), vf.createIRI(DEFAULT_BASE_SCH_NS + "a"), vf.createLiteral(1L)),
              vf.createStatement(vf.createIRI(BASE_INDIV_NS + "0"), vf.createIRI(DEFAULT_BASE_SCH_NS + "b"), vf.createLiteral("hello")),
              vf.createStatement(vf.createIRI(BASE_INDIV_NS + "0"), vf.createIRI(DEFAULT_BASE_SCH_NS + "CONNECTED_TO"), vf.createIRI(BASE_INDIV_NS + "1")),
              vf.createStatement(vf.createIRI(BASE_INDIV_NS + "1"), RDF.TYPE, vf.createIRI(DEFAULT_BASE_SCH_NS + "Node")),
              vf.createStatement(vf.createIRI(BASE_INDIV_NS + "1"), vf.createIRI(DEFAULT_BASE_SCH_NS + "b2"), vf.createLiteral("bye","en")),
              vf.createStatement(vf.createIRI(BASE_INDIV_NS + "1"), vf.createIRI(DEFAULT_BASE_SCH_NS + "a"), vf.createLiteral(2L))));

      int resultCount = 0;
      while (res.hasNext()) {
        Statement returnedStatement = recordAsStatement(vf, res.next());
        assertTrue(expectedStatememts.contains(returnedStatement));
        resultCount++;
      }
      assertEquals(resultCount,expectedStatememts.size());
    }
  }

  @Test
  public void testExportFromCypherOnLPGWithMappings() throws Exception {
    try (Driver driver = GraphDatabase.driver(neo4j.boltURI(),
            Config.builder().withoutEncryption().build()); Session session = driver.session()) {

      session
              .run(
                      "CREATE (n:Node { a: 1, b: 'hello' })-[:CONNECTED_TO]->(:Node {  a:2, b2:'bye@en'})");

      session.run("call n10s.nsprefixes.add('foaf','http://xmlns.com/foaf/0.1/')");
      session.run("call n10s.nsprefixes.add('myv','http://myvoc.org/testing#')");
      assertEquals(2L, session.run("call n10s.nsprefixes.list() yield prefix return count(*) as ct").next().get("ct").asLong());
      session.run("call n10s.mapping.add('http://xmlns.com/foaf/0.1/linkedTo','CONNECTED_TO')");
      session.run("call n10s.mapping.add('http://xmlns.com/foaf/0.1/Thang','Node')");
      session.run("call n10s.mapping.add('http://myvoc.org/testing#propA','a')");
      session.run("call n10s.mapping.add('http://myvoc.org/testing#propB','b')");
      List<Object> mappings = session.run("call n10s.mapping.list() yield schemaNs, schemaElement, elemName\n" +
              "return collect ({uri: schemaNs + schemaElement, elem: elemName}) as m").next().get("m").asList();
      assertEquals(4L, mappings.size());


      Result res
              = session
              .run(" CALL n10s.rdf.export.cypher(' MATCH path = (n)-[r]->(m) RETURN path ', {}) ");
      assertTrue(res.hasNext());

      final ValueFactory vf = SimpleValueFactory.getInstance();
      Set<Statement> expectedStatememts = new HashSet<>(Arrays.asList(
              vf.createStatement(vf.createIRI(BASE_INDIV_NS + "0"), RDF.TYPE, vf.createIRI("http://xmlns.com/foaf/0.1/Thang")),
              vf.createStatement(vf.createIRI(BASE_INDIV_NS + "0"), vf.createIRI("http://myvoc.org/testing#propA"), vf.createLiteral(1L)),
              vf.createStatement(vf.createIRI(BASE_INDIV_NS + "0"), vf.createIRI("http://myvoc.org/testing#propB"), vf.createLiteral("hello")),
              vf.createStatement(vf.createIRI(BASE_INDIV_NS + "0"), vf.createIRI("http://xmlns.com/foaf/0.1/linkedTo"), vf.createIRI(BASE_INDIV_NS + "1")),
              vf.createStatement(vf.createIRI(BASE_INDIV_NS + "1"), RDF.TYPE, vf.createIRI("http://xmlns.com/foaf/0.1/Thang")),
              vf.createStatement(vf.createIRI(BASE_INDIV_NS + "1"), vf.createIRI(DEFAULT_BASE_SCH_NS + "b2"), vf.createLiteral("bye","en")),
              vf.createStatement(vf.createIRI(BASE_INDIV_NS + "1"), vf.createIRI("http://myvoc.org/testing#propA"), vf.createLiteral(2L))));

      int resultCount = 0;
      while (res.hasNext()) {
        Statement returnedStatement = recordAsStatement(vf, res.next());
        assertTrue(expectedStatememts.contains(returnedStatement));
        resultCount++;
      }
      assertEquals(resultCount,expectedStatememts.size());
    }
  }

  @Test
  public void testExportFromCypherOnRDF() throws Exception {
    try (Driver driver = GraphDatabase.driver(neo4j.boltURI(),
            Config.builder().withoutEncryption().build())) {

      Session session = driver.session();

      initialiseGraphDB(neo4j.defaultDatabaseService(),
              " { handleVocabUris: 'SHORTEN' } ");

    }
    try (Driver driver = GraphDatabase.driver(neo4j.boltURI(),
            Config.builder().withoutEncryption().build()); Session session = driver.session()) {

      Result importResults1 = session.run("CALL n10s.rdf.import.inline('" +
              jsonLdFragment + "','JSON-LD')");
      assertEquals(11L, importResults1.single().get("triplesLoaded").asLong());

      Result res
              = session
              .run(" CALL n10s.rdf.export.cypher(' MATCH path = (n)-[r]->(m) RETURN path ', {}) ");
      assertTrue(res.hasNext());

      final ValueFactory vf = SimpleValueFactory.getInstance();
      Set<Statement> expectedStatememts = new HashSet<>(Arrays.asList(
              vf.createStatement(vf.createIRI("http://me.markus-lanthaler.com/"), RDF.TYPE, vf.createIRI("http://xmlns.com/foaf/0.1/Individual")),
              vf.createStatement(vf.createIRI("http://me.markus-lanthaler.com/"), FOAF.NAME, vf.createLiteral("Markus Lanthaler")),
              vf.createStatement(vf.createIRI("http://me.markus-lanthaler.com/"), FOAF.KNOWS, vf.createIRI("http://manu.sporny.org/about#manu")),
              vf.createStatement(vf.createIRI("http://manu.sporny.org/about#manu"), RDF.TYPE,vf.createIRI("http://xmlns.com/foaf/0.1/Subject")),
              vf.createStatement(vf.createIRI("http://manu.sporny.org/about#manu"), FOAF.NAME, vf.createLiteral("Manu Sporny")),
              vf.createStatement(vf.createIRI("http://manu.sporny.org/about#manu"), RDF.TYPE,vf.createIRI("http://xmlns.com/foaf/0.1/Citizen"))
              ));

      int resultCount = 0;
      while (res.hasNext()) {
        Statement returnedStatement = recordAsStatement(vf, res.next());
        assertTrue(returnedStatement.getSubject().stringValue().startsWith("bnode://") ||
                returnedStatement.getObject().stringValue().startsWith("bnode://")  ||
                expectedStatememts.contains(returnedStatement));
        resultCount++;
      }
      assertEquals(9,resultCount);
    }
  }

  private Statement recordAsStatement(ValueFactory vf,  Record r) {
    IRI s = vf.createIRI(r.get("subject").asString());
    IRI p = vf.createIRI(r.get("predicate").asString());
    if(r.get("isLiteral").asBoolean()){
      IRI datatype = vf.createIRI(r.get("literalType").asString());
      Literal o;
      if (datatype.equals(RDF.LANGSTRING)){
        o = (r.get("literalLang").isNull()? vf.createLiteral(r.get("object").asString()):
                vf.createLiteral(r.get("object").asString(),r.get("literalLang").asString() ));
      } else if (datatype.equals(XSD.LONG)){
        o = vf.createLiteral(Long.parseLong(r.get("object").asString()));
      } else if (datatype.equals(XSD.BOOLEAN)){
        o = vf.createLiteral(Boolean.valueOf(r.get("object").asString()));
      } else {
        //string default
        o = vf.createLiteral(r.get("object").asString());
      }
      return vf.createStatement(s, p, o);
    } else {
      return vf.createStatement(s, p, vf.createIRI(r.get("object").asString()));
    }
  }


  @Test
  public void testExportFromTriplePatternNoGraphConfig() throws Exception {
    try (Driver driver = GraphDatabase.driver(neo4j.boltURI(),
            Config.builder().withoutEncryption().build())) {

      Session session = driver.session();

      Transaction tx = session.beginTransaction();
      tx.run(Files.readString(Paths.get(
              RDFEndpointTest.class.getClassLoader().getResource("movies.cypher").getPath())));
      tx.run("MERGE (pb:Person {name:'Paul Blythe'}) SET pb:Critic ");
      tx.run( "MERGE (as:Person {name:'Angela Scope'}) SET as:Critic " );
      tx.run( "MERGE (jt:Person {name:'Jessica Thompson'}) SET jt:Critic " );
      tx.run("MERGE (jt2:Person {name:'James Thompson'}) SET jt2:Critic ");
      tx.commit();
    }
    allTriplePatternsOnLPG();
  }

  @Test
  public void testExportFromTriplePatternOnRDFGraphShortenDefault() throws Exception {
    try (Driver driver = GraphDatabase.driver(neo4j.boltURI(),
        Config.builder().withoutEncryption().build())) {

      Session session = driver.session();

      initialiseGraphDB(neo4j.defaultDatabaseService(),
          " {} ");

      Result importResults1 = session.run("CALL n10s.rdf.import.inline('" +
          jsonLdFragment + "','JSON-LD')");
      assertEquals(11L, importResults1.single().get("triplesLoaded").asLong());

    }
    allTriplePatterns(1);

  }

  @Test
  public void testExportFromTriplePatternOnRDFGraphShortenMultival() throws Exception {
    try (Driver driver = GraphDatabase.driver(neo4j.boltURI(),
            Config.builder().withoutEncryption().build())) {

      Session session = driver.session();

      initialiseGraphDB(neo4j.defaultDatabaseService(),
              " { handleMultival: 'ARRAY'} ");

      Result importResults1 = session.run("CALL n10s.rdf.import.inline('" +
              jsonLdFragment + "','JSON-LD')");
      assertEquals(11L, importResults1.single().get("triplesLoaded").asLong());

    }
    allTriplePatterns(2);

  }

  @Test
  public void testExportFromTriplePatternOnRDFGraphShortenTypesAsNodes() throws Exception {
    try (Driver driver = GraphDatabase.driver(neo4j.boltURI(),
            Config.builder().withoutEncryption().build())) {

      Session session = driver.session();

      initialiseGraphDB(neo4j.defaultDatabaseService(),
              " { handleRDFTypes: 'NODES'} ");

      Result importResults1 = session.run("CALL n10s.rdf.import.inline('" +
              jsonLdFragment + "','JSON-LD')");
      assertEquals(11L, importResults1.single().get("triplesLoaded").asLong());

    }
    allTriplePatterns(1);

  }

  @Test
  public void testExportFromTriplePatternOnRDFGraphKeepDefault() throws Exception {
    try (Driver driver = GraphDatabase.driver(neo4j.boltURI(),
            Config.builder().withoutEncryption().build())) {

      Session session = driver.session();

      initialiseGraphDB(neo4j.defaultDatabaseService(),
              " { handleVocabUris: 'KEEP' } ");

      Result importResults1 = session.run("CALL n10s.rdf.import.inline('" +
              jsonLdFragment + "','JSON-LD')");
      assertEquals(11L, importResults1.single().get("triplesLoaded").asLong());

    }
    allTriplePatterns(1);
  }

  @Test
  public void testExportFromTriplePatternOnRDFGraphShortenDefaultWithMappings() throws Exception {
    try (Driver driver = GraphDatabase.driver(neo4j.boltURI(),
            Config.builder().withoutEncryption().build())) {

      Session session = driver.session();

      initialiseGraphDB(neo4j.defaultDatabaseService(),
              " { handleVocabUris: 'SHORTEN_STRICT' } ");

      session.run("call n10s.nsprefixes.add('foaf','http://xmlns.com/foaf/0.1/')");
      session.run("call n10s.nsprefixes.add('rdf','http://www.w3.org/1999/02/22-rdf-syntax-ns#')");
      assertEquals(2L, session.run("call n10s.nsprefixes.list() yield prefix return count(*) as ct").next().get("ct").asLong());

    }
    try (Driver driver = GraphDatabase.driver(neo4j.boltURI(),
            Config.builder().withoutEncryption().build())) {

      Session session = driver.session();

      Result importResults1 = session.run("CALL n10s.rdf.import.inline('" +
              jsonLdFragment + "','JSON-LD')");
      assertEquals(11L, importResults1.single().get("triplesLoaded").asLong());

    }
    allTriplePatterns(1);
  }


  @Test
  public void testExportFromTriplePatternOnRDFGraphShortenTypesAsNodes2() throws Exception {
    try (Driver driver = GraphDatabase.driver(neo4j.boltURI(),
            Config.builder().withoutEncryption().build())) {

      Session session = driver.session();

      initialiseGraphDB(neo4j.defaultDatabaseService(),
              " { handleRDFTypes: 'LABELS_AND_NODES'} ");

      Result importResults1 = session.run("CALL n10s.rdf.import.inline('" +
              turtleFragment + "','Turtle')");
      assertEquals(19L, importResults1.single().get("triplesLoaded").asLong());

    }

    try (Driver driver = GraphDatabase.driver(neo4j.boltURI(),
            Config.builder().withoutEncryption().build())) {

      Session session = driver.session();

      String expected = "@prefix neo4voc: <http://neo4j.org/vocab/sw#> .\n" +
              "@prefix neo4ind: <http://neo4j.org/ind#> .\n" +
              "\n" +
              "neo4ind:nsmntx3502 neo4voc:version \"3.5.0.2\" .\n" ;

      assertTrue(ModelTestUtils
              .compareModels(expected, RDFFormat.TURTLE,
                      getNTriplesGraphFromSPOPattern(session, null, null,
                              "3.5.0.2", true, null, null), RDFFormat.NTRIPLES));

      assertTrue(ModelTestUtils
              .compareModels(expected, RDFFormat.TURTLE,
                      getNTriplesGraphFromSPOPatternLiteralDefaults(session, null, "http://neo4j.org/vocab/sw#version",
                              "3.5.0.2"), RDFFormat.NTRIPLES));

      expected = "@prefix neo4voc: <http://neo4j.org/vocab/sw#> .\n" +
              "@prefix neo4ind: <http://neo4j.org/ind#> .\n" +
              "\n" +
              "neo4ind:nsmntx3502 a neo4voc:Neo4jPlugin .\n" ;

      assertTrue(ModelTestUtils
              .compareModels(expected, RDFFormat.TURTLE,
                      getNTriplesGraphFromSPOPattern(session, "http://neo4j.org/ind#nsmntx3502", "http://www.w3.org/1999/02/22-rdf-syntax-ns#type",
                              null, null, null, null), RDFFormat.NTRIPLES));


      expected = "@prefix neo4voc: <http://neo4j.org/vocab/sw#> .\n" +
              "@prefix neo4ind: <http://neo4j.org/ind#> .\n" +
              "\n" +
              "neo4ind:nsmntx3502 a neo4voc:Neo4jPlugin . " +
              "neo4ind:graphql3502 a neo4voc:Neo4jPlugin . " +
              "neo4ind:apoc3502 a neo4voc:Neo4jPlugin ." ;

      assertTrue(ModelTestUtils
              .compareModels(expected, RDFFormat.TURTLE,
                      getNTriplesGraphFromSPOPattern(session, null, null,
                              "http://neo4j.org/vocab/sw#Neo4jPlugin", null, null, null), RDFFormat.NTRIPLES));


    }
  }

  @Test
  public void testExportFromTriplePatternOnRDFGraphIgnoreDefault() throws Exception {
    try (Driver driver = GraphDatabase.driver(neo4j.boltURI(),
            Config.builder().withoutEncryption().build())) {

      Session session = driver.session();

      initialiseGraphDB(neo4j.defaultDatabaseService(),
              " { handleVocabUris: 'IGNORE' } ");

      Result importResults1 = session.run("CALL n10s.rdf.import.inline('" +
              jsonLdFragment + "','JSON-LD')");
      assertEquals(11L, importResults1.single().get("triplesLoaded").asLong());

    }
    allTriplePatternsIgnore(1);

  }

  @Test
  public void testExportFromTriplePatternOnRDFGraphIgnoreDefaultMultivalued() throws Exception {
    try (Driver driver = GraphDatabase.driver(neo4j.boltURI(),
            Config.builder().withoutEncryption().build())) {

      Session session = driver.session();

      initialiseGraphDB(neo4j.defaultDatabaseService(),
              " { handleVocabUris: 'IGNORE' , handleMultival: 'ARRAY'} ");

      Result importResults1 = session.run("CALL n10s.rdf.import.inline('" +
              jsonLdFragment + "','JSON-LD')");
      assertEquals(11L, importResults1.single().get("triplesLoaded").asLong());

    }
    allTriplePatternsIgnore(2);

  }

  private void allTriplePatternsOnLPG() throws IOException {
    try (Driver driver = GraphDatabase.driver(neo4j.boltURI(),
            Config.builder().withoutEncryption().build()); Session session = driver.session()) {


      //getting a node's assigned uri
      long emilId = session
              .run("MATCH (n:Person) WHERE n.name = \"Emil Eifrem\" RETURN id(n) as id ").next().get("id").asLong();

      long theMatrixId = session
              .run("MATCH (n:Movie) WHERE n.title = \"The Matrix\" RETURN id(n) as id ").next().get("id").asLong();

      long robReinerId = session
              .run("MATCH (n:Person) WHERE n.name = \"Rob Reiner\" RETURN id(n) as id ").next().get("id").asLong();

      List<Object> critics = session
              .run("MATCH (n:Critic) RETURN collect(id(n)) as ids ").next().get("ids").asList();


      String expected = null;

      assertTrue(ModelTestUtils
              .compareModels("{}", RDFFormat.JSONLD,
                      getNTriplesGraphFromSPOPattern(session,"http://base/about#nonexistingresource",null, null, false, null, null), RDFFormat.NTRIPLES));

      assertTrue(ModelTestUtils
              .compareModels("{}", RDFFormat.JSONLD,
                      getNTriplesGraphFromSPOPattern(session,"http://base/about#nonexistingresource",DEFAULT_BASE_SCH_NS + "name", null, false, null, null), RDFFormat.NTRIPLES));

      assertTrue(ModelTestUtils
              .compareModels("{}", RDFFormat.JSONLD,
                      getNTriplesGraphFromSPOPattern(session,"http://base/about#nonexistingresource",DEFAULT_BASE_SCH_NS + "name", "MS", true, "http://www.w3.org/2001/XMLSchema#string", null), RDFFormat.NTRIPLES));

      expected = "{\n" +
              "  \"@id\" : \"n4ind:8\",\n" +
              "  \"@type\" : \"n4sch:Person\",\n" +
              "  \"n4sch:ACTED_IN\" : {\n" +
              "    \"@id\" : \"n4ind:0\"\n" +
              "  },\n" +
              "  \"n4sch:born\" : {\n" +
              "    \"@type\" : \"http://www.w3.org/2001/XMLSchema#long\",\n" +
              "    \"@value\" : \"1978\"\n" +
              "  },\n" +
              "  \"n4sch:name\" : \"Emil Eifrem\",\n" +
              "  \"@context\" : {\n" +
              "    \"n4sch\" : \"neo4j://graph.schema#\",\n" +
              "    \"n4ind\" : \"neo4j://graph.individuals#\"\n" +
              "  }\n" +
              "}";

      assertTrue(ModelTestUtils
              .compareModels(expected, RDFFormat.JSONLD,
                      getNTriplesGraphFromSPOPattern(session,BASE_INDIV_NS + emilId,null, null, false, null, null), RDFFormat.NTRIPLES));

      expected = "{\n" +
              "  \"@id\" : \"n4ind:8\",\n" +
              "  \"n4sch:name\" : \"Emil Eifrem\",\n" +
              "  \"@context\" : {\n" +
              "    \"n4sch\" : \"neo4j://graph.schema#\",\n" +
              "    \"n4ind\" : \"neo4j://graph.individuals#\"\n" +
              "  }\n" +
              "}";

      assertTrue(ModelTestUtils
              .compareModels(expected, RDFFormat.JSONLD,
                      getNTriplesGraphFromSPOPattern(session,BASE_INDIV_NS + emilId,DEFAULT_BASE_SCH_NS + "name", null, false, null, null), RDFFormat.NTRIPLES));


      expected = "{\n" +
              "  \"@id\" : \"n4ind:8\",\n" +
              "  \"@type\" : \"n4sch:Person\",\n" +
              "  \"@context\" : {\n" +
              "    \"n4sch\" : \"neo4j://graph.schema#\",\n" +
              "    \"n4ind\" : \"neo4j://graph.individuals#\"\n" +
              "  }\n" +
              "}";


      assertTrue(ModelTestUtils
              .compareModels(expected, RDFFormat.JSONLD,
                      getNTriplesGraphFromSPOPattern(session,BASE_INDIV_NS + emilId, "http://www.w3.org/1999/02/22-rdf-syntax-ns#type", null, false, null, null), RDFFormat.NTRIPLES));


      expected = "{\n" +
              "  \"@id\" : \"n4ind:8\",\n" +
              "  \"n4sch:name\" : \"Emil Eifrem\",\n" +
              "  \"@context\" : {\n" +
              "    \"n4sch\" : \"neo4j://graph.schema#\",\n" +
              "    \"n4ind\" : \"neo4j://graph.individuals#\"\n" +
              "  }\n" +
              "}";

      assertTrue(ModelTestUtils
              .compareModels(expected, RDFFormat.JSONLD,
                      getNTriplesGraphFromSPOPattern(session,BASE_INDIV_NS + emilId,DEFAULT_BASE_SCH_NS + "name", "Emil Eifrem", true, "http://www.w3.org/2001/XMLSchema#string", null), RDFFormat.NTRIPLES));

      expected = "{\n" +
              "  \"@id\" : \"n4ind:8\",\n" +
              "  \"n4sch:born\" : {\n" +
              "    \"@type\" : \"http://www.w3.org/2001/XMLSchema#long\",\n" +
              "    \"@value\" : \"1978\"\n" +
              "  },\n" +
              "  \"@context\" : {\n" +
              "    \"n4sch\" : \"neo4j://graph.schema#\",\n" +
              "    \"n4ind\" : \"neo4j://graph.individuals#\"\n" +
              "  }\n" +
              "}";

      assertTrue(ModelTestUtils
              .compareModels(expected, RDFFormat.JSONLD,
                      getNTriplesGraphFromSPOPattern(session,BASE_INDIV_NS + emilId,DEFAULT_BASE_SCH_NS + "born", "1978", true, "http://www.w3.org/2001/XMLSchema#long", null), RDFFormat.NTRIPLES));

      assertTrue(ModelTestUtils
              .compareModels(expected, RDFFormat.JSONLD,
                      getNTriplesGraphFromSPOPattern(session,BASE_INDIV_NS + emilId,DEFAULT_BASE_SCH_NS + "born", null, true, null, null), RDFFormat.NTRIPLES));

      assertTrue(ModelTestUtils
              .compareModels("{}", RDFFormat.JSONLD,
                      getNTriplesGraphFromSPOPattern(session,BASE_INDIV_NS + emilId,DEFAULT_BASE_SCH_NS + "name",  "Manuela", true, "http://www.w3.org/2001/XMLSchema#string", null), RDFFormat.NTRIPLES));


      expected = "{\n" +
              "  \"@id\" : \"n4ind:8\",\n" +
              "  \"@type\" : \"n4sch:Person\",\n" +
              "  \"@context\" : {\n" +
              "    \"n4sch\" : \"neo4j://graph.schema#\",\n" +
              "    \"n4ind\" : \"neo4j://graph.individuals#\"\n" +
              "  }\n" +
              "}";

      assertTrue(ModelTestUtils
              .compareModels(expected, RDFFormat.JSONLD,
                      getNTriplesGraphFromSPOPattern(session,BASE_INDIV_NS + emilId, "http://www.w3.org/1999/02/22-rdf-syntax-ns#type", DEFAULT_BASE_SCH_NS + "Person", false, null, null), RDFFormat.NTRIPLES));

      assertTrue(ModelTestUtils
              .compareModels("{}", RDFFormat.JSONLD,
                      getNTriplesGraphFromSPOPattern(session,BASE_INDIV_NS + emilId,DEFAULT_BASE_SCH_NS + "title", "The Matrix", true, "http://www.w3.org/2001/XMLSchema#string", null), RDFFormat.NTRIPLES));

      assertTrue(ModelTestUtils
              .compareModels("{}", RDFFormat.JSONLD,
                      getNTriplesGraphFromSPOPattern(session,BASE_INDIV_NS + emilId,"http://undefinedvoc.org/name", "MS", true, "http://www.w3.org/2001/XMLSchema#string", null), RDFFormat.NTRIPLES));

      assertTrue(ModelTestUtils
              .compareModels("{}", RDFFormat.JSONLD,
                      getNTriplesGraphFromSPOPattern(session,BASE_INDIV_NS + emilId,DEFAULT_BASE_SCH_NS + "ACTED_IN", BASE_INDIV_NS + emilId, false, null, null), RDFFormat.NTRIPLES));

      expected = "<" + BASE_INDIV_NS + emilId + ">  <neo4j://graph.schema#ACTED_IN> " + "<" + BASE_INDIV_NS + theMatrixId + "> .";

      assertTrue(ModelTestUtils
              .compareModels(expected, RDFFormat.NTRIPLES,
                      getNTriplesGraphFromSPOPattern(session,BASE_INDIV_NS + emilId,DEFAULT_BASE_SCH_NS + "ACTED_IN", null, false, null, null), RDFFormat.NTRIPLES));

      assertTrue(ModelTestUtils
              .compareModels(expected, RDFFormat.NTRIPLES,
                      getNTriplesGraphFromSPOPattern(session,BASE_INDIV_NS + emilId,DEFAULT_BASE_SCH_NS + "ACTED_IN", BASE_INDIV_NS + theMatrixId, false, null, null), RDFFormat.NTRIPLES));

      assertTrue(ModelTestUtils
              .compareModels("{}", RDFFormat.JSONLD,
                      getNTriplesGraphFromSPOPattern(session,null,"http://undefinedvoc.org/name", null, false, null, null), RDFFormat.NTRIPLES));


      // if we hardcode the ids here what's the point of getting them at the beginning for Emil, Renier and The Matrix?
      //TODO: do this right
      String titleTriples = "<neo4j://graph.individuals#0> <neo4j://graph.schema#title> \"The Matrix\" .\n" +
              "<neo4j://graph.individuals#9> <neo4j://graph.schema#title> \"The Matrix Reloaded\" .\n" +
              "<neo4j://graph.individuals#10> <neo4j://graph.schema#title> \"The Matrix Revolutions\" .\n" +
              "<neo4j://graph.individuals#11> <neo4j://graph.schema#title> \"The Devil's Advocate\" .\n" +
              "<neo4j://graph.individuals#15> <neo4j://graph.schema#title> \"A Few Good Men\" .\n" +
              "<neo4j://graph.individuals#29> <neo4j://graph.schema#title> \"Top Gun\" .\n" +
              "<neo4j://graph.individuals#37> <neo4j://graph.schema#title> \"Jerry Maguire\" .\n" +
              "<neo4j://graph.individuals#46> <neo4j://graph.schema#title> \"Stand By Me\" .\n" +
              "<neo4j://graph.individuals#52> <neo4j://graph.schema#title> \"As Good as It Gets\" .\n" +
              "<neo4j://graph.individuals#56> <neo4j://graph.schema#title> \"What Dreams May Come\" .\n" +
              "<neo4j://graph.individuals#62> <neo4j://graph.schema#title> \"Snow Falling on Cedars\" .\n" +
              "<neo4j://graph.individuals#67> <neo4j://graph.schema#title> \"You've Got Mail\" .\n" +
              "<neo4j://graph.individuals#73> <neo4j://graph.schema#title> \"Sleepless in Seattle\" .\n" +
              "<neo4j://graph.individuals#78> <neo4j://graph.schema#title> \"Joe Versus the Volcano\" .\n" +
              "<neo4j://graph.individuals#81> <neo4j://graph.schema#title> \"When Harry Met Sally\" .\n" +
              "<neo4j://graph.individuals#85> <neo4j://graph.schema#title> \"That Thing You Do\" .\n" +
              "<neo4j://graph.individuals#87> <neo4j://graph.schema#title> \"The Replacements\" .\n" +
              "<neo4j://graph.individuals#92> <neo4j://graph.schema#title> \"RescueDawn\" .\n" +
              "<neo4j://graph.individuals#95> <neo4j://graph.schema#title> \"The Birdcage\" .\n" +
              "<neo4j://graph.individuals#97> <neo4j://graph.schema#title> \"Unforgiven\" .\n" +
              "<neo4j://graph.individuals#100> <neo4j://graph.schema#title> \"Johnny Mnemonic\" .\n" +
              "<neo4j://graph.individuals#105> <neo4j://graph.schema#title> \"Cloud Atlas\" .\n" +
              "<neo4j://graph.individuals#111> <neo4j://graph.schema#title> \"The Da Vinci Code\" .\n" +
              "<neo4j://graph.individuals#116> <neo4j://graph.schema#title> \"V for Vendetta\" .\n" +
              "<neo4j://graph.individuals#121> <neo4j://graph.schema#title> \"Speed Racer\" .\n" +
              "<neo4j://graph.individuals#128> <neo4j://graph.schema#title> \"Ninja Assassin\" .\n" +
              "<neo4j://graph.individuals#130> <neo4j://graph.schema#title> \"The Green Mile\" .\n" +
              "<neo4j://graph.individuals#137> <neo4j://graph.schema#title> \"Frost/Nixon\" .\n" +
              "<neo4j://graph.individuals#141> <neo4j://graph.schema#title> \"Hoffa\" .\n" +
              "<neo4j://graph.individuals#144> <neo4j://graph.schema#title> \"Apollo 13\" .\n" +
              "<neo4j://graph.individuals#147> <neo4j://graph.schema#title> \"Twister\" .\n" +
              "<neo4j://graph.individuals#150> <neo4j://graph.schema#title> \"Cast Away\" .\n" +
              "<neo4j://graph.individuals#152> <neo4j://graph.schema#title> \"One Flew Over the Cuckoo's Nest\" .\n" +
              "<neo4j://graph.individuals#154> <neo4j://graph.schema#title> \"Something's Gotta Give\" .\n" +
              "<neo4j://graph.individuals#157> <neo4j://graph.schema#title> \"Bicentennial Man\" .\n" +
              "<neo4j://graph.individuals#159> <neo4j://graph.schema#title> \"Charlie Wilson's War\" .\n" +
              "<neo4j://graph.individuals#161> <neo4j://graph.schema#title> \"The Polar Express\" .\n" +
              "<neo4j://graph.individuals#162> <neo4j://graph.schema#title> \"A League of Their Own\" .";


      assertTrue(ModelTestUtils
              .compareModels(titleTriples, RDFFormat.NTRIPLES,
                      getNTriplesGraphFromSPOPattern(session,null,DEFAULT_BASE_SCH_NS + "title", null, false, null, null), RDFFormat.NTRIPLES));

      expected = "{\n" +
              "  \"@id\" : \"n4ind:8\",\n" +
              "  \"n4sch:born\" : {\n" +
              "    \"@type\" : \"http://www.w3.org/2001/XMLSchema#long\",\n" +
              "    \"@value\" : \"1978\"\n" +
              "  },\n" +
              "  \"@context\" : {\n" +
              "    \"n4sch\" : \"neo4j://graph.schema#\",\n" +
              "    \"n4ind\" : \"neo4j://graph.individuals#\"\n" +
              "  }\n" +
              "}";

      assertTrue(ModelTestUtils
              .modelContains(getNTriplesGraphFromSPOPattern(session,null,DEFAULT_BASE_SCH_NS + "born", null, false, null, null), RDFFormat.NTRIPLES,
                      expected, RDFFormat.JSONLD));


      expected = "<" + BASE_INDIV_NS + theMatrixId + "> <" + DEFAULT_BASE_SCH_NS + "title> \"The Matrix\" .";

      assertTrue(ModelTestUtils
              .compareModels(expected, RDFFormat.NTRIPLES,
                      getNTriplesGraphFromSPOPattern(session,null,DEFAULT_BASE_SCH_NS + "title", "The Matrix", true, "http://www.w3.org/2001/XMLSchema#string", null), RDFFormat.NTRIPLES));


      assertTrue(ModelTestUtils
              .compareModels(expected, RDFFormat.NTRIPLES,
                      getNTriplesGraphFromSPOPattern(session,null,null, "The Matrix", true, "http://www.w3.org/2001/XMLSchema#string", null), RDFFormat.NTRIPLES));

      expected = "<" + BASE_INDIV_NS + robReinerId + "> <" + DEFAULT_BASE_SCH_NS + "name> \"Rob Reiner\"^^<http://www.w3.org/2001/XMLSchema#string> .";

      assertTrue(ModelTestUtils
              .compareModels(expected, RDFFormat.NTRIPLES,
                      getNTriplesGraphFromSPOPattern(session,null,null, "Rob Reiner", true, "http://www.w3.org/2001/XMLSchema#string", null), RDFFormat.NTRIPLES));

      StringBuilder criticTypesTriples  = new StringBuilder();
      critics.forEach(id->
        { criticTypesTriples.append("<" + BASE_INDIV_NS + id + "> <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <" + DEFAULT_BASE_SCH_NS +  "Critic> .\n");
          criticTypesTriples.append("<" + BASE_INDIV_NS + id + "> <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <" + DEFAULT_BASE_SCH_NS +  "Person> .\n");} );

      String someMovies = "<neo4j://graph.individuals#0> <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <neo4j://graph.schema#Movie> .\n" +
              "<neo4j://graph.individuals#9> <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <neo4j://graph.schema#Movie> .\n" +
              "<neo4j://graph.individuals#10> <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <neo4j://graph.schema#Movie> .\n" +
              "<neo4j://graph.individuals#11> <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <neo4j://graph.schema#Movie> .\n" +
              "<neo4j://graph.individuals#15> <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <neo4j://graph.schema#Movie> .\n" +
              "<neo4j://graph.individuals#29> <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <neo4j://graph.schema#Movie> .\n" +
              "<neo4j://graph.individuals#37> <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <neo4j://graph.schema#Movie> .\n" +
              "<neo4j://graph.individuals#46> <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <neo4j://graph.schema#Movie> .\n" +
              "<neo4j://graph.individuals#52> <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <neo4j://graph.schema#Movie> .\n" ;

      assertTrue(ModelTestUtils
              .modelContains(getNTriplesGraphFromSPOPattern(session,null,"http://www.w3.org/1999/02/22-rdf-syntax-ns#type", null, false, null, null), RDFFormat.NTRIPLES,
              criticTypesTriples.toString() + someMovies, RDFFormat.NTRIPLES));

      StringBuilder criticTypesTriples2  = new StringBuilder();
      critics.forEach(id-> criticTypesTriples2.append("<" + BASE_INDIV_NS + id + "> <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <" + DEFAULT_BASE_SCH_NS +  "Critic> .\n") );

      assertTrue(ModelTestUtils
              .compareModels(criticTypesTriples2.toString(), RDFFormat.NTRIPLES,
                      getNTriplesGraphFromSPOPattern(session,null,"http://www.w3.org/1999/02/22-rdf-syntax-ns#type", DEFAULT_BASE_SCH_NS +  "Critic", false, null, null), RDFFormat.NTRIPLES));

      String allGraphAsNTriples = getNTriplesGraphFromSPOPattern(session, null, null, null, false, null, null);
      assertTrue(ModelTestUtils.modelContains(allGraphAsNTriples, RDFFormat.NTRIPLES,
                      criticTypesTriples.toString() + someMovies + titleTriples, RDFFormat.NTRIPLES ));

      assertFalse(ModelTestUtils.modelContains(allGraphAsNTriples, RDFFormat.NTRIPLES,
              "<neo4j://graph.individuals#0> <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <neo4j://graph.schema#AwesomeMovie> ." , RDFFormat.NTRIPLES ));

    }
  }


  private void allTriplePatterns( int mode) throws IOException {
    try (Driver driver = GraphDatabase.driver(neo4j.boltURI(),
        Config.builder().withoutEncryption().build()); Session session = driver.session()) {

      //getting a bnode's assigned uri
      String bnodeUri = session
              .run(" CALL n10s.rdf.export.spo(null,"
                      + "'http://xmlns.com/foaf/0.1/name','Dave Longley',true,'http://www.w3.org/2001/XMLSchema#string',null) ").next().get("subject").asString();


      String expected = null;

      assertTrue(ModelTestUtils
              .compareModels("{}", RDFFormat.JSONLD,
                      getNTriplesGraphFromSPOPattern(session,"http://manu.sporny.org/about#nonexistingresource",null, null, false, null, null), RDFFormat.NTRIPLES));

      assertTrue(ModelTestUtils
              .compareModels("{}", RDFFormat.JSONLD,
                      getNTriplesGraphFromSPOPattern(session,"http://manu.sporny.org/about#nonexistingresource","http://xmlns.com/foaf/0.1/name", null, false, null, null), RDFFormat.NTRIPLES));

      assertTrue(ModelTestUtils
              .compareModels("{}", RDFFormat.JSONLD,
                      getNTriplesGraphFromSPOPattern(session,"http://manu.sporny.org/about#nonexistingresource","http://xmlns.com/foaf/0.1/name", "MS", true, "http://www.w3.org/2001/XMLSchema#string", null), RDFFormat.NTRIPLES));


        if (mode == 1) {
          expected = "{\n" +
                  "  \"@context\": {\n" +
                  "    \"name\": \"http://xmlns.com/foaf/0.1/name\",\n" +
                  "    \"knows\": \"http://xmlns.com/foaf/0.1/knows\",\n" +
                  "\t\"modified\": \"http://xmlns.com/foaf/0.1/modified\"\n" +
                  "  },\n" +
                  "      \"@id\": \"http://manu.sporny.org/about#manu\",\n" +
                  "      \"name\": [\"Manu Sporny\"] ,\n" +
                  "      \"@type\": [\"http://xmlns.com/foaf/0.1/Subject\"," +
                  "                   \"http://xmlns.com/foaf/0.1/Citizen\"]\n" +
                  "}";
        } else if (mode == 2) {
          expected = "{\n" +
                  "  \"@context\": {\n" +
                  "    \"name\": \"http://xmlns.com/foaf/0.1/name\",\n" +
                  "    \"knows\": \"http://xmlns.com/foaf/0.1/knows\",\n" +
                  "\t\"modified\": \"http://xmlns.com/foaf/0.1/modified\"\n" +
                  "  },\n" +
                  "      \"@id\": \"http://manu.sporny.org/about#manu\",\n" +
                  "      \"name\": [\"MS\", \"Mr Sporny\",\"Manu Sporny\"] ,\n" +
                  "      \"@type\": [\"http://xmlns.com/foaf/0.1/Subject\"," +
                  "                   \"http://xmlns.com/foaf/0.1/Citizen\"]\n" +
                  "}";
        }

        assertTrue(ModelTestUtils
                .compareModels(expected, RDFFormat.JSONLD,
                        getNTriplesGraphFromSPOPattern(session, "http://manu.sporny.org/about#manu", null, null, false, null, null), RDFFormat.NTRIPLES));



        if (mode == 1) {
          expected = "{\n" +
                  "  \"@context\": {\n" +
                  "    \"name\": \"http://xmlns.com/foaf/0.1/name\"\n" +
                  "  },\n" +
                  "      \"@id\": \"http://manu.sporny.org/about#manu\",\n" +
                  "      \"name\": [\"Manu Sporny\"] \n" +
                  "}";
        } else if (mode == 2) {
          expected = "{\n" +
                  "  \"@context\": {\n" +
                  "    \"name\": \"http://xmlns.com/foaf/0.1/name\"\n" +
                  "  },\n" +
                  "      \"@id\": \"http://manu.sporny.org/about#manu\",\n" +
                  "      \"name\": [\"MS\", \"Mr Sporny\",\"Manu Sporny\"] \n" +
                  "}";
        }


        assertTrue(ModelTestUtils
                .compareModels(expected, RDFFormat.JSONLD,
                        getNTriplesGraphFromSPOPattern(session, "http://manu.sporny.org/about#manu", "http://xmlns.com/foaf/0.1/name", null, false, null, null), RDFFormat.NTRIPLES));



      expected = "{\n" +
              "  \"@context\": {\n" +
              "    \"name\": \"http://xmlns.com/foaf/0.1/name\",\n" +
              "    \"knows\": \"http://xmlns.com/foaf/0.1/knows\",\n" +
              "\t\"modified\": \"http://xmlns.com/foaf/0.1/modified\"\n" +
              "  },\n" +
              "      \"@id\": \"http://manu.sporny.org/about#manu\",\n" +
              "      \"@type\": [\"http://xmlns.com/foaf/0.1/Subject\"," +
              "                   \"http://xmlns.com/foaf/0.1/Citizen\"]\n" +
              "}";


      assertTrue(ModelTestUtils
              .compareModels(expected, RDFFormat.JSONLD,
                      getNTriplesGraphFromSPOPattern(session,"http://manu.sporny.org/about#manu","http://www.w3.org/1999/02/22-rdf-syntax-ns#type", null, false, null, null), RDFFormat.NTRIPLES));


      expected = "{\n" +
              "  \"@context\": {\n" +
              "    \"name\": \"http://xmlns.com/foaf/0.1/name\",\n" +
              "    \"knows\": \"http://xmlns.com/foaf/0.1/knows\",\n" +
              "\t\"modified\": \"http://xmlns.com/foaf/0.1/modified\"\n" +
              "  },\n" +
              "      \"@id\": \"http://manu.sporny.org/about#manu\",\n" +
              "      \"name\": \"Manu Sporny\" \n" +
              "}";

      assertTrue(ModelTestUtils
              .compareModels(expected, RDFFormat.JSONLD,
                      getNTriplesGraphFromSPOPattern(session,"http://manu.sporny.org/about#manu","http://xmlns.com/foaf/0.1/name", "Manu Sporny", true, "http://www.w3.org/2001/XMLSchema#string", null), RDFFormat.NTRIPLES));


      assertTrue(ModelTestUtils
              .compareModels("{}", RDFFormat.JSONLD,
                      getNTriplesGraphFromSPOPattern(session,"http://manu.sporny.org/about#manu","http://xmlns.com/foaf/0.1/name", "Manuela", true, "http://www.w3.org/2001/XMLSchema#string", null), RDFFormat.NTRIPLES));


      expected = "{\n" +
              "  \"@context\": {\n" +
              "    \"name\": \"http://xmlns.com/foaf/0.1/name\",\n" +
              "    \"knows\": \"http://xmlns.com/foaf/0.1/knows\",\n" +
              "\t\"modified\": \"http://xmlns.com/foaf/0.1/modified\"\n" +
              "  },\n" +
              "      \"@id\": \"http://manu.sporny.org/about#manu\",\n" +
              "      \"@type\": [\"http://xmlns.com/foaf/0.1/Subject\"]\n" +
              "}";

      assertTrue(ModelTestUtils
              .compareModels(expected, RDFFormat.JSONLD,
                      getNTriplesGraphFromSPOPattern(session,"http://manu.sporny.org/about#manu","http://www.w3.org/1999/02/22-rdf-syntax-ns#type", "http://xmlns.com/foaf/0.1/Subject", false, null, null), RDFFormat.NTRIPLES));

      assertTrue(ModelTestUtils
              .compareModels("{}", RDFFormat.JSONLD,
                      getNTriplesGraphFromSPOPattern(session,"http://manu.sporny.org/about#manu","http://xmlns.com/foaf/0.1/familyname", "Manuela", true, "http://www.w3.org/2001/XMLSchema#string", null), RDFFormat.NTRIPLES));

      assertTrue(ModelTestUtils
              .compareModels("{}", RDFFormat.JSONLD,
                      getNTriplesGraphFromSPOPattern(session,"http://manu.sporny.org/about#manu","http://undefinedvoc.org/name", "MS", true, "http://www.w3.org/2001/XMLSchema#string", null), RDFFormat.NTRIPLES));

      assertTrue(ModelTestUtils
              .compareModels("{}", RDFFormat.JSONLD,
                      getNTriplesGraphFromSPOPattern(session,"http://manu.sporny.org/about#manu","http://xmlns.com/foaf/0.1/knows", "http://manu.sporny.org/about#manu", false, null, null), RDFFormat.NTRIPLES));

      expected = "{\n" +
              "  \"@context\": {\n" +
              "    \"name\": \"http://xmlns.com/foaf/0.1/name\",\n" +
              "    \"knows\": \"http://xmlns.com/foaf/0.1/knows\",\n" +
              "\t\"modified\": \"http://xmlns.com/foaf/0.1/modified\"\n" +
              "  },\n" +
              "  \"@id\": \"http://me.markus-lanthaler.com/\",\n" +
              "  \"knows\": [\n" +
              "    {\n" +
              "      \"@id\": \"http://manu.sporny.org/about#manu\" },\n" +
              "    {\n" +
              "      \"@id\": \"" + bnodeUri +"\" }\n" +
              "  ]\n" +
              "}";

      assertTrue(ModelTestUtils
              .compareModels(expected, RDFFormat.JSONLD,
                      getNTriplesGraphFromSPOPattern(session,"http://me.markus-lanthaler.com/","http://xmlns.com/foaf/0.1/knows", null, false, null, null), RDFFormat.NTRIPLES));

      expected = "{\n" +
              "  \"@context\": {\n" +
              "    \"name\": \"http://xmlns.com/foaf/0.1/name\",\n" +
              "    \"knows\": \"http://xmlns.com/foaf/0.1/knows\",\n" +
              "\t\"modified\": \"http://xmlns.com/foaf/0.1/modified\"\n" +
              "  },\n" +
              "  \"@id\": \"http://me.markus-lanthaler.com/\",\n" +
              "  \"knows\": [\n" +
              "    {\n" +
              "      \"@id\": \"http://manu.sporny.org/about#manu\" }" +
              "  ]\n" +
              "}";

      assertTrue(ModelTestUtils
              .compareModels(expected, RDFFormat.JSONLD,
                      getNTriplesGraphFromSPOPattern(session,"http://me.markus-lanthaler.com/","http://xmlns.com/foaf/0.1/knows", "http://manu.sporny.org/about#manu", false, null, null), RDFFormat.NTRIPLES));

      assertTrue(ModelTestUtils
              .compareModels("{}", RDFFormat.JSONLD,
                      getNTriplesGraphFromSPOPattern(session,null,"http://undefinedvoc.org/name", null, false, null, null), RDFFormat.NTRIPLES));


      if( mode ==1){
        expected = "{ \"@context\": {\n" +
                "    \"name\": \"http://xmlns.com/foaf/0.1/name\",\n" +
                "    \"knows\": \"http://xmlns.com/foaf/0.1/knows\",\n" +
                "    \"modified\": \"http://xmlns.com/foaf/0.1/modified\"\n" +
                "  },\"@graph\": [" +
                "{ \"@id\": \"http://me.markus-lanthaler.com/\",\n" +
                "  \"name\": \"Markus Lanthaler\"} , " +
                "{ \"@id\": \"http://manu.sporny.org/about#manu\",\n" +
                "  \"name\": \"Manu Sporny\" },\n" +
                "{ \"@id\": \""+ bnodeUri +"\",\n" +
                "  \"name\": \"Dave Longley\" }\n" +
                "]}";
      } else if ( mode ==2 ){
        expected = "{ \"@context\": {\n" +
                "    \"name\": \"http://xmlns.com/foaf/0.1/name\",\n" +
                "    \"knows\": \"http://xmlns.com/foaf/0.1/knows\",\n" +
                "    \"modified\": \"http://xmlns.com/foaf/0.1/modified\"\n" +
                "  },\"@graph\": [" +
                "{ \"@id\": \"http://me.markus-lanthaler.com/\",\n" +
                "  \"name\": \"Markus Lanthaler\"} , " +
                "{ \"@id\": \"http://manu.sporny.org/about#manu\",\n" +
                "  \"name\": [\"MS\", \"Mr Sporny\",\"Manu Sporny\"] },\n" +
                "{ \"@id\": \""+ bnodeUri +"\",\n" +
                "  \"name\": \"Dave Longley\" }\n" +
                "]}";
      }


      assertTrue(ModelTestUtils
              .compareModels(expected, RDFFormat.JSONLD,
                      getNTriplesGraphFromSPOPattern(session,null,"http://xmlns.com/foaf/0.1/name", null, false, null, null), RDFFormat.NTRIPLES));


      expected = "{ \"@context\": {\n" +
              "    \"name\": \"http://xmlns.com/foaf/0.1/name\",\n" +
              "    \"knows\": \"http://xmlns.com/foaf/0.1/knows\",\n" +
              "    \"modified\": \"http://xmlns.com/foaf/0.1/modified\"\n" +
              "  }, " +
              " \"@id\": \"http://me.markus-lanthaler.com/\",\n" +
              "  \"name\": \"Markus Lanthaler\"}";

      assertTrue(ModelTestUtils
              .compareModels(expected, RDFFormat.JSONLD,
                      getNTriplesGraphFromSPOPattern(session,null,"http://xmlns.com/foaf/0.1/name", "Markus Lanthaler", true, "http://www.w3.org/2001/XMLSchema#string", null), RDFFormat.NTRIPLES));


      assertTrue(ModelTestUtils
              .compareModels(expected, RDFFormat.JSONLD,
                      getNTriplesGraphFromSPOPattern(session,null,null, "Markus Lanthaler", true, "http://www.w3.org/2001/XMLSchema#string", null), RDFFormat.NTRIPLES));

      expected = "<" + bnodeUri + "> <http://xmlns.com/foaf/0.1/name> \"Dave Longley\"^^<http://www.w3.org/2001/XMLSchema#string> .";

      assertTrue(ModelTestUtils
              .compareModels(expected, RDFFormat.NTRIPLES,
                      getNTriplesGraphFromSPOPattern(session,null,null, "Dave Longley", true, "http://www.w3.org/2001/XMLSchema#string", null), RDFFormat.NTRIPLES));

      expected = "<http://me.markus-lanthaler.com/> <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://xmlns.com/foaf/0.1/Individual> .\n" +
              "<http://manu.sporny.org/about#manu> <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://xmlns.com/foaf/0.1/Subject> .\n" +
              "<http://manu.sporny.org/about#manu> <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://xmlns.com/foaf/0.1/Citizen> .";

      assertTrue(ModelTestUtils
              .compareModels(expected, RDFFormat.NTRIPLES,
                      getNTriplesGraphFromSPOPattern(session,null,"http://www.w3.org/1999/02/22-rdf-syntax-ns#type", null, false, null, null), RDFFormat.NTRIPLES));

      expected = " <http://manu.sporny.org/about#manu> <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://xmlns.com/foaf/0.1/Subject> . ";

      assertTrue(ModelTestUtils
              .compareModels(expected, RDFFormat.NTRIPLES,
                      getNTriplesGraphFromSPOPattern(session,null,"http://www.w3.org/1999/02/22-rdf-syntax-ns#type", "http://xmlns.com/foaf/0.1/Subject", false, null, null), RDFFormat.NTRIPLES));

      if( mode ==1 ) {
        expected = "{\n" +
                "  \"@context\": {\n" +
                "    \"name\": \"http://xmlns.com/foaf/0.1/name\",\n" +
                "    \"knows\": \"http://xmlns.com/foaf/0.1/knows\",\n" +
                "\t\"modified\": \"http://xmlns.com/foaf/0.1/modified\"\n" +
                "  },\n" +
                "  \"@id\": \"http://me.markus-lanthaler.com/\",\n" +
                "  \"name\": \"Markus Lanthaler\",\n" +
                "  \"@type\": \"http://xmlns.com/foaf/0.1/Individual\",\n" +
                "  \"knows\": [\n" +
                "    {\n" +
                "      \"@id\": \"http://manu.sporny.org/about#manu\",\n" +
                "      \"name\": [\"Manu Sporny\"] ,\n" +
                "      \"@type\": [\"http://xmlns.com/foaf/0.1/Subject\"," +
                "                   \"http://xmlns.com/foaf/0.1/Citizen\"]\n" +
                "    },\n" +
                "    {\n" +
                "      \"@id\": \"" + bnodeUri + "\",\n" +
                "      \"name\": \"Dave Longley\",\n" +
                "\t  \"modified\":\n" +
                "\t    {\n" +
                "\t      \"@value\": \"2010-05-29T14:17:39\",\n" +
                "\t      \"@type\": \"http://www.w3.org/2001/XMLSchema#dateTime\"\n" +
                "\t    }\n" +
                "    }\n" +
                "  ]\n" +
                "}";
      } else if ( mode == 2){
        expected = "{\n" +
                "  \"@context\": {\n" +
                "    \"name\": \"http://xmlns.com/foaf/0.1/name\",\n" +
                "    \"knows\": \"http://xmlns.com/foaf/0.1/knows\",\n" +
                "\t\"modified\": \"http://xmlns.com/foaf/0.1/modified\"\n" +
                "  },\n" +
                "  \"@id\": \"http://me.markus-lanthaler.com/\",\n" +
                "  \"name\": \"Markus Lanthaler\",\n" +
                "  \"@type\": \"http://xmlns.com/foaf/0.1/Individual\",\n" +
                "  \"knows\": [\n" +
                "    {\n" +
                "      \"@id\": \"http://manu.sporny.org/about#manu\",\n" +
                "      \"name\": [\"MS\", \"Mr Sporny\",\"Manu Sporny\"] ,\n" +
                "      \"@type\": [\"http://xmlns.com/foaf/0.1/Subject\"," +
                "                   \"http://xmlns.com/foaf/0.1/Citizen\"]\n" +
                "    },\n" +
                "    {\n" +
                "      \"@id\": \"" + bnodeUri + "\",\n" +
                "      \"name\": \"Dave Longley\",\n" +
                "\t  \"modified\":\n" +
                "\t    {\n" +
                "\t      \"@value\": \"2010-05-29T14:17:39\",\n" +
                "\t      \"@type\": \"http://www.w3.org/2001/XMLSchema#dateTime\"\n" +
                "\t    }\n" +
                "    }\n" +
                "  ]\n" +
                "}";;
      }

      assertTrue(ModelTestUtils
              .compareModels(expected, RDFFormat.JSONLD,
                      getNTriplesGraphFromSPOPattern(session,null,null, null, false, null, null), RDFFormat.NTRIPLES));


    }
  }


  private void allTriplePatternsIgnore( int mode) throws IOException {
    try (Driver driver = GraphDatabase.driver(neo4j.boltURI(),
            Config.builder().withoutEncryption().build()); Session session = driver.session()) {

      //getting a bnode's assigned uri
      String bnodeUri = session
              .run(" CALL n10s.rdf.export.spo(null,"
                      + "'" + DEFAULT_BASE_SCH_NS + "name','Dave Longley',true,'http://www.w3.org/2001/XMLSchema#string',null) ").next().get("subject").asString();


      String expected = null;

      assertTrue(ModelTestUtils
              .compareModels("{}", RDFFormat.JSONLD,
                      getNTriplesGraphFromSPOPattern(session,"http://manu.sporny.org/about#nonexistingresource",null, null, false, null, null), RDFFormat.NTRIPLES));

      assertTrue(ModelTestUtils
              .compareModels("{}", RDFFormat.JSONLD,
                      getNTriplesGraphFromSPOPattern(session,"http://manu.sporny.org/about#nonexistingresource","http://xmlns.com/foaf/0.1/name", null, false, null, null), RDFFormat.NTRIPLES));

      assertTrue(ModelTestUtils
              .compareModels("{}", RDFFormat.JSONLD,
                      getNTriplesGraphFromSPOPattern(session,"http://manu.sporny.org/about#nonexistingresource","http://xmlns.com/foaf/0.1/name", "MS", true, "http://www.w3.org/2001/XMLSchema#string", null), RDFFormat.NTRIPLES));


      if (mode == 1) {
        expected = "{\n" +
                "  \"@context\": {\n" +
                "    \"name\": \"" + DEFAULT_BASE_SCH_NS + "name\",\n" +
                "    \"knows\": \"" + DEFAULT_BASE_SCH_NS + "knows\",\n" +
                "\t\"modified\": \"" + DEFAULT_BASE_SCH_NS + "modified\"\n" +
                "  },\n" +
                "      \"@id\": \"http://manu.sporny.org/about#manu\",\n" +
                "      \"name\": [\"Manu Sporny\"] ,\n" +
                "      \"@type\": [\"" + DEFAULT_BASE_SCH_NS + "Subject\"," +
                "                   \"" + DEFAULT_BASE_SCH_NS + "Citizen\"]\n" +
                "}";
      } else if (mode == 2) {
        expected = "{\n" +
                "  \"@context\": {\n" +
                "    \"name\": \"" + DEFAULT_BASE_SCH_NS + "name\",\n" +
                "    \"knows\": \"" + DEFAULT_BASE_SCH_NS + "knows\",\n" +
                "\t\"modified\": \"" + DEFAULT_BASE_SCH_NS + "modified\"\n" +
                "  },\n" +
                "      \"@id\": \"http://manu.sporny.org/about#manu\",\n" +
                "      \"name\": [\"MS\", \"Mr Sporny\",\"Manu Sporny\"] ,\n" +
                "      \"@type\": [\"" + DEFAULT_BASE_SCH_NS + "Subject\"," +
                "                   \"" + DEFAULT_BASE_SCH_NS + "Citizen\"]\n" +
                "}";
      }

      assertTrue(ModelTestUtils
              .compareModels(expected, RDFFormat.JSONLD,
                      getNTriplesGraphFromSPOPattern(session, "http://manu.sporny.org/about#manu", null, null, false, null, null), RDFFormat.NTRIPLES));


      if (mode == 1) {
        expected = "{\n" +
                "  \"@context\": {\n" +
                "    \"name\": \"" + DEFAULT_BASE_SCH_NS + "name\",\n" +
                "    \"knows\": \"" + DEFAULT_BASE_SCH_NS + "knows\",\n" +
                "\t\"modified\": \"" + DEFAULT_BASE_SCH_NS + "modified\"\n" +
                "  },\n" +
                "      \"@id\": \"http://manu.sporny.org/about#manu\",\n" +
                "      \"name\": [\"Manu Sporny\"] \n" +
                "}";
      } else if (mode == 2) {
        expected = "{\n" +
                "  \"@context\": {\n" +
                "    \"name\": \"" + DEFAULT_BASE_SCH_NS + "name\",\n" +
                "    \"knows\": \"" + DEFAULT_BASE_SCH_NS + "knows\",\n" +
                "\t\"modified\": \"" + DEFAULT_BASE_SCH_NS + "modified\"\n" +
                "  },\n" +
                "      \"@id\": \"http://manu.sporny.org/about#manu\",\n" +
                "      \"name\": [\"MS\", \"Mr Sporny\",\"Manu Sporny\"] \n" +
                "}";
      }

        assertTrue(ModelTestUtils
                .compareModels("{}", RDFFormat.JSONLD,
                        getNTriplesGraphFromSPOPattern(session, "http://manu.sporny.org/about#manu", "http://xmlns.com/foaf/0.1/name", null, false, null, null), RDFFormat.NTRIPLES));

      assertTrue(ModelTestUtils
              .compareModels(expected, RDFFormat.JSONLD,
                      getNTriplesGraphFromSPOPattern(session, "http://manu.sporny.org/about#manu", DEFAULT_BASE_SCH_NS + "name", null, false, null, null), RDFFormat.NTRIPLES));


      expected = "{\n" +
              "  \"@context\": {\n" +
              "    \"name\": \"http://xmlns.com/foaf/0.1/name\",\n" +
              "    \"knows\": \"http://xmlns.com/foaf/0.1/knows\",\n" +
              "\t\"modified\": \"http://xmlns.com/foaf/0.1/modified\"\n" +
              "  },\n" +
              "      \"@id\": \"http://manu.sporny.org/about#manu\",\n" +
              "      \"@type\": [\"" + DEFAULT_BASE_SCH_NS + "Subject\"," +
              "                   \"" + DEFAULT_BASE_SCH_NS + "Citizen\"]\n" +
              "}";


      assertTrue(ModelTestUtils
              .compareModels(expected, RDFFormat.JSONLD,
                      getNTriplesGraphFromSPOPattern(session,"http://manu.sporny.org/about#manu","http://www.w3.org/1999/02/22-rdf-syntax-ns#type", null, false, null, null), RDFFormat.NTRIPLES));


      expected = "{\n" +
              "  \"@context\": {\n" +
              "    \"name\": \"" + DEFAULT_BASE_SCH_NS + "name\"\n" +
              "  },\n" +
              "      \"@id\": \"http://manu.sporny.org/about#manu\",\n" +
              "      \"name\": \"Manu Sporny\" \n" +
              "}";

      assertTrue(ModelTestUtils
              .compareModels("{}", RDFFormat.JSONLD,
                      getNTriplesGraphFromSPOPattern(session,"http://manu.sporny.org/about#manu","http://xmlns.com/foaf/0.1/name", "Manu Sporny", true, "http://www.w3.org/2001/XMLSchema#string", null), RDFFormat.NTRIPLES));


      assertTrue(ModelTestUtils
              .compareModels(expected, RDFFormat.JSONLD,
                      getNTriplesGraphFromSPOPattern(session,"http://manu.sporny.org/about#manu",DEFAULT_BASE_SCH_NS + "name", "Manu Sporny", true, "http://www.w3.org/2001/XMLSchema#string", null), RDFFormat.NTRIPLES));


      assertTrue(ModelTestUtils
              .compareModels("{}", RDFFormat.JSONLD,
                      getNTriplesGraphFromSPOPattern(session,"http://manu.sporny.org/about#manu",DEFAULT_BASE_SCH_NS + "name", "Manuela", true, "http://www.w3.org/2001/XMLSchema#string", null), RDFFormat.NTRIPLES));


      expected = "{\n" +
              "  \"@context\": {},\n" +
              "      \"@id\": \"http://manu.sporny.org/about#manu\",\n" +
              "      \"@type\": [\"" + DEFAULT_BASE_SCH_NS + "Subject\" ]\n" +
              "}";

      assertTrue(ModelTestUtils
              .compareModels(expected, RDFFormat.JSONLD,
                      getNTriplesGraphFromSPOPattern(session,"http://manu.sporny.org/about#manu","http://www.w3.org/1999/02/22-rdf-syntax-ns#type", DEFAULT_BASE_SCH_NS + "Subject", false, null, null), RDFFormat.NTRIPLES));

      assertTrue(ModelTestUtils
              .compareModels("{}", RDFFormat.JSONLD,
                      getNTriplesGraphFromSPOPattern(session,"http://manu.sporny.org/about#manu",DEFAULT_BASE_SCH_NS + "familyname", "Manuela", true, "http://www.w3.org/2001/XMLSchema#string", null), RDFFormat.NTRIPLES));

      assertTrue(ModelTestUtils
              .compareModels("{}", RDFFormat.JSONLD,
                      getNTriplesGraphFromSPOPattern(session,"http://manu.sporny.org/about#manu","http://undefinedvoc.org/name", "MS", true, "http://www.w3.org/2001/XMLSchema#string", null), RDFFormat.NTRIPLES));

      assertTrue(ModelTestUtils
              .compareModels("{}", RDFFormat.JSONLD,
                      getNTriplesGraphFromSPOPattern(session,"http://manu.sporny.org/about#manu",DEFAULT_BASE_SCH_NS + "knows", "http://manu.sporny.org/about#manu", false, null, null), RDFFormat.NTRIPLES));

      expected = "{\n" +
              "  \"@context\": {\n" +
              "    \"knows\": \"" + DEFAULT_BASE_SCH_NS + "knows\"\n" +
              "  },\n" +
              "  \"@id\": \"http://me.markus-lanthaler.com/\",\n" +
              "  \"knows\": [\n" +
              "    {\n" +
              "      \"@id\": \"http://manu.sporny.org/about#manu\" },\n" +
              "    {\n" +
              "      \"@id\": \"" + bnodeUri +"\" }\n" +
              "  ]\n" +
              "}";

      assertTrue(ModelTestUtils
              .compareModels(expected, RDFFormat.JSONLD,
                      getNTriplesGraphFromSPOPattern(session,"http://me.markus-lanthaler.com/", DEFAULT_BASE_SCH_NS + "knows", null, false, null, null), RDFFormat.NTRIPLES));

      expected = "{\n" +
              "  \"@context\": {\n" +
              "    \"knows\": \"" + DEFAULT_BASE_SCH_NS + "knows\"\n" +
              "  },\n" +
              "  \"@id\": \"http://me.markus-lanthaler.com/\",\n" +
              "  \"knows\": [\n" +
              "    {\n" +
              "      \"@id\": \"http://manu.sporny.org/about#manu\" }\n" +
              "  ]\n" +
              "}";

      assertTrue(ModelTestUtils
              .compareModels(expected, RDFFormat.JSONLD,
                      getNTriplesGraphFromSPOPattern(session,"http://me.markus-lanthaler.com/",DEFAULT_BASE_SCH_NS + "knows", "http://manu.sporny.org/about#manu", false, null, null), RDFFormat.NTRIPLES));

      assertTrue(ModelTestUtils
              .compareModels("{}", RDFFormat.JSONLD,
                      getNTriplesGraphFromSPOPattern(session,null,"http://undefinedvoc.org/name", null, false, null, null), RDFFormat.NTRIPLES));


      if( mode ==1){
        expected = "{ \"@context\": {\n" +
                "    \"name\": \"" + DEFAULT_BASE_SCH_NS + "name\"\n" +
                "  },\"@graph\": [" +
                "{ \"@id\": \"http://me.markus-lanthaler.com/\",\n" +
                "  \"name\": \"Markus Lanthaler\"} , " +
                "{ \"@id\": \"http://manu.sporny.org/about#manu\",\n" +
                "  \"name\": \"Manu Sporny\" },\n" +
                "{ \"@id\": \""+ bnodeUri +"\",\n" +
                "  \"name\": \"Dave Longley\" }\n" +
                "]}";
      } else if ( mode ==2 ){
        expected = "{ \"@context\": {\n" +
                "    \"name\": \"" + DEFAULT_BASE_SCH_NS + "name\"\n" +
                "  },\"@graph\": [" +
                "{ \"@id\": \"http://me.markus-lanthaler.com/\",\n" +
                "  \"name\": \"Markus Lanthaler\"} , " +
                "{ \"@id\": \"http://manu.sporny.org/about#manu\",\n" +
                "  \"name\": [\"MS\", \"Mr Sporny\",\"Manu Sporny\"] },\n" +
                "{ \"@id\": \""+ bnodeUri +"\",\n" +
                "  \"name\": \"Dave Longley\" }\n" +
                "]}";
      }


      assertTrue(ModelTestUtils
              .compareModels(expected, RDFFormat.JSONLD,
                      getNTriplesGraphFromSPOPattern(session,null,DEFAULT_BASE_SCH_NS + "name", null, false, null, null), RDFFormat.NTRIPLES));


      expected = "{ \"@context\": {\n" +
              "    \"name\": \"" + DEFAULT_BASE_SCH_NS + "name\"\n" +
              "  }, " +
              " \"@id\": \"http://me.markus-lanthaler.com/\",\n" +
              "  \"name\": \"Markus Lanthaler\"}";

      assertTrue(ModelTestUtils
              .compareModels(expected, RDFFormat.JSONLD,
                      getNTriplesGraphFromSPOPattern(session,null,DEFAULT_BASE_SCH_NS + "name", "Markus Lanthaler", true, "http://www.w3.org/2001/XMLSchema#string", null), RDFFormat.NTRIPLES));


      assertTrue(ModelTestUtils
              .compareModels(expected, RDFFormat.JSONLD,
                      getNTriplesGraphFromSPOPattern(session,null,null, "Markus Lanthaler", true, "http://www.w3.org/2001/XMLSchema#string", null), RDFFormat.NTRIPLES));

      expected = "<" + bnodeUri + "> <" + DEFAULT_BASE_SCH_NS + "name> \"Dave Longley\"^^<http://www.w3.org/2001/XMLSchema#string> .";

      assertTrue(ModelTestUtils
              .compareModels(expected, RDFFormat.NTRIPLES,
                      getNTriplesGraphFromSPOPattern(session,null,null, "Dave Longley", true, "http://www.w3.org/2001/XMLSchema#string", null), RDFFormat.NTRIPLES));

      expected = "<http://me.markus-lanthaler.com/> <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <" + DEFAULT_BASE_SCH_NS + "Individual> .\n" +
              "<http://manu.sporny.org/about#manu> <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <" + DEFAULT_BASE_SCH_NS + "Subject> .\n" +
              "<http://manu.sporny.org/about#manu> <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <" + DEFAULT_BASE_SCH_NS + "Citizen> .";

      assertTrue(ModelTestUtils
              .compareModels(expected, RDFFormat.NTRIPLES,
                      getNTriplesGraphFromSPOPattern(session,null,"http://www.w3.org/1999/02/22-rdf-syntax-ns#type", null, false, null, null), RDFFormat.NTRIPLES));

      expected = " <http://manu.sporny.org/about#manu> <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <" + DEFAULT_BASE_SCH_NS + "Subject> . ";

      assertTrue(ModelTestUtils
              .compareModels(expected, RDFFormat.NTRIPLES,
                      getNTriplesGraphFromSPOPattern(session,null,"http://www.w3.org/1999/02/22-rdf-syntax-ns#type",  DEFAULT_BASE_SCH_NS + "Subject", false, null, null), RDFFormat.NTRIPLES));

      assertTrue(ModelTestUtils
              .compareModels("", RDFFormat.NTRIPLES,
                      getNTriplesGraphFromSPOPattern(session,null,"http://www.w3.org/1999/02/22-rdf-syntax-ns#type", "http://xmlns.com/foaf/0.1/Subject", false, null, null), RDFFormat.NTRIPLES));


      if( mode ==1 ) {
        expected = "{\n" +
                "  \"@context\": {\n" +
                "    \"name\": \"" + DEFAULT_BASE_SCH_NS + "name\",\n" +
                "    \"knows\": \"" + DEFAULT_BASE_SCH_NS + "knows\",\n" +
                "    \"modified\": \"" + DEFAULT_BASE_SCH_NS + "modified\"\n" +
                "  },\n" +
                "  \"@id\": \"http://me.markus-lanthaler.com/\",\n" +
                "  \"name\": \"Markus Lanthaler\",\n" +
                "  \"@type\": \"" + DEFAULT_BASE_SCH_NS + "Individual\",\n" +
                "  \"knows\": [\n" +
                "    {\n" +
                "      \"@id\": \"http://manu.sporny.org/about#manu\",\n" +
                "      \"name\": [\"Manu Sporny\"] ,\n" +
                "      \"@type\": [\"" + DEFAULT_BASE_SCH_NS + "Subject\"," +
                "                   \"" + DEFAULT_BASE_SCH_NS + "Citizen\"]\n" +
                "    },\n" +
                "    {\n" +
                "      \"@id\": \"" + bnodeUri + "\",\n" +
                "      \"name\": \"Dave Longley\",\n" +
                "\t  \"modified\":\n" +
                "\t    {\n" +
                "\t      \"@value\": \"2010-05-29T14:17:39\",\n" +
                "\t      \"@type\": \"http://www.w3.org/2001/XMLSchema#dateTime\"\n" +
                "\t    }\n" +
                "    }\n" +
                "  ]\n" +
                "}";
      } else if ( mode == 2){
        expected = "{\n" +
                "  \"@context\": {\n" +
                "    \"name\": \"" + DEFAULT_BASE_SCH_NS + "name\",\n" +
                "    \"knows\": \"" + DEFAULT_BASE_SCH_NS + "knows\",\n" +
                "    \"modified\": \"" + DEFAULT_BASE_SCH_NS + "modified\"\n" +
                "  },\n" +
                "  \"@id\": \"http://me.markus-lanthaler.com/\",\n" +
                "  \"name\": \"Markus Lanthaler\",\n" +
                "  \"@type\": \"" + DEFAULT_BASE_SCH_NS + "Individual\",\n" +
                "  \"knows\": [\n" +
                "    {\n" +
                "      \"@id\": \"http://manu.sporny.org/about#manu\",\n" +
                "      \"name\": [\"MS\", \"Mr Sporny\",\"Manu Sporny\"] ,\n" +
                "      \"@type\": [\"" + DEFAULT_BASE_SCH_NS + "Subject\"," +
                "                   \"" + DEFAULT_BASE_SCH_NS + "Citizen\"]\n" +
                "    },\n" +
                "    {\n" +
                "      \"@id\": \"" + bnodeUri + "\",\n" +
                "      \"name\": \"Dave Longley\",\n" +
                "\t  \"modified\":\n" +
                "\t    {\n" +
                "\t      \"@value\": \"2010-05-29T14:17:39\",\n" +
                "\t      \"@type\": \"http://www.w3.org/2001/XMLSchema#dateTime\"\n" +
                "\t    }\n" +
                "    }\n" +
                "  ]\n" +
                "}";;
      }

      assertTrue(ModelTestUtils
              .compareModels(expected, RDFFormat.JSONLD,
                      getNTriplesGraphFromSPOPattern(session,null,null, null, false, null, null), RDFFormat.NTRIPLES));


    }
  }

  private String getNTriplesGraphFromSPOPattern(Session session,  String s, String p, String o, Boolean lit, String type, String lang) {
    Result res
        = session
        .run(" CALL n10s.rdf.export.spo(" + (s!=null?"'"+s+"'":"null") + ","
                + (p!=null?"'"+p+"'":"null") + "," + (o!=null?"'"+o+"'":"null") + ","
                + lit +"," + (type!=null?"'"+type+"'":"null") + "," + (lang!=null?"'"+lang+"'":"null") +") ");
    StringBuilder sb = new StringBuilder();
    while (res.hasNext()) {
      //System.out.println(res.next());
      Record record = res.next();
      sb.append("<").append(record.get("subject").asString()).append("> ");
      sb.append("<").append(record.get("predicate").asString()).append("> ");
      if(record.get("isLiteral").asBoolean()){
        if (!record.get("literalLang").isNull()) {
          sb.append("\"").append(record.get("object").asString()).append("\"@").append(record.get("literalLang").asString()) ;
        } else {
          sb.append("\"").append(record.get("object").asString()).append("\"^^<").append(record.get("literalType").asString()).append(">");
        }
      } else{
        sb.append("<").append(record.get("object").asString()).append("> ");
      }
      sb.append(".\n");
    }
    return sb.toString();
  }

  private String getNTriplesGraphFromSPOPatternLiteralDefaults(Session session,  String s, String p, String o) {
    Result res
            = session
            .run(" CALL n10s.rdf.export.spo(" + (s!=null?"'"+s+"'":"null") + ","
                    + (p!=null?"'"+p+"'":"null") + "," + (o!=null?"'"+o+"'":"null") + ",true) ");
    StringBuilder sb = new StringBuilder();
    while (res.hasNext()) {
      //System.out.println(res.next());
      Record record = res.next();
      sb.append("<").append(record.get("subject").asString()).append("> ");
      sb.append("<").append(record.get("predicate").asString()).append("> ");
      if(record.get("isLiteral").asBoolean()){
        if (!record.get("literalLang").isNull()) {
          sb.append("\"").append(record.get("object").asString()).append("\"@").append(record.get("literalLang").asString()) ;
        } else {
          sb.append("\"").append(record.get("object").asString()).append("\"^^<").append(record.get("literalType").asString()).append(">");
        }
      } else{
        sb.append("<").append(record.get("object").asString()).append("> ");
      }
      sb.append(".\n");
    }
    return sb.toString();
  }

  private void initialiseGraphDB(GraphDatabaseService db, String graphConfigParams) {
    db.executeTransactionally("CREATE CONSTRAINT n10s_unique_uri "
        + "ON (r:Resource) ASSERT r.uri IS UNIQUE");
    db.executeTransactionally("CALL n10s.graphconfig.init(" +
        (graphConfigParams != null ? graphConfigParams : "{}") + ")");
  }


  private String jsonLdFragment = "{\n" +
      "  \"@context\": {\n" +
      "    \"name\": \"http://xmlns.com/foaf/0.1/name\",\n" +
      "    \"knows\": \"http://xmlns.com/foaf/0.1/knows\",\n" +
      "\t\"modified\": \"http://xmlns.com/foaf/0.1/modified\"\n" +
      "  },\n" +
      "  \"@id\": \"http://me.markus-lanthaler.com/\",\n" +
      "  \"name\": \"Markus Lanthaler\",\n" +
      "  \"@type\": \"http://xmlns.com/foaf/0.1/Individual\",\n" +
      "  \"knows\": [\n" +
      "    {\n" +
      "      \"@id\": \"http://manu.sporny.org/about#manu\",\n" +
      "      \"name\": [\"MS\", \"Mr Sporny\",\"Manu Sporny\"] ,\n" +
      "      \"@type\": [\"http://xmlns.com/foaf/0.1/Subject\"," +
      "                   \"http://xmlns.com/foaf/0.1/Citizen\"]\n" +
      "    },\n" +
      "    {\n" +
      "      \"name\": \"Dave Longley\",\n" +
      "\t  \"modified\":\n" +
      "\t    {\n" +
      "\t      \"@value\": \"2010-05-29T14:17:39\",\n" +
      "\t      \"@type\": \"http://www.w3.org/2001/XMLSchema#dateTime\"\n" +
      "\t    }\n" +
      "    }\n" +
      "  ]\n" +
      "}";

  private String turtleFragment = "@prefix neo4voc: <http://neo4j.org/vocab/sw#> .\n" +
          "@prefix neo4ind: <http://neo4j.org/ind#> .\n" +
          "\n" +
          "neo4ind:nsmntx3502 neo4voc:name \"NSMNTX\" ;\n" +
          "\t\t\t   a neo4voc:Neo4jPlugin ;\n" +
          "\t\t\t   neo4voc:version \"3.5.0.2\" ;\n" +
          "\t\t\t   neo4voc:releaseDate \"03-06-2019\" ;\n" +
          "\t\t\t   neo4voc:runsOn neo4ind:neo4j355 .\n" +
          "\n" +
          "neo4ind:apoc3502 neo4voc:name \"APOC\" ;\n" +
          "\t\t\t   a neo4voc:Neo4jPlugin ;\n" +
          "\t\t\t   neo4voc:version \"3.5.0.4\" ;\n" +
          "\t\t\t   neo4voc:releaseDate \"05-31-2019\" ;\n" +
          "\t\t\t   neo4voc:runsOn neo4ind:neo4j355 .\n" +
          "\n" +
          "neo4ind:graphql3502 neo4voc:name \"Neo4j-GraphQL\" ;\n" +
          "\t\t\t   a neo4voc:Neo4jPlugin ;\n" +
          "\t\t\t   neo4voc:version \"3.5.0.3\" ;\n" +
          "\t\t\t   neo4voc:releaseDate \"05-05-2019\" ;\n" +
          "\t\t\t   neo4voc:runsOn neo4ind:neo4j355 .\n" +
          "\n" +
          "neo4ind:neo4j355 neo4voc:name \"neo4j\" ;\n" +
          "\t\t\t   a neo4voc:GraphPlatform , neo4voc:AwesomePlatform ;\n" +
          "\t\t\t   neo4voc:version \"3.5.5\" .\n";

}
