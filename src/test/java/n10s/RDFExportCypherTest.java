package n10s;

import n10s.aux.AuxProcedures;
import n10s.graphconfig.GraphConfigProcedures;
import n10s.mapping.MappingUtils;
import n10s.nsprefixes.NsPrefixDefProcedures;
import n10s.rdf.RDFProcedures;
import n10s.rdf.export.RDFExportProcedures;
import n10s.rdf.load.RDFLoadProcedures;
import n10s.validation.ValidationProcedures;
import org.eclipse.rdf4j.model.*;
import org.eclipse.rdf4j.model.impl.SimpleValueFactory;
import org.eclipse.rdf4j.model.vocabulary.FOAF;
import org.eclipse.rdf4j.model.vocabulary.RDF;
import org.eclipse.rdf4j.model.vocabulary.XSD;
import org.eclipse.rdf4j.rio.RDFFormat;
import org.junit.*;
import org.neo4j.driver.*;
import org.neo4j.driver.Record;
import org.neo4j.driver.internal.value.NodeValue;
import org.neo4j.driver.types.Node;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.harness.junit.rule.Neo4jRule;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static n10s.CommonProcedures.UNIQUENESS_CONSTRAINT_ON_URI;
import static n10s.CommonProcedures.UNIQUENESS_CONSTRAINT_STATEMENT;
import static n10s.graphconfig.Params.*;
import static org.junit.Assert.*;

public class RDFExportCypherTest {
  public static Driver driver;

  @ClassRule
  public static Neo4jRule neo4j = new Neo4jRule()
          .withProcedure(RDFExportProcedures.class)
          .withProcedure(MappingUtils.class)
          .withProcedure(RDFLoadProcedures.class)
          .withProcedure(GraphConfigProcedures.class)
          .withProcedure(NsPrefixDefProcedures.class);

  @BeforeClass
  public static void init() {
    driver = GraphDatabase.driver(neo4j.boltURI(),
            Config.builder().withoutEncryption().build());
  }

  @Before
  public void cleanDatabase() {
    driver.session().run("match (n) detach delete n").consume();
    driver.session().run("drop constraint n10s_unique_uri if exists").consume();
  }

  @Test
  public void testExportFromCypherOnLPG() throws Exception {
    try (Session session = driver.session()) {

      Record record = session
              .run("CREATE (n0:Node { a: 1, b: 'hello' })-[:CONNECTED_TO]->(n1:Node {  a:2, b2:'bye@en'}) return n0, n1")
              .next();

      Map<String, String> idMap = new HashMap<>();
      idMap.put( "0", String.valueOf( ((NodeValue) record.get("n0")).asNode().id() ));
      idMap.put( "1", String.valueOf( ((NodeValue) record.get("n1")).asNode().id() ));

      Result res
          = session
          .run(" CALL n10s.rdf.export.cypher(' MATCH path = (n)-[r]->(m) RETURN path ', {}) ");
      assertTrue(res.hasNext());

      final ValueFactory vf = SimpleValueFactory.getInstance();
      Set<Statement> expectedStatememts = new HashSet<>(Arrays.asList(
              vf.createStatement(vf.createIRI(BASE_INDIV_NS + idMap.get("0")), RDF.TYPE, vf.createIRI(DEFAULT_BASE_SCH_NS + "Node")),
              vf.createStatement(vf.createIRI(BASE_INDIV_NS + idMap.get("0")), vf.createIRI(DEFAULT_BASE_SCH_NS + "a"), vf.createLiteral(1L)),
              vf.createStatement(vf.createIRI(BASE_INDIV_NS + idMap.get("0")), vf.createIRI(DEFAULT_BASE_SCH_NS + "b"), vf.createLiteral("hello")),
              vf.createStatement(vf.createIRI(BASE_INDIV_NS + idMap.get("0")), vf.createIRI(DEFAULT_BASE_SCH_NS + "CONNECTED_TO"), vf.createIRI(BASE_INDIV_NS + idMap.get("1"))),
              vf.createStatement(vf.createIRI(BASE_INDIV_NS + idMap.get("1")), RDF.TYPE, vf.createIRI(DEFAULT_BASE_SCH_NS + "Node")),
              vf.createStatement(vf.createIRI(BASE_INDIV_NS + idMap.get("1")), vf.createIRI(DEFAULT_BASE_SCH_NS + "b2"), vf.createLiteral("bye","en")),
              vf.createStatement(vf.createIRI(BASE_INDIV_NS + idMap.get("1")), vf.createIRI(DEFAULT_BASE_SCH_NS + "a"), vf.createLiteral(2L))));

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
  public void testExportFromCypherOnLPGPropsOnRels() throws Exception {
    try (Session session = driver.session()) {

      Record record = session
              .run("CREATE (n0:Node { a: 1, b: 'hello' })-[:CONNECTED_TO" +
                       " { since: 12345 , kind: 'principal' }]->(n1:Node {  a:2, b2:'bye@en'}) return n0, n1")
              .next();

      Map<String, String> idMap = new HashMap<>();
      idMap.put( "0", String.valueOf( ((NodeValue) record.get("n0")).asNode().id() ));
      idMap.put( "1", String.valueOf( ((NodeValue) record.get("n1")).asNode().id() ));

      Result res
              = session
              .run(" CALL n10s.rdf.export.cypher(' MATCH path = (n)-[r]->(m) RETURN path ', {}) ");
      assertTrue(res.hasNext());

      final ValueFactory vf = SimpleValueFactory.getInstance();
      Set<Statement> expectedStatememts = new HashSet<>(Arrays.asList(
              vf.createStatement(vf.createIRI(BASE_INDIV_NS + idMap.get("0")), RDF.TYPE, vf.createIRI(DEFAULT_BASE_SCH_NS + "Node")),
              vf.createStatement(vf.createIRI(BASE_INDIV_NS + idMap.get("0")), vf.createIRI(DEFAULT_BASE_SCH_NS + "a"), vf.createLiteral(1L)),
              vf.createStatement(vf.createIRI(BASE_INDIV_NS + idMap.get("0")), vf.createIRI(DEFAULT_BASE_SCH_NS + "b"), vf.createLiteral("hello")),
              vf.createStatement(vf.createIRI(BASE_INDIV_NS + idMap.get("0")), vf.createIRI(DEFAULT_BASE_SCH_NS + "CONNECTED_TO"), vf.createIRI(BASE_INDIV_NS + idMap.get("1"))),
              vf.createStatement(vf.createIRI(BASE_INDIV_NS + idMap.get("1")), RDF.TYPE, vf.createIRI(DEFAULT_BASE_SCH_NS + "Node")),
              vf.createStatement(vf.createIRI(BASE_INDIV_NS + idMap.get("1")), vf.createIRI(DEFAULT_BASE_SCH_NS + "b2"), vf.createLiteral("bye","en")),
              vf.createStatement(vf.createIRI(BASE_INDIV_NS + idMap.get("1")), vf.createIRI(DEFAULT_BASE_SCH_NS + "a"), vf.createLiteral(2L)),
              vf.createStatement(vf.createTriple(vf.createIRI(BASE_INDIV_NS + idMap.get("0")), vf.createIRI(DEFAULT_BASE_SCH_NS + "CONNECTED_TO"), vf.createIRI(BASE_INDIV_NS + idMap.get("1"))),
                      vf.createIRI(DEFAULT_BASE_SCH_NS + "since"), vf.createLiteral(12345L)),
              vf.createStatement(vf.createTriple(vf.createIRI(BASE_INDIV_NS + idMap.get("0")), vf.createIRI(DEFAULT_BASE_SCH_NS + "CONNECTED_TO"), vf.createIRI(BASE_INDIV_NS + idMap.get("1"))),
                      vf.createIRI(DEFAULT_BASE_SCH_NS + "kind"), vf.createLiteral("principal"))));

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
  public void testCypherOnRDFGraphPropsOnRels() throws Exception {
    try (Session session = driver.session();) {
      initialiseGraphDB(neo4j.defaultDatabaseService(),
              " { handleVocabUris: 'SHORTEN_STRICT' } ");

      session.run("call n10s.nsprefixes.add('msc','http://neo4j.com/voc/music#')");

      assertEquals(1L, session.run("call n10s.nsprefixes.list() yield prefix return count(*) as ct").next().get("ct").asLong());

    }

    try (Session session = driver.session();) {
      Result importResults1 = session.run("CALL n10s.rdf.import.fetch('" +
              RDFExportCypherTest.class.getClassLoader().getResource("rdfstar/beatles.ttls")
                      .toURI() + "','Turtle-star')");
      assertEquals(14L, importResults1.single().get("triplesLoaded").asLong());

    }

    try (Session session = driver.session();) {
      Result res
              = session
              .run(" CALL n10s.rdf.export.cypher(' MATCH path = (n)-[r]->(m) RETURN path ') ");

      assertTrue(res.hasNext());

      final ValueFactory vf = SimpleValueFactory.getInstance();

      String inputAsString = Files.readString(Paths.get(RDFExportCypherTest.class.getClassLoader().getResource("rdfstar/beatles.ttls")
              .toURI()));

      Model expected = ModelTestUtils.getAsModel(inputAsString, RDFFormat.TURTLESTAR);


      int resultCount = 0;
      while (res.hasNext()) {
        Statement returnedStatement = recordAsStatement(vf, res.next());
        assertTrue(expected.contains(returnedStatement));
        resultCount++;
      }
      assertEquals(resultCount, expected.size());
    }

  }

  @Test
  public void testSPOExportOnRDFGraphPropsOnRels() throws Exception {
    try (Session session = driver.session();) {
      initialiseGraphDB(neo4j.defaultDatabaseService(),
              " { handleVocabUris: 'SHORTEN_STRICT' } ");

      session.run("call n10s.nsprefixes.add('msc','http://neo4j.com/voc/music#')");

      assertEquals(1L, session.run("call n10s.nsprefixes.list() yield prefix return count(*) as ct").next().get("ct").asLong());
    }

    try (Session session = driver.session();) {
      Result importResults1 = session.run("CALL n10s.rdf.import.fetch('" +
              RDFExportCypherTest.class.getClassLoader().getResource("rdfstar/beatles.ttls")
                      .toURI() + "','Turtle-star')");
      assertEquals(14L, importResults1.single().get("triplesLoaded").asLong());
    }

    try (Session session = driver.session();) {
      Result res
              = session.run(" CALL n10s.rdf.export.spo(null,null,null)");

      assertTrue(res.hasNext());

      final ValueFactory vf = SimpleValueFactory.getInstance();

      String inputAsString = Files.readString(Paths.get(RDFExportCypherTest.class.getClassLoader().getResource("rdfstar/beatles.ttls")
              .toURI()));

      Model expected = ModelTestUtils.getAsModel(inputAsString, RDFFormat.TURTLESTAR);


      int resultCount = 0;
      while (res.hasNext()) {
        Statement returnedStatement = recordAsStatement(vf, res.next());
        assertTrue(expected.contains(returnedStatement));
        resultCount++;
      }
      assertEquals(resultCount, expected.size());
    }
  }

  @Test
  public void testExportFromCypherOnLPGWithMappings() throws Exception {
    try (Session session = driver.session()) {

      Record record = session
              .run("CREATE (n0:Node { a: 1, b: 'hello' })-[:CONNECTED_TO]->(n1:Node {  a:2, b2:'bye@en'}) return n0, n1")
              .next();

      Map<String, String> idMap = new HashMap<>();
      idMap.put( "0", String.valueOf( ((NodeValue) record.get("n0")).asNode().id() ));
      idMap.put( "1", String.valueOf( ((NodeValue) record.get("n1")).asNode().id() ));

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
              vf.createStatement(vf.createIRI(BASE_INDIV_NS + idMap.get("0")), RDF.TYPE, vf.createIRI("http://xmlns.com/foaf/0.1/Thang")),
              vf.createStatement(vf.createIRI(BASE_INDIV_NS + idMap.get("0")), vf.createIRI("http://myvoc.org/testing#propA"), vf.createLiteral(1L)),
              vf.createStatement(vf.createIRI(BASE_INDIV_NS + idMap.get("0")), vf.createIRI("http://myvoc.org/testing#propB"), vf.createLiteral("hello")),
              vf.createStatement(vf.createIRI(BASE_INDIV_NS + idMap.get("0")), vf.createIRI("http://xmlns.com/foaf/0.1/linkedTo"), vf.createIRI(BASE_INDIV_NS + idMap.get("1"))),
              vf.createStatement(vf.createIRI(BASE_INDIV_NS + idMap.get("1")), RDF.TYPE, vf.createIRI("http://xmlns.com/foaf/0.1/Thang")),
              vf.createStatement(vf.createIRI(BASE_INDIV_NS + idMap.get("1")), vf.createIRI(DEFAULT_BASE_SCH_NS + "b2"), vf.createLiteral("bye","en")),
              vf.createStatement(vf.createIRI(BASE_INDIV_NS + idMap.get("1")), vf.createIRI("http://myvoc.org/testing#propA"), vf.createLiteral(2L))));

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
    try (Session session = driver.session();) {

      initialiseGraphDB(neo4j.defaultDatabaseService(),
              " { handleVocabUris: 'SHORTEN' } ");

    }
    try (Session session = driver.session()) {

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

  @Test
  public void testExportFromTriplePatternNoGraphConfig() throws Exception {
    try (Session session = driver.session();) {
      Transaction tx = session.beginTransaction();
      tx.run(Files.readString(Paths.get(
              RDFExportCypherTest.class.getClassLoader().getResource("movies.cypher").getPath())));
      tx.run("MERGE (pb:Person {name:'Paul Blythe'}) SET pb:Critic ");
      tx.run( "MERGE (as:Person {name:'Angela Scope'}) SET as:Critic " );
      tx.run( "MERGE (jt:Person {name:'Jessica Thompson'}) SET jt:Critic " );
      tx.run("MERGE (jt2:Person {name:'James Thompson'}) SET jt2:Critic ");
      tx.commit();
    }
    allTriplePatternsOnLPG();
  }

  @Test
  public void testExportFromCypherOnLPGPointTypeProperties() throws Exception {
    try (Session session = driver.session()) {

      Map<String, String> idMap = new HashMap<>();
      {
        Record record = session
                .run("CREATE (n0:GeoLocatedThing { hi: 'hello' , where: point({x: -0.1275, y: 51.507222222}) }) return n0")
                .next();


        idMap.put("0", String.valueOf(((NodeValue) record.get("n0")).asNode().id()));
      }
      Result res
              = session
              .run(" CALL n10s.rdf.export.cypher(' MATCH (n:GeoLocatedThing) RETURN n ', {}) ");
      assertTrue(res.hasNext());

      final ValueFactory vf = SimpleValueFactory.getInstance();
      Set<Statement> expectedStatememts = new HashSet<>(Arrays.asList(
              vf.createStatement(vf.createIRI(BASE_INDIV_NS + idMap.get("0")), RDF.TYPE, vf.createIRI(DEFAULT_BASE_SCH_NS + "GeoLocatedThing")),
              vf.createStatement(vf.createIRI(BASE_INDIV_NS + idMap.get("0")), vf.createIRI(DEFAULT_BASE_SCH_NS + "hi"), vf.createLiteral("hello")),
              vf.createStatement(vf.createIRI(BASE_INDIV_NS + idMap.get("0")), vf.createIRI(DEFAULT_BASE_SCH_NS + "where"),
                      vf.createLiteral("Point(-0.1275 51.507222222)",vf.createIRI(GEOSPARQL_NS + WKTLITERAL)))));

      int resultCount = 0;
      while (res.hasNext()) {
        Statement returnedStatement = recordAsStatement(vf, res.next());
        assertTrue(expectedStatememts.contains(returnedStatement));
        resultCount++;
      }
      assertEquals(resultCount,expectedStatememts.size());

      {
        Record record = session
                .run("CREATE (n1:GeoLocatedThing3D { hi: 'hello' , where: point({x: -0.1275, y: 51.507222222, z: 34.0 })}) return n1")
                .next();

        idMap.put("1", String.valueOf(((NodeValue) record.get("n1")).asNode().id()));
      }
      res
              = session
              .run(" CALL n10s.rdf.export.cypher(' MATCH (n:GeoLocatedThing3D) RETURN n ', {}) ");
      assertTrue(res.hasNext());

      expectedStatememts = new HashSet<>(Arrays.asList(
              vf.createStatement(vf.createIRI(BASE_INDIV_NS + idMap.get("1")), RDF.TYPE, vf.createIRI(DEFAULT_BASE_SCH_NS + "GeoLocatedThing3D")),
              vf.createStatement(vf.createIRI(BASE_INDIV_NS + idMap.get("1")), vf.createIRI(DEFAULT_BASE_SCH_NS + "hi"), vf.createLiteral("hello")),
              vf.createStatement(vf.createIRI(BASE_INDIV_NS + idMap.get("1")), vf.createIRI(DEFAULT_BASE_SCH_NS + "where"),
                      vf.createLiteral("Point(-0.1275 51.507222222 34.0)",vf.createIRI(GEOSPARQL_NS + WKTLITERAL)))));

      resultCount = 0;
      while (res.hasNext()) {
        Statement returnedStatement = recordAsStatement(vf, res.next());
        assertTrue(expectedStatememts.contains(returnedStatement));
        resultCount++;
      }
      assertEquals(resultCount,expectedStatememts.size());
    }
  }

  private Statement recordAsStatement(ValueFactory vf,  Record r) {
    Resource s;
    if (!r.get("subjectSPO").isNull()){
      List<String> subjectSPO = r.get("subjectSPO").asList(Values.ofString());
      s = vf.createTriple(vf.createIRI(subjectSPO.get(0)),vf.createIRI(subjectSPO.get(1)),vf.createIRI(subjectSPO.get(2)));
    } else {
      s = vf.createIRI(r.get("subject").asString());
    }

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
      }else if (datatype.equals(vf.createIRI(GEOSPARQL_NS + WKTLITERAL))){
        o = vf.createLiteral(r.get("object").asString(), vf.createIRI(GEOSPARQL_NS + WKTLITERAL));
      } else {
        //string default
        o = vf.createLiteral(r.get("object").asString());
      }
      return vf.createStatement(s, p, o);
    } else {
      return vf.createStatement(s, p, vf.createIRI(r.get("object").asString()));
    }
  }

  private void allTriplePatternsOnLPG() throws IOException {
    try (Session session = driver.session()) {
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
              "  \"@id\" : \"n4ind:" + emilId + "\",\n" +
              "  \"@type\" : \"n4sch:Person\",\n" +
              "  \"n4sch:ACTED_IN\" : {\n" +
              "    \"@id\" : \"n4ind:" + theMatrixId + "\"\n" +
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
              "  \"@id\" : \"n4ind:" + emilId + "\",\n" +
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
              "  \"@id\" : \"n4ind:" + emilId + "\",\n" +
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
              "  \"@id\" : \"n4ind:" + emilId + "\",\n" +
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
              "  \"@id\" : \"n4ind:" + emilId + "\",\n" +
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
              "  \"@id\" : \"n4ind:" + emilId + "\",\n" +
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

      StringBuilder titleTriplesSb = new StringBuilder();
      Result titlesQueryResult = session.run("MATCH (n:Movie) RETURN id(n) as id, n.title as title ");
      while(titlesQueryResult.hasNext()){
        Record movie = titlesQueryResult.next();
        titleTriplesSb.append("<neo4j://graph.individuals#").append(movie.get("id").asLong()).append("> <neo4j://graph.schema#title> \"")
                .append(movie.get("title").asString()).append("\" .\n");
      }

      assertTrue(ModelTestUtils
              .compareModels(titleTriplesSb.toString(), RDFFormat.NTRIPLES,
                      getNTriplesGraphFromSPOPattern(session,null,DEFAULT_BASE_SCH_NS + "title", null, false, null, null), RDFFormat.NTRIPLES));

      expected = "{\n" +
              "  \"@id\" : \"n4ind:" + emilId + "\",\n" +
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

      StringBuilder someMoviesSb = new StringBuilder();
      Result someMoviesQueryResult = session.run("MATCH (n:Movie) RETURN id(n) as id skip 30 limit 10");
      while(someMoviesQueryResult.hasNext()){
        Record movie = someMoviesQueryResult.next();
        someMoviesSb.append("<neo4j://graph.individuals#").append(movie.get("id").asLong())
                .append("> <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <neo4j://graph.schema#Movie> .\n");
      }

      assertTrue(ModelTestUtils
              .modelContains(getNTriplesGraphFromSPOPattern(session,null,"http://www.w3.org/1999/02/22-rdf-syntax-ns#type", null, false, null, null), RDFFormat.NTRIPLES,
              criticTypesTriples.toString() + someMoviesSb.toString(), RDFFormat.NTRIPLES));

      StringBuilder criticTypesTriples2  = new StringBuilder();
      critics.forEach(id-> criticTypesTriples2.append("<" + BASE_INDIV_NS + id + "> <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <" + DEFAULT_BASE_SCH_NS +  "Critic> .\n") );

      assertTrue(ModelTestUtils
              .compareModels(criticTypesTriples2.toString(), RDFFormat.NTRIPLES,
                      getNTriplesGraphFromSPOPattern(session,null,"http://www.w3.org/1999/02/22-rdf-syntax-ns#type", DEFAULT_BASE_SCH_NS +  "Critic", false, null, null), RDFFormat.NTRIPLES));

      String allGraphAsNTriples = getNTriplesGraphFromSPOPattern(session, null, null, null, false, null, null);
      assertTrue(ModelTestUtils.modelContains(allGraphAsNTriples, RDFFormat.NTRIPLES,
                      criticTypesTriples.toString() + someMoviesSb.toString() + titleTriplesSb.toString(), RDFFormat.NTRIPLES ));

      assertFalse(ModelTestUtils.modelContains(allGraphAsNTriples, RDFFormat.NTRIPLES,
              "<neo4j://graph.individuals#0> <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <neo4j://graph.schema#AwesomeMovie> ." , RDFFormat.NTRIPLES ));

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
      if(record.get("subjectSPO").isNull()) {
        // we are skipping the RDF-star triples for these tests.
        // No N-Triples serialisation for rdf-star.
        sb.append("<").append(record.get("subject").asString()).append("> ");
        sb.append("<").append(record.get("predicate").asString()).append("> ");
        if (record.get("isLiteral").asBoolean()) {
          if (!record.get("literalLang").isNull()) {
            sb.append("\"").append(record.get("object").asString()).append("\"@").append(record.get("literalLang").asString());
          } else {
            sb.append("\"").append(record.get("object").asString()).append("\"^^<").append(record.get("literalType").asString()).append(">");
          }
        } else {
          sb.append("<").append(record.get("object").asString()).append("> ");
        }
        sb.append(".\n");
      }
    }
    return sb.toString();
  }

  private void initialiseGraphDB(GraphDatabaseService db, String graphConfigParams) {
    db.executeTransactionally(UNIQUENESS_CONSTRAINT_STATEMENT);
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
      "\t      \"@value\": \"2010-05-29T14:17:39.262+02:00\",\n" +
      "\t      \"@type\": \"http://www.w3.org/2001/XMLSchema#dateTime\"\n" +
      "\t    }\n" +
      "    }\n" +
      "  ]\n" +
      "}";
}
