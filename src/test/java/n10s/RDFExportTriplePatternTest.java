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

public class RDFExportTriplePatternTest {
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
  public void testExportFromTriplePatternOnRDFGraphShortenDefault() throws Exception {
    try (Session session = driver.session();) {
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
    try (Session session = driver.session();) {
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
    try (Session session = driver.session();) {
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
    try (Session session = driver.session();) {
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
    try (Session session = driver.session();) {
      initialiseGraphDB(neo4j.defaultDatabaseService(),
              " { handleVocabUris: 'SHORTEN_STRICT' } ");

      session.run("call n10s.nsprefixes.add('foaf','http://xmlns.com/foaf/0.1/')");
      session.run("call n10s.nsprefixes.add('rdf','http://www.w3.org/1999/02/22-rdf-syntax-ns#')");
      assertEquals(2L, session.run("call n10s.nsprefixes.list() yield prefix return count(*) as ct").next().get("ct").asLong());

    }
    try (Session session = driver.session();) {
      Result importResults1 = session.run("CALL n10s.rdf.import.inline('" +
              jsonLdFragment + "','JSON-LD')");
      assertEquals(11L, importResults1.single().get("triplesLoaded").asLong());

    }
    allTriplePatterns(1);
  }

  @Test
  public void testExportFromTriplePatternOnRDFGraphShortenTypesAsNodes2() throws Exception {
    try (Session session = driver.session();) {
      initialiseGraphDB(neo4j.defaultDatabaseService(),
              " { handleRDFTypes: 'LABELS_AND_NODES'} ");

      Result importResults1 = session.run("CALL n10s.rdf.import.inline('" +
              turtleFragment + "','Turtle')");
      assertEquals(19L, importResults1.single().get("triplesLoaded").asLong());

    }

    try (Session session = driver.session();) {
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
    try (Session session = driver.session();) {
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
    try (Session session = driver.session();) {
      initialiseGraphDB(neo4j.defaultDatabaseService(),
              " { handleVocabUris: 'IGNORE' , handleMultival: 'ARRAY'} ");

      Result importResults1 = session.run("CALL n10s.rdf.import.inline('" +
              jsonLdFragment + "','JSON-LD')");
      assertEquals(11L, importResults1.single().get("triplesLoaded").asLong());

    }
    allTriplePatternsIgnore(2);
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

  private void allTriplePatterns( int mode) throws IOException {
    try (Session session = driver.session()) {

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
                "\t      \"@value\": \"2010-05-29T14:17:39.262+02:00\",\n" +
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
                "\t      \"@value\": \"2010-05-29T14:17:39.262+02:00\",\n" +
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
    try (Session session = driver.session()) {

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
                "\t      \"@value\": \"2010-05-29T14:17:39.262+02:00\",\n" +
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
                "\t      \"@value\": \"2010-05-29T14:17:39.262+02:00\",\n" +
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
