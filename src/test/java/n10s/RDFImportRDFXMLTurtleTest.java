package n10s;

import static n10s.CommonProcedures.UNIQUENESS_CONSTRAINT_ON_URI;
import static n10s.CommonProcedures.UNIQUENESS_CONSTRAINT_STATEMENT;
import static n10s.graphconfig.Params.PREFIX_SEPARATOR;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.neo4j.driver.Values.NULL;
import static org.neo4j.driver.Values.ofNode;

import java.net.URI;
import java.net.URISyntaxException;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.OffsetDateTime;
import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import n10s.experimental.ExperimentalImports;
import n10s.graphconfig.GraphConfigProcedures;
import n10s.mapping.MappingUtils;
import n10s.nsprefixes.NsPrefixDefProcedures;
import n10s.quadrdf.delete.QuadRDFDeleteProcedures;
import n10s.quadrdf.load.QuadRDFLoadProcedures;
import n10s.rdf.RDFProcedures;
import n10s.rdf.delete.RDFDeleteProcedures;
import n10s.rdf.load.RDFLoadProcedures;
import n10s.rdf.preview.RDFPreviewProcedures;
import n10s.rdf.stream.RDFStreamProcedures;
import n10s.skos.load.SKOSLoadProcedures;
import org.eclipse.rdf4j.model.vocabulary.XMLSchema;
import org.junit.*;
import org.neo4j.driver.*;
import org.neo4j.driver.Record;
import org.neo4j.driver.internal.InternalRelationship;
import org.neo4j.driver.internal.value.IntegerValue;
import org.neo4j.driver.internal.value.ListValue;
import org.neo4j.driver.types.Point;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.harness.junit.rule.Neo4jRule;

public class RDFImportRDFXMLTurtleTest {
  public static Driver driver;

  @ClassRule
  public static Neo4jRule neo4j = new Neo4jRule()
          .withProcedure(RDFLoadProcedures.class)
          .withProcedure(RDFDeleteProcedures.class)
          .withProcedure(RDFPreviewProcedures.class)
          .withProcedure(RDFStreamProcedures.class)
          .withFunction(RDFProcedures.class)
          .withProcedure(QuadRDFLoadProcedures.class)
          .withProcedure(QuadRDFDeleteProcedures.class)
          .withProcedure(MappingUtils.class)
          .withProcedure(GraphConfigProcedures.class)
          .withProcedure(NsPrefixDefProcedures.class)
          .withProcedure(ExperimentalImports.class)
          .withProcedure(SKOSLoadProcedures.class);

  @BeforeClass
  public static void init() {
    driver = GraphDatabase.driver(neo4j.boltURI(),
            Config.builder().withoutEncryption().build());
  }

  @Before
  public void cleanDatabase() {
    driver.session().run("match (n) detach delete n").consume();
    driver.session().run("drop constraint n10s_unique_uri if exists").consume();
    driver.session().run("drop index uri_index if exists").consume();
  }

  final String CREATE_URI_INDEX = "CREATE INDEX uri_index FOR (n:Resource) ON (n.uri)";

  private String wrongUriTtl = "@prefix pr: <http://example.org/vocab/show/> .\n" +
          "pr:ent" +
          "      pr:P854 <https://suasprod.noc-science.at/XLCubedWeb/WebForm/ShowReport.aspx?rep=004+studierende%2f001+universit%u00e4ten%2f003+studierende+nach+universit%u00e4ten.xml&toolbar=true> ;\n"
          +
          "      pr:P813 \"2017-10-11T00:00:00Z\"^^xsd:dateTime .\n";

  private static URI file(String path) {
    try {
      return RDFImportRDFXMLTurtleTest.class.getClassLoader().getResource(path).toURI();
    } catch (URISyntaxException e) {
      String msg = String.format("Failed to load the resource with path '%s'", path);
      throw new RuntimeException(msg, e);
    }
  }

  private void initialiseGraphDB(GraphDatabaseService db, String graphConfigParams) {
    db.executeTransactionally(UNIQUENESS_CONSTRAINT_STATEMENT);
    db.executeTransactionally("CALL n10s.graphconfig.init(" +
            (graphConfigParams != null ? graphConfigParams : "{}") + ")");
  }

  @Test
  public void testImportRDFXML() throws Exception {
    try (Session session = driver.session()) {

      initialiseGraphDB(neo4j.defaultDatabaseService(),
              "{ handleVocabUris: 'KEEP', handleRDFTypes: 'LABELS' }");

      Result importResults
              = session.run("CALL n10s.rdf.import.fetch('" +
              RDFImportRDFXMLTurtleTest.class.getClassLoader()
                      .getResource("jeu-de-donnees-des-jeux-de-donnees-open-data-paris.rdf")
                      .toURI()
              + "','RDF/XML', { commitSize: 500 } )");
      assertEquals(38L, importResults
              .next().get("triplesLoaded").asLong());
      assertEquals(7L,
              session
                      .run("MATCH ()-[r:`http://purl.org/dc/terms/relation`]->(b) RETURN count(b) as count")
                      .next().get("count").asLong());
      assertEquals(
              "http://opendata.paris.fr/opendata/jsp/site/Portal.jsp?document_id=109&portlet_id=106",
              session.run(
                              "MATCH (x:Resource) WHERE x.`http://www.w3.org/2000/01/rdf-schema#label` = 'harvest_dataset_url'"
                                      +
                                      "\nRETURN x.`http://www.w3.org/1999/02/22-rdf-syntax-ns#value` AS datasetUrl")
                      .next().get("datasetUrl").asString());

    }
  }

  @Test
  public void testImportRDFXMLShortening() throws Exception {
    try (Session session = driver.session()) {

      initialiseGraphDB(neo4j.defaultDatabaseService(), null);

      Result importResults
              = session.run("CALL n10s.rdf.import.fetch('" +
              RDFImportRDFXMLTurtleTest.class.getClassLoader()
                      .getResource("jeu-de-donnees-des-jeux-de-donnees-open-data-paris.rdf")
                      .toURI()
              + "','RDF/XML',{ commitSize: 500})");
      assertEquals(38L, importResults
              .next().get("triplesLoaded").asLong());
      assertEquals(7L,
              session
                      .run("MATCH ()-[r]->(b) WHERE type(r) CONTAINS 'relation' RETURN count(b) as count")
                      .next().get("count").asLong());

      assertEquals(
              "http://opendata.paris.fr/opendata/jsp/site/Portal.jsp?document_id=109&portlet_id=106",
              session.run(
                              "MATCH (x:Resource) WHERE x.rdfs" + PREFIX_SEPARATOR + "label = 'harvest_dataset_url'"

                                      + "\nRETURN x.rdf" + PREFIX_SEPARATOR + "value AS datasetUrl").next()
                      .get("datasetUrl").asString());

      assertEquals("ns0",
              session.run("call n10s.nsprefixes.list() yield prefix, namespace\n"
                      + "with prefix, namespace where namespace = 'http://www.w3.org/ns/dcat#'\n"
                      + "return prefix, namespace").next().get("prefix").asString());

    }
  }

  @Test
  public void testImportRDFXMLShorteningWithPrefixPreDefinition() throws Exception {
    try (Session session = driver.session()) {

      initialiseGraphDB(neo4j.defaultDatabaseService(), null);
      session.run("call n10s.nsprefixes.add('dct','http://purl.org/dc/terms/')");
      session.run("call n10s.nsprefixes.add('rdf','http://www.w3.org/1999/02/22-rdf-syntax-ns#')");
      session.run("call n10s.nsprefixes.add('owl','http://www.w3.org/2002/07/owl#')");
      session.run("call n10s.nsprefixes.add('dcat','http://www.w3.org/ns/dcat#')");
      session.run("call n10s.nsprefixes.add('rdfs','http://www.w3.org/2000/01/rdf-schema#')");
      session.run("call n10s.nsprefixes.add('foaf','http://xmlns.com/foaf/0.1/')");

      Result importResults
              = session.run("CALL n10s.rdf.import.fetch('" +
              RDFImportRDFXMLTurtleTest.class.getClassLoader()
                      .getResource("jeu-de-donnees-des-jeux-de-donnees-open-data-paris.rdf")
                      .toURI()
              + "','RDF/XML', { handleVocabUris: 'SHORTEN', handleRDFTypes: 'LABELS', commitSize: 500})");
      assertEquals(38L, importResults
              .next().get("triplesLoaded").asLong());
      assertEquals(7L,
              session
                      .run("MATCH ()-[r:dct" + PREFIX_SEPARATOR + "relation]->(b) RETURN count(b) as count")
                      .next().get("count").asLong());

      assertEquals(
              "http://opendata.paris.fr/opendata/jsp/site/Portal.jsp?document_id=109&portlet_id=106",
              session
                      .run("MATCH (x) WHERE x.rdfs" + PREFIX_SEPARATOR + "label = 'harvest_dataset_url'" +
                              "\nRETURN x.rdf" + PREFIX_SEPARATOR + "value AS datasetUrl").next()
                      .get("datasetUrl").asString());

      assertEquals("dcat",
              session.run("call n10s.nsprefixes.list() yield prefix, namespace\n"
                              + " with prefix, namespace where namespace = 'http://www.w3.org/ns/dcat#' \n"
                              + " return prefix, namespace")
                      .next().get("prefix").asString());

    }
  }

  @Test
  public void testImportRDFXMLShorteningWithPrefixPreDefinitionOneTriple() throws Exception {
    try (Session session = driver.session()) {

      initialiseGraphDB(neo4j.defaultDatabaseService(), null);
      session.run("call n10s.nsprefixes.add('voc','http://neo4j.com/voc/')");

      Result importResults = session.run("CALL n10s.rdf.import.fetch('" +
              RDFImportRDFXMLTurtleTest.class.getClassLoader().getResource("oneTriple.rdf")
                      .toURI()
              + "','RDF/XML',{ handleVocabUris: 'SHORTEN', handleRDFTypes: 'LABELS', commitSize: 500})");
      assertEquals(1L, importResults.next().get("triplesLoaded").asLong());
      assertEquals("JB",
              session.run(
                              "MATCH (jb {uri: 'http://neo4j.com/invividual/JB'}) RETURN jb.voc" + PREFIX_SEPARATOR
                                      + "name AS name")
                      .next().get("name").asString());

      assertEquals("voc",
              session.run("call n10s.nsprefixes.list() yield prefix, namespace "
                              + " with prefix where namespace = 'http://neo4j.com/voc/' "
                              + " return  prefix ")
                      .next().get("prefix").asString());
    }
  }

  @Test
  public void testImportBadUrisTtl() throws Exception {
    try (Session session = driver.session()) {

      initialiseGraphDB(neo4j.defaultDatabaseService(),
              "{ handleVocabUris: 'SHORTEN', handleRDFTypes: 'LABELS' }");

      session.run("call n10s.nsprefixes.add('pr','http://example.org/vocab/show/')");

      Result importResults
              = session.run("CALL n10s.rdf.import.fetch('" +
              RDFImportRDFXMLTurtleTest.class.getClassLoader().getResource("badUri.ttl")
                      .toURI()
              + "','Turtle',{ commitSize: 500, verifyUriSyntax: false})");
      assertEquals(2L, importResults
              .next().get("triplesLoaded").asLong());
      assertEquals("test name",
              session.run("MATCH (jb {uri: 'http://example.org/vocab/show/ent'}) RETURN jb.pr"
                              + PREFIX_SEPARATOR + "name AS name")
                      .next().get("name").asString());
    }
  }

  @Test
  public void testImportTtlBadUrisException() throws Exception {
    try (Session session = driver.session()) {

      initialiseGraphDB(neo4j.defaultDatabaseService(), null);

      session.run("WITH {`http://example.org/vocab/show/`:'pr' } as nslist\n" +
              "MERGE (n:_NsPrefDef)\n" +
              "SET n+=nslist " +
              "RETURN n ");

      Result importResults
              = session.run("CALL n10s.rdf.import.fetch('" +
              RDFImportRDFXMLTurtleTest.class.getClassLoader().getResource("badUri.ttl")
                      .toURI()
              + "','Turtle',{ handleVocabUris: 'SHORTEN', handleRDFTypes: 'LABELS', commitSize: 500})");
      assertEquals(0, importResults
              .next().get("triplesLoaded").asLong());
      assertFalse(session.run("MATCH (jb {uri: 'http://example.org/vocab/show/ent'}) RETURN jb.pr"
                      + PREFIX_SEPARATOR + "name AS name")
              .hasNext());
    }
  }

  @Test
  public void testImportRDFXMLBadUris() throws Exception {
    try (Session session = driver.session()) {

      initialiseGraphDB(neo4j.defaultDatabaseService(), null);
      session.run("call n10s.nsprefixes.add('voc','http://neo4j.com/voc/')");

      Result importResults
              = session.run("CALL n10s.rdf.import.fetch('" +
              RDFImportRDFXMLTurtleTest.class.getClassLoader().getResource("badUris.rdf")
                      .toURI()
              + "','RDF/XML',{ handleVocabUris: 'SHORTEN', handleRDFTypes: 'LABELS', commitSize: 500})");
      assertEquals(1L, importResults
              .next().get("triplesLoaded").asLong());
      assertEquals("JB",
              session.run("MATCH (jb {uri: 'http://neo4j.com/invividual/JB\\'sUri'}) RETURN jb.voc"
                              + PREFIX_SEPARATOR + "name AS name")
                      .next().get("name").asString());
    }
  }

  @Test
  public void testImportTurtle() throws Exception {
    try (Session session = driver.session()) {

      initialiseGraphDB(neo4j.defaultDatabaseService(),
              "{ handleVocabUris: 'KEEP', handleRDFTypes: 'LABELS' }");

      Result importResults
              = session.run("CALL n10s.rdf.import.fetch('" +
              RDFImportRDFXMLTurtleTest.class.getClassLoader().getResource("opentox-example.ttl")
                      .toURI()
              + "','Turtle',{ commitSize: 500 })");
      assertEquals(157L, importResults
              .next().get("triplesLoaded").asLong());
      Result algoNames = session
              .run("MATCH (n:`http://www.opentox.org/api/1.1#Algorithm`) " +
                      "\nRETURN n.`http://purl.org/dc/elements/1.1/title` AS algos ORDER By algos");

      assertEquals("J48", algoNames.next().get("algos").asString());
      assertEquals("XLogP", algoNames.next().get("algos").asString());

      Result compounds = session.run(
              "MATCH ()-[r:`http://www.opentox.org/api/1.1#compound`]->(c) RETURN DISTINCT c.uri AS compound order by compound");
      assertEquals("http://www.opentox.org/example/1.1#benzene",
              compounds.next().get("compound").asString());
      assertEquals("http://www.opentox.org/example/1.1#phenol",
              compounds.next().get("compound").asString());

    }
  }

  /**
   * Can we populate the cache correctly when we have a miss?
   */
  @Test
  public void testImportTurtle02() throws Exception {
    try (Session session = driver.session()) {

      initialiseGraphDB(neo4j.defaultDatabaseService(), null);
      session.run("call n10s.nsprefixes.add('ex','http://www.example.com/ontology/1.0.0#')");
      session.run("call n10s.nsprefixes.add('rdf','http://www.w3.org/1999/02/22-rdf-syntax-ns#')");

      Result importResults = session.run(String.format(
              "CALL n10s.rdf.import.fetch('%s','Turtle',{nodeCacheSize: 1})",
              file("myrdf/testImportTurtle02.ttl")));
      assertEquals(5, importResults.next().get("triplesLoaded").asInt());

      Result result = session.run(
              "MATCH (:ex" + PREFIX_SEPARATOR + "DISTANCEVALUE)-[:ex" + PREFIX_SEPARATOR
                      + "units]->(mu) " +
                      "RETURN mu.uri AS unitsUri, mu.ex" + PREFIX_SEPARATOR + "name as unitsName");
      Record first = result.next();
      assertEquals("http://www.example.com/ontology/1.0.0/common#MEASUREMENTUNIT-T1510615421640",
              first.get("unitsUri").asString());
      assertEquals("metres", first.get("unitsName").asString());
    }
  }
}
