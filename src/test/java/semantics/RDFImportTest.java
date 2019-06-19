package semantics;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.neo4j.driver.v1.Values.NULL;
import static org.neo4j.driver.v1.Values.ofNode;
import static semantics.RDFImport.PREFIX_SEPARATOR;

import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.junit.Rule;
import org.junit.Test;
import org.neo4j.driver.internal.value.IntegerValue;
import org.neo4j.driver.internal.value.ListValue;
import org.neo4j.driver.v1.Config;
import org.neo4j.driver.v1.Driver;
import org.neo4j.driver.v1.GraphDatabase;
import org.neo4j.driver.v1.Record;
import org.neo4j.driver.v1.Session;
import org.neo4j.driver.v1.StatementResult;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.harness.junit.Neo4jRule;
import semantics.mapping.MappingUtils;

/**
 * Created by jbarrasa on 21/03/2016.
 */
public class RDFImportTest {

  String jsonLdFragment = "{\n" +
      "  \"@context\": {\n" +
      "    \"name\": \"http://xmlns.com/foaf/0.1/name\",\n" +
      "    \"knows\": \"http://xmlns.com/foaf/0.1/knows\",\n" +
      "\t\"modified\": \"http://xmlns.com/foaf/0.1/modified\"\n" +
      "  },\n" +
      "  \"@id\": \"http://me.markus-lanthaler.com/\",\n" +
      "  \"name\": \"Markus Lanthaler\",\n" +
      "  \"knows\": [\n" +
      "    {\n" +
      "      \"@id\": \"http://manu.sporny.org/about#manu\",\n" +
      "      \"name\": \"Manu Sporny\"\n" +
      "    },\n" +
      "    {\n" +
      "      \"name\": \"Dave Longley\",\n" +
      "\t  \"modified\":\n" +
      "\t    {\n" +
      "\t      \"@value\": \"2010-05-29T14:17:39+02:00\",\n" +
      "\t      \"@type\": \"http://www.w3.org/2001/XMLSchema#dateTime\"\n" +
      "\t    }\n" +
      "    }\n" +
      "  ]\n" +
      "}";

  String turtleFragment = "@prefix show: <http://example.org/vocab/show/> .\n" +
      "\n" +
      "show:218 show:localName \"That Seventies Show\"@en .                 # literal with a language tag\n"
      +
      "show:218 show:localName \"Cette Série des Années Soixante-dix\"@fr . \n" +
      "show:218 show:localName \"Cette Série des Années Septante\"@fr-be .  # literal with a region subtag";

  String wrongUriTtl = "@prefix pr: <http://example.org/vocab/show/> .\n" +
      "pr:ent" +
      "      pr:P854 <https://suasprod.noc-science.at/XLCubedWeb/WebForm/ShowReport.aspx?rep=004+studierende%2f001+universit%u00e4ten%2f003+studierende+nach+universit%u00e4ten.xml&toolbar=true> ;\n"
      +
      "      pr:P813 \"2017-10-11T00:00:00Z\"^^xsd:dateTime .\n";

  @Rule
  public Neo4jRule neo4j = new Neo4jRule()
      .withProcedure(RDFImport.class).withFunction(RDFImport.class)
      .withProcedure(MappingUtils.class);

  @Test
  public void testAbortIfNoIndices() throws Exception {
    try (Driver driver = GraphDatabase.driver(neo4j.boltURI(),
        Config.build().withEncryptionLevel(Config.EncryptionLevel.NONE).toConfig())) {

      Session session = driver.session();

      StatementResult importResults1 = session.run("CALL semantics.importRDF('" +
          RDFImportTest.class.getClassLoader().getResource("mini-ld.json").toURI() +
          "','JSON-LD',{ handleVocabUris: 'SHORTEN', typesToLabels: true, commitSize: 500})");

      Map<String, Object> singleResult = importResults1.single().asMap();

      assertEquals(0L, singleResult.get("triplesLoaded"));
      assertEquals("KO", singleResult.get("terminationStatus"));
      assertEquals("The required index on :Resource(uri) could not be found",
          singleResult.get("extraInfo"));
    }
  }

  @Test
  public void testImportJSONLD() throws Exception {
    try (Driver driver = GraphDatabase.driver(neo4j.boltURI(),
        Config.build().withEncryptionLevel(Config.EncryptionLevel.NONE).toConfig())) {

      Session session = driver.session();

      createIndices(neo4j.getGraphDatabaseService());

      StatementResult importResults1 = session.run("CALL semantics.importRDF('" +
          RDFImportTest.class.getClassLoader().getResource("mini-ld.json").toURI() + "','JSON-LD',"
          +
          "{ handleVocabUris: 'KEEP', typesToLabels: true, commitSize: 500, " +
          "headerParams : { authorization: 'Basic bla bla bla', accept: 'rdf/xml' } })");
      assertEquals(6L, importResults1.single().get("triplesLoaded").asLong());
      assertEquals("http://me.markus-lanthaler.com/",
          session.run(
              "MATCH (n{`http://xmlns.com/foaf/0.1/name` : 'Markus Lanthaler'}) RETURN n.uri AS uri")
              .next().get("uri").asString());
      assertEquals(1L,
          session.run(
              "MATCH (n) WHERE exists(n.`http://xmlns.com/foaf/0.1/modified`) RETURN count(n) AS count")
              .next().get("count").asLong());
    }
  }

  @Test
  public void testImportJSONLDShortening() throws Exception {
    try (Driver driver = GraphDatabase.driver(neo4j.boltURI(),
        Config.build().withEncryptionLevel(Config.EncryptionLevel.NONE).toConfig())) {

      Session session = driver.session();
      createIndices(neo4j.getGraphDatabaseService());

      StatementResult importResults1 = session.run("CALL semantics.importRDF('" +
          RDFImportTest.class.getClassLoader().getResource("mini-ld.json").toURI() + "','JSON-LD',"
          +
          "{ handleVocabUris: 'SHORTEN', typesToLabels: true, commitSize: 500})");
      assertEquals(6L, importResults1.next().get("triplesLoaded").asLong());
      assertEquals("http://me.markus-lanthaler.com/",
          session.run(
              "MATCH (n{ns0" + PREFIX_SEPARATOR + "name : 'Markus Lanthaler'}) RETURN n.uri AS uri")
              .next().get("uri").asString());
      assertEquals(1L,
          session.run("MATCH (n) WHERE exists(n.ns0" + PREFIX_SEPARATOR
              + "modified) RETURN count(n) AS count")
              .next().get("count").asLong());

      assertEquals("ns0",
          session.run(
              "MATCH (n:NamespacePrefixDefinition) RETURN n.`http://xmlns.com/foaf/0.1/` AS prefix")
              .next().get("prefix").asString());
    }

  }

  @Test
  public void testImportRDFXML() throws Exception {
    try (Driver driver = GraphDatabase.driver(neo4j.boltURI(),
        Config.build().withEncryptionLevel(Config.EncryptionLevel.NONE).toConfig())) {

      Session session = driver.session();
      createIndices(neo4j.getGraphDatabaseService());

      StatementResult importResults1 = session.run("CALL semantics.importRDF('" +
          RDFImportTest.class.getClassLoader()
              .getResource("jeu-de-donnees-des-jeux-de-donnees-open-data-paris.rdf")
              .toURI()
          + "','RDF/XML',{ handleVocabUris: 'KEEP', typesToLabels: true, commitSize: 500})");
      assertEquals(38L, importResults1.next().get("triplesLoaded").asLong());
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
    try (Driver driver = GraphDatabase.driver(neo4j.boltURI(),
        Config.build().withEncryptionLevel(Config.EncryptionLevel.NONE).toConfig())) {

      Session session = driver.session();
      createIndices(neo4j.getGraphDatabaseService());

      StatementResult importResults1 = session.run("CALL semantics.importRDF('" +
          RDFImportTest.class.getClassLoader()
              .getResource("jeu-de-donnees-des-jeux-de-donnees-open-data-paris.rdf")
              .toURI()
          + "','RDF/XML',{ handleVocabUris: 'SHORTEN', typesToLabels: true, commitSize: 500})");
      assertEquals(38L, importResults1.next().get("triplesLoaded").asLong());
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
          session.run("MATCH (n:NamespacePrefixDefinition) \n" +
              "RETURN n.`http://www.w3.org/ns/dcat#` as prefix")
              .next().get("prefix").asString());

    }
  }

  @Test
  public void testImportRDFXMLShorteningWithPrefixPreDefinition() throws Exception {
    try (Driver driver = GraphDatabase.driver(neo4j.boltURI(),
        Config.build().withEncryptionLevel(Config.EncryptionLevel.NONE).toConfig())) {

      Session session = driver.session();
      createIndices(neo4j.getGraphDatabaseService());

      session.run("WITH {`http://purl.org/dc/terms/`:'dc',\n" +
          "`http://www.w3.org/1999/02/22-rdf-syntax-ns#`:'rdf',\n" +
          "`http://www.w3.org/2002/07/owl#`:'owl',\n" +
          "`http://www.w3.org/ns/dcat#`:'dcat',\n" +
          "`http://www.w3.org/2000/01/rdf-schema#`:'rdfs',\n" +
          "`http://xmlns.com/foaf/0.1/`:'foaf'} as nslist\n" +
          "MERGE (n:NamespacePrefixDefinition)\n" +
          "SET n+=nslist");

      StatementResult importResults1 = session.run("CALL semantics.importRDF('" +
          RDFImportTest.class.getClassLoader()
              .getResource("jeu-de-donnees-des-jeux-de-donnees-open-data-paris.rdf")
              .toURI()
          + "','RDF/XML', { handleVocabUris: 'SHORTEN', typesToLabels: true, commitSize: 500})");
      assertEquals(38L, importResults1.next().get("triplesLoaded").asLong());
      assertEquals(7L,
          session
              .run("MATCH ()-[r:dc" + PREFIX_SEPARATOR + "relation]->(b) RETURN count(b) as count")
              .next().get("count").asLong());

      assertEquals(
          "http://opendata.paris.fr/opendata/jsp/site/Portal.jsp?document_id=109&portlet_id=106",
          session
              .run("MATCH (x) WHERE x.rdfs" + PREFIX_SEPARATOR + "label = 'harvest_dataset_url'" +
                  "\nRETURN x.rdf" + PREFIX_SEPARATOR + "value AS datasetUrl").next()
              .get("datasetUrl").asString());

      assertEquals("dcat",
          session.run("MATCH (n:NamespacePrefixDefinition) \n" +
              "RETURN n.`http://www.w3.org/ns/dcat#` as prefix")
              .next().get("prefix").asString());

    }
  }


  @Test
  public void testImportRDFXMLShorteningWithPrefixPreDefinitionOneTriple() throws Exception {
    try (Driver driver = GraphDatabase.driver(neo4j.boltURI(),
        Config.build().withEncryptionLevel(Config.EncryptionLevel.NONE).toConfig())) {

      Session session = driver.session();
      createIndices(neo4j.getGraphDatabaseService());

      session.run("WITH {`http://neo4j.com/voc/`:'voc' } as nslist\n" +
          "MERGE (n:NamespacePrefixDefinition)\n" +
          "SET n+=nslist " +
          "RETURN n ");

      StatementResult importResults1 = session.run("CALL semantics.importRDF('" +
          RDFImportTest.class.getClassLoader().getResource("oneTriple.rdf")
              .toURI()
          + "','RDF/XML',{ handleVocabUris: 'SHORTEN', typesToLabels: true, commitSize: 500})");
      assertEquals(1L, importResults1.next().get("triplesLoaded").asLong());
      assertEquals("JB",
          session.run(
              "MATCH (jb {uri: 'http://neo4j.com/invividual/JB'}) RETURN jb.voc" + PREFIX_SEPARATOR
                  + "name AS name")
              .next().get("name").asString());

      assertEquals("voc",
          session.run("MATCH (n:NamespacePrefixDefinition) \n" +
              "RETURN n.`http://neo4j.com/voc/` as prefix")
              .next().get("prefix").asString());

    }
  }

  @Test
  public void testImportBadUrisTtl() throws Exception {
    try (Driver driver = GraphDatabase.driver(neo4j.boltURI(),
        Config.build().withEncryptionLevel(Config.EncryptionLevel.NONE).toConfig())) {

      Session session = driver.session();
      createIndices(neo4j.getGraphDatabaseService());

      session.run("WITH {`http://example.org/vocab/show/`:'pr' } as nslist\n" +
          "MERGE (n:NamespacePrefixDefinition)\n" +
          "SET n+=nslist " +
          "RETURN n ");

      StatementResult importResults1 = session.run("CALL semantics.importRDF('" +
          RDFImportTest.class.getClassLoader().getResource("badUri.ttl")
              .toURI()
          + "','Turtle',{ handleVocabUris: 'SHORTEN', typesToLabels: true, commitSize: 500, verifyUriSyntax: false})");
      assertEquals(2L, importResults1.next().get("triplesLoaded").asLong());
      assertEquals("test name",
          session.run("MATCH (jb {uri: 'http://example.org/vocab/show/ent'}) RETURN jb.pr"
              + PREFIX_SEPARATOR + "name AS name")
              .next().get("name").asString());
    }
  }

  @Test
  public void testImportTtlBadUrisException() throws Exception {
    try (Driver driver = GraphDatabase.driver(neo4j.boltURI(),
        Config.build().withEncryptionLevel(Config.EncryptionLevel.NONE).toConfig())) {

      Session session = driver.session();
      createIndices(neo4j.getGraphDatabaseService());

      session.run("WITH {`http://example.org/vocab/show/`:'pr' } as nslist\n" +
          "MERGE (n:NamespacePrefixDefinition)\n" +
          "SET n+=nslist " +
          "RETURN n ");

      StatementResult importResults1 = session.run("CALL semantics.importRDF('" +
          RDFImportTest.class.getClassLoader().getResource("badUri.ttl")
              .toURI()
          + "','Turtle',{ handleVocabUris: 'SHORTEN', typesToLabels: true, commitSize: 500})");
      assertEquals(0, importResults1.next().get("triplesLoaded").asLong());
      assertEquals(false,
          session.run("MATCH (jb {uri: 'http://example.org/vocab/show/ent'}) RETURN jb.pr"
              + PREFIX_SEPARATOR + "name AS name")
              .hasNext());
    }
  }

  @Test
  public void testImportRDFXMLBadUris() throws Exception {
    try (Driver driver = GraphDatabase.driver(neo4j.boltURI(),
        Config.build().withEncryptionLevel(Config.EncryptionLevel.NONE).toConfig())) {

      Session session = driver.session();
      createIndices(neo4j.getGraphDatabaseService());

      session.run("WITH {`http://neo4j.com/voc/`:'voc' } as nslist\n" +
          "MERGE (n:NamespacePrefixDefinition)\n" +
          "SET n+=nslist " +
          "RETURN n ");

      StatementResult importResults1 = session.run("CALL semantics.importRDF('" +
          RDFImportTest.class.getClassLoader().getResource("badUris.rdf")
              .toURI()
          + "','RDF/XML',{ handleVocabUris: 'SHORTEN', typesToLabels: true, commitSize: 500})");
      assertEquals(1L, importResults1.next().get("triplesLoaded").asLong());
      assertEquals("JB",
          session.run("MATCH (jb {uri: 'http://neo4j.com/invividual/JB\\'sUri'}) RETURN jb.voc"
              + PREFIX_SEPARATOR + "name AS name")
              .next().get("name").asString());
    }
  }

  @Test
  public void testImportLangFilter() throws Exception {
    try (Driver driver = GraphDatabase.driver(neo4j.boltURI(),
        Config.build().withEncryptionLevel(Config.EncryptionLevel.NONE).toConfig())) {

      Session session = driver.session();
      createIndices(neo4j.getGraphDatabaseService());

      session.run("WITH {`http://example.org/vocab/show/`:'voc' } as nslist\n" +
          "MERGE (n:NamespacePrefixDefinition)\n" +
          "SET n+=nslist " +
          "RETURN n ");

      StatementResult importResults1 = session.run("CALL semantics.importRDF('" +
          RDFImportTest.class.getClassLoader().getResource("multilang.ttl")
              .toURI()
          + "','Turtle',{ handleVocabUris: 'SHORTEN', typesToLabels: true, languageFilter: 'en', commitSize: 500})");
      assertEquals(1L, importResults1.next().get("triplesLoaded").asLong());
      assertEquals("That Seventies Show",
          session.run(
              "MATCH (t {uri: 'http://example.org/vocab/show/218'}) RETURN t.voc" + PREFIX_SEPARATOR
                  + "localName AS name")
              .next().get("name").asString());

      session.run("MATCH (t {uri: 'http://example.org/vocab/show/218'}) DETACH DELETE t ");

      importResults1 = session.run("CALL semantics.importRDF('" +
          RDFImportTest.class.getClassLoader().getResource("multilang.ttl")
              .toURI()
          + "','Turtle',{ handleVocabUris: 'SHORTEN', typesToLabels: true, languageFilter: 'fr', commitSize: 500})");
      assertEquals(1L, importResults1.next().get("triplesLoaded").asLong());
      assertEquals("Cette Série des Années Soixante-dix",
          session.run(
              "MATCH (t {uri: 'http://example.org/vocab/show/218'}) RETURN t.voc" + PREFIX_SEPARATOR
                  + "localName AS name")
              .next().get("name").asString());

      session.run("MATCH (t {uri: 'http://example.org/vocab/show/218'}) DETACH DELETE t ");

      importResults1 = session.run("CALL semantics.importRDF('" +
          RDFImportTest.class.getClassLoader().getResource("multilang.ttl")
              .toURI()
          + "','Turtle',{ handleVocabUris: 'SHORTEN', typesToLabels: true, languageFilter: 'fr-be', commitSize: 500})");
      assertEquals(1L, importResults1.next().get("triplesLoaded").asLong());
      assertEquals("Cette Série des Années Septante",
          session.run(
              "MATCH (t {uri: 'http://example.org/vocab/show/218'}) RETURN t.voc" + PREFIX_SEPARATOR
                  + "localName AS name")
              .next().get("name").asString());

      session.run("MATCH (t {uri: 'http://example.org/vocab/show/218'}) DETACH DELETE t ");

      importResults1 = session.run("CALL semantics.importRDF('" +
          RDFImportTest.class.getClassLoader().getResource("multilang.ttl")
              .toURI()
          + "','Turtle',{ handleVocabUris: 'SHORTEN', typesToLabels: true, commitSize: 500})");
      // no language filter means three triples are ingested
      assertEquals(3L, importResults1.next().get("triplesLoaded").asLong());
      //default option is overwrite, so only the last value is kept
      assertEquals("Cette Série des Années Septante",
          session.run(
              "MATCH (t {uri: 'http://example.org/vocab/show/218'}) RETURN t.voc" + PREFIX_SEPARATOR
                  + "localName AS name")
              .next().get("name").asString());

    }
  }

  @Test
  public void testImportMultivalLangTag() throws Exception {
    try (Driver driver = GraphDatabase.driver(neo4j.boltURI(),
        Config.build().withEncryptionLevel(Config.EncryptionLevel.NONE).toConfig())) {

      Session session = driver.session();
      createIndices(neo4j.getGraphDatabaseService());
      String importCypher = "CALL semantics.importRDF('" +
          RDFImportTest.class.getClassLoader().getResource("multilang.ttl")
              .toURI() + "','Turtle',{ keepLangTag : true, handleMultival: 'ARRAY'})";
      StatementResult importResults1 = session.run(importCypher);
      Record next = importResults1.next();
      assertEquals(3, next.get("triplesLoaded").asInt());

      importResults1 = session.run(
          "match (n:Resource) return n.ns0__localName as all, semantics.getLangValue('en',n.ns0__localName) as en_name, "
              +
              "semantics.getLangValue('fr',n.ns0__localName) as fr_name, semantics.getLangValue('fr-be',n.ns0__localName) as frbe_name");
      next = importResults1.next();
      assertEquals("That Seventies Show", next.get("en_name").asString());
      assertEquals("Cette Série des Années Soixante-dix", next.get("fr_name").asString());
      assertEquals("Cette Série des Années Septante", next.get("frbe_name").asString());
    }
  }

  @Test
  public void testImportMultivalWithMultivalList() throws Exception {
    try (Driver driver = GraphDatabase.driver(neo4j.boltURI(),
        Config.build().withEncryptionLevel(Config.EncryptionLevel.NONE).toConfig())) {

      Session session = driver.session();
      createIndices(neo4j.getGraphDatabaseService());
      String importCypher = "CALL semantics.importRDF('" +
          RDFImportTest.class.getClassLoader().getResource("multival.ttl")
              .toURI()
          + "','Turtle',{ handleMultival: 'ARRAY', multivalPropList : ['http://example.org/vocab/show/availableInLang','http://example.org/vocab/show/localName'] })";
      StatementResult importResults1 = session.run(importCypher);
      Record next = importResults1.next();

      assertEquals(9, next.get("triplesLoaded").asInt());

      importResults1 = session.run(
          "match (n:Resource) return n.ns0__localName as all, n.ns0__availableInLang as ail, n.ns0__showId as sid, n.ns0__producer as prod ");
      next = importResults1.next();
      List<String> localNames = new ArrayList<String>();
      localNames.add("That Seventies Show");
      localNames.add("Cette Série des Années Soixante-dix");
      localNames.add("Cette Série des Années Septante");
      assertEquals(localNames, next.get("all").asList());
      List<String> availableInLang = new ArrayList<String>();
      availableInLang.add("EN");
      availableInLang.add("FR");
      availableInLang.add("ES");
      assertEquals(availableInLang, next.get("ail").asList());
      assertEquals(218, next.get("sid").asLong());
      assertEquals("Joanna Smith", next.get("prod").asString());
    }
  }

  @Test
  public void testImportMultivalWithExclusionList() throws Exception {
    try (Driver driver = GraphDatabase.driver(neo4j.boltURI(),
        Config.build().withEncryptionLevel(Config.EncryptionLevel.NONE).toConfig())) {

      Session session = driver.session();
      createIndices(neo4j.getGraphDatabaseService());
      String importCypher = "CALL semantics.importRDF('" +
          RDFImportTest.class.getClassLoader().getResource("multival.ttl")
              .toURI()
          + "','Turtle',{ handleMultival: 'ARRAY', predicateExclusionList : ['http://example.org/vocab/show/availableInLang','http://example.org/vocab/show/localName'] })";
      StatementResult importResults1 = session.run(importCypher);
      Record next = importResults1.next();

      assertEquals(3, next.get("triplesLoaded").asInt());

      importResults1 = session.run(
          "match (n:Resource) return n.ns0__localName as all, n.ns0__availableInLang as ail, n.ns0__showId as sid, n.ns0__producer as prod ");
      next = importResults1.next();
      assertTrue(next.get("all").isNull());
      assertTrue(next.get("ail").isNull());
      List<Long> sids = new ArrayList<Long>();
      sids.add(218L);
      assertEquals(sids, next.get("sid").asList());
      List<String> prod = new ArrayList<String>();
      prod.add("John Smith");
      prod.add("Joanna Smith");
      assertEquals(prod, next.get("prod").asList());
    }
  }

  @Test
  public void testImportTurtle() throws Exception {
    try (Driver driver = GraphDatabase.driver(neo4j.boltURI(),
        Config.build().withEncryptionLevel(Config.EncryptionLevel.NONE).toConfig())) {

      Session session = driver.session();
      createIndices(neo4j.getGraphDatabaseService());

      StatementResult importResults1 = session.run("CALL semantics.importRDF('" +
          RDFImportTest.class.getClassLoader().getResource("opentox-example.ttl")
              .toURI()
          + "','Turtle',{ handleVocabUris: 'KEEP', typesToLabels: true, commitSize: 500})");
      assertEquals(157L, importResults1.next().get("triplesLoaded").asLong());
      StatementResult algoNames = session
          .run("MATCH (n:`http://www.opentox.org/api/1.1#Algorithm`) " +
              "\nRETURN n.`http://purl.org/dc/elements/1.1/title` AS algos ORDER By algos");

      assertEquals("J48", algoNames.next().get("algos").asString());
      assertEquals("XLogP", algoNames.next().get("algos").asString());

      StatementResult compounds = session.run(
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
    try (Driver driver = GraphDatabase.driver(neo4j.boltURI(),
        Config.build().withEncryptionLevel(Config.EncryptionLevel.NONE).toConfig())) {

      Session session = driver.session();
      createIndices(neo4j.getGraphDatabaseService());
      session.run("CREATE (rdf:NamespacePrefixDefinition {" +
          "  `http://www.example.com/ontology/1.0.0#`: 'ex'," +
          "  `http://www.w3.org/1999/02/22-rdf-syntax-ns#`: 'rdfs'})");
      StatementResult importResults = session.run(String.format(
          "CALL semantics.importRDF('%s','Turtle',{nodeCacheSize: 1})",
          file("myrdf/testImportTurtle02.ttl")));
      assertEquals(5, importResults.next().get("triplesLoaded").asInt());

      StatementResult result = session.run(
          "MATCH (:ex" + PREFIX_SEPARATOR + "DISTANCEVALUE)-[:ex" + PREFIX_SEPARATOR
              + "units]->(mu) " +
              "RETURN mu.uri AS unitsUri, mu.ex" + PREFIX_SEPARATOR + "name as unitsName");
      Record first = result.next();
      assertEquals("http://www.example.com/ontology/1.0.0/common#MEASUREMENTUNIT-T1510615421640",
          first.get("unitsUri").asString());
      assertEquals("metres", first.get("unitsName").asString());
    }
  }

  @Test
  public void testPreviewFromSnippetPassWrongUri() throws Exception {
    try (Driver driver = GraphDatabase.driver(neo4j.boltURI(),
        Config.build().withEncryptionLevel(Config.EncryptionLevel.NONE).toConfig())) {

      Session session = driver.session();
      createIndices(neo4j.getGraphDatabaseService());

      StatementResult importResults1 = session
          .run("CALL semantics.previewRDFSnippet('" + wrongUriTtl
              + "','Turtle',{ handleVocabUris: 'KEEP', typesToLabels: false, verifyUriSyntax: false})");
      Map<String, Object> next = importResults1.next().asMap();
      final List<Node> nodes = (List<Node>) next.get("nodes");
      assertEquals(2, nodes.size());
      final List<Relationship> rels = (List<Relationship>) next.get("relationships");
      assertEquals(1, rels.size());
    }
  }

  @Test
  public void testPreviewFromSnippetFailWrongUri() throws Exception {
    try (Driver driver = GraphDatabase.driver(neo4j.boltURI(),
        Config.build().withEncryptionLevel(Config.EncryptionLevel.NONE).toConfig())) {

      Session session = driver.session();
      createIndices(neo4j.getGraphDatabaseService());

      StatementResult importResults1 = session
          .run("CALL semantics.previewRDFSnippet('" + wrongUriTtl
              + "','Turtle',{ handleVocabUris: 'KEEP', typesToLabels: false})");
      Map<String, Object> next = importResults1.next().asMap();
      final List<Node> nodes = (List<Node>) next.get("nodes");
      assertEquals(0, nodes.size());
      final List<Relationship> rels = (List<Relationship>) next.get("relationships");
      assertEquals(0, rels.size());
    }
  }

  @Test
  public void testPreviewFromSnippet() throws Exception {
    try (Driver driver = GraphDatabase.driver(neo4j.boltURI(),
        Config.build().withEncryptionLevel(Config.EncryptionLevel.NONE).toConfig())) {

      Session session = driver.session();
      createIndices(neo4j.getGraphDatabaseService());

      StatementResult importResults1 = session
          .run("CALL semantics.previewRDFSnippet('" + jsonLdFragment
              + "','JSON-LD',{ handleVocabUris: 'KEEP', typesToLabels: false})");
      Map<String, Object> next = importResults1.next().asMap();
      final List<Node> nodes = (List<Node>) next.get("nodes");
      assertEquals(3, nodes.size());
      final List<Relationship> rels = (List<Relationship>) next.get("relationships");
      assertEquals(2, rels.size());
    }
  }

  @Test
  public void testPreviewFromSnippetLangFilter() throws Exception {
    try (Driver driver = GraphDatabase.driver(neo4j.boltURI(),
        Config.build().withEncryptionLevel(Config.EncryptionLevel.NONE).toConfig())) {

      Session session = driver.session();
      createIndices(neo4j.getGraphDatabaseService());

      StatementResult importResults1 = session
          .run("CALL semantics.previewRDFSnippet('" + turtleFragment
              + "','Turtle',{ handleVocabUris: 'KEEP', typesToLabels: false, languageFilter: 'fr'})");
      Record next = importResults1.next();
      assertEquals(1, next.get("nodes").size());
      assertEquals("Cette Série des Années Soixante-dix",
          next.get("nodes").asList(ofNode()).get(0).get("http://example.org/vocab/show/localName")
              .asString());
      assertEquals(0, next.get("relationships").size());

      importResults1 = session.run("CALL semantics.previewRDFSnippet('" + turtleFragment
          + "','Turtle',{ handleVocabUris: 'KEEP', typesToLabels: false, languageFilter: 'en'})");
      assertEquals("That Seventies Show", importResults1.next().get("nodes").asList(ofNode()).get(0)
          .get("http://example.org/vocab/show/localName").asString());

    }
  }

  @Test
  public void testPreviewFromFile() throws Exception {
    try (Driver driver = GraphDatabase.driver(neo4j.boltURI(),
        Config.build().withEncryptionLevel(Config.EncryptionLevel.NONE).toConfig())) {

      Session session = driver.session();
      createIndices(neo4j.getGraphDatabaseService());

      StatementResult importResults1 = session.run("CALL semantics.previewRDF('" +
          RDFImportTest.class.getClassLoader()
              .getResource("jeu-de-donnees-des-jeux-de-donnees-open-data-paris.rdf")
              .toURI() + "','RDF/XML',{ handleVocabUris: 'KEEP', typesToLabels: false})");
      Map<String, Object> next = importResults1.next().asMap();
      final List<Node> nodes = (List<Node>) next.get("nodes");
      assertEquals(15, nodes.size());
      final List<Relationship> rels = (List<Relationship>) next.get("relationships");
      assertEquals(15, rels.size());
    }
  }

  @Test
  public void testPreviewFromBadUriFile() throws Exception {
    try (Driver driver = GraphDatabase.driver(neo4j.boltURI(),
        Config.build().withEncryptionLevel(Config.EncryptionLevel.NONE).toConfig())) {

      Session session = driver.session();
      createIndices(neo4j.getGraphDatabaseService());

      StatementResult importResults1 = session.run("CALL semantics.previewRDF('" +
          RDFImportTest.class.getClassLoader()
              .getResource("badUri.ttl")
              .toURI()
          + "','Turtle',{ handleVocabUris: 'KEEP', typesToLabels: false, verifyUriSyntax: false})");
      Map<String, Object> next = importResults1.next().asMap();
      final List<Node> nodes = (List<Node>) next.get("nodes");
      assertEquals(2, nodes.size());
      final List<Relationship> rels = (List<Relationship>) next.get("relationships");
      assertEquals(1, rels.size());
    }
  }

  @Test
  public void testPreviewFromBadUriFileFail() throws Exception {
    try (Driver driver = GraphDatabase.driver(neo4j.boltURI(),
        Config.build().withEncryptionLevel(Config.EncryptionLevel.NONE).toConfig())) {

      Session session = driver.session();
      createIndices(neo4j.getGraphDatabaseService());

      StatementResult importResults1 = session.run("CALL semantics.previewRDF('" +
          RDFImportTest.class.getClassLoader()
              .getResource("badUri.ttl")
              .toURI() + "','Turtle',{ handleVocabUris: 'KEEP', typesToLabels: false})");
      Map<String, Object> next = importResults1.next().asMap();
      final List<Node> nodes = (List<Node>) next.get("nodes");
      assertEquals(0, nodes.size());
      final List<Relationship> rels = (List<Relationship>) next.get("relationships");
      assertEquals(0, rels.size());
    }
  }

  @Test
  public void testPreviewFromFileLangFilter() throws Exception {
    try (Driver driver = GraphDatabase.driver(neo4j.boltURI(),
        Config.build().withEncryptionLevel(Config.EncryptionLevel.NONE).toConfig())) {

      Session session = driver.session();
      createIndices(neo4j.getGraphDatabaseService());

      StatementResult importResults1 = session.run("CALL semantics.previewRDF('" +
          RDFImportTest.class.getClassLoader().getResource("multilang.ttl")
              .toURI()
          + "','Turtle',{ handleVocabUris: 'KEEP', typesToLabels: false, languageFilter: 'fr', keepLangTag : false })");
      Record next = importResults1.next();

      assertEquals(1, next.get("nodes").size());
      assertEquals("Cette Série des Années Soixante-dix",
          next.get("nodes").asList(ofNode()).get(0).get("http://example.org/vocab/show/localName")
              .asString());
      assertEquals(0, (next.get("relationships")).size());

      importResults1 = session.run("CALL semantics.previewRDF('" +
          RDFImportTest.class.getClassLoader().getResource("multilang.ttl").toURI()
          + "','Turtle',{ handleVocabUris: 'KEEP', typesToLabels: false, languageFilter: 'en', keepLangTag : false })");
      assertEquals("That Seventies Show", importResults1.next().get("nodes").asList(ofNode()).get(0)
          .get("http://example.org/vocab/show/localName").asString());
    }
  }

  @Test
  public void testImportFromFileWithMapping() throws Exception {
    try (Driver driver = GraphDatabase.driver(neo4j.boltURI(),
        Config.build().withEncryptionLevel(Config.EncryptionLevel.NONE).toConfig())) {

      Session session = driver.session();
      createIndices(neo4j.getGraphDatabaseService());

      String addMapping1 =
          " call semantics.mapping.addSchema(\"http://neo4j.com/voc/\",\"voc\") yield node as sch\n"
              +
              "call semantics.mapping.addMappingToSchema(sch,\"uniqueName\",\"name\") yield node as mapping1\n"
              +
              "return *";
      session.run(addMapping1);
      String addMapping2 =
          " call semantics.mapping.addSchema(\"http://neo4j.com/category/\",\"cats\") yield node as sch\n"
              +
              "call semantics.mapping.addMappingToSchema(sch,\"Media\",\"Publication\") yield node as mapping1\n"
              +
              "return *";
      session.run(addMapping2);

      StatementResult importResults1 = session.run("CALL semantics.importRDF('" +
          RDFImportTest.class.getClassLoader().getResource("myrdf/three.rdf")
              .toURI() + "','RDF/XML',{ handleVocabUris: 'MAP'})");
      assertEquals(6L, importResults1.next().get("triplesLoaded").asLong());
      StatementResult mediaNames = session.run("MATCH (m:Media) " +
          "\nRETURN m.uniqueName AS nm, m.uri AS uri");

      Record next = mediaNames.next();
      assertEquals("The Financial Times", next.get("nm").asString());
      assertEquals("http://neo4j.com/invividual/FT", next.get("uri").asString());

      StatementResult personNames = session.run("MATCH (m { PersonName : 'JC'}) " +
          "\nRETURN m.LivesIn AS li, m.uri AS uri");

      next = personNames.next();
      assertEquals("Chesham", next.get("li").asString());
      assertEquals("http://neo4j.com/invividual/JC", next.get("uri").asString());
    }
  }

  @Test
  public void testImportFromFileIgnoreNs() throws Exception {
    try (Driver driver = GraphDatabase.driver(neo4j.boltURI(),
        Config.build().withEncryptionLevel(Config.EncryptionLevel.NONE).toConfig())) {

      Session session = driver.session();
      createIndices(neo4j.getGraphDatabaseService());

      StatementResult importResults1 = session.run("CALL semantics.importRDF('" +
          RDFImportTest.class.getClassLoader().getResource("myrdf/three.rdf")
              .toURI() + "','RDF/XML',{ handleVocabUris: 'IGNORE'})");
      assertEquals(6L, importResults1.next().get("triplesLoaded").asLong());
      StatementResult mediaNames = session.run("MATCH (m:Publication) " +
          "\nRETURN m.name AS nm, m.uri AS uri");

      Record next = mediaNames.next();
      assertEquals("The Financial Times", next.get("nm").asString());
      assertEquals("http://neo4j.com/invividual/FT", next.get("uri").asString());

      StatementResult rels = session.run(
          "MATCH ({ PersonName: 'JC'})-[r:reads]-(:Publication { name: 'The Financial Times'}) " +
              "\nRETURN count(r) as ct");

      next = rels.next();
      assertEquals(1L, next.get("ct").asLong());

    }
  }

  @Test
  public void testImportFromFileIgnoreNsApplyNeoNaming() throws Exception {
    try (Driver driver = GraphDatabase.driver(neo4j.boltURI(),
        Config.build().withEncryptionLevel(Config.EncryptionLevel.NONE).toConfig())) {

      Session session = driver.session();
      createIndices(neo4j.getGraphDatabaseService());

      StatementResult importResults1 = session.run("CALL semantics.importRDF('" +
          RDFImportTest.class.getClassLoader().getResource("myrdf/three.rdf")
              .toURI() + "','RDF/XML',{ handleVocabUris: 'IGNORE', applyNeo4jNaming: true })");
      assertEquals(6L, importResults1.next().get("triplesLoaded").asLong());
      StatementResult mediaNames = session.run("MATCH (m:Publication) " +
          "\nRETURN m.name AS nm, m.uri AS uri");

      Record next = mediaNames.next();
      assertEquals("The Financial Times", next.get("nm").asString());
      assertEquals("http://neo4j.com/invividual/FT", next.get("uri").asString());

      StatementResult rels = session.run(
          "MATCH ({ personName: 'JC'})-[r:READS]-(:Publication { name: 'The Financial Times'}) " +
              "\nRETURN count(r) as ct");

      next = rels.next();
      assertEquals(1L, next.get("ct").asLong());

    }
  }

  @Test
  public void testImportFromFileWithPredFilter() throws Exception {
    try (Driver driver = GraphDatabase.driver(neo4j.boltURI(),
        Config.build().withEncryptionLevel(Config.EncryptionLevel.NONE).toConfig())) {

      Session session = driver.session();
      createIndices(neo4j.getGraphDatabaseService());

      String addMapping1 =
          " call semantics.mapping.addSchema(\"http://schema.org/\",\"sch\") yield node as sch\n" +
              "call semantics.mapping.addMappingToSchema(sch,\"WHERE\",\"location\") yield node as mapping1\n"
              +
              "call semantics.mapping.addMappingToSchema(sch,\"desc\",\"description\") yield node as mapping2\n"
              +
              "return *";
      session.run(addMapping1);

      StatementResult importResults1 = session.run("CALL semantics.importRDF('" +
          RDFImportTest.class.getClassLoader().getResource("event.json")
              .toURI()
          + "','JSON-LD',{ handleVocabUris: 'MAP', predicateExclusionList: ['http://schema.org/price','http://schema.org/priceCurrency'] })");
      assertEquals(26L, importResults1.next().get("triplesLoaded").asLong());

      StatementResult postalAddresses = session.run("MATCH (m:PostalAddress) " +
          "\nRETURN m.postalCode as zip");

      Record next = postalAddresses.next();
      assertEquals("95051", next.get("zip").asString());

      StatementResult whereRels = session.run("MATCH (e:Event)-[:WHERE]->(p:Place) " +
          "\nRETURN p.name as placeName, e.desc as desc ");

      next = whereRels.next();
      assertEquals(
          "Join us for an afternoon of Jazz with Santa Clara resident and pianist Andy Lagunoff. Complimentary food and beverages will be served.",
          next.get("desc").asString());
      assertEquals("Santa Clara City Library, Central Park Library",
          next.get("placeName").asString());

    }
  }

  @Test
  public void testStreamFromFile() throws Exception {
    try (Driver driver = GraphDatabase.driver(neo4j.boltURI(),
        Config.build().withEncryptionLevel(Config.EncryptionLevel.NONE).toConfig())) {

      Session session = driver.session();
      createIndices(neo4j.getGraphDatabaseService());

      StatementResult importResults1 = session.run("CALL semantics.streamRDF('" +
          RDFImportTest.class.getClassLoader().getResource("oneTriple.rdf")
              .toURI() + "','RDF/XML',{})");
      Map<String, Object> next = importResults1.next().asMap();
      assertEquals("http://neo4j.com/invividual/JB", next.get("subject"));
      assertEquals("http://neo4j.com/voc/name", next.get("predicate"));
      assertEquals("JB", next.get("object"));
      assertEquals(true, next.get("isLiteral"));
      assertEquals("http://www.w3.org/2001/XMLSchema#string", next.get("literalType"));
      assertNull(next.get("literalLang"));
    }
  }

  @Test
  public void testStreamFromBadUriFile() throws Exception {
    try (Driver driver = GraphDatabase.driver(neo4j.boltURI(),
        Config.build().withEncryptionLevel(Config.EncryptionLevel.NONE).toConfig())) {

      Session session = driver.session();
      createIndices(neo4j.getGraphDatabaseService());

      StatementResult importResults1 = session.run("CALL semantics.streamRDF('" +
          RDFImportTest.class.getClassLoader().getResource("badUri.ttl")
              .toURI() + "','Turtle',{verifyUriSyntax: false})");
      Map<String, Object> next = importResults1.next().asMap();
      assertEquals("http://example.org/vocab/show/ent", next.get("subject"));
      assertEquals("http://example.org/vocab/show/P854", next.get("predicate"));
      assertEquals(
          "https://suasprod.noc-science.at/XLCubedWeb/WebForm/ShowReport.aspx?rep=004+studierende%2f001+universit%u00e4",
          next.get("object"));
      assertEquals(false, next.get("isLiteral"));
    }
  }

  @Test
  public void testStreamFromBadUriFileFail() throws Exception {
    try (Driver driver = GraphDatabase.driver(neo4j.boltURI(),
        Config.build().withEncryptionLevel(Config.EncryptionLevel.NONE).toConfig())) {

      Session session = driver.session();
      createIndices(neo4j.getGraphDatabaseService());

      StatementResult importResults1 = session.run("CALL semantics.streamRDF('" +
          RDFImportTest.class.getClassLoader().getResource("badUri.ttl")
              .toURI() + "','Turtle',{})");
      assertEquals(false, importResults1.hasNext());
    }
  }

  @Test
  public void testGetLangUDF() throws Exception {
    try (Driver driver = GraphDatabase.driver(neo4j.boltURI(),
        Config.build().withEncryptionLevel(Config.EncryptionLevel.NONE).toConfig())) {

      Session session = driver.session();
      createIndices(neo4j.getGraphDatabaseService());

      StatementResult importResults1 = session.run(
          "return semantics.getLangValue('fr',[\"The Hague@en\", \"Den Haag@nl\", \"La Haye@fr\"]) as val");
      Map<String, Object> next = importResults1.next().asMap();
      assertEquals("La Haye", next.get("val"));

      importResults1 = session.run(
          "return semantics.getLangValue('es',[\"The Hague@en\", \"Den Haag@nl\", \"La Haye@fr\"]) as val");
      next = importResults1.next().asMap();
      assertEquals(null, next.get("val"));

      importResults1 = session.run("return semantics.getLangValue('fr','La Haye@fr') as val");
      next = importResults1.next().asMap();
      assertEquals("La Haye", next.get("val"));

      importResults1 = session.run("return semantics.getLangValue('es','La Haye@fr') as val");
      next = importResults1.next().asMap();
      assertEquals(null, next.get("val"));

      importResults1 = session.run("return semantics.getLangValue('es',[2, 45, 3]) as val");
      next = importResults1.next().asMap();
      assertEquals(null, next.get("val"));

      session.run(
          "create (n:Thing { prop: [\"That Seventies Show@en\", \"Cette Série des Années Soixante-dix@fr\", \"Cette Série des Années Septante@fr-be\"] })");
      importResults1 = session.run(
          "match (n:Thing) return semantics.getLangValue('en',n.prop) as en_name, semantics.getLangValue('fr',n.prop) as fr_name, semantics.getLangValue('fr-be',n.prop) as frbe_name");
      next = importResults1.next().asMap();
      assertEquals("Cette Série des Années Soixante-dix", next.get("fr_name"));
      assertEquals("That Seventies Show", next.get("en_name"));
      assertEquals("Cette Série des Années Septante", next.get("frbe_name"));

      session.run("match (x:Thing) delete x");
      session.run(
          "create (n:Thing { prop: [\"That Seventies Show@en-US\", \"Cette Série des Années Soixante-dix@fr-custom-tag\", \"你好@zh-Hans-CN\"] })");
      importResults1 = session.run(
          "match (n:Thing) return semantics.getLangValue('en-US',n.prop) as enus_name, semantics.getLangValue('fr-custom-tag',n.prop) as frcust_name, semantics.getLangValue('zh-Hans-CN',n.prop) as cn_name");
      next = importResults1.next().asMap();
      assertEquals("Cette Série des Années Soixante-dix", next.get("frcust_name"));
      assertEquals("That Seventies Show", next.get("enus_name"));
      assertEquals("你好", next.get("cn_name"));
    }
  }

  @Test
  public void testGetUriFromShortAndShortFromUri() throws Exception {
    try (Driver driver = GraphDatabase.driver(neo4j.boltURI(),
        Config.build().withEncryptionLevel(Config.EncryptionLevel.NONE).toConfig())) {

      Session session = driver.session();
      createIndices(neo4j.getGraphDatabaseService());

      StatementResult importResults1 = session.run("CALL semantics.importRDF('" +
          RDFImportTest.class.getClassLoader().getResource("mini-ld.json").toURI() + "','JSON-LD',"
          +
          "{ handleVocabUris: 'SHORTEN', typesToLabels: true, commitSize: 500})");
      assertEquals(6L, importResults1.next().get("triplesLoaded").asLong());
      assertEquals("http://xmlns.com/foaf/0.1/knows",
          session.run("MATCH (n{ns0" + PREFIX_SEPARATOR + "name : 'Markus Lanthaler'})-[r]-() " +
              " RETURN semantics.uriFromShort(type(r)) AS uri")
              .next().get("uri").asString());

      assertEquals("ns0" + PREFIX_SEPARATOR + "knows",
          session.run("RETURN semantics.shortFromUri('http://xmlns.com/foaf/0.1/knows') AS uri")
              .next().get("uri").asString());
    }
  }

  @Test
  public void testAddNamespacePrefixInitial() throws Exception {
    try (Driver driver = GraphDatabase.driver(neo4j.boltURI(),
        Config.build().withEncryptionLevel(Config.EncryptionLevel.NONE).toConfig())) {

      Session session = driver.session();

      StatementResult res = session.run("CALL semantics.addNamespacePrefix('abc','http://myvoc#')");
      assertTrue(res.hasNext());
      Record next = res.next();
      assertEquals("abc", next.get("prefix").asString());
      assertEquals("http://myvoc#", next.get("namespace").asString());
      assertFalse(res.hasNext());
    }
  }

  @Test
  public void testAddNamespacePrefixExisting() throws Exception {
    try (Driver driver = GraphDatabase.driver(neo4j.boltURI(),
        Config.build().withEncryptionLevel(Config.EncryptionLevel.NONE).toConfig())) {

      Session session = driver.session();

      createIndices(neo4j.getGraphDatabaseService());
      StatementResult res1 = session.run("CALL semantics.importRDF('" +
          RDFImportTest.class.getClassLoader().getResource("mini-ld.json").toURI()
          + "','JSON-LD')");
      assertTrue(res1.hasNext());
      Map<String, Object> preAddition = res1.next().get("namespaces").asMap();
      StatementResult res2 = session
          .run("CALL semantics.addNamespacePrefix('abc','http://myvoc#')");
      assertTrue(res2.hasNext());
      StatementResult res3 = session.run("MATCH (n:NamespacePrefixDefinition) RETURN n");
      assertTrue(res3.hasNext());
      Map<String, Object> postAddition = res3.next().get("n").asNode().asMap();
      assertFalse(res3.hasNext());
      Set<String> keys = new HashSet<>(postAddition.keySet());
      keys.removeAll(preAddition.keySet());
      assertTrue(keys.size() == 1);
      assertEquals("http://myvoc#", keys.iterator().next());
    }
  }

  @Test
  public void testGetDataType() throws Exception {
    try (Driver driver = GraphDatabase.driver(neo4j.boltURI(),
        Config.build().withEncryptionLevel(Config.EncryptionLevel.NONE).toConfig())) {

      Session session = driver.session();
      createIndices(neo4j.getGraphDatabaseService());

      StatementResult importResults = session
          .run("return semantics.getDataType('2008-04-17^^ns1__date') AS val");
      Map<String, Object> next = importResults.next().asMap();
      assertEquals("ns1__date", next.get("val"));

      importResults = session
          .run("return semantics.getDataType('10000^^http://example.org/USD') AS val");
      next = importResults.next().asMap();
      assertEquals("http://example.org/USD", next.get("val"));

      importResults = session.run("return semantics.getDataType('10000') AS val");
      next = importResults.next().asMap();
      assertEquals("http://www.w3.org/2001/XMLSchema#string", next.get("val"));

      importResults = session.run("return semantics.getDataType(10000) AS val");
      next = importResults.next().asMap();
      assertEquals("http://www.w3.org/2001/XMLSchema#long", next.get("val"));

      importResults = session.run("return semantics.getDataType(10000.0) AS val");
      next = importResults.next().asMap();
      assertEquals("http://www.w3.org/2001/XMLSchema#double", next.get("val"));

      importResults = session.run("return semantics.getDataType(true) AS val");
      next = importResults.next().asMap();
      assertEquals("http://www.w3.org/2001/XMLSchema#boolean", next.get("val"));

    }
  }

  @Test
  public void testGetValue() throws Exception {
    try (Driver driver = GraphDatabase.driver(neo4j.boltURI(),
        Config.build().withEncryptionLevel(Config.EncryptionLevel.NONE).toConfig())) {

      Session session = driver.session();
      createIndices(neo4j.getGraphDatabaseService());

      StatementResult importResults = session
          .run("return semantics.getValue('2008-04-17^^ns1__date') AS val");
      Map<String, Object> next = importResults.next().asMap();
      assertEquals("2008-04-17", next.get("val"));

      importResults = session.run(
          "return semantics.getValue('10000^^http://example.org/USD') AS val");
      next = importResults.next().asMap();
      assertEquals("10000", next.get("val"));

      importResults = session.run("return semantics.getValue('10000') AS val");
      next = importResults.next().asMap();
      assertEquals("10000", next.get("val"));

      importResults = session.run("return semantics.getValue('This is a test@en') AS val");
      next = importResults.next().asMap();
      assertEquals("This is a test", next.get("val"));
    }
  }

  @Test
  public void testCustomDataTypesKeepURIs() throws Exception {
    try (Driver driver = GraphDatabase.driver(neo4j.boltURI(),
        Config.build().withEncryptionLevel(Config.EncryptionLevel.NONE).toConfig())) {

      Session session = driver.session();
      createIndices(neo4j.getGraphDatabaseService());

      StatementResult importResults1 = session.run("CALL semantics.importRDF('" +
          RDFImportTest.class.getClassLoader().getResource("customDataTypes.ttl")
              .toURI() + "','Turtle',{keepLangTag: true, handleMultival: 'ARRAY', " +
          "multivalPropList: ['http://example.com/price', 'http://example.com/power', 'http://example.com/class'], keepCustomDataTypes: true, "
          +
          "customDataTypedPropList: ['http://example.com/price', 'http://example.com/color', 'http://example.com/power'], "
          +
          "handleVocabUris: 'KEEP', typesToLabels: true, commitSize: 500})");
      assertEquals(10L, importResults1.next().get("triplesLoaded").asLong());
      StatementResult cars = session.run("MATCH (n:`http://example.com/Car`) " +
          "\nRETURN n.`http://example.com/price` AS price," +
          "n.`http://example.com/power` AS power, " +
          "n.`http://example.com/color` AS color, " +
          "n.`http://example.com/class` AS class, n.`http://example.com/released` AS released, " +
          "n.`http://example.com/type` AS type ORDER BY price");

      Record car = cars.next();
      List price = car.get("price").asList();
      assertEquals(2, price.size());
      assertEquals("10000^^http://example.com/EUR", price.get(0));
      assertEquals("11000^^http://example.com/USD", price.get(1));
      assertEquals("300^^http://example.com/HP", car.get("power").get(0).asString());
      assertEquals("223,71^^http://example.com/kW", car.get("power").get(1).asString());
      assertEquals("red^^http://example.com/Color", car.get("color").asString());
      assertEquals("A-Klasse@de", car.get("class").asList().get(0));
      assertEquals("A-Class@en", car.get("class").asList().get(1));
      assertEquals(2019, car.get("released").asLong());
      assertEquals("Cabrio", car.get("type").asString());
    }
  }

  @Test
  public void testCustomDataTypesShortenURIs() throws Exception {
    try (Driver driver = GraphDatabase.driver(neo4j.boltURI(),
        Config.build().withEncryptionLevel(Config.EncryptionLevel.NONE).toConfig())) {

      Session session = driver.session();
      createIndices(neo4j.getGraphDatabaseService());

      StatementResult importResults1 = session.run("CALL semantics.importRDF('" +
          RDFImportTest.class.getClassLoader().getResource("customDataTypes.ttl")
              .toURI() + "','Turtle',{keepLangTag: true, handleMultival: 'ARRAY', " +
          "multivalPropList: ['http://example.com/price', 'http://example.com/power', 'http://example.com/class'], keepCustomDataTypes: true, "
          +
          "customDataTypedPropList: ['http://example.com/price', 'http://example.com/color', 'http://example.com/power'], "
          +
          "handleVocabUris: 'SHORTEN', typesToLabels: true, commitSize: 500})");
      assertEquals(10L, importResults1.next().get("triplesLoaded").asLong());
      StatementResult cars = session.run("MATCH (n:ns0__Car) " +
          "\nRETURN n.ns0__price AS price," +
          "n.ns0__power AS power, " +
          "n.ns0__color AS color, " +
          "n.ns0__class AS class, n.ns0__released AS released, " +
          "n.ns0__type AS type ORDER BY price");

      Record car = cars.next();
      List price = car.get("price").asList();
      assertEquals(2, price.size());
      assertEquals("10000^^ns0__EUR", price.get(0));
      assertEquals("11000^^ns0__USD", price.get(1));
      assertEquals("300^^ns0__HP", car.get("power").get(0).asString());
      assertEquals("223,71^^ns0__kW", car.get("power").get(1).asString());
      assertEquals("red^^ns0__Color", car.get("color").asString());
      assertEquals("A-Klasse@de", car.get("class").asList().get(0));
      assertEquals("A-Class@en", car.get("class").asList().get(1));
      assertEquals(2019, car.get("released").asLong());
      assertEquals("Cabrio", car.get("type").asString());
    }
  }

  @Test
  public void testImportMultiValAfterImportSingelVal() throws Exception {
    try (Driver driver = GraphDatabase.driver(neo4j.boltURI(),
        Config.build().withEncryptionLevel(Config.EncryptionLevel.NONE).toConfig())) {

      Session session = driver.session();
      createIndices(neo4j.getGraphDatabaseService());
      String importCypher = "CALL semantics.importRDF('" +
          RDFImportTest.class.getClassLoader()
              .getResource("testImportMultiValAfterImportSingelVal.ttl")
              .toURI() + "','Turtle',{handleMultival: 'OVERWRITE', handleVocabUris: 'KEEP'})";
      StatementResult importResults = session.run(importCypher);
      Record next = importResults.next();
      assertEquals(3, next.get("triplesLoaded").asInt());
      StatementResult queryResults = session
          .run("MATCH (n:Resource) RETURN n.`http://example.com/price` AS price");
      Object imports = queryResults.next().get("price");
      assertEquals(IntegerValue.class, imports.getClass());

      importCypher = "CALL semantics.importRDF('" +
          RDFImportTest.class.getClassLoader()
              .getResource("testImportMultiValAfterImportSingelVal.ttl")
              .toURI() + "','Turtle',{handleMultival: 'ARRAY', handleVocabUris: 'KEEP'})";

      importResults = session.run(importCypher);

      next = importResults.next();
      assertEquals(3, next.get("triplesLoaded").asInt());

      queryResults = session.run("MATCH (n:Resource) RETURN n.`http://example.com/price` AS price");
      imports = queryResults.next().get("price");
      assertEquals(ListValue.class, imports.getClass());
    }
  }

  @Test
  public void testReificationImport() throws Exception {
    try (Driver driver = GraphDatabase.driver(neo4j.boltURI(),
        Config.build().withEncryptionLevel(Config.EncryptionLevel.NONE).toConfig())) {

      Session session = driver.session();
      createIndices(neo4j.getGraphDatabaseService());

      StatementResult importResults1 = session.run("CALL semantics.importRDF('" +
          RDFImportTest.class.getClassLoader().getResource("reification.ttl")
              .toURI()
          + "','Turtle',{ handleVocabUris: 'KEEP', typesToLabels: true, commitSize: 500})");
      assertEquals(25L, importResults1.next().get("triplesLoaded").asLong());
      StatementResult dates = session
          .run("MATCH (n:`http://www.w3.org/1999/02/22-rdf-syntax-ns#Statement`) " +
              "\nRETURN n.`http://example.com/from` AS fromDates ORDER BY fromDates DESC");

      assertEquals("2019-09-01", dates.next().get("fromDates").asString());
      assertEquals("2016-09-01", dates.next().get("fromDates").asString());

      StatementResult statements = session.run("MATCH (statement)\n" +
          "WHERE (statement)-[:`http://www.w3.org/1999/02/22-rdf-syntax-ns#subject`]->()\n" +
          "AND (statement)-[:`http://www.w3.org/1999/02/22-rdf-syntax-ns#predicate`]->()\n" +
          "AND (statement)-[:`http://www.w3.org/1999/02/22-rdf-syntax-ns#object`]->()\n" +
          "RETURN statement.uri AS statement ORDER BY statement");

      assertEquals("http://example.com/studyInformation1",
          statements.next().get("statement").asString());
      assertEquals("http://example.com/studyInformation2",
          statements.next().get("statement").asString());
    }
  }

  @Test
  public void testIncrementalLoadMultivaluesInArray() throws Exception {
    try (Driver driver = GraphDatabase.driver(neo4j.boltURI(),
        Config.build().withEncryptionLevel(Config.EncryptionLevel.NONE).toConfig())) {

      Session session = driver.session();
      createIndices(neo4j.getGraphDatabaseService());

      StatementResult importResults1 = session.run("CALL semantics.importRDF('" +
          RDFImportTest.class.getClassLoader().getResource("incremental/step1.ttl")
              .toURI() + "','Turtle',{ handleMultival: 'ARRAY' })");
      assertEquals(2L, importResults1.next().get("triplesLoaded").asLong());
      importResults1 = session.run("CALL semantics.importRDF('" +
          RDFImportTest.class.getClassLoader().getResource("incremental/step2.ttl")
              .toURI() + "','Turtle',{ handleMultival: 'ARRAY' })");
      assertEquals(2L, importResults1.next().get("triplesLoaded").asLong());

      StatementResult result = session.run("MATCH (n:ns0__Thing) " +
          "\nRETURN n.ns0__prop as multival ");

      List<String> vals = new ArrayList<String>();
      vals.add("one");
      vals.add("two");
      assertEquals(vals, result.next().get("multival").asList());


    }
  }

  @Test
  public void testIncrementalLoadNamespaces() throws Exception {
    try (Driver driver = GraphDatabase.driver(neo4j.boltURI(),
        Config.build().withEncryptionLevel(Config.EncryptionLevel.NONE).toConfig())) {

      Session session = driver.session();
      createIndices(neo4j.getGraphDatabaseService());

      StatementResult importResults1 = session.run("CALL semantics.importRDF('" +
          RDFImportTest.class.getClassLoader().getResource("event.json")
              .toURI() + "','JSON-LD')");
      assertEquals(28L, importResults1.next().get("triplesLoaded").asLong());
      StatementResult nsDefResult = session.run("MATCH (n:NamespacePrefixDefinition) "
          + "RETURN properties(n) as defs");
      assertTrue(nsDefResult.hasNext());
      Map<String, Object> defsPre = nsDefResult.next().get("defs").asMap();
      assertFalse(nsDefResult.hasNext());
      importResults1 = session.run("CALL semantics.importRDF('" +
          RDFImportTest.class.getClassLoader().getResource("fibo-fragment.rdf")
              .toURI() + "','RDF/XML')");
      assertEquals(171L, importResults1.next().get("triplesLoaded").asLong());
      nsDefResult = session.run("MATCH (n:NamespacePrefixDefinition) "
          + "RETURN properties(n) as defs");
      assertTrue(nsDefResult.hasNext());
      Map<String, Object> defsPost = nsDefResult.next().get("defs").asMap();
      assertFalse(nsDefResult.hasNext());
      assertTrue(getPrePostDelta(defsPre, defsPost).isEmpty());
      importResults1 = session.run("CALL semantics.importRDF('" +
          RDFImportTest.class.getClassLoader().getResource("oneTriple.rdf")
              .toURI() + "','RDF/XML')");
      assertEquals(1L, importResults1.next().get("triplesLoaded").asLong());
      nsDefResult = session.run("MATCH (n:NamespacePrefixDefinition) "
          + "RETURN properties(n) as defs");
      assertTrue(nsDefResult.hasNext());
      Map<String, Object> defsPost2 = nsDefResult.next().get("defs").asMap();
      assertFalse(nsDefResult.hasNext());
      assertTrue(getPrePostDelta(defsPost, defsPost2).isEmpty());

    }
  }

  @Test
  public void testLoadNamespacesWithCustomPredefined() throws Exception {
    try (Driver driver = GraphDatabase.driver(neo4j.boltURI(),
        Config.build().withEncryptionLevel(Config.EncryptionLevel.NONE).toConfig())) {

      Session session = driver.session();
      createIndices(neo4j.getGraphDatabaseService());

      StatementResult importResults1 = session.run("CREATE (:NamespacePrefixDefinition {\n"
          + "  `http://www.w3.org/2000/01/rdf-schema#`: 'myschema',\n"
          + "  `http://www.w3.org/1999/02/22-rdf-syntax-ns#`: 'myrdf'})");
      StatementResult nsDefResult = session.run("MATCH (n:NamespacePrefixDefinition) "
          + "RETURN properties(n) as defs");
      assertTrue(nsDefResult.hasNext());
      Map<String, Object> defsPre = nsDefResult.next().get("defs").asMap();
      assertFalse(nsDefResult.hasNext());
      importResults1 = session.run("CALL semantics.importRDF('" +
          RDFImportTest.class.getClassLoader().getResource("fibo-fragment.rdf")
              .toURI() + "','RDF/XML')");
      assertEquals(171L, importResults1.next().get("triplesLoaded").asLong());
      nsDefResult = session.run("MATCH (n:NamespacePrefixDefinition) "
          + "RETURN properties(n) as defs");
      assertTrue(nsDefResult.hasNext());
      Map<String, Object> defsPost = nsDefResult.next().get("defs").asMap();
      assertFalse(nsDefResult.hasNext());
    }
  }

  private Map<String, Object> getPrePostDelta(Map<String, Object> defsPre,
      Map<String, Object> defsPost) {
    Map<String, Object> delta = new HashMap<>();
    defsPre.forEach((k, v) -> {
      if (!defsPost.containsKey(k) || !defsPost.get(k).equals(v)) {
        delta.put(k, v);
      }
    });
    return delta;
  }

  @Test
  public void testIncrementalLoadArrayOnPreviouslyAtomicValue() throws Exception {
    try (Driver driver = GraphDatabase.driver(neo4j.boltURI(),
        Config.build().withEncryptionLevel(Config.EncryptionLevel.NONE).toConfig())) {

      Session session = driver.session();
      createIndices(neo4j.getGraphDatabaseService());

      StatementResult importResults1 = session.run("CALL semantics.importRDF('" +
          RDFImportTest.class.getClassLoader().getResource("incremental/step1.ttl")
              .toURI() + "','Turtle')");
      assertEquals(2L, importResults1.next().get("triplesLoaded").asLong());
      importResults1 = session.run("CALL semantics.importRDF('" +
          RDFImportTest.class.getClassLoader().getResource("incremental/step2.ttl")
              .toURI() + "','Turtle',{ handleMultival: 'ARRAY' })");
      assertEquals(2L, importResults1.next().get("triplesLoaded").asLong());

      StatementResult result = session.run("MATCH (n:ns0__Thing) " +
          "\nRETURN n.ns0__prop as multival ");

      List<String> vals = new ArrayList<String>();
      vals.add("one");
      vals.add("two");
      assertEquals(vals, result.next().get("multival").asList());


    }
  }

  @Test
  public void testIncrementalLoadAtomicValueOnPreviouslyArray() throws Exception {
    try (Driver driver = GraphDatabase.driver(neo4j.boltURI(),
        Config.build().withEncryptionLevel(Config.EncryptionLevel.NONE).toConfig())) {

      Session session = driver.session();
      createIndices(neo4j.getGraphDatabaseService());

      StatementResult importResults1 = session.run("CALL semantics.importRDF('" +
          RDFImportTest.class.getClassLoader().getResource("incremental/step1.ttl")
              .toURI() + "','Turtle',{ handleMultival: 'ARRAY' })");
      assertEquals(2L, importResults1.next().get("triplesLoaded").asLong());
      importResults1 = session.run("CALL semantics.importRDF('" +
          RDFImportTest.class.getClassLoader().getResource("incremental/step3.ttl")
              .toURI() + "','Turtle')");
      assertEquals(2L, importResults1.next().get("triplesLoaded").asLong());

      StatementResult result = session.run("MATCH (n:ns0__Thing) " +
          "\nRETURN n.ns0__prop as singleVal ");

      assertEquals(230L, result.next().get("singleVal").asLong());


    }
  }

  @Test
  public void testLargerFileManyTransactions() throws Exception {
    try (Driver driver = GraphDatabase.driver(neo4j.boltURI(),
        Config.build().withEncryptionLevel(Config.EncryptionLevel.NONE).toConfig())) {

      Session session = driver.session();
      createIndices(neo4j.getGraphDatabaseService());

      StatementResult importResults1 = session.run("CALL semantics.importRDF('" +
          RDFImportTest.class.getClassLoader().getResource("100k.nt").toURI() + "','N-Triples',"
          + "{ commitSize: 5 , predicateExclusionList: ['http://www.w3.org/2004/02/skos/core#prefLabel']})");
      assertEquals(92712L, importResults1.next().get("triplesLoaded").asLong());
    }

  }

  @Test
  public void testDeleteRelationshipKeepURIs() throws Exception {
    try (Driver driver = GraphDatabase.driver(neo4j.boltURI(),
        Config.build().withEncryptionLevel(Config.EncryptionLevel.NONE).toConfig())) {

      Session session = driver.session();
      createIndices(neo4j.getGraphDatabaseService());

      StatementResult importResults = session.run("CALL semantics.importRDF('" +
          RDFImportTest.class.getClassLoader().getResource("deleteRDF/dataset1.ttl")
              .toURI()
          + "','Turtle',{ handleVocabUris: 'KEEP', typesToLabels: true, commitSize: 500, keepCustomDataTypes: true, handleMultival: 'ARRAY'})");

      assertEquals(15L, importResults.next().get("triplesLoaded").asLong());
      StatementResult result = session.run("MATCH (n {uri: 'http://example.org/Resource1'}),"
          + "(m {uri: 'http://example.org/Resource2'})"
          + "OPTIONAL MATCH (n)-[r]->(m) "
          + "RETURN n.uri AS nUri, type(r) AS type, m.uri AS mUri");
      Record record = result.next();
      assertEquals("http://example.org/Predicate3", record.get("type").asString());
      assertEquals("http://example.org/Resource1", record.get("nUri").asString());
      assertEquals("http://example.org/Resource2", record.get("mUri").asString());

      StatementResult deleteResults = session.run("CALL semantics.deleteRDF('" +
          RDFImportTest.class.getClassLoader().getResource("deleteRDF/dataset1Delete1.ttl").toURI()
          + "', 'Turtle', {handleVocabUris: 'KEEP', typesToLabels: true, commitSize: 500, keepCustomDataTypes: true})");

      assertEquals(1L, deleteResults.next().get("triplesDeleted").asLong());

      result = session.run("MATCH (n {uri: 'http://example.org/Resource1'}),"
          + "(m {uri: 'http://example.org/Resource2'})"
          + "OPTIONAL MATCH (n)-[r]->(m) "
          + "RETURN n.uri AS nUri, type(r) AS type, m.uri AS mUri");
      record = result.next();
      assertEquals(NULL, record.get("type"));
      assertEquals("http://example.org/Resource1", record.get("nUri").asString());
      assertEquals("http://example.org/Resource2", record.get("mUri").asString());

    }
  }

  @Test
  public void testDeleteRelationshipShortenURIs() throws Exception {
    try (Driver driver = GraphDatabase.driver(neo4j.boltURI(),
        Config.build().withEncryptionLevel(Config.EncryptionLevel.NONE).toConfig())) {

      Session session = driver.session();
      createIndices(neo4j.getGraphDatabaseService());

      StatementResult importResults = session.run("CALL semantics.importRDF('" +
          RDFImportTest.class.getClassLoader().getResource("deleteRDF/dataset1.ttl")
              .toURI()
          + "','Turtle',{ handleVocabUris: 'SHORTEN', typesToLabels: true, commitSize: 500, keepCustomDataTypes: true, handleMultival: 'ARRAY'})");

      assertEquals(15L, importResults.next().get("triplesLoaded").asLong());
      StatementResult result = session.run("MATCH (n {uri: 'http://example.org/Resource1'}),"
          + "(m {uri: 'http://example.org/Resource2'})"
          + "OPTIONAL MATCH (n)-[r]->(m) "
          + "RETURN n.uri AS nUri, type(r) AS type, m.uri AS mUri");
      Record record = result.next();
      assertEquals("ns0__Predicate3", record.get("type").asString());
      assertEquals("http://example.org/Resource1", record.get("nUri").asString());
      assertEquals("http://example.org/Resource2", record.get("mUri").asString());

      StatementResult deleteResults = session.run("CALL semantics.deleteRDF('" +
          RDFImportTest.class.getClassLoader().getResource("deleteRDF/dataset1Delete1.ttl").toURI()
          + "', 'Turtle', {handleVocabUris: 'SHORTEN', typesToLabels: true, commitSize: 500, keepCustomDataTypes: true})");

      assertEquals(1L, deleteResults.next().get("triplesDeleted").asLong());

      result = session.run("MATCH (n {uri: 'http://example.org/Resource1'}),"
          + "(m {uri: 'http://example.org/Resource2'})"
          + "OPTIONAL MATCH (n)-[r]->(m) "
          + "RETURN n.uri AS nUri, type(r) AS type, m.uri AS mUri");
      record = result.next();
      assertEquals(NULL, record.get("type"));
      assertEquals("http://example.org/Resource1", record.get("nUri").asString());
      assertEquals("http://example.org/Resource2", record.get("mUri").asString());

    }
  }

  @Test
  public void testDeleteLiteralKeepURIs() throws Exception {
    try (Driver driver = GraphDatabase.driver(neo4j.boltURI(),
        Config.build().withEncryptionLevel(Config.EncryptionLevel.NONE).toConfig())) {

      Session session = driver.session();
      createIndices(neo4j.getGraphDatabaseService());

      StatementResult importResults = session.run("CALL semantics.importRDF('" +
          RDFImportTest.class.getClassLoader().getResource("deleteRDF/dataset1.ttl")
              .toURI()
          + "','Turtle',{ handleVocabUris: 'KEEP', typesToLabels: true, commitSize: 500, keepCustomDataTypes: true, handleMultival: 'ARRAY'})");

      assertEquals(15L, importResults.next().get("triplesLoaded").asLong());
      StatementResult result = session.run("MATCH (n {uri: 'http://example.org/Resource1'})"
          + "RETURN n.`http://example.org/Predicate2` AS nP2");

      Record record = result.next();
      assertEquals(1, record.get("nP2").asList().size());
      assertTrue(record.get("nP2").asList().contains("test"));

      StatementResult deleteResults = session.run("CALL semantics.deleteRDF('" +
          RDFImportTest.class.getClassLoader().getResource("deleteRDF/dataset1Delete2.ttl").toURI()
          + "', 'Turtle', {handleVocabUris: 'KEEP', typesToLabels: true, commitSize: 500, keepCustomDataTypes: true})");

      assertEquals(1L, deleteResults.next().get("triplesDeleted").asLong());

      result = session.run("MATCH (n {uri: 'http://example.org/Resource1'})"
          + "RETURN n.`http://example.org/Predicate2` AS nP2");
      record = result.next();
      assertEquals(NULL, record.get("nP2"));

    }
  }

  @Test
  public void testDeleteLiteralShortenURIs() throws Exception {
    try (Driver driver = GraphDatabase.driver(neo4j.boltURI(),
        Config.build().withEncryptionLevel(Config.EncryptionLevel.NONE).toConfig())) {

      Session session = driver.session();
      createIndices(neo4j.getGraphDatabaseService());

      StatementResult importResults = session.run("CALL semantics.importRDF('" +
          RDFImportTest.class.getClassLoader().getResource("deleteRDF/dataset1.ttl")
              .toURI()
          + "','Turtle',{ handleVocabUris: 'SHORTEN', typesToLabels: true, commitSize: 500, keepCustomDataTypes: true, handleMultival: 'ARRAY'})");

      assertEquals(15L, importResults.next().get("triplesLoaded").asLong());
      StatementResult result = session.run("MATCH (n {uri: 'http://example.org/Resource1'})"
          + "RETURN n.ns0__Predicate2 AS nP2");

      Record record = result.next();
      assertEquals(1, record.get("nP2").asList().size());
      assertTrue(record.get("nP2").asList().contains("test"));

      StatementResult deleteResults = session.run("CALL semantics.deleteRDF('" +
          RDFImportTest.class.getClassLoader().getResource("deleteRDF/dataset1Delete2.ttl").toURI()
          + "', 'Turtle', {handleVocabUris: 'SHORTEN', typesToLabels: true, commitSize: 500, keepCustomDataTypes: true})");

      assertEquals(1L, deleteResults.next().get("triplesDeleted").asLong());

      result = session.run("MATCH (n {uri: 'http://example.org/Resource1'})"
          + "RETURN n.ns0__Predicate2 AS nP2");
      record = result.next();
      assertEquals(NULL, record.get("nP2"));

    }
  }


  @Test
  public void testDeleteTypeFromResource() throws Exception {
    try (Driver driver = GraphDatabase.driver(neo4j.boltURI(),
        Config.build().withEncryptionLevel(Config.EncryptionLevel.NONE).toConfig())) {

      Session session = driver.session();
      createIndices(neo4j.getGraphDatabaseService());

      StatementResult importResults = session.run("CALL semantics.importRDF('" +
          RDFImportTest.class.getClassLoader().getResource("deleteRDF/dataset1.ttl")
              .toURI()
          + "','Turtle',{ handleVocabUris: 'KEEP', typesToLabels: true, commitSize: 500, keepCustomDataTypes: true, handleMultival: 'ARRAY'})");

      assertEquals(15L, importResults.next().get("triplesLoaded").asLong());
      StatementResult result = session.run("MATCH (n:Resource)"
          + "RETURN n");
      assertEquals(3, result.list().size());
      result = session.run("MATCH (n {uri: 'http://example.org/Resource2'})"
          + "RETURN labels(n) AS labels");
      Record record = result.next();
      assertEquals(2, record.get("labels").asList().size());

      StatementResult deleteResult = session.run("CALL semantics.deleteRDF('" +
          RDFImportTest.class.getClassLoader().getResource("deleteRDF/dataset1Delete3.ttl").toURI()
          + "', 'Turtle', {handleVocabUris: 'KEEP', typesToLabels: true, commitSize: 500, keepCustomDataTypes: true, handleMultival: 'ARRAY'})");

      assertEquals(1L, deleteResult.next().get("triplesDeleted").asLong());

      result = session.run("MATCH (n {uri: 'http://example.org/Resource2'})"
          + "RETURN labels(n) AS labels");
      record = result.next();
      assertEquals(1, record.get("labels").asList().size());

    }
  }

  @Test
  public void testDeleteAllTriplesRelatedToResource() throws Exception {
    try (Driver driver = GraphDatabase.driver(neo4j.boltURI(),
        Config.build().withEncryptionLevel(Config.EncryptionLevel.NONE).toConfig())) {

      Session session = driver.session();
      createIndices(neo4j.getGraphDatabaseService());

      StatementResult importResults = session.run("CALL semantics.importRDF('" +
          RDFImportTest.class.getClassLoader().getResource("deleteRDF/dataset1.ttl")
              .toURI()
          + "','Turtle',{ handleVocabUris: 'KEEP', typesToLabels: true, commitSize: 500, keepCustomDataTypes: true, handleMultival: 'ARRAY'})");

      assertEquals(15L, importResults.next().get("triplesLoaded").asLong());
      StatementResult result = session.run("MATCH (n:Resource)"
          + "RETURN n");
      assertEquals(3, result.list().size());

      StatementResult deleteResult = session.run("CALL semantics.deleteRDF('" +
          RDFImportTest.class.getClassLoader().getResource("deleteRDF/dataset1Delete4.ttl").toURI()
          + "', 'Turtle', {handleVocabUris: 'KEEP', typesToLabels: true, commitSize: 500, keepCustomDataTypes: true, handleMultival: 'ARRAY'})");

      assertEquals(8L, deleteResult.next().get("triplesDeleted").asLong());

      result = session.run("MATCH (n:Resource)"
          + "RETURN n");
      assertEquals(1, result.list().size());

    }
  }

  @Test
  public void testDeleteMultiLiteral() throws Exception {
    try (Driver driver = GraphDatabase.driver(neo4j.boltURI(),
        Config.build().withEncryptionLevel(Config.EncryptionLevel.NONE).toConfig())) {

      Session session = driver.session();
      createIndices(neo4j.getGraphDatabaseService());

      StatementResult importResults = session.run("CALL semantics.importRDF('" +
          RDFImportTest.class.getClassLoader().getResource("deleteRDF/dataset1.ttl")
              .toURI()
          + "','Turtle',{ handleVocabUris: 'KEEP', typesToLabels: true, commitSize: 500, keepCustomDataTypes: true, handleMultival: 'ARRAY'})");

      assertEquals(15L, importResults.next().get("triplesLoaded").asLong());
      StatementResult result = session.run("MATCH (n {uri: 'http://example.org/Resource1'}), "
          + "(m {uri: 'http://example.org/Resource2'})"
          + "RETURN n.`http://example.org/Predicate4` AS nP4, "
          + "m.`http://example.org/Predicate3` AS mP3, "
          + "m.`http://example.org/Predicate4` AS mP4");

      Record record = result.next();
      assertArrayEquals(new String[]{"val1", "val2", "val3", "val4"},
          record.get("nP4").asList().toArray());
      assertTrue(record.get("mP3").asList().contains(100L));
      assertTrue(record.get("mP3").asList().contains(200L));
      assertTrue(record.get("mP4").asList().contains(300.0));
      assertTrue(record.get("mP4").asList().contains(400.0));

      StatementResult deleteResult = session.run("CALL semantics.deleteRDF('" +
          RDFImportTest.class.getClassLoader().getResource("deleteRDF/dataset1Delete5.ttl").toURI()
          + "', 'Turtle', {handleVocabUris: 'KEEP', typesToLabels: true, commitSize: 500, keepCustomDataTypes: true, handleMultival: 'ARRAY'})");

      assertEquals(3L, deleteResult.next().get("triplesDeleted").asLong());

      result = session.run("MATCH (n {uri: 'http://example.org/Resource1'})"
          + "RETURN n.`http://example.org/Predicate4` AS nP4");
      record = result.next();
      assertArrayEquals(new String[]{"val2"}, record.get("nP4").asList().toArray());

      deleteResult = session.run("CALL semantics.deleteRDF('" +
          RDFImportTest.class.getClassLoader().getResource("deleteRDF/dataset1Delete6.ttl").toURI()
          + "', 'Turtle', {handleVocabUris: 'KEEP', typesToLabels: true, commitSize: 500, keepCustomDataTypes: true, handleMultival: 'ARRAY'})");

      assertEquals(2L, deleteResult.next().get("triplesDeleted").asLong());

      result = session.run("MATCH (n {uri: 'http://example.org/Resource2'})"
          + "RETURN n.`http://example.org/Predicate3` AS nP3, n.`http://example.org/Predicate4` AS nP4");
      record = result.next();
      assertFalse(record.get("nP3").asList().contains(100L));
      assertFalse(record.get("nP4").asList().contains(400.0));

    }
  }

  @Test
  public void testDeleteSubjectNode() throws Exception {
    try (Driver driver = GraphDatabase.driver(neo4j.boltURI(),
        Config.build().withEncryptionLevel(Config.EncryptionLevel.NONE).toConfig())) {

      Session session = driver.session();
      createIndices(neo4j.getGraphDatabaseService());

      StatementResult importResults = session.run("CALL semantics.importRDF('" +
          RDFImportTest.class.getClassLoader().getResource("deleteRDF/dataset1.ttl")
              .toURI()
          + "','Turtle',{ handleVocabUris: 'KEEP', typesToLabels: true, commitSize: 500, keepCustomDataTypes: true, handleMultival: 'ARRAY'})");

      assertEquals(15L, importResults.next().get("triplesLoaded").asLong());
      StatementResult result = session.run("MATCH (n:Resource)"
          + "RETURN n");
      assertEquals(3, result.list().size());
      result = session.run("MATCH (n {uri: 'http://example.org/Resource3'})"
          + "RETURN n.uri");
      Record record = result.next();
      assertEquals("http://example.org/Resource3", record.get("n.uri").asString());

      StatementResult deleteResult = session.run("CALL semantics.deleteRDF('" +
          RDFImportTest.class.getClassLoader().getResource("deleteRDF/dataset1Delete7.ttl").toURI()
          + "', 'Turtle', {handleVocabUris: 'KEEP', typesToLabels: true, commitSize: 500, keepCustomDataTypes: true, handleMultival: 'ARRAY'})");

      assertEquals(1L, deleteResult.next().get("triplesDeleted").asLong());

      result = session.run("MATCH (n {uri: 'http://example.org/Resource3'})"
          + "RETURN n.uri");
      assertFalse(result.hasNext());

    }
  }

  @Test
  public void testRepetitiveDeletion() throws Exception {
    try (Driver driver = GraphDatabase.driver(neo4j.boltURI(),
        Config.build().withEncryptionLevel(Config.EncryptionLevel.NONE).toConfig())) {

      Session session = driver.session();
      createIndices(neo4j.getGraphDatabaseService());

      StatementResult importResults = session.run("CALL semantics.importRDF('" +
          RDFImportTest.class.getClassLoader().getResource("deleteRDF/dataset1.ttl")
              .toURI()
          + "','Turtle',{ handleVocabUris: 'KEEP', typesToLabels: true, commitSize: 500, keepCustomDataTypes: true, handleMultival: 'ARRAY'})");

      assertEquals(15L, importResults.next().get("triplesLoaded").asLong());
      StatementResult result = session.run("MATCH (n:Resource)"
          + "RETURN n");
      assertEquals(3, result.list().size());
      result = session.run("MATCH (n {uri: 'http://example.org/Resource3'})"
          + "RETURN n.uri");
      Record record = result.next();
      assertEquals("http://example.org/Resource3", record.get("n.uri").asString());

      StatementResult deleteResult = session.run("CALL semantics.deleteRDF('" +
          RDFImportTest.class.getClassLoader().getResource("deleteRDF/dataset1Delete4.ttl").toURI()
          + "', 'Turtle', {handleVocabUris: 'KEEP', typesToLabels: true, commitSize: 500, keepCustomDataTypes: true, handleMultival: 'ARRAY'})");

      assertEquals(8L, deleteResult.next().get("triplesDeleted").asLong());

      deleteResult = session.run("CALL semantics.deleteRDF('" +
          RDFImportTest.class.getClassLoader().getResource("deleteRDF/dataset1Delete4.ttl").toURI()
          + "', 'Turtle', {handleVocabUris: 'KEEP', typesToLabels: true, commitSize: 500, keepCustomDataTypes: true, handleMultival: 'ARRAY'})");

      assertEquals(0L, deleteResult.next().get("triplesDeleted").asLong());

    }
  }

  @Test
  public void testImportRDFDatasetTriG() throws Exception {
    try (Driver driver = GraphDatabase.driver(neo4j.boltURI(),
        Config.build().withEncryptionLevel(Config.EncryptionLevel.NONE).toConfig())) {

      Session session = driver.session();
      createIndices(neo4j.getGraphDatabaseService());

      StatementResult importResults = session.run("CALL semantics.importRDFDataset('" +
          RDFImportTest.class.getClassLoader().getResource("RDFDatasets/RDFDataset.trig")
              .toURI()
          + "','TriG',{ handleVocabUris: 'KEEP', typesToLabels: true, commitSize: 500, keepCustomDataTypes: true, handleMultival: 'ARRAY'})");

      assertEquals(13L, importResults.next().get("triplesLoaded").asLong());
      StatementResult result = session
          .run("MATCH (n:Resource {uri: 'http://www.example.org/exampleDocument#Monica'})"
              + "RETURN count(n) AS count");
      assertEquals(3, result.next().get("count").asInt());
      result = session
          .run("MATCH (n:Resource {uri: 'http://www.example.org/exampleDocument#John'})"
              + "RETURN count(n) AS count");
      assertEquals(3, result.next().get("count").asInt());
      result = session
          .run("MATCH (n:Resource {uri: 'http://www.example.org/exampleDocument#Monica'})"
              + "RETURN n.graphUri AS graphUri ORDER BY graphUri");
      List<Record> list = result.list();
      assertEquals("http://www.example.org/exampleDocument#G1",
          list.get(0).get("graphUri").asString());
      assertEquals("http://www.example.org/exampleDocument#G2",
          list.get(1).get("graphUri").asString());
      result = session.run("MATCH (n:Resource {uri: 'http://www.example.org/exampleDocument#G1'})"
          + "RETURN n.`http://www.example.org/vocabulary#created` AS created");
      assertEquals("06.06.2019^^http://www.w3.org/2001/XMLSchema#date",
          result.next().get("created").asList().get(0));
      result = session.run("MATCH (n {uri: 'http://www.example.org/exampleDocument#Monica'})"
          + "WHERE NOT EXISTS(n.graphUri)"
          + "RETURN labels(n) AS labels");
      Record record = result.next();
      assertEquals("Resource",
          record.get("labels").asList().get(0));
      assertEquals("http://www.example.org/vocabulary#Person",
          record.get("labels").asList().get(1));
      result = session.run(
          "MATCH (n {uri: 'http://www.example.org/exampleDocument#John', "
              + "graphUri: 'http://www.example.org/exampleDocument#G3'})"
              + "RETURN labels(n) AS labels");
      record = result.next();
      assertEquals("Resource",
          record.get("labels").asList().get(0));
      assertEquals("http://www.example.org/vocabulary#Person",
          record.get("labels").asList().get(1));
      result = session
          .run(
              "MATCH (n:Resource)"
                  + "-[:`http://www.example.org/vocabulary#friendOf`]->"
                  + "(m:Resource)"
                  + "RETURN NOT EXISTS(n.graphUri) AND NOT EXISTS(m.graphUri) AS result");
      assertTrue(result.next().get("result").asBoolean());
      result = session
          .run(
              "MATCH (n:Resource)"
                  + "-[:`http://www.example.org/vocabulary#knows`]->"
                  + "(m:Resource)"
                  + "RETURN NOT EXISTS(n.graphUri) AND NOT EXISTS(m.graphUri) AS result");
      assertFalse(result.next().get("result").asBoolean());
    }
  }

  @Test
  public void testImportRDFDatasetNQuads() throws Exception {
    try (Driver driver = GraphDatabase.driver(neo4j.boltURI(),
        Config.build().withEncryptionLevel(Config.EncryptionLevel.NONE).toConfig())) {

      Session session = driver.session();
      createIndices(neo4j.getGraphDatabaseService());

      StatementResult importResults = session.run("CALL semantics.importRDFDataset('" +
          RDFImportTest.class.getClassLoader().getResource("RDFDatasets/RDFDataset.nq")
              .toURI()
          + "','N-Quads',{ handleVocabUris: 'KEEP', typesToLabels: true, commitSize: 500, keepCustomDataTypes: true, handleMultival: 'ARRAY'})");

      assertEquals(13L, importResults.next().get("triplesLoaded").asLong());
      StatementResult result = session
          .run("MATCH (n:Resource {uri: 'http://www.example.org/exampleDocument#Monica'})"
              + "RETURN count(n) AS count");
      assertEquals(3, result.next().get("count").asInt());
      result = session
          .run("MATCH (n:Resource {uri: 'http://www.example.org/exampleDocument#John'})"
              + "RETURN count(n) AS count");
      assertEquals(3, result.next().get("count").asInt());
      result = session
          .run("MATCH (n:Resource {uri: 'http://www.example.org/exampleDocument#Monica'})"
              + "RETURN n.graphUri AS graphUri ORDER BY graphUri");
      List<Record> list = result.list();
      assertEquals("http://www.example.org/exampleDocument#G1",
          list.get(0).get("graphUri").asString());
      assertEquals("http://www.example.org/exampleDocument#G2",
          list.get(1).get("graphUri").asString());
      result = session.run("MATCH (n:Resource {uri: 'http://www.example.org/exampleDocument#G1'})"
          + "RETURN n.`http://www.example.org/vocabulary#created` AS created");
      assertEquals("06.06.2019^^http://www.w3.org/2001/XMLSchema#date",
          result.next().get("created").asList().get(0));
      result = session.run("MATCH (n {uri: 'http://www.example.org/exampleDocument#Monica'})"
          + "WHERE NOT EXISTS(n.graphUri)"
          + "RETURN labels(n) AS labels");
      Record record = result.next();
      assertEquals("Resource",
          record.get("labels").asList().get(0));
      assertEquals("http://www.example.org/vocabulary#Person",
          record.get("labels").asList().get(1));
      result = session.run(
          "MATCH (n {uri: 'http://www.example.org/exampleDocument#John', "
              + "graphUri: 'http://www.example.org/exampleDocument#G3'})"
              + "RETURN labels(n) AS labels");
      record = result.next();
      assertEquals("Resource",
          record.get("labels").asList().get(0));
      assertEquals("http://www.example.org/vocabulary#Person",
          record.get("labels").asList().get(1));
      result = session
          .run(
              "MATCH (n:Resource)"
                  + "-[:`http://www.example.org/vocabulary#friendOf`]->"
                  + "(m:Resource)"
                  + "RETURN NOT EXISTS(n.graphUri) AND NOT EXISTS(m.graphUri) AS result");
      assertTrue(result.next().get("result").asBoolean());
      result = session
          .run(
              "MATCH (n:Resource)"
                  + "-[:`http://www.example.org/vocabulary#knows`]->"
                  + "(m:Resource)"
                  + "RETURN NOT EXISTS(n.graphUri) AND NOT EXISTS(m.graphUri) AS result");
      assertFalse(result.next().get("result").asBoolean());
    }
  }

  @Test
  public void testDeleteRDFDatasetTriG() throws Exception {
    try (Driver driver = GraphDatabase.driver(neo4j.boltURI(),
        Config.build().withEncryptionLevel(Config.EncryptionLevel.NONE).toConfig())) {

      Session session = driver.session();
      createIndices(neo4j.getGraphDatabaseService());

      StatementResult importResults = session.run("CALL semantics.importRDFDataset('" +
          RDFImportTest.class.getClassLoader().getResource("RDFDatasets/RDFDataset.trig")
              .toURI()
          + "','TriG',{ handleVocabUris: 'KEEP', typesToLabels: true, commitSize: 500, keepCustomDataTypes: true, handleMultival: 'ARRAY'})");

      assertEquals(13L, importResults.next().get("triplesLoaded").asLong());
      StatementResult result = session.run("MATCH (n:Resource)"
          + "RETURN n");
      assertEquals(12, result.list().size());

      StatementResult deleteResult = session.run("CALL semantics.deleteRDFDataset('" +
          RDFImportTest.class.getClassLoader().getResource("RDFDatasets/RDFDatasetDelete.trig")
              .toURI()
          + "', 'TriG', {handleVocabUris: 'KEEP', typesToLabels: true, commitSize: 500, keepCustomDataTypes: true, handleMultival: 'ARRAY'})");

      assertEquals(9L, deleteResult.next().get("triplesDeleted").asLong());

      result = session.run("MATCH (n:Resource)"
          + "RETURN n");
      assertEquals(5, result.list().size());

    }
  }

  @Test
  public void testDeleteRDFDatasetNQuads() throws Exception {
    try (Driver driver = GraphDatabase.driver(neo4j.boltURI(),
        Config.build().withEncryptionLevel(Config.EncryptionLevel.NONE).toConfig())) {

      Session session = driver.session();
      createIndices(neo4j.getGraphDatabaseService());

      StatementResult importResults = session.run("CALL semantics.importRDFDataset('" +
          RDFImportTest.class.getClassLoader().getResource("RDFDatasets/RDFDataset.nq")
              .toURI()
          + "','N-Quads',{ handleVocabUris: 'KEEP', typesToLabels: true, commitSize: 500, keepCustomDataTypes: true, handleMultival: 'ARRAY'})");

      assertEquals(13L, importResults.next().get("triplesLoaded").asLong());
      StatementResult result = session.run("MATCH (n:Resource)"
          + "RETURN n");
      assertEquals(12, result.list().size());

      StatementResult deleteResult = session.run("CALL semantics.deleteRDFDataset('" +
          RDFImportTest.class.getClassLoader().getResource("RDFDatasets/RDFDatasetDelete.nq")
              .toURI()
          + "', 'N-Quads', {handleVocabUris: 'KEEP', typesToLabels: true, commitSize: 500, keepCustomDataTypes: true, handleMultival: 'ARRAY'})");

      assertEquals(9L, deleteResult.next().get("triplesDeleted").asLong());

      result = session.run("MATCH (n:Resource)"
          + "RETURN n");
      assertEquals(5, result.list().size());

    }
  }

  private void createIndices(GraphDatabaseService db) {
    db.execute("CREATE INDEX ON :Resource(uri)");
  }

  private static URI file(String path) {
    try {
      return RDFImportTest.class.getClassLoader().getResource(path).toURI();
    } catch (URISyntaxException e) {
      String msg = String.format("Failed to load the resource with path '%s'", path);
      throw new RuntimeException(msg, e);
    }
  }
}
