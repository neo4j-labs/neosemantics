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

public class RDFImportConfigTest {
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

  private static URI file(String path) {
    try {
      return RDFImportConfigTest.class.getClassLoader().getResource(path).toURI();
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
  public void testImportLangFilter() throws Exception {
    try (Session session = driver.session()) {

      initialiseGraphDB(neo4j.defaultDatabaseService(), null);

      session.run("call n10s.nsprefixes.add('voc','http://example.org/vocab/show/')");

      Result importResults
              = session.run("CALL n10s.rdf.import.fetch('" +
              RDFImportConfigTest.class.getClassLoader().getResource("multilang.ttl")
                      .toURI()
              + "','Turtle',{ languageFilter: 'en', commitSize: 500})");
      assertEquals(1L, importResults
              .next().get("triplesLoaded").asLong());
      assertEquals("That Seventies Show",
              session.run(
                              "MATCH (t {uri: 'http://example.org/vocab/show/218'}) RETURN t.voc" + PREFIX_SEPARATOR
                                      + "localName AS name")
                      .next().get("name").asString());

      session.run("MATCH (t {uri: 'http://example.org/vocab/show/218'}) DETACH DELETE t ");

      importResults
              = session.run("CALL n10s.rdf.import.fetch('" +
              RDFImportConfigTest.class.getClassLoader().getResource("multilang.ttl")
                      .toURI()
              + "','Turtle',{ languageFilter: 'fr', commitSize: 500})");
      assertEquals(1L, importResults
              .next().get("triplesLoaded").asLong());
      assertEquals("Cette Série des Années Soixante-dix",
              session.run(
                              "MATCH (t {uri: 'http://example.org/vocab/show/218'}) RETURN t.voc" + PREFIX_SEPARATOR
                                      + "localName AS name")
                      .next().get("name").asString());

      session.run("MATCH (t {uri: 'http://example.org/vocab/show/218'}) DETACH DELETE t ");

      importResults
              = session.run("CALL n10s.rdf.import.fetch('" +
              RDFImportConfigTest.class.getClassLoader().getResource("multilang.ttl")
                      .toURI()
              + "','Turtle',{ handleVocabUris: 'SHORTEN', handleRDFTypes: 'LABELS', languageFilter: 'fr-be', commitSize: 500})");
      assertEquals(1L, importResults
              .next().get("triplesLoaded").asLong());
      assertEquals("Cette Série des Années Septante",
              session.run(
                              "MATCH (t {uri: 'http://example.org/vocab/show/218'}) RETURN t.voc" + PREFIX_SEPARATOR
                                      + "localName AS name")
                      .next().get("name").asString());

      session.run("MATCH (t {uri: 'http://example.org/vocab/show/218'}) DETACH DELETE t ");

      importResults
              = session.run("CALL n10s.rdf.import.fetch('" +
              RDFImportConfigTest.class.getClassLoader().getResource("multilang.ttl")
                      .toURI()
              + "','Turtle',{ handleVocabUris: 'SHORTEN', handleRDFTypes: 'LABELS', commitSize: 500})");
      // no language filter means three triples are ingested
      assertEquals(3L, importResults
              .next().get("triplesLoaded").asLong());
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
    try (Session session = driver.session()) {

      initialiseGraphDB(neo4j.defaultDatabaseService(),
              "{ keepLangTag : true, handleMultival: 'ARRAY'}");
      String importCypher = "CALL n10s.rdf.import.fetch('" +
              RDFImportConfigTest.class.getClassLoader().getResource("multilang.ttl")
                      .toURI() + "','Turtle')";
      Result importResults
              = session.run(importCypher);
      Record next = importResults
              .next();
      assertEquals(3, next.get("triplesLoaded").asInt());

      importResults
              = session.run(
              "match (n:Resource) return n.ns0__localName as all, n10s.rdf.getLangValue('en',n.ns0__localName) as en_name, "
                      +
                      "n10s.rdf.getLangValue('fr',n.ns0__localName) as fr_name, n10s.rdf.getLangValue('fr-be',n.ns0__localName) as frbe_name");
      next = importResults
              .next();
      assertEquals("That Seventies Show", next.get("en_name").asString());
      assertEquals("Cette Série des Années Soixante-dix", next.get("fr_name").asString());
      assertEquals("Cette Série des Années Septante", next.get("frbe_name").asString());
    }
  }

  @Test
  public void testImportMultivalWithMultivalList() throws Exception {
    try (Session session = driver.session()) {

      initialiseGraphDB(neo4j.defaultDatabaseService(),
              "{ handleMultival: 'ARRAY', " +
                      "multivalPropList : ['http://example.org/vocab/show/availableInLang','http://example.org/vocab/show/localName'] }");
      String importCypher = "CALL n10s.rdf.import.fetch('" +
              RDFImportConfigTest.class.getClassLoader().getResource("multival.ttl")
                      .toURI()
              + "','Turtle')";
      Result importResults
              = session.run(importCypher);
      Record next = importResults
              .next();

      assertEquals(9, next.get("triplesLoaded").asInt());

      importResults
              = session.run(
              "match (n:Resource) return n.ns0__localName as all, n.ns0__availableInLang as ail, n.ns0__showId as sid, n.ns0__producer as prod ");
      next = importResults
              .next();
      List<String> localNames = new ArrayList<>();
      localNames.add("That Seventies Show");
      localNames.add("Cette Série des Années Soixante-dix");
      localNames.add("Cette Série des Années Septante");
      assertTrue(next.get("all").asList().containsAll(localNames));
      List<String> availableInLang = new ArrayList<>();
      availableInLang.add("EN");
      availableInLang.add("FR");
      availableInLang.add("ES");
      assertTrue(next.get("ail").asList().containsAll(availableInLang));
      assertEquals(218, next.get("sid").asLong());
      assertEquals("Joanna Smith", next.get("prod").asString());
    }
  }

  @Test
  public void testImportMultivalWithExclusionList() throws Exception {
    try (Session session = driver.session()) {

      initialiseGraphDB(neo4j.defaultDatabaseService(),
              "{ handleMultival: 'ARRAY' }");
      String importCypher = "CALL n10s.rdf.import.fetch('" +
              RDFImportConfigTest.class.getClassLoader().getResource("multival.ttl")
                      .toURI()
              + "','Turtle',  { predicateExclusionList : ['http://example.org/vocab/show/availableInLang','http://example.org/vocab/show/localName'] })";
      Result importResults
              = session.run(importCypher);
      Record next = importResults
              .next();

      assertEquals(3, next.get("triplesLoaded").asInt());

      importResults
              = session.run(
              "match (n:Resource) return n.ns0__localName as all, n.ns0__availableInLang as ail, n.ns0__showId as sid, n.ns0__producer as prod ");
      next = importResults
              .next();
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
  public void testImportTurtleSnippetWithPoints() throws Exception {
    try (Session session = driver.session();) {
      initialiseGraphDB(neo4j.defaultDatabaseService(),
              " { handleVocabUris: 'SHORTEN' } ");

      String turtleFragmentWithPoints = "@prefix n4sch: <neo4j://graph.schema#> .\n" +
              "@prefix n4ind: <neo4j://graph.individuals#> .\n" +
              "\n" +
              "n4ind:0 a n4sch:Cable;\n" +
              "  n4sch:id \"1\"^^<http://www.w3.org/2001/XMLSchema#long>;\n" +
              "  n4sch:createdAt \"2021-08-05T16:18:45.262\"^^<http://www.w3.org/2001/XMLSchema#dateTime>;\n" +
              "  n4sch:name \"cable_1\" .\n" +
              "\n" +
              "n4ind:1 a n4sch:CableRoutingPoint;\n" +
              "  n4sch:id \"1\"^^<http://www.w3.org/2001/XMLSchema#long>;\n" +
              "  n4sch:inspectionDates \"2010-05-29T14:17:39.262+02:00\"^^<http://www.w3.org/2001/XMLSchema#dateTime>;\n" +
              "  n4sch:typeCodes \"A\", \"B\", \"C\";\n" +
              "  n4sch:location \"Point(0.13677937940559515 0.12571228469149265 0.5158046487527811)\"^^<http://www.opengis.net/ont/geosparql#wktLiteral> .\n" +
              "\n" +
              "n4ind:2 a n4sch:CableRoutingPoint;\n" +
              "  n4sch:typeCodes \"C\", \"B\", \"A\";\n" +
              "  n4sch:location \"Point(0.7387649148541224 0.9566884508913421 0.08440601703554396)\"^^<http://www.opengis.net/ont/geosparql#wktLiteral>;\n" +
              "  n4sch:inspectionDates \"2021-08-05T16:18:45.262\"^^<http://www.w3.org/2001/XMLSchema#dateTime>;\n" +
              "  n4sch:id \"2\"^^<http://www.w3.org/2001/XMLSchema#long> .\n";

      Result importResults1 = session.run("CALL n10s.rdf.import.inline('" +
              turtleFragmentWithPoints + "','Turtle')");
      assertEquals(18L, importResults1.single().get("triplesLoaded").asLong());

      assertEquals(0.08440601703554396D,
              session.run(
                              "MATCH (r:Resource { uri: 'neo4j://graph.individuals#2'}) return r.ns0__location.z as h")
                      .next().get("h").asDouble(), 0.00000000001D);

      assertEquals(ZonedDateTime.parse("2010-05-29T14:17:39.262+02:00"),
              session.run(
                              "MATCH (r:Resource { uri: 'neo4j://graph.individuals#1'}) return r.ns0__inspectionDates as h")
                      .next().get("h").asZonedDateTime());

    }
  }

  @Test
  public void testImportFromFileWithMapping() throws Exception {
    try (Session session = driver.session()) {
      session.run("call n10s.nsprefixes.add('voc','http://neo4j.com/voc/')");
      session.run("call n10s.nsprefixes.add('cats','http://neo4j.com/category/')");
    }

    try (Session session = driver.session()) {

      initialiseGraphDB(neo4j.defaultDatabaseService(), "{ handleVocabUris: 'MAP'}");

      session.run(" call n10s.mapping.add('http://neo4j.com/voc/name','uniqueName') ");
      session.run(" call n10s.mapping.add('http://neo4j.com/category/Publication','Media') ");

      Result importResults
              = session.run("CALL n10s.rdf.import.fetch('" +
              RDFImportConfigTest.class.getClassLoader().getResource("myrdf/three.rdf")
                      .toURI() + "','RDF/XML')");
      assertEquals(6L, importResults
              .next().get("triplesLoaded").asLong());
      Result mediaNames = session.run("MATCH (m:Media) " +
              "\nRETURN m.uniqueName AS nm, m.uri AS uri");

      Record next = mediaNames.next();
      assertEquals("The Financial Times", next.get("nm").asString());
      assertEquals("http://neo4j.com/invividual/FT", next.get("uri").asString());

      Result personNames = session.run("MATCH (m { PersonName : 'JC'}) " +
              "\nRETURN m.LivesIn AS li, m.uri AS uri");

      next = personNames.next();
      assertEquals("Chesham", next.get("li").asString());
      assertEquals("http://neo4j.com/invividual/JC", next.get("uri").asString());
    }
  }

  @Test
  public void testImportFromFileIgnoreNs() throws Exception {
    try (Session session = driver.session()) {

      initialiseGraphDB(neo4j.defaultDatabaseService(), "{ handleVocabUris: 'IGNORE'}");

      Result importResults
              = session.run("CALL n10s.rdf.import.fetch('" +
              RDFImportConfigTest.class.getClassLoader().getResource("myrdf/three.rdf")
                      .toURI() + "','RDF/XML')");
      assertEquals(6L, importResults
              .next().get("triplesLoaded").asLong());
      Result mediaNames = session.run("MATCH (m:Publication) " +
              "\nRETURN m.name AS nm, m.uri AS uri");

      Record next = mediaNames.next();
      assertEquals("The Financial Times", next.get("nm").asString());
      assertEquals("http://neo4j.com/invividual/FT", next.get("uri").asString());

      Result rels = session.run(
              "MATCH ({ PersonName: 'JC'})-[r:reads]-(:Publication { name: 'The Financial Times'}) " +
                      "\nRETURN count(r) as ct");

      next = rels.next();
      assertEquals(1L, next.get("ct").asLong());

    }
  }

  @Test
  public void testImportFromFileIgnoreNsApplyNeoNaming() throws Exception {
    try (Session session = driver.session()) {

      initialiseGraphDB(neo4j.defaultDatabaseService(),
              "{ handleVocabUris: 'IGNORE', applyNeo4jNaming: true }");

      Result importResults
              = session.run("CALL n10s.rdf.import.fetch('" +
              RDFImportConfigTest.class.getClassLoader().getResource("myrdf/three.rdf")
                      .toURI() + "','RDF/XML')");
      assertEquals(6L, importResults
              .next().get("triplesLoaded").asLong());
      Result mediaNames = session.run("MATCH (m:Publication) " +
              "\nRETURN m.name AS nm, m.uri AS uri");

      Record next = mediaNames.next();
      assertEquals("The Financial Times", next.get("nm").asString());
      assertEquals("http://neo4j.com/invividual/FT", next.get("uri").asString());

      Result rels = session.run(
              "MATCH ({ personName: 'JC'})-[r:READS]-(:Publication { name: 'The Financial Times'}) " +
                      "\nRETURN count(r) as ct");

      next = rels.next();
      assertEquals(1L, next.get("ct").asLong());

    }
  }

  @Test
  public void testImportFromFileWithPredFilter() throws Exception {

    try (Session session = driver.session()) {
      session.run("call n10s.nsprefixes.add('sch','http://schema.org/')");
      session.run("call n10s.nsprefixes.add('cats','http://neo4j.com/category/')");
    }

    try (Session session = driver.session()) {

      initialiseGraphDB(neo4j.defaultDatabaseService(), "{ handleVocabUris: 'MAP'}");

      session.run(" call n10s.mapping.add(\"http://schema.org/location\",\"WHERE\") ");
      session.run(" call n10s.mapping.add(\"http://schema.org/description\",\"desc\") ");

      Result importResults
              = session.run("CALL n10s.rdf.import.fetch('" +
              RDFImportConfigTest.class.getClassLoader().getResource("event.json")
                      .toURI()
              + "','JSON-LD', {predicateExclusionList: ['http://schema.org/price','http://schema.org/priceCurrency'] })");
      assertEquals(26L, importResults
              .next().get("triplesLoaded").asLong());

      Result postalAddresses = session.run("MATCH (m:PostalAddress) " +
              "\nRETURN m.postalCode as zip");

      Record next = postalAddresses.next();
      assertEquals("95051", next.get("zip").asString());

      Result whereRels = session.run("MATCH (e:Event)-[:WHERE]->(p:Place) " +
              "\nRETURN p.name as placeName, e.desc as desc ");

      next = whereRels.next();
      assertEquals(
              "Join us for an afternoon of Jazz with Santa Clara resident and pianist Andy Lagunoff. " +
                      "Complimentary food and beverages will be served.",
              next.get("desc").asString());
      assertEquals("Santa Clara City Library, Central Park Library",
              next.get("placeName").asString());

    }
  }

  @Test
  public void testImportMultiValAfterImportSingelVal() throws Exception {
    try (Session session = driver.session()) {

      initialiseGraphDB(neo4j.defaultDatabaseService(),
              "{ handleMultival: 'OVERWRITE', handleVocabUris: 'KEEP'  }");
      String importCypher = "CALL n10s.rdf.import.fetch('" +
              RDFImportConfigTest.class.getClassLoader()
                      .getResource("testImportMultiValAfterImportSingelVal.ttl")
                      .toURI() + "','Turtle')";
      Result importResults = session.run(importCypher);
      Record next = importResults.next();
      assertEquals(3, next.get("triplesLoaded").asInt());
      Result queryResults = session
              .run("MATCH (n:Resource) RETURN n.`http://example.com/price` AS price");
      Object imports = queryResults.next().get("price");
      assertEquals(IntegerValue.class, imports.getClass());

      session.run("MATCH (n) DETACH DELETE n ;");
      //set graph config
      session
              .run("CALL n10s.graphconfig.init({ handleMultival: 'ARRAY', handleVocabUris: 'KEEP' });");

      importCypher = "CALL n10s.rdf.import.fetch('" +
              RDFImportConfigTest.class.getClassLoader()
                      .getResource("testImportMultiValAfterImportSingelVal.ttl")
                      .toURI() + "','Turtle')";

      importResults = session.run(importCypher);

      next = importResults.next();
      assertEquals(3, next.get("triplesLoaded").asInt());

      queryResults = session.run("MATCH (n:Resource) RETURN n.`http://example.com/price` AS price");
      imports = queryResults.next().get("price");
      assertEquals(ListValue.class, imports.getClass());
    }
  }
}
