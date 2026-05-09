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

public class RDFUDFTest {
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
      return RDFUDFTest.class.getClassLoader().getResource(path).toURI();
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
  public void testGetLangValUDF() throws Exception {
    try (Session session = driver.session()) {

      initialiseGraphDB(neo4j.defaultDatabaseService(), null);

      Result importResults
              = session.run(
              "return n10s.rdf.getLangValue('fr',[\"The Hague@en\", \"Den Haag@nl\", \"La Haye@fr\"]) as val");
      Map<String, Object> next = importResults
              .next().asMap();
      assertEquals("La Haye", next.get("val"));

      importResults
              = session.run(
              "return n10s.rdf.getLangValue('es',[\"The Hague@en\", \"Den Haag@nl\", \"La Haye@fr\"]) as val");
      next = importResults
              .next().asMap();
      assertNull(next.get("val"));

      importResults
              = session.run("return n10s.rdf.getLangValue('fr','La Haye@fr') as val");
      next = importResults
              .next().asMap();
      assertEquals("La Haye", next.get("val"));

      importResults
              = session.run("return n10s.rdf.getLangValue('es','La Haye@fr') as val");
      next = importResults
              .next().asMap();
      assertNull(next.get("val"));

      importResults
              = session.run("return n10s.rdf.getLangValue('es',[2, 45, 3]) as val");
      next = importResults
              .next().asMap();
      assertNull(next.get("val"));

      session.run(
              "create (n:Thing { prop: [\"That Seventies Show@en\", \"Cette Série des Années Soixante-dix@fr\", \"Cette Série des Années Septante@fr-be\"] })");
      importResults
              = session.run(
              "match (n:Thing) return n10s.rdf.getLangValue('en',n.prop) as en_name, n10s.rdf.getLangValue('fr',n.prop) as fr_name, n10s.rdf.getLangValue('fr-be',n.prop) as frbe_name");
      next = importResults
              .next().asMap();
      assertEquals("Cette Série des Années Soixante-dix", next.get("fr_name"));
      assertEquals("That Seventies Show", next.get("en_name"));
      assertEquals("Cette Série des Années Septante", next.get("frbe_name"));

      session.run("match (x:Thing) delete x");
      session.run(
              "create (n:Thing { prop: [\"That Seventies Show@en-US\", \"Cette Série des Années Soixante-dix@fr-custom-tag\", \"你好@zh-Hans-CN\"] })");
      importResults
              = session.run(
              "match (n:Thing) return n10s.rdf.getLangValue('en-US',n.prop) as enus_name, n10s.rdf.getLangValue('fr-custom-tag',n.prop) as frcust_name, n10s.rdf.getLangValue('zh-Hans-CN',n.prop) as cn_name");
      next = importResults
              .next().asMap();
      assertEquals("Cette Série des Années Soixante-dix", next.get("frcust_name"));
      assertEquals("That Seventies Show", next.get("enus_name"));
      assertEquals("你好", next.get("cn_name"));
    }
  }

  @Test
  public void testGetLangTagUDF() throws Exception {
    try (Session session = driver.session()) {

      initialiseGraphDB(neo4j.defaultDatabaseService(), null);

      Result importResults
              = session.run("return n10s.rdf.getLangTag('The Hague@en') as val_en,"
              + "n10s.rdf.getLangTag('Den Haag@nl') as val_nl, "
              + "n10s.rdf.getLangTag('La Haye@fr') as val_fr,"
              + "n10s.rdf.getLangTag('That Seventies Show@en-US') as val_us,"
              + "n10s.rdf.getLangTag([2, 45, 3]) as val_array,"
              + "n10s.rdf.getLangTag('hello') as val_no_tag");
      Map<String, Object> next = importResults
              .next().asMap();
      assertEquals("en", next.get("val_en"));
      assertEquals("fr", next.get("val_fr"));
      assertEquals("nl", next.get("val_nl"));
      assertEquals("en-US", next.get("val_us"));
      assertNull(next.get("val_array"));
      assertNull(next.get("val_no_tag"));

      session.run(
              "create (n:Thing { prop: [\"That Seventies Show@en-US\", \"Cette Série des Années Soixante-dix@fr-custom-tag\", \"你好@zh-Hans-CN\"] })");
      importResults
              = session.run(
              "match (n:Thing) return n10s.rdf.getLangTag(n.prop[0]) as enus_tag, "
                      + "n10s.rdf.getLangTag(n.prop[1]) as frcust_tag, n10s.rdf.getLangTag(n.prop[2]) as cn_tag");
      next = importResults
              .next().asMap();
      assertEquals("fr-custom-tag", next.get("frcust_tag"));
      assertEquals("en-US", next.get("enus_tag"));
      assertEquals("zh-Hans-CN", next.get("cn_tag"));
    }
  }


  @Test
  public void testHasLangTagUDF() throws Exception {
    try (Session session = driver.session()) {

      initialiseGraphDB(neo4j.defaultDatabaseService(), null);

      Result importResults
              = session.run("return n10s.rdf.hasLangTag('en','The Hague@en') as val_en,"
              + "n10s.rdf.hasLangTag('nl','Den Haag@nl') as val_nl, "
              + "n10s.rdf.hasLangTag('en','La Haye@fr') as val_fr_no,"
              + "n10s.rdf.hasLangTag('en-US','That Seventies Show@en-US') as val_us,"
              + "n10s.rdf.hasLangTag('it',[2, 45, 3]) as val_array,"
              + "n10s.rdf.hasLangTag('ru','hello') as val_no_tag");
      Map<String, Object> next = importResults
              .next().asMap();
      assertEquals(true, next.get("val_en"));
      assertEquals(false, next.get("val_fr_no"));
      assertEquals(true, next.get("val_nl"));
      assertEquals(true, next.get("val_us"));
      assertEquals(false, next.get("val_array"));
      assertEquals(false, next.get("val_no_tag"));

      session.run(
              "create (n:Thing { prop: [\"That Seventies Show@en-US\", \"Cette Série des Années Soixante-dix@fr-custom-tag\", \"你好@zh-Hans-CN\"] })");
      importResults
              = session.run(
              "match (n:Thing) return n10s.rdf.hasLangTag('en-US',n.prop[0]) as enus_tag, "
                      + "n10s.rdf.hasLangTag('fr-custom-tag',n.prop[1]) as frcust_tag, "
                      + "n10s.rdf.hasLangTag('es',n.prop[1]) as frcust_tag_no, "
                      + "n10s.rdf.hasLangTag('zh-Hans-CN',n.prop[2]) as cn_tag");
      next = importResults
              .next().asMap();
      assertEquals(true, next.get("frcust_tag"));
      assertEquals(false, next.get("frcust_tag_no"));
      assertEquals(true, next.get("enus_tag"));
      assertEquals(true, next.get("cn_tag"));
    }
  }

  @Test
  public void testGetUriFromShortAndShortFromUri() throws Exception {
    try (Session session = driver.session()) {

      initialiseGraphDB(neo4j.defaultDatabaseService(),
              "{ handleVocabUris: 'SHORTEN', handleRDFTypes: 'LABELS' }");

      Result importResults
              = session.run("CALL n10s.rdf.import.fetch('" +
              RDFUDFTest.class.getClassLoader().getResource("mini-ld.json").toURI()
              + "','JSON-LD',"
              +
              "{ commitSize: 500 })");
      assertEquals(6L, importResults
              .next().get("triplesLoaded").asLong());
      assertEquals("http://xmlns.com/foaf/0.1/knows",
              session.run("MATCH (n{ns0" + PREFIX_SEPARATOR + "name : 'Markus Lanthaler'})-[r]-() " +
                              " RETURN n10s.rdf.fullUriFromShortForm(type(r)) AS uri")
                      .next().get("uri").asString());

      assertEquals("ns0" + PREFIX_SEPARATOR + "knows",
              session
                      .run("RETURN n10s.rdf.shortFormFromFullUri('http://xmlns.com/foaf/0.1/knows') AS uri")
                      .next().get("uri").asString());
    }
  }

  @Test
  public void testGetDataType() throws Exception {
    try (Session session = driver.session()) {

      initialiseGraphDB(neo4j.defaultDatabaseService(), null);

      Result importResults = session
              .run("return n10s.rdf.getDataType('2008-04-17^^ns1__date') AS val");
      Map<String, Object> next = importResults.next().asMap();
      assertEquals("ns1__date", next.get("val"));

      importResults = session
              .run("return n10s.rdf.getDataType('10000^^http://example.org/USD') AS val");
      next = importResults.next().asMap();
      assertEquals("http://example.org/USD", next.get("val"));

      importResults = session.run("return n10s.rdf.getDataType('10000') AS val");
      next = importResults.next().asMap();
      assertEquals(XMLSchema.STRING.stringValue(), next.get("val"));

      importResults = session.run("return n10s.rdf.getDataType(10000) AS val");
      next = importResults.next().asMap();
      assertEquals(XMLSchema.LONG.stringValue(), next.get("val"));

      importResults = session.run("return n10s.rdf.getDataType(10000.0) AS val");
      next = importResults.next().asMap();
      assertEquals(XMLSchema.DOUBLE.stringValue(), next.get("val"));

      importResults = session.run("return n10s.rdf.getDataType(true) AS val");
      next = importResults.next().asMap();
      assertEquals(XMLSchema.BOOLEAN.stringValue(), next.get("val"));

      importResults = session.run("return n10s.rdf.getDataType(date('1986-07-19')) AS val");
      next = importResults.next().asMap();
      assertEquals(XMLSchema.DATE.stringValue(), next.get("val"));

      importResults = session
              .run("return n10s.rdf.getDataType(localdatetime('1986-07-09T18:06:36')) AS val");
      next = importResults.next().asMap();
      assertEquals(XMLSchema.DATETIME.stringValue(), next.get("val"));

    }
  }

  @Test
  public void testGetValue() throws Exception {
    try (Session session = driver.session()) {

      initialiseGraphDB(neo4j.defaultDatabaseService(), null);

      Result importResults = session
              .run("return n10s.rdf.getValue('2008-04-17^^ns1__date') AS val");
      Map<String, Object> next = importResults.next().asMap();
      assertEquals("2008-04-17", next.get("val"));

      importResults = session.run(
              "return n10s.rdf.getValue('10000^^http://example.org/USD') AS val");
      next = importResults.next().asMap();
      assertEquals("10000", next.get("val"));

      importResults = session.run("return n10s.rdf.getValue('10000') AS val");
      next = importResults.next().asMap();
      assertEquals("10000", next.get("val"));

      importResults = session.run("return n10s.rdf.getValue('This is a test@en') AS val");
      next = importResults.next().asMap();
      assertEquals("This is a test", next.get("val"));
    }
  }

  @Test
  public void testCustomDataTypesKeepURIs() throws Exception {
    try (Session session = driver.session()) {

      initialiseGraphDB(neo4j.defaultDatabaseService(),
              "{ keepLangTag: true, handleMultival: 'ARRAY',  " +
                      "multivalPropList: ['http://example.com/price', 'http://example.com/power', 'http://example.com/class'], "
                      +
                      "keepCustomDataTypes: true,  handleVocabUris: 'KEEP', handleRDFTypes: 'LABELS', " +
                      "customDataTypePropList: ['http://example.com/price', 'http://example.com/color', 'http://example.com/power'] }");

      Result importResults
              = session.run("CALL n10s.rdf.import.fetch('" +
              RDFUDFTest.class.getClassLoader().getResource("customDataTypes.ttl")
                      .toURI() + "','Turtle',{ commitSize: 500 })");
      assertEquals(10L, importResults
              .next().get("triplesLoaded").asLong());
      Result cars = session.run("MATCH (n:`http://example.com/Car`) " +
              "\nRETURN n.`http://example.com/price` AS price," +
              "n.`http://example.com/power` AS power, " +
              "n.`http://example.com/color` AS color, " +
              "n.`http://example.com/class` AS class, n.`http://example.com/released` AS released, " +
              "n.`http://example.com/type` AS type ORDER BY price");

      Record car = cars.next();
      List price = car.get("price").asList();
      assertEquals(2, price.size());
      assertTrue(price.containsAll(Arrays.asList("10000^^http://example.com/EUR", "11000^^http://example.com/USD")));
      assertTrue(car.get("power").asList().containsAll(Arrays.asList("300^^http://example.com/HP", "223,71^^http://example.com/kW")));
      assertEquals("red^^http://example.com/Color", car.get("color").asString());
      assertTrue(car.get("class").asList().containsAll(Arrays.asList("A-Klasse@de", "A-Class@en")));
      assertEquals(2019, car.get("released").asLong());
      assertEquals("Cabrio", car.get("type").asString());
    }
  }

  @Test
  public void testCustomDataTypesShortenURIs() throws Exception {
    try (Session session = driver.session()) {

      initialiseGraphDB(neo4j.defaultDatabaseService(),
              " { keepLangTag: true, handleMultival: 'ARRAY',  " +
                      "multivalPropList: ['http://example.com/price', 'http://example.com/power', 'http://example.com/class'], "
                      +
                      "keepCustomDataTypes: true,  " +
                      "customDataTypePropList: ['http://example.com/price', 'http://example.com/color', 'http://example.com/power'], "
                      +
                      "handleVocabUris: 'SHORTEN', handleRDFTypes: 'LABELS' }");

      Result importResults
              = session.run("CALL n10s.rdf.import.fetch('" +
              RDFUDFTest.class.getClassLoader().getResource("customDataTypes.ttl")
                      .toURI() + "','Turtle',{ commitSize: 500 })");
      assertEquals(10L, importResults
              .next().get("triplesLoaded").asLong());
      Result cars = session.run("MATCH (n:ns0__Car) " +
              "\nRETURN n.ns0__price AS price," +
              "n.ns0__power AS power, " +
              "n.ns0__color AS color, " +
              "n.ns0__class AS class, n.ns0__released AS released, " +
              "n.ns0__type AS type ORDER BY price");

      Record car = cars.next();
      List price = car.get("price").asList();
      assertEquals(2, price.size());
      assertTrue(price.contains("10000^^ns0__EUR"));
      assertTrue(price.contains("11000^^ns0__USD"));
      assertTrue(car.get("power").asList().contains("300^^ns0__HP"));
      assertTrue(car.get("power").asList().contains("223,71^^ns0__kW"));
      assertEquals("red^^ns0__Color", car.get("color").asString());
      assertTrue(car.get("class").asList().contains("A-Klasse@de"));
      assertTrue(car.get("class").asList().contains("A-Class@en"));
      assertEquals(2019, car.get("released").asLong());
      assertEquals("Cabrio", car.get("type").asString());
    }
  }
}
