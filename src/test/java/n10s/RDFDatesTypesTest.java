package n10s;

import static n10s.CommonProcedures.UNIQUENESS_CONSTRAINT_STATEMENT;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.net.URI;
import java.net.URISyntaxException;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.HashSet;
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
import org.junit.*;
import org.neo4j.driver.*;
import org.neo4j.driver.Record;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.harness.junit.rule.Neo4jRule;

public class RDFDatesTypesTest {
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

  private static URI file(String path) {
    try {
      return RDFDatesTypesTest.class.getClassLoader().getResource(path).toURI();
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
  public void testImportDatesAndTimes() throws Exception {
    try (Session session = driver.session();) {
      initialiseGraphDB(neo4j.defaultDatabaseService(), null);

      Result importResults1 = session.run("CALL n10s.rdf.import.fetch('" +
              RDFDatesTypesTest.class.getClassLoader().getResource("datetime/datetime-simple.ttl")
                      .toURI()
              + "','Turtle')");
      assertEquals(2L, importResults1.single().get("triplesLoaded").asLong());
      Record result = session.run(
                      "MATCH (n:Resource) RETURN n.ns0__reportedOn AS rep, n.`ns0__creation-date` AS cre")
              .next();
      assertEquals(LocalDateTime.parse("2012-12-31T23:57"),
              result.get("rep").asLocalDateTime());
      assertEquals(LocalDate.parse("1999-08-16"),
              result.get("cre").asLocalDate());
    }
  }

  @Test
  public void testImportDatesAndTimes2() throws Exception {
    try (Session session = driver.session();) {
      initialiseGraphDB(neo4j.defaultDatabaseService(), null);

      Result importResults1 = session.run("CALL n10s.rdf.import.fetch('" +
              RDFDatesTypesTest.class.getClassLoader().getResource("datetime/datetime-complex.ttl")
                      .toURI()
              + "','Turtle')");
      assertEquals(23L, importResults1.single().get("triplesLoaded").asLong());
      Record result = session.run(
                      "MATCH (n:ns0__Issue) RETURN n.ns0__reportedOn AS report, n.ns0__reproducedOn AS reprod")
              .next();
      assertEquals(LocalDateTime.parse("2012-12-31T23:57:00"),
              result.get("report").asLocalDateTime());
      assertEquals(LocalDateTime.parse("2012-11-30T23:57:00"),
              result.get("reprod").asLocalDateTime());
    }
  }

  @Test
  public void testImportDatesAndTimesMultivalued() throws Exception {
    try (Session session = driver.session();) {

      initialiseGraphDB(neo4j.defaultDatabaseService(), "{ handleMultival: 'ARRAY' }");

      Result importResults1 = session.run("CALL n10s.rdf.import.fetch('" +
              RDFDatesTypesTest.class.getClassLoader()
                      .getResource("datetime/datetime-simple-multivalued.ttl").toURI()
              + "','Turtle')");
      assertEquals(5L, importResults1.single().get("triplesLoaded").asLong());

      Set<LocalDate> expectedDates = new HashSet<>();
      expectedDates.add(LocalDate.parse("1999-08-16"));
      expectedDates.add(LocalDate.parse("1999-08-17"));
      expectedDates.add(LocalDate.parse("1999-08-18"));

      Set<LocalDateTime> expectedDatetimes = new HashSet<>();
      expectedDatetimes.add(LocalDateTime.parse("2012-12-31T23:57:00"));
      expectedDatetimes.add(LocalDateTime.parse("2012-12-30T23:57:00"));

      Record result = session.run(
                      "MATCH (n:Resource) RETURN n.ns0__someDateValue as dates, n.ns0__someDateTimeValues as dateTimes")
              .next();
      Set<LocalDate> actualDates = new HashSet<LocalDate>();
      result.get("dates").asList().forEach(x -> actualDates.add((LocalDate) x));

      Set<LocalDateTime> actualDateTimes = new HashSet<LocalDateTime>();
      result.get("dateTimes").asList().forEach(x -> actualDateTimes.add((LocalDateTime) x));

      assertTrue(actualDates.containsAll(expectedDates));
      assertTrue(expectedDates.containsAll(actualDates));

      assertTrue(actualDateTimes.containsAll(expectedDatetimes));
      assertTrue(expectedDatetimes.containsAll(actualDateTimes));

    }
  }
}
