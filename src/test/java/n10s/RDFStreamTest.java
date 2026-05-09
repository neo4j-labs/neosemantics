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

public class RDFStreamTest {
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
      return RDFStreamTest.class.getClassLoader().getResource(path).toURI();
    } catch (URISyntaxException e) {
      String msg = String.format("Failed to load the resource with path '%s'", path);
      throw new RuntimeException(msg, e);
    }
  }

  @Test
  public void testStreamFromFile() throws Exception {
    try (Session session = driver.session()) {

      Result importResults
              = session.run("CALL n10s.rdf.stream.fetch('" +
              RDFStreamTest.class.getClassLoader().getResource("oneTriple.rdf")
                      .toURI() + "','RDF/XML',{})");
      Map<String, Object> next = importResults
              .next().asMap();
      assertEquals("http://neo4j.com/invividual/JB", next.get("subject"));
      assertEquals("http://neo4j.com/voc/name", next.get("predicate"));
      assertEquals("JB", next.get("object"));
      assertEquals(true, next.get("isLiteral"));
      assertEquals("http://www.w3.org/2001/XMLSchema#string", next.get("literalType"));
      assertNull(next.get("literalLang"));
    }
  }

  @Test
  public void testStreamFromFileWithLimit() throws Exception {
    try (Session session = driver.session()) {

      Result importResults
              = session.run("CALL n10s.rdf.stream.fetch('" +
              RDFStreamTest.class.getClassLoader().getResource("event.json")
                      .toURI() + "','JSON-LD',{ limit: 2})");
      assertTrue(importResults.hasNext());
      importResults.next();
      importResults.next();
      assertFalse(importResults.hasNext());
    }
  }

  @Test
  public void testStreamFromFileWithExclusionList() throws Exception {
    try (Session session = driver.session()) {

      String filteredPred = "http://schema.org/image";

      Result importResults
              = session.run("CALL n10s.rdf.stream.fetch('" +
              RDFStreamTest.class.getClassLoader().getResource("event.json")
                      .toURI() + "','JSON-LD')");

      int tripleCount = 0;
      int filteredPredicatesCount = 0;
      while (importResults.hasNext()) {
        Record next = importResults.next();
        if (next.get("predicate").asString().equals(filteredPred)) {
          filteredPredicatesCount++;
        }
        tripleCount++;
      }
      assertEquals(28, tripleCount);
      assertEquals(3, filteredPredicatesCount);


      importResults
              = session.run("CALL n10s.rdf.stream.fetch('" +
              RDFStreamTest.class.getClassLoader().getResource("event.json")
                      .toURI() + "','JSON-LD',{ predicateExclusionList: ['" + filteredPred + "'] })");

      tripleCount = 0;
      filteredPredicatesCount = 0;
      while (importResults.hasNext()) {
        Record next = importResults.next();
        if (next.get("predicate").asString().equals(filteredPred)) {
          filteredPredicatesCount++;
        }
        tripleCount++;
      }
      assertEquals(25, tripleCount);
      assertEquals(0, filteredPredicatesCount);
    }
  }

  @Test
  public void testStreamFromString() throws Exception {
    try (Session session = driver.session()) {

      String rdf = "<rdf:RDF xmlns:owl=\"http://www.w3.org/2002/07/owl#\"\n"
              + "         xmlns:rdfs=\"http://www.w3.org/2000/01/rdf-schema#\"\n"
              + "         xmlns:rdf=\"http://www.w3.org/1999/02/22-rdf-syntax-ns#\"\n"
              + "         xmlns:voc=\"http://neo4j.com/voc/\">\n"
              + "         <rdf:Description rdf:about=\"http://neo4j.com/invividual/JB\">\n"
              + "            <voc:name>JB</voc:name>\n"
              + "         </rdf:Description>\n"
              + "</rdf:RDF>";

      Result importResults
              = session.run("CALL n10s.rdf.stream.inline('" + rdf + "','RDF/XML',{})");
      Map<String, Object> next = importResults
              .next().asMap();
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
    try (Session session = driver.session()) {

      Result importResults
              = session.run("CALL n10s.rdf.stream.fetch('" +
              RDFStreamTest.class.getClassLoader().getResource("badUri.ttl")
                      .toURI() + "','Turtle',{verifyUriSyntax: false})");
      Map<String, Object> next = importResults
              .next().asMap();
      assertEquals("http://example.org/vocab/show/ent", next.get("subject"));
      assertEquals("http://example.org/vocab/show/P854", next.get("predicate"));
      assertEquals(
              "https://suasprod.noc-science.at/XLCubedWeb/WebForm/ShowReport.aspx?rep=004+studierende%2f001+universit%u00e4",
              next.get("object"));
      assertEquals(false, next.get("isLiteral"));
    }
  }

  @Test
  public void testStreamFromBadUriString() throws Exception {
    try (Session session = driver.session()) {

      String rdf = "@prefix pr: <http://example.org/vocab/show/> .\n"
              + "pr:ent\n"
              + "      pr:P854 <https://suasprod.noc-science.at/XLCubedWeb/WebForm/ShowReport.aspx?rep=004+studierende%2f001+universit%u00e4> ;\n"
              + "      pr:name \"test name\" .";
      Result importResults
              = session
              .run("CALL n10s.rdf.stream.inline('" + rdf + "','Turtle',{verifyUriSyntax: false})");
      Map<String, Object> next = importResults
              .next().asMap();
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
    try (Session session = driver.session()) {

      try {
        Result importResults
                = session.run("CALL n10s.rdf.stream.fetch('" +
                RDFStreamTest.class.getClassLoader().getResource("badUri.ttl")
                        .toURI() + "','Turtle')");
        importResults.hasNext();
        assertFalse(true);
      } catch (Exception e) {
        assertTrue(e.getMessage().contains("Illegal percent encoding"));
      }
    }
  }
}
