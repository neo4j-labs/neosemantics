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

public class RDFConfigIndexTest {
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

  private String turtleFragment = "@prefix show: <http://example.org/vocab/show/> .\n" +
          "\n" +
          "show:218 show:localName \"That Seventies Show\"@en .                 # literal with a language tag\n"
          +
          "show:218 show:localName \"Cette Série des Années Soixante-dix\"@fr . \n" +
          "show:218 show:localName \"Cette Série des Années Septante\"@fr-be .  # literal with a region subtag";

  private static URI file(String path) {
    try {
      return RDFConfigIndexTest.class.getClassLoader().getResource(path).toURI();
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
  public void testAbortIfNoIndices() throws Exception {
    try (Session session = driver.session()) {

      Result importResults
              = session.run("CALL n10s.rdf.import.fetch('" +
              RDFConfigIndexTest.class.getClassLoader().getResource("mini-ld.json").toURI() +
              "','JSON-LD',{ handleVocabUris: 'SHORTEN', handleRDFTypes: 'LABELS', commitSize: 500})");

      Map<String, Object> singleResult = importResults
              .single().asMap();

      assertEquals(0L, singleResult.get("triplesLoaded"));
      assertEquals("KO", singleResult.get("terminationStatus"));
      assertEquals("The following constraint is required for importing RDF. Please "
                      + "run '" + UNIQUENESS_CONSTRAINT_STATEMENT + "' and try again.",
              singleResult.get("extraInfo"));
    }
  }

  @Test
  public void testFullTextIndexesPresent() throws Exception {
    try (Session session = driver.session()) {

      session.run("CREATE FULLTEXT INDEX titlesAndDescriptions FOR (n:Movie|Book) ON EACH [n.title, n.description]");

      Result importResults
              = session.run("CALL n10s.rdf.import.fetch('file:///fileDoesnotExist.txt','JSON-LD',{})");

      Map<String, Object> singleResult = importResults
              .single().asMap();

      assertEquals(0L, singleResult.get("triplesLoaded"));
      assertEquals("KO", singleResult.get("terminationStatus"));
      assertEquals("The following constraint is required for importing RDF. "
                      + "Please run '" + UNIQUENESS_CONSTRAINT_STATEMENT + "' and try again.",
              singleResult.get("extraInfo"));
    }
  }

  @Test
  public void testCompositeIndexesPresent() throws Exception {
    try (Session session = driver.session()) {
      session.run("CREATE INDEX IF NOT EXISTS FOR (n:Person) ON (n.age, n.country)");

      Result importResults
              = session.run("CALL n10s.rdf.import.fetch('file:///fileDoesnotExist.txt','JSON-LD',{})");

      Map<String, Object> singleResult = importResults
              .single().asMap();

      assertEquals(0L, singleResult.get("triplesLoaded"));
      assertEquals("KO", singleResult.get("terminationStatus"));
      assertEquals("The following constraint is required for importing RDF. Please "
                      + "run '" + UNIQUENESS_CONSTRAINT_STATEMENT + "' "
                      + "and try again.",
              singleResult.get("extraInfo"));
    }
  }

  @Test
  public void testAbortIfNoIndicesImportSnippet() throws Exception {
    try (Session session = driver.session();) {

      Result importResults1 = session.run("CALL n10s.rdf.import.inline('" +
              turtleFragment +
              "','Turtle')");

      Map<String, Object> singleResult = importResults1.single().asMap();

      assertEquals(0L, singleResult.get("triplesLoaded"));
      assertEquals("KO", singleResult.get("terminationStatus"));
      assertEquals("The following constraint is required for importing RDF. Please run "
                      + "'" + UNIQUENESS_CONSTRAINT_STATEMENT + "' and try again.",
              singleResult.get("extraInfo"));
    }
  }
}
