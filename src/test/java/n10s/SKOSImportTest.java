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

public class SKOSImportTest {
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

  private static final String SKOS_FRAGMENT_TURTLE =
          "@prefix skos: <http://www.w3.org/2004/02/skos/core#> .\n"
                  + "@prefix thesaurus: <http://vocabularies.unesco.org/thesaurus/> .\n"
                  + "@prefix isothes: <http://purl.org/iso25964/skos-thes#> .\n"
                  + "@prefix dc: <http://purl.org/dc/terms/> .\n"
                  + "@prefix xsd: <http://www.w3.org/2001/XMLSchema#> .\n"
                  + "\n"
                  + "<http://vocabularies.unesco.org/thesaurus>\n"
                  + "  a skos:ConceptScheme ;\n"
                  + "  skos:prefLabel \"UNESCO Thesaurus\"@en, \"Thésaurus de lUNESCO\"@fr, \"Тезаурус ЮНЕСКО\"@ru, \"Tesauro de la UNESCO\"@es .\n"
                  + "\n"
                  + "thesaurus:concept2094\n"
                  + "  a skos:Concept ;\n"
                  + "  skos:prefLabel \"Lengua altaica\"@es, \"Langue altaïque\"@fr, \"Алтайские языки\"@ru, \"Altaic languages\"@en ;\n"
                  + "  skos:narrower thesaurus:concept2096 .\n"
                  + "\n"
                  + "thesaurus:mt3.35\n"
                  + "  a isothes:ConceptGroup, <http://vocabularies.unesco.org/ontology#MicroThesaurus>, skos:Collection ;\n"
                  + "  skos:prefLabel \"Languages\"@en, \"Lenguas\"@es, \"Langues\"@fr, \"Языки\"@ru ;\n"
                  + "  skos:member thesaurus:concept2096 .\n"
                  + "\n"
                  + "thesaurus:concept2096\n"
                  + "  dc:modified \"2006-05-23T00:00:00\"^^xsd:dateTime ;\n"
                  + "  a skos:Concept ;\n"
                  + "  skos:inScheme <http://vocabularies.unesco.org/thesaurus> ;\n"
                  + "  skos:prefLabel \"Azerbaijani\"@en, \"Azéri\"@fr, \"Азербайджанский язык\"@ru, \"Azerbaiyano\"@es ;\n"
                  + "  skos:hiddenLabel \"Azeri\"@fr, \"Азербаиджанскии язык\"@ru ;\n"
                  + "  skos:broader thesaurus:concept2094 .\n"
                  + "\n"
                  + "thesaurus:domain3\n"
                  + "  a isothes:ConceptGroup, <http://vocabularies.unesco.org/ontology#Domain>, skos:Collection ;\n"
                  + "  skos:prefLabel \"Culture\"@en, \"Culture\"@fr, \"Культура\"@ru, \"Cultura\"@es ;\n"
                  + "  skos:member thesaurus:mt3.35 .";

  private static URI file(String path) {
    try {
      return SKOSImportTest.class.getClassLoader().getResource(path).toURI();
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
  public void testImportSKOSInline() throws Exception {
    try (Session session = driver.session()) {

      initialiseGraphDB(neo4j.defaultDatabaseService(),
              "{ handleVocabUris: 'IGNORE', handleMultival: 'ARRAY' }");

      Result importResults
              = session.run("CALL n10s.skos.import.inline('" +
              SKOS_FRAGMENT_TURTLE + "','Turtle')"); //,{   commitSize: 1000 }

      assertEquals(26L, importResults
              .single().get("triplesLoaded").asLong());
      Result queryResults = session.run(
              "MATCH (n:Resource  { uri: 'http://vocabularies.unesco.org/thesaurus/concept2096'}) RETURN [ x in labels(n) where x <> 'Resource' | x][0] AS label, n.prefLabel  as name limit 1");
      assertTrue(queryResults.hasNext());
      Record result = queryResults.next();
      assertEquals("Class", result.get("label").asString());
      assertEquals(Arrays.asList("Azerbaijani", "Azéri", "Азербайджанский язык", "Azerbaiyano"),
              result.get("name").asList());
      assertFalse(queryResults.hasNext());
    }
  }

  @Test
  public void testImportSKOSFetchWithParams() throws Exception {
    try (Session session = driver.session()) {

      initialiseGraphDB(neo4j.defaultDatabaseService(),
              "{}");

      Result importResults
              = session.run("CALL n10s.skos.import.fetch('" +
              SKOSImportTest.class.getClassLoader().getResource("unesco-thesaurus.ttl").toURI()
              + "','Turtle',{   commitSize: 5000, languageFilter: 'fr' })"); //

      assertEquals(57185L, importResults
              .single().get("triplesLoaded").asLong());
      Result queryResults = session.run(
              "MATCH (n:n4sch__Class  { uri: 'http://vocabularies.unesco.org/thesaurus/concept10928'}) "
                      + "RETURN labels(n) AS labels, properties(n) as props limit 1");
      assertTrue(queryResults.hasNext());
      Record result = queryResults.next();
      List<Object> labels = result.get("labels").asList();
      assertTrue(labels.contains("Resource"));
      assertTrue(labels.contains("n4sch__Class"));
      assertEquals(2, labels.size());
      Map<String, Object> props = result.get("props").asMap();
      assertEquals("concept10928", props.get("n4sch__name"));
      assertEquals("Industrie alimentaire", props.get("skos__prefLabel"));
      assertFalse(queryResults.hasNext());
    }
  }

  @Test
  public void testImportSKOSFetchWithParamsCustomSchemaNs() throws Exception {
    try (Session session = driver.session()) {

      initialiseGraphDB(neo4j.defaultDatabaseService(),
              "{ baseSchemaNamespace: 'http://baseschema.mine/voc1#' , " +
                      "baseSchemaPrefix: 'basev' }");

      Result importResults
              = session.run("CALL n10s.skos.import.fetch('" +
              SKOSImportTest.class.getClassLoader().getResource("unesco-thesaurus.ttl").toURI()
              + "','Turtle',{   commitSize: 5000, languageFilter: 'fr' })"); //

      assertEquals(57185L, importResults
              .single().get("triplesLoaded").asLong());
      Result queryResults = session.run(
              "MATCH (n:basev__Class  { uri: 'http://vocabularies.unesco.org/thesaurus/concept10928'}) "
                      + "RETURN labels(n) AS labels, properties(n) as props limit 1");
      assertTrue(queryResults.hasNext());
      Record result = queryResults.next();
      List<Object> labels = result.get("labels").asList();
      assertTrue(labels.contains("Resource"));
      assertTrue(labels.contains("basev__Class"));
      assertEquals(2, labels.size());
      Map<String, Object> props = result.get("props").asMap();
      assertEquals("concept10928", props.get("basev__name"));
      assertEquals("Industrie alimentaire", props.get("skos__prefLabel"));
      assertFalse(queryResults.hasNext());
    }
  }

  @Test
  public void testImportSKOSFetchMultivalArray() throws Exception {
    try (Session session = driver.session()) {

      initialiseGraphDB(neo4j.defaultDatabaseService(),
              "{ handleMultival: 'ARRAY', keepLangTag: true }");

      Result importResults
              = session.run("CALL n10s.skos.import.fetch('" +
              SKOSImportTest.class.getClassLoader().getResource("unesco-thesaurus.ttl").toURI()
              + "','Turtle')"); //,{   commitSize: 1000 }

      assertEquals(57185, importResults
              .single().get("triplesLoaded").asLong());
      Result queryResults = session.run(
              "MATCH (n:Resource  { uri: 'http://vocabularies.unesco.org/thesaurus/concept10928'}) "
                      + "RETURN [ x in labels(n) where x <> 'Resource' | x][0] AS label, n.n4sch__name  as name, " +
                      "n.skos__prefLabel  as labels limit 1");
      assertTrue(queryResults.hasNext());
      Record result = queryResults.next();
      assertEquals("n4sch__Class", result.get("label").asString());
      assertEquals("concept10928", result.get("name").asString());
      List<Object> labels = result.get("labels").asList();
      String values[] = new String[]{
              "Food industry@en", "Industrie alimentaire@fr", "Industria alimentaria@es", "Пищевая промышленность@ru"};
      for (String x : values) {
        assertTrue(labels.contains(x));
      }
      assertFalse(queryResults.hasNext());
    }
  }
}
