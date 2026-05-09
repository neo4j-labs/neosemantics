package n10s;

import static n10s.CommonProcedures.UNIQUENESS_CONSTRAINT_STATEMENT;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.net.URI;
import java.net.URISyntaxException;
import java.util.List;
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

public class RDFJSONTreeTest {
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
      return RDFJSONTreeTest.class.getClassLoader().getResource(path).toURI();
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
  public void testLoadJSONAsTreeEmptyJSON() throws Exception {
    try (Session session = driver.session()) {

      initialiseGraphDB(neo4j.defaultDatabaseService(), null);

      Result importResults
              = session
              .run("CREATE (n:Node)  WITH n "
                      + " CALL n10s.experimental.importJSONAsTree(n, '')"
                      + " YIELD node RETURN node ");
      assertFalse(importResults.hasNext());
    }
  }

  @Test
  public void testLoadJSONAsTreeListAtRoot() throws Exception {
    try (Session session = driver.session()) {

      initialiseGraphDB(neo4j.defaultDatabaseService(), "{ handleVocabUris: 'IGNORE' }");

      //String jsonFragment = "[]";
      String jsonFragment = "[{\"menu\": {\n"
              + "  \"id\": \"file\",\n"
              + "  \"value\": \"File\",\n"
              + "  \"popup\": {\n"
              + "    \"menuitem\": [\n"
              + "      {\"value\": \"New\", \"onclick\": \"CreateNewDoc()\"},\n"
              + "      {\"value\": \"Open\", \"onclick\": \"OpenDoc()\"},\n"
              + "      {\"value\": \"Close\", \"onclick\": \"CloseDoc()\"}\n"
              + "    ]\n"
              + "  }\n"
              + "}}, { \"message\": \"hello!\"} ]";

      Result importResults
              = session.run("CREATE (n:Node)  WITH n "
              + " CALL n10s.experimental.importJSONAsTree(n, '" + jsonFragment + "','MY_JSON')"
              + " YIELD node RETURN node ");
      assertTrue(importResults.hasNext());
      Result queryresult = session
              .run("match (n:Node)-[:MY_JSON]->(r) return count(r) as ct ");
      assertEquals(2, queryresult.next().get("ct").asInt());
      queryresult = session
              .run("match (n:Node)-[:MY_JSON]->()-[:menu]->(thing)-[:popup]->() return thing ");
      assertEquals("File", queryresult.next().get("thing").asNode().asMap().get("value"));
      queryresult = session
              .run("match (n:Node)-[:MY_JSON]->(thing) where not (thing)-->() "
                      + "return thing.message as msg ");
      assertEquals("hello!", queryresult.next().get("msg").asString());
    }
  }

  @Test
  public void testLoadJSONAsTree() throws Exception {
    try (Session session = driver.session()) {

      initialiseGraphDB(neo4j.defaultDatabaseService(), "{ handleVocabUris: 'IGNORE' }");

      String jsonFragment = "{\"menu\": {\n"
              + "  \"id\": \"file\",\n"
              + "  \"value\": \"File\",\n"
              + "  \"popup\": {\n"
              + "    \"menuitem\": [\n"
              + "      {\"value\": \"New\", \"onclick\": \"CreateNewDoc()\"},\n"
              + "      {\"value\": \"Open\", \"onclick\": \"OpenDoc()\"},\n"
              + "      {\"value\": \"Close\", \"onclick\": \"CloseDoc()\"}\n"
              + "    ]\n"
              + "  }\n"
              + "}}";

      Result importResults
              = session
              .run("CREATE (n:Node { id: 'record node'})  WITH n "
                      + " CALL n10s.experimental.importJSONAsTree(n, '" + jsonFragment + "') YIELD node "
                      + " RETURN node ");
      assertTrue(importResults.hasNext());
      assertEquals("record node", importResults.next().get("node").asNode()
              .get("id").asString());
      Result queryresult = session
              .run("match (n:Node:Resource)-[:_jsonTree]->()-[:menu]->()-[:popup]->()"
                      + "-[:menuitem]->(mi { value: 'Open', onclick: 'OpenDoc()'}) return mi ");
      assertEquals("Resource",
              queryresult.next().get("mi").asNode().labels().iterator().next());
    }
  }

  @Test
  public void testLoadJSONAsTreeWithUrisAndContext() throws Exception {
    try (Session session = driver.session()) {

      initialiseGraphDB(neo4j.defaultDatabaseService(), "{ handleVocabUris: 'IGNORE' }");

      String jsonFragment = "{\n"
              + "  \"@context\": {\n"
              + "    \"name\": \"http://xmlns.com/foaf/0.1/name\",\n"
              + "    \"knows\": \"http://xmlns.com/foaf/0.1/knows\",\n"
              + "\t\"modified\": \"http://xmlns.com/foaf/0.1/modified\"\n"
              + "  },\n"
              + "  \"@id\": \"http://me.markus-lanthaler.com/\",\n"
              + "  \"name\": \"Markus Lanthaler\",\n"
              + "  \"knows\": [\n"
              + "    {\n"
              + "      \"@id\": \"http://manu.sporny.org/about#manu\",\n"
              + "      \"name\": \"Manu Sporny\"\n"
              + "    },\n"
              + "    {\n"
              + "      \"name\": \"Dave Longley\",\n"
              + "\t  \"modified\":\n"
              + "\t    {\n"
              + "\t      \"@value\": \"2010-05-29T14:17:39+02:00\",\n"
              + "\t      \"@type\": \"http://www.w3.org/2001/XMLSchema#dateTime\"\n"
              + "\t    }\n"
              + "    }\n"
              + "  ]\n"
              + "}";

      Result importResults
              = session
              .run("CREATE (n:Node { id: 'I\\'m the hook node'})  WITH n "
                      + " CALL n10s.experimental.importJSONAsTree(n, '" + jsonFragment + "') YIELD node "
                      + " RETURN node ");
      assertTrue(importResults.hasNext());
      assertEquals("I'm the hook node",
              importResults.next().get("node").asNode().get("id").asString());
      Result queryresult = session
              .run("match (n:Node:Resource)-[l:_jsonTree]->"
                      + "(:Resource { uri: 'http://me.markus-lanthaler.com/'}) return l ");
      assertTrue(queryresult.hasNext());
      queryresult = session
              .run("match (n:Node:Resource)-[:_jsonTree]->"
                      + "(:Resource { uri: 'http://me.markus-lanthaler.com/'})-[:knows]->"
                      + "(friend) return collect(friend.name) as friends ");
      assertTrue(queryresult.hasNext());
      List<Object> friends = queryresult.next().get("friends").asList();
      assertTrue(friends.contains("Dave Longley"));
      assertTrue(friends.contains("Manu Sporny"));
      assertEquals(2, friends.size());
    }
  }

  @Test
  public void testLoadJSONAsTree2() throws Exception {
    try (Session session = driver.session()) {

      initialiseGraphDB(neo4j.defaultDatabaseService(), "{ handleVocabUris: 'IGNORE' }");

      String jsonFragment = "{\"widget\": {\n"
              + "    \"debug\": \"on\",\n"
              + "    \"window\": {\n"
              + "        \"title\": \"Sample Konfabulator Widget\",\n"
              + "        \"name\": \"main_window\",\n"
              + "        \"width\": 333,\n"
              + "        \"height\": 500\n"
              + "    },\n"
              + "    \"image\": { \n"
              + "        \"src\": \"Images/Sun.png\",\n"
              + "        \"name\": \"sun1\",\n"
              + "        \"hOffset\": 250,\n"
              + "        \"vOffset\": 250,\n"
              + "        \"alignment\": \"center\"\n"
              + "    },\n"
              + "    \"text\": {\n"
              + "        \"data\": \"Click Here\",\n"
              + "        \"size\": 36,\n"
              + "        \"style\": \"bold\",\n"
              + "        \"name\": \"text1\",\n"
              + "        \"hOffset\": 250,\n"
              + "        \"vOffset\": 100,\n"
              + "        \"alignment\": \"center\",\n"
              + "        \"onMouseUp\": \"sun1.opacity = (sun1.opacity / 100) * 90;\"\n"
              + "    }\n"
              + "}}    ";

      Result importResults
              = session
              .run("CREATE (n:Node)  WITH n "
                      + " CALL n10s.experimental.importJSONAsTree(n, '" + jsonFragment + "') YIELD node "
                      + " RETURN node ");
      assertTrue(importResults.hasNext());
      Result queryresult = session
              .run("match (n:Node)-[:_jsonTree]->()-[:widget]->( { debug: 'on'})"
                      + "-[:window]->(w) return w.title as title, w.width as width ");
      Record next = queryresult.next();
      assertEquals("Sample Konfabulator Widget", next.get("title").asString());
      assertEquals(333, next.get("width").asInt());

      queryresult = session
              .run("match (n:Node)-[:_jsonTree]->()-[:widget]->( { debug: 'on'})"
                      + "-->(w) return count(w) as ct ");
      assertEquals(3, queryresult.next().get("ct").asInt());
    }
  }

  @Test
  public void testLoadJSONAsTree3() throws Exception {
    try (Session session = driver.session()) {

      initialiseGraphDB(neo4j.defaultDatabaseService(), "{ handleVocabUris: 'IGNORE' }");

      String jsonFragment = "{\"menu\": {\n"
              + "    \"header\": \"SVG Viewer\",\n"
              + "    \"items\": [\n"
              + "        {\"id\": \"Open\"},\n"
              + "        {\"id\": \"OpenNew\", \"label\": \"Open New\"},\n"
              + "        null,\n"
              + "        {\"id\": \"ZoomIn\", \"label\": \"Zoom In\"},\n"
              + "        {\"id\": \"ZoomOut\", \"label\": \"Zoom Out\"},\n"
              + "        {\"id\": \"OriginalView\", \"label\": \"Original View\"},\n"
              + "        null,\n"
              + "        {\"id\": \"Quality\"},\n"
              + "        {\"id\": \"Pause\"},\n"
              + "        {\"id\": \"Mute\"},\n"
              + "        null,\n"
              + "        {\"id\": \"Find\", \"label\": \"Find...\"},\n"
              + "        {\"id\": \"FindAgain\", \"label\": \"Find Again\"},\n"
              + "        {\"id\": \"Copy\"},\n"
              + "        {\"id\": \"CopyAgain\", \"label\": \"Copy Again\"},\n"
              + "        {\"id\": \"CopySVG\", \"label\": \"Copy SVG\"},\n"
              + "        {\"id\": \"ViewSVG\", \"label\": \"View SVG\"},\n"
              + "        {\"id\": \"ViewSource\", \"label\": \"View Source\"},\n"
              + "        {\"id\": \"SaveAs\", \"label\": \"Save As\"},\n"
              + "        null,\n"
              + "        {\"id\": \"Help\"},\n"
              + "        {\"id\": \"About\", \"label\": \"About Adobe CVG Viewer...\"}\n"
              + "    ]\n"
              + "}}";

      Result importResults
              = session
              .run("CREATE (n:Node)  WITH n "
                      + " CALL n10s.experimental.importJSONAsTree(n, '" + jsonFragment + "') YIELD node "
                      + " RETURN node ");
      assertTrue(importResults.hasNext());
      Result queryresult = session
              .run("match (n:Node)-[:_jsonTree]->()-[:menu]->( { header: 'SVG Viewer'})"
                      + "-[:items]->(item) return count(item) as itemcount, "
                      + " count(distinct item.label) as labelcount ");
      Record next = queryresult.next();
      assertEquals(18, next.get("itemcount").asInt());
      assertEquals(12, next.get("labelcount").asInt());

      queryresult = session
              .run("match (n:Node)-[:_jsonTree]->()-[:menu]->( { header: 'SVG Viewer'})"
                      + "-[:items]->(item { id: 'ViewSource'}) return item.label as label ");
      assertEquals("View Source", queryresult.next().get("label").asString());
    }
  }
}
