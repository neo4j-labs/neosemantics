package n10s.nsprefixes;

import static n10s.CommonProcedures.UNIQUENESS_CONSTRAINT_STATEMENT;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import n10s.graphconfig.GraphConfigProcedures;
import n10s.rdf.load.AddProcedures;
import n10s.rdf.load.RDFLoadProcedures;
import org.junit.*;
import org.neo4j.driver.Config;
import org.neo4j.driver.Driver;
import org.neo4j.driver.GraphDatabase;
import org.neo4j.driver.Record;
import org.neo4j.driver.Result;
import org.neo4j.driver.Session;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.harness.junit.rule.Neo4jRule;

/**
 * Created by jbarrasa on 21/03/2016.
 */
public class NsPrefixDefProceduresTest {
  public static Driver driver;

  @ClassRule
  public static Neo4jRule neo4j = new Neo4jRule()
          .withProcedure(NsPrefixDefProcedures.class)
          .withProcedure(RDFLoadProcedures.class)
          .withProcedure(GraphConfigProcedures.class);

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
  public void testAddNamespacePrefixInitial() throws Exception {
      Session session = driver.session();

      Result res = session.run("CALL n10s.nsprefixes.add('abc','http://myvoc#')");
      assertTrue(res.hasNext());
      Record next = res.next();
      assertEquals("abc", next.get("prefix").asString());
      assertEquals("http://myvoc#", next.get("namespace").asString());
      assertFalse(res.hasNext());

      session.run("CALL n10s.nsprefixes.remove('abc')");
      session.run("CALL n10s.nsprefixes.list()");
      assertFalse(res.hasNext());

      res = session.run("CALL n10s.nsprefixes.add('abc','http://myvoc2#')");
      next = res.next();
      assertEquals("abc", next.get("prefix").asString());
      assertEquals("http://myvoc2#", next.get("namespace").asString());
  }

  @Test
  public void testAddNamespacePrefixInUse() throws Exception {
      Session session = driver.session();

      Result res = session.run("CALL n10s.nsprefixes.add('abc','http://myvoc2#')");
      assertTrue(res.hasNext());
      try {
        res = session.run("CALL n10s.nsprefixes.add('xyz','http://myvoc2#')");
        res.hasNext();
        assertTrue(false);
      } catch (Exception e) {
        assertTrue(e.getMessage().contains("n10s.utils.NamespacePrefixConflictException"));
      }
      try {
        res = session.run("CALL n10s.nsprefixes.add('abc','http://myvoc3#')");
        res.hasNext();
        assertTrue(false);
      } catch (Exception e) {
        assertTrue(e.getMessage().contains("n10s.utils.NamespacePrefixConflictException"));
      }
      res = session.run("CALL n10s.nsprefixes.add('abc','http://myvoc2#')");
      assertTrue(res.hasNext());
  }

  @Test
  public void testAddNamespaceWithPopulatedGraph() throws Exception {
      Session session = driver.session();

      initialiseGraphDB(neo4j.defaultDatabaseService(), null);
      Result res1 = session.run("CALL n10s.rdf.import.fetch('" +
          NsPrefixDefProceduresTest.class.getClassLoader().getResource("mini-ld.json").toURI()
          + "','JSON-LD')");
      assertTrue(res1.hasNext());
      Map<String, Object> preAddition = res1.next().get("namespaces").asMap();
      String ns_prefix = "abc";
      Result res2 = session
          .run("CALL n10s.nsprefixes.add('" + ns_prefix + "','http://myvoc#')");
      assertTrue(res2.hasNext());
      Result res3 = session.run("MATCH (n:_NsPrefDef) RETURN n");
      assertTrue(res3.hasNext());
      Map<String, Object> postAddition = res3.next().get("n").asNode().asMap();
      assertFalse(res3.hasNext());
      Set<String> keys = new HashSet<>(postAddition.keySet());
      keys.removeAll(preAddition.keySet());
      assertEquals(1, keys.size());
      assertEquals(ns_prefix, keys.iterator().next());
  }

  @Test
  public void testDeleteNamespaceWithPopulatedGraph() throws Exception {
    Session session = driver.session();

    initialiseGraphDB(neo4j.defaultDatabaseService(), null);
      Result res1 = session.run("CALL n10s.rdf.import.fetch('" +
          NsPrefixDefProceduresTest.class.getClassLoader().getResource("mini-ld.json").toURI()
          + "','JSON-LD')");
      assertTrue(res1.hasNext());
      try {
        Result res = session.run("CALL n10s.nsprefixes.list() yield prefix WITH prefix LIMIT 1 "
            + " CALL n10s.nsprefixes.remove(prefix) yield prefix as prefix2 "
            + " RETURN prefix2 ");
        res.hasNext();
        assertTrue(false);
      } catch (Exception e) {
        assertTrue(e.getMessage().contains("n10s.nsprefixes.NsPrefixDefProcedures$"
            + "NsPrefixOperationNotAllowed: A namespace prefix definition cannot be removed "
            + "when the graph is non-empty."));
      }
      try {
        Result res = session.run("CALL n10s.nsprefixes.removeAll()");
        res.hasNext();
        assertTrue(false);
      } catch (Exception e) {
        assertTrue(e.getMessage().contains("n10s.nsprefixes.NsPrefixDefProcedures$"
            + "NsPrefixOperationNotAllowed: Namespace prefix definitions cannot be removed "
            + "when the graph is non-empty."));
      }
  }

  @Test
  public void testAddNamespacePrefixFromText() throws Exception {
    Session session = driver.session();

    String turtle = "@prefix tr-common: <http://permid.org/ontology/common/> . \n"
          + "@prefix fibo-be-le-cb: <http://www.omg.org/spec/EDMC-FIBO/BE/LegalEntities/CorporateBodies/> . \n"
          + "@prefix xsd:   <http://www.w3.org/2001/XMLSchema#> . \n"
          + "@prefix vcard: <http://www.w3.org/2006/vcard/ns#> . \n"
          + "@prefix tr-org: <http://permid.org/ontology/organization/> . \n";

      Map<String, String> validPairs = new HashMap<>();
      validPairs.put("tr_common", "http://permid.org/ontology/common/");
      validPairs.put("fibo_be_le_cb",
          "http://www.omg.org/spec/EDMC-FIBO/BE/LegalEntities/CorporateBodies/");
      validPairs.put("xsd", "http://www.w3.org/2001/XMLSchema#");
      validPairs.put("vcard", "http://www.w3.org/2006/vcard/ns#");
      validPairs.put("tr_org", "http://permid.org/ontology/organization/");

      checkValidPrefixesCreated(session, validPairs, turtle);

      session.run("match  (n) detach delete n");

      String turtleNoSpacesPostColon = "@prefix swo:<http://edamontology.org/> .\n" +
              "@prefix sio:<http://semanticscience.org/resource/> .\n" +
              "@prefix pathway:<http://www.southgreen.fr/agrold/biocyc.pathway/> .\n" +
              "@prefix tigr:<http://www.southgreen.fr/agrold/tigr.locus/> .\n" +
              "@prefix tigr_gene:<http://identifiers.org/ricegap/> .\n" +
              "@prefix ensembl:<http://identifiers.org/ensembl.plant/> .";

      Map<String, String> validPairsNoSpacesPostColon = new HashMap<>();
      validPairsNoSpacesPostColon.put("swo", "http://edamontology.org/");
      validPairsNoSpacesPostColon.put("sio", "http://semanticscience.org/resource/");
      validPairsNoSpacesPostColon.put("pathway", "http://www.southgreen.fr/agrold/biocyc.pathway/");
      validPairsNoSpacesPostColon.put("tigr", "http://www.southgreen.fr/agrold/tigr.locus/");
      validPairsNoSpacesPostColon.put("tigr_gene", "http://identifiers.org/ricegap/");
      validPairsNoSpacesPostColon.put("ensembl", "http://identifiers.org/ensembl.plant/");

      checkValidPrefixesCreated(session, validPairsNoSpacesPostColon, turtleNoSpacesPostColon);

      session.run("match  (n) detach delete n");

      String rdfxml =
          "<rdf:RDF xml:base=\"https://spec.edmcouncil.org/fibo/ontology/BE/Corporations/Corporations/\"\n"
              + "         xmlns:afn=\"http://jena.apache.org/ARQ/function#\"\n"
              + "         xmlns:dc=\"http://purl.org/dc/elements/1.1/\"\n"
              + "         xmlns:fibo-be-corp-corp=\"https://spec.edmcouncil.org/fibo/ontology/BE/Corporations/Corporations/\"\n"
              + "         xmlns:fibo-be-le-cb=\"https://spec.edmcouncil.org/fibo/ontology/BE/LegalEntities/CorporateBodies/\"\n"
              + "         xmlns:fibo-be-le-fbo=\"https://spec.edmcouncil.org/fibo/ontology/BE/LegalEntities/FormalBusinessOrganizations/\"\n"
              + "         xmlns:grddl=\"http://www.w3.org/2003/g/data-view#\"\n"
              + "         xmlns:sm=\"http://www.omg.org/techprocess/ab/SpecificationMetadata/\"\n"
              + "         xmlns:xsd=\"http://www.w3.org/2001/XMLSchema#\">\n";

      validPairs.clear();
      validPairs.put("afn", "http://jena.apache.org/ARQ/function#");
      validPairs.put("dc", "http://purl.org/dc/elements/1.1/");
      validPairs.put("fibo_be_corp_corp",
          "https://spec.edmcouncil.org/fibo/ontology/BE/Corporations/Corporations/");
      validPairs.put("fibo_be_le_cb",
          "https://spec.edmcouncil.org/fibo/ontology/BE/LegalEntities/CorporateBodies/");
      validPairs.put("fibo_be_le_fbo",
          "https://spec.edmcouncil.org/fibo/ontology/BE/LegalEntities/FormalBusinessOrganizations/");
      validPairs.put("grddl", "http://www.w3.org/2003/g/data-view#");
      validPairs.put("sm", "http://www.omg.org/techprocess/ab/SpecificationMetadata/");
      validPairs.put("xsd", "http://www.w3.org/2001/XMLSchema#");

      checkValidPrefixesCreated(session, validPairs, rdfxml);

      session.run("match  (n) detach delete n");

      String sparql = "PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>        \n"
          + "PREFIX type: <http://dbpedia.org/class/yago/>\n"
          + "PREFIX prop: <http://dbpedia.org/property/>\n";

      validPairs.clear();
      validPairs.put("rdfs", "http://www.w3.org/2000/01/rdf-schema#");
      validPairs.put("type", "http://dbpedia.org/class/yago/");
      validPairs.put("prop", "http://dbpedia.org/property/");

      checkValidPrefixesCreated(session, validPairs, sparql);
  }

  private void checkValidPrefixesCreated(Session session, Map<String, String> validPairs,
      String textFragment) {
    Result res;
    int resultCount;
    res = session.run("CALL n10s.nsprefixes.addFromText('" + textFragment + "')");
    assertTrue(res.hasNext());
    resultCount = 0;
    while (res.hasNext()) {
      Record next = res.next();
      assertTrue(isExpectedPair(validPairs, next.get("prefix").asString(),
          next.get("namespace").asString()));
      resultCount++;
    }
    assertEquals(validPairs.size(), resultCount);
  }

  private boolean isExpectedPair(Map<String, String> validPairs, String prefix, String namespace) {
    return validPairs.get(prefix).equals(namespace);
  }

  @Test
  public void testRemoveAllNamespaces() throws Exception {
    Session session = driver.session();

    session.run("CALL n10s.nsprefixes.add('a1','http://myvoc1#')");
      session.run("CALL n10s.nsprefixes.add('a2','http://myvoc2#')");
      session.run("CALL n10s.nsprefixes.add('a3','http://myvoc3#')");
      Result res = session.run("CALL n10s.nsprefixes.add('a4','http://myvoc4#') yield prefix "
          + "with prefix return count(prefix) as ct");
      assertTrue(res.hasNext());
      assertEquals(4, res.next().get("ct").asInt());
      assertFalse(res.hasNext());

      session.run("CALL n10s.nsprefixes.removeAll()");
      res = session.run("CALL n10s.nsprefixes.list()");
      assertFalse(res.hasNext());

      res = session.run("MATCH (nspd:_NsPrefDef) return nspd");
      assertFalse(res.hasNext());
  }

  private void initialiseGraphDB(GraphDatabaseService db, String graphConfigParams) {
    db.executeTransactionally(UNIQUENESS_CONSTRAINT_STATEMENT);
    db.executeTransactionally("CALL n10s.graphconfig.init(" +
        (graphConfigParams != null ? graphConfigParams : "{}") + ")");
  }

}
