package n10s.inference;

import static org.junit.Assert.*;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Set;

import n10s.aux.AuxProcedures;
import n10s.graphconfig.GraphConfigProcedures;
import org.junit.*;
import org.neo4j.driver.Config;
import org.neo4j.driver.Driver;
import org.neo4j.driver.GraphDatabase;
import org.neo4j.driver.Record;
import org.neo4j.driver.Result;
import org.neo4j.driver.Session;
import org.neo4j.harness.junit.rule.Neo4jRule;

public class MicroReasonersTest {

  public static Driver driver;

  @ClassRule
  public static Neo4jRule neo4j = new Neo4jRule()
          .withProcedure(MicroReasoners.class)
          .withFunction(MicroReasoners.class)
          .withProcedure(GraphConfigProcedures.class);

  @BeforeClass
  public static void init() {
    driver = GraphDatabase.driver(neo4j.boltURI(),
            Config.builder().withoutEncryption().build());
  }

  @Before
  public void cleanDatabase() {
    driver.session().run("match (n) detach delete n").consume();
    driver.session().run("drop constraint n10s_unique_uri if exists");
  }

  @Test
  public void testGetNodesNoOnto() throws Exception {

      Session session = driver.session();
      session.run("CREATE (b:B {id:'iamb'}) CREATE (a:A {id: 'iama' }) ");
      Result results = null;
      try {
        results = session.run(
                "CALL n10s.inference.nodesLabelled('B') YIELD node RETURN count(node) as ct, collect(node.id) as nodes");
        results.hasNext();
        assertTrue(false);
      } catch (Exception mie){
        assertTrue(mie.getMessage().contains("Caused by: n10s.inference.MicroReasonerException: " +
                "No GraphConfig or in-procedure params. Method cannot be run."));
      }



      results = session.run(
              "CALL n10s.inference.nodesLabelled('B',{ catLabel: 'something', catNameProp : 'something', subCatRel: 'something'}) " +
                      "YIELD node RETURN count(node) as ct, collect(node.id) as nodes");
      assertEquals(true, results.hasNext());
      Record next = results.next();
      Set<String> expectedNodeIds = new HashSet<String>();
      expectedNodeIds.add("iamb");
      assertEquals(expectedNodeIds, new HashSet<>(next.get("nodes").asList()));
      assertEquals(1L, next.get("ct").asLong());
      assertEquals(false, results.hasNext());
  }

  @Test
  public void testGetNodesDefault() throws Exception {

      Session session = driver.session();
      session.run("call n10s.graphconfig.init({classLabel: 'Label', subClassOfRel: 'SLO'})");

      session.run("CREATE (b:B {id:'iamb'}) CREATE (a:A {id: 'iama' }) ");
      session.run("CREATE (b:Label { name: \"B\"}) CREATE (a:Label { name: \"A\"})-[:SLO]->(b) ");
      Result results = session.run(
          "CALL n10s.inference.nodesLabelled('B') YIELD node RETURN count(node) as ct, collect(node.id) as nodes");
      assertEquals(true, results.hasNext());
      Record next = results.next();
      Set<String> expectedNodeIds = new HashSet<String>();
      expectedNodeIds.add("iama");
      expectedNodeIds.add("iamb");
      assertEquals(expectedNodeIds, new HashSet<>(next.get("nodes").asList()));
      assertEquals(2L, next.get("ct").asLong());
      assertEquals(false, results.hasNext());
  }

  @Test
  public void testGetNodesCustomHierarchy() throws Exception {

      Session session = driver.session();

      session.run("CREATE (b:B {id:'iamb'}) CREATE (a:A {id: 'iama' }) ");
      session.run("CREATE (b:Class { cname: \"B\"}) CREATE (a:Class { cname: \"A\"})-[:SCO]->(b) ");
      Result results = session.run(
          "CALL n10s.inference.nodesLabelled('B', { catLabel: 'Class', catNameProp: 'cname', subCatRel: 'SCO'}) YIELD node RETURN count(node) as ct, collect(node.id) as nodes");
      assertEquals(true, results.hasNext());
      Record next = results.next();
      Set<String> expectedNodeIds = new HashSet<String>();
      expectedNodeIds.add("iama");
      expectedNodeIds.add("iamb");
      assertEquals(expectedNodeIds, new HashSet<>(next.get("nodes").asList()));
      assertEquals(2L, next.get("ct").asLong());
      assertEquals(false, results.hasNext());
  }

  @Test
  public void testGetNodesLinkedTo() throws Exception {
      Session session = driver.session();

      session.run("CREATE (b:Thing {id:'iamb'}) CREATE (a:Thing {id: 'iama' }) ");
      session.run(
          "CREATE (b:Category { name: \"B\"}) CREATE (a:Category { name: \"A\"})-[:SCO]->(b) ");
      session.run(
          "MATCH (b:Thing {id:'iamb'}),(a:Thing {id: 'iama' }),(bcat:Category { name: \"B\"}),(acat:Category { name: \"A\"}) "
              +
              "CREATE (a)-[:IN_CAT]->(acat) CREATE (b)-[:IN_CAT]->(bcat)");

      Result results;

      try {
        results = session.run(
                "MATCH (bcat:Category { name: \"B\"}) CALL n10s.inference.nodesInCategory(bcat) YIELD node "
                        +
                        "RETURN count(node) as ct, collect(node.id) as nodes");
        results.hasNext();
        assertTrue(false);
      } catch (Exception mie){
        assertTrue(mie.getMessage().contains("Caused by: n10s.inference.MicroReasonerException: " +
                "No GraphConfig or in-procedure params. Method cannot be run."));
      }

      session.run("call n10s.graphconfig.init({ classLabel: 'Category', " +
              "subClassOfRel: 'SCO' })");

      results = session.run(
          "MATCH (bcat:Category { name: \"B\"}) CALL n10s.inference.nodesInCategory(bcat, { inCatRel: 'IN_CAT' }) YIELD node "
              +
              "RETURN count(node) as ct, collect(node.id) as nodes");
      assertEquals(true, results.hasNext());
      Record next = results.next();
      Set<String> expectedNodeIds = new HashSet<String>();
      expectedNodeIds.add("iama");
      expectedNodeIds.add("iamb");
      assertEquals(expectedNodeIds, new HashSet<>(next.get("nodes").asList()));
      assertEquals(2L, next.get("ct").asLong());
      assertEquals(false, results.hasNext());

      //Non-existent relationship
      results = session.run(
          "MATCH (bcat:Category { name: \"B\"}) CALL n10s.inference.nodesInCategory(bcat, { inCatRel: 'TYPE' } ) YIELD node RETURN node");
      assertEquals(false, results.hasNext());

      //Using Default
      results = session.run(
              "MATCH (bcat:Category { name: \"B\"}) CALL n10s.inference.nodesInCategory(bcat) YIELD node RETURN node");
      assertEquals(false, results.hasNext());

      //Custom relationship
      session.run("MATCH (a)-[ic:IN_CAT]->(b) CREATE (a)-[:TYPE]->(b) DELETE ic");
      results = session.run(
          "MATCH (bcat:Category { name: \"B\"}) CALL n10s.inference.nodesInCategory(bcat, { inCatRel: 'TYPE'} ) YIELD node RETURN count(node) as ct, collect(node.id) as nodes");
      assertEquals(true, results.hasNext());
      next = results.next();
      expectedNodeIds = new HashSet<String>();
      expectedNodeIds.add("iama");
      expectedNodeIds.add("iamb");
      assertEquals(expectedNodeIds, new HashSet<>(next.get("nodes").asList()));
      assertEquals(2L, next.get("ct").asLong());
      assertEquals(false, results.hasNext());
      session.run("MATCH (a)-[sco:SCO]->(b) CREATE (a)-[:SUBCAT_OF]->(b) DELETE sco");

      results = session.run(
          "MATCH (bcat:Category { name: \"B\"}) CALL n10s.inference.nodesInCategory(bcat, { inCatRel: 'TYPE', subCatRel: 'SUBCAT_OF'}) YIELD node RETURN count(node) as ct, collect(node.id) as nodes");
      assertEquals(true, results.hasNext());
      next = results.next();
      expectedNodeIds = new HashSet<String>();
      expectedNodeIds.add("iama");
      expectedNodeIds.add("iamb");
      assertEquals(expectedNodeIds, new HashSet<>(next.get("nodes").asList()));
      assertEquals(2L, next.get("ct").asLong());
      assertEquals(false, results.hasNext());


      //default relationship
      session.run("MATCH (a)-[ic:TYPE]->(b) CREATE (a)-[:IN_CATEGORY]->(b) DELETE ic");
      session.run("MATCH (a)-[sco:SUBCAT_OF]->(b) CREATE (a)-[:SCO]->(b) DELETE sco");
      results = session.run(
              "MATCH (bcat:Category { name: \"B\"}) CALL n10s.inference.nodesInCategory(bcat) YIELD node RETURN count(node) as ct, collect(node.id) as nodes");
      assertEquals(true, results.hasNext());
      next = results.next();
      expectedNodeIds = new HashSet<String>();
      expectedNodeIds.add("iama");
      expectedNodeIds.add("iamb");
      assertEquals(expectedNodeIds, new HashSet<>(next.get("nodes").asList()));
      assertEquals(2L, next.get("ct").asLong());
      assertEquals(false, results.hasNext());

  }


  @Test
  public void testGetNodesLinkedToOnModelWithUriNames() throws Exception {
      Session session = driver.session();

      session
          .run("create (c:Resource {`http://www.w3.org/2000/01/rdf-schema#label`: \"MyClass\"}) ");
      Result results = session.run(
          "match (c:Resource {`http://www.w3.org/2000/01/rdf-schema#label`: \"MyClass\"}) "
              + "call n10s.inference.nodesInCategory(c, "
              + "{inCatRel:'http://www.w3.org/1999/02/22-rdf-syntax-ns#type', "
              + "subCatRel:'http://www.w3.org/2000/01/rdf-schema#subClassOf'}) yield node return node");
      //just checking syntax is valid, no results expected.
      assertEquals(false, results.hasNext());
  }

  @Test
  public void testGetRelsNoOnto() throws Exception {
      Session session = driver.session();

      session.run(
          "CREATE (b:B {id:'iamb'})-[:REL1 { prop: 123 }]->(a:A {id: 'iama' }) CREATE (b)-[:REL2 { prop: 456 }]->(a)");
      Result results = session.run(
          "MATCH (b:B) CALL n10s.inference.getRels(b,'REL2',{ relLabel: 'Something', subRelRel: 'Something'}) YIELD rel, node RETURN b.id as source, type(rel) as relType, rel.prop as propVal, node.id as target");
      assertEquals(true, results.hasNext());
      Record next = results.next();
      assertEquals("iamb", next.get("source").asString());
      assertEquals("REL2", next.get("relType").asString());
      assertEquals(456L, next.get("propVal").asLong());
      assertEquals("iama", next.get("target").asString());
      assertEquals(false, results.hasNext());
      assertEquals(false, session.run(
          "MATCH (b:B) CALL n10s.inference.getRels(b,'GENERIC',{ relLabel: 'Something', subRelRel: 'Something'}) YIELD rel, node RETURN b.id as source, type(rel) as relType, rel.prop as propVal, node.id as target")
          .hasNext());
  }

  @Test
  public void testGetRels() throws Exception {
      Session session = driver.session();

      session.run(
          "CREATE (b:B {id:'iamb'})-[:REL1 { prop: 123 }]->(a:A {id: 'iama' }) CREATE (b)-[:REL2 { prop: 456 }]->(a)");
      session.run(
          "CREATE (n:Relationship { name: 'REL1'})-[:SRO]->(:Relationship { name: 'GENERIC'})");
      Result results;
      try {
        results = session.run(
                "MATCH (b:B) CALL n10s.inference.getRels(b,'GENERIC',{ relDir: '>'}) YIELD rel, node RETURN b.id as source, type(rel) as relType, rel.prop as propVal, node.id as target");
        assertEquals(true, results.hasNext());
        assertTrue(false);
      }catch (Exception e){
        assertTrue(e.getMessage().contains("Caused by: n10s.inference.MicroReasonerException: No GraphConfig or in-procedure params. Method cannot be run"));
      }

      results = session.run(
              "MATCH (b:B) CALL n10s.inference.getRels(b,'GENERIC',{ relDir: '>', relLabel: 'Relationship', subRelRel: 'SRO'}) YIELD rel, node RETURN b.id as source, type(rel) as relType, rel.prop as propVal, node.id as target");

      Record next = results.next();
      assertEquals("iamb", next.get("source").asString());
      assertEquals("REL1", next.get("relType").asString());
      assertEquals(123L, next.get("propVal").asLong());
      assertEquals("iama", next.get("target").asString());
      assertEquals(false, results.hasNext());

      session.run("call n10s.graphconfig.init()");

      results = session.run(
              "MATCH (b:B) CALL n10s.inference.getRels(b,'GENERIC',{ relDir: '>'}) YIELD rel, node RETURN b.id as source, type(rel) as relType, rel.prop as propVal, node.id as target");

      assertEquals(false, results.hasNext());

      session.run("call n10s.graphconfig.init({ objectPropertyLabel: 'Relationship', " +
              "subPropertyOfRel: 'SRO' })");

      results = session.run(
              "MATCH (b:B) CALL n10s.inference.getRels(b,'GENERIC',{ relDir: '>'}) YIELD rel, node RETURN b.id as source, type(rel) as relType, rel.prop as propVal, node.id as target");

      next = results.next();
      assertEquals("iamb", next.get("source").asString());
      assertEquals("REL1", next.get("relType").asString());
      assertEquals(123L, next.get("propVal").asLong());
      assertEquals("iama", next.get("target").asString());
      assertEquals(false, results.hasNext());
  }

  @Test
  public void testGetRelsCustom() throws Exception {
      Session session = driver.session();

      session.run("call n10s.graphconfig.init({ objectPropertyLabel: 'ObjectProperty', " +
              "subPropertyOfRel: 'SubPropertyOf' })");

      session.run(
          "CREATE (b:B {id:'iamb'})-[:REL1 { prop: 123 }]->(a:A {id: 'iama' }) CREATE (b)-[:REL2 { prop: 456 }]->(a)");
      session.run(
          "CREATE (n:ObjectProperty { name: 'REL1'})-[:SubPropertyOf]->(:ObjectProperty { name: 'GENERIC'})"); //relNameProp: 'opName',
      Result results = session.run(
          "MATCH (b:B) CALL n10s.inference.getRels(b,'GENERIC',{ relDir: '>'}) YIELD rel, node RETURN b.id as source, type(rel) as relType, rel.prop as propVal, node.id as target");
      assertEquals(true, results.hasNext());
      Record next = results.next();
      assertEquals("iamb", next.get("source").asString());
      assertEquals("REL1", next.get("relType").asString());
      assertEquals(123L, next.get("propVal").asLong());
      assertEquals("iama", next.get("target").asString());
      assertEquals(false, results.hasNext());
  }

  @Test
  public void testHasLabelNoOnto() throws Exception {
      Session session = driver.session();

      session
          .run("CREATE (:A {id:'iamA1'}) CREATE (:A {id: 'iamA2' }) CREATE (:B {id: 'iamB1' }) ");
      Result results = null;
      try {
        results = session.run(
                "MATCH (n) WHERE n10s.inference.hasLabel(n,'A') RETURN count(n) as ct, collect(n.id) as nodes");
        results.hasNext();
        assertTrue(false);
      } catch (Exception mie){
        assertTrue(mie.getMessage().contains("Caused by: n10s.inference.MicroReasonerException: " +
                "No GraphConfig or in-function params. Method cannot be run."));
      }

      results = session.run(
              "MATCH (n) WHERE n10s.inference.hasLabel(n,'A', { catLabel: 'something', catNameProp : 'something', subCatRel: 'something'}) " +
                      "RETURN count(n) as ct, collect(n.id) as nodes");
      Record next = results.next();
      Set<String> expectedNodeIds = new HashSet<String>();
      expectedNodeIds.add("iamA1");
      expectedNodeIds.add("iamA2");
      assertEquals(expectedNodeIds, new HashSet<>(next.get("nodes").asList()));
      assertEquals(2L, next.get("ct").asLong());
      assertEquals(false, results.hasNext());
      assertEquals(false,
          session.run("MATCH (n:A) WHERE n10s.inference.hasLabel(n,'C', { catLabel: 'something', catNameProp : 'something', subCatRel: 'something'}) RETURN *").hasNext());
  }

  @Test
  public void testHasLabel() throws Exception {
      Session session = driver.session();

      session
          .run("CREATE (:A {id:'iamA1'}) CREATE (:A {id: 'iamA2' }) CREATE (:B {id: 'iamB1' }) ");
      session.run("CREATE (b:Label { name: \"B\"}) CREATE (a:Label { name: \"A\"})-[:SLO]->(b) ");
      Result results;
      try {
        results = session.run(
                "MATCH (n) WHERE n10s.inference.hasLabel(n,'B') RETURN count(n) as ct, collect(n.id) as nodes");
        assertEquals(true, results.hasNext());
        results.hasNext();
        assertTrue(false);
      } catch (Exception e){
        assertTrue(e.getMessage().contains("Caused by: n10s.inference.MicroReasonerException: No GraphConfig or in-function params. Method cannot be run"));
      }

      results = session.run(
              "MATCH (n) WHERE n10s.inference.hasLabel(n,'B', { catLabel:'Label', subCatRel:'SLO'}) RETURN count(n) as ct, collect(n.id) as nodes");
      Record next = results.next();
      Set<String> expectedNodeIds = new HashSet<String>();
      expectedNodeIds.add("iamA1");
      expectedNodeIds.add("iamA2");
      expectedNodeIds.add("iamB1");
      assertEquals(expectedNodeIds, new HashSet<>(next.get("nodes").asList()));
      assertEquals(3L, next.get("ct").asLong());

      session.run("call n10s.graphconfig.init({ classLabel: 'Label', " +
              "subClassOfRel: 'SLO' })");

      results = session.run(
              "MATCH (n) WHERE n10s.inference.hasLabel(n,'B') RETURN count(n) as ct, collect(n.id) as nodes");
      next = results.next();
      expectedNodeIds = new HashSet<String>();
      expectedNodeIds.add("iamA1");
      expectedNodeIds.add("iamA2");
      expectedNodeIds.add("iamB1");
      assertEquals(expectedNodeIds, new HashSet<>(next.get("nodes").asList()));
      assertEquals(3L, next.get("ct").asLong());
  }

  @Test
  public void testHasLabelCustom() throws Exception {
      Session session = driver.session();

      session
          .run("CREATE (:A {id:'iamA1'}) CREATE (:A {id: 'iamA2' }) CREATE (:B {id: 'iamB1' }) ");
      session.run(
          "CREATE (b:Class { cname: \"B\"}) CREATE (a:Class { cname: \"A\"})-[:SubClassOf]->(b) ");
      Result results = session.run(
          "MATCH (n) WHERE n10s.inference.hasLabel(n,'B', { catLabel: 'Class', catNameProp: 'cname', subCatRel: 'SubClassOf'}) RETURN count(n) as ct, collect(n.id) as nodes");
      assertEquals(true, results.hasNext());
      Record next = results.next();
      Set<String> expectedNodeIds = new HashSet<String>();
      expectedNodeIds.add("iamA1");
      expectedNodeIds.add("iamA2");
      expectedNodeIds.add("iamB1");
      assertEquals(expectedNodeIds, new HashSet<>(next.get("nodes").asList()));
      assertEquals(3L, next.get("ct").asLong());
  }

  @Test
  public void testInCategory() throws Exception {
      Session session = driver.session();

      session.run("CREATE (b:Thing {id:'iamb'}) CREATE (a:Thing {id: 'iama' }) ");
      session.run(
          "CREATE (b:Category { name: \"B\"}) CREATE (a:Category { name: \"A\"})-[:SCO]->(b) ");
      session.run(
          "MATCH (b:Thing {id:'iamb'}),(a:Thing {id: 'iama' }),(bcat:Category { name: \"B\"}),(acat:Category { name: \"A\"}) CREATE (a)-[:IN_CAT]->(acat) CREATE (b)-[:IN_CAT]->(bcat)");
      Result results = session
          .run("MATCH (bcat:Category)<-[:IN_CAT]-(b:Thing {id:'iamb'}) RETURN bcat.name as inCat");
      assertEquals(true, results.hasNext());
      assertEquals("B", results.next().get("inCat").asString());

      session.run("call n10s.graphconfig.init({classLabel: 'Category', subClassOfRel: 'SCO'})");

      String cypherString = "MATCH (x:Thing {id:$thingId}),(y:Category) WHERE n10s.inference.inCategory(x,y, { inCatRel: 'IN_CAT' }) RETURN collect(y.name) as cats";

      results = session.run(cypherString, new HashMap<String, Object>() {{
        put("thingId", "iama");
      }});
      assertEquals(true, results.hasNext());
      assertEquals(new HashSet<String>() {{
        add("A");
        add("B");
      }}, new HashSet<>(results.next().get("cats").asList()));

      results = session.run(cypherString, new HashMap<String, Object>() {{
        put("thingId", "iamb");
      }});
      assertEquals(true, results.hasNext());
      assertEquals(new HashSet<String>() {{
        add("B");
      }}, new HashSet<>(results.next().get("cats").asList()));

      results = session.run(
          "MATCH (x:Thing {id: 'iama' }),(y:Category { name: \"B\"}) RETURN n10s.inference.inCategory(x,y, { inCatRel: 'IN_CAT' } ) as islinked ");
      assertEquals(true, results.hasNext());
      assertEquals(true, results.next().get("islinked").asBoolean());

      //Non-existent relationship
      results = session.run(
          "MATCH (x:Thing {id: 'iama' } ),(y:Category) WHERE n10s.inference.inCategory(x,y) RETURN collect(y.name) as cats");
      assertEquals(true, results.hasNext());
      assertEquals(new HashSet<String>(), new HashSet<>(results.next().get("cats").asList()));
//
//            //Custom relationship
//            session.run("MATCH (a)-[ic:IN_CAT]->(b) CREATE (a)-[:TYPE]->(b) DELETE ic");
//            results = session.run("MATCH (bcat:Category { name: \"B\"}) CALL n10s.inference.getNodesLinkedTo(bcat, 'TYPE') YIELD node RETURN count(node) as ct, collect(node.id) as nodes");
//            assertEquals(true, results.hasNext());
//            next = results.next();
//            expectedNodeIds = new HashSet<String>();
//            expectedNodeIds.add("iama");
//            expectedNodeIds.add("iamb");
//            assertEquals(expectedNodeIds,new HashSet<>(next.get("nodes").asList()));
//            assertEquals(2L, next.get("ct").asLong());
//            assertEquals(false, results.hasNext());
//            session.run("MATCH (a)-[sco:SCO]->(b) CREATE (a)-[:SUBCAT_OF]->(b) DELETE sco");
//
//            results = session.run("MATCH (bcat:Category { name: \"B\"}) CALL n10s.inference.getNodesLinkedTo(bcat, 'TYPE', 'SUBCAT_OF') YIELD node RETURN count(node) as ct, collect(node.id) as nodes");
//            assertEquals(true, results.hasNext());
//            next = results.next();
//            expectedNodeIds = new HashSet<String>();
//            expectedNodeIds.add("iama");
//            expectedNodeIds.add("iamb");
//            assertEquals(expectedNodeIds,new HashSet<>(next.get("nodes").asList()));
//            assertEquals(2L, next.get("ct").asLong());
//            assertEquals(false, results.hasNext());
  }

  //TODO: test modifying the ontology

  //TODO: test relationship with directions

  //TODO: test use of UDF in return expression

}
