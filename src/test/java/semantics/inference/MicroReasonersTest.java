package semantics.inference;

import org.junit.Rule;
import org.junit.Test;
import org.neo4j.driver.v1.*;
import org.neo4j.harness.junit.Neo4jRule;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import static org.junit.Assert.assertEquals;

public class MicroReasonersTest {

    @Rule
    public Neo4jRule neo4j = new Neo4jRule()
            .withProcedure( MicroReasoners.class ).withFunction(MicroReasoners.class);
    @Test
    public void testGetNodesNoOnto() throws Exception {
        try (Driver driver = GraphDatabase.driver(neo4j.boltURI(), Config.build().withEncryptionLevel(Config.EncryptionLevel.NONE).toConfig())) {

            Session session = driver.session();

            session.run("CREATE (b:B {id:'iamb'}) CREATE (a:A {id: 'iama' }) ");
            StatementResult results = session.run("CALL semantics.inference.getNodesWithLabel('B') YIELD node RETURN count(node) as ct, collect(node.id) as nodes");
            assertEquals(true, results.hasNext());
            Record next = results.next();
            Set<String> expectedNodeIds = new HashSet<String>();
            expectedNodeIds.add("iamb");
            assertEquals(expectedNodeIds,new HashSet<>(next.get("nodes").asList()));
            assertEquals(1L, next.get("ct").asLong());
            assertEquals(false, results.hasNext());
        }
    }

    @Test
    public void testGetNodes() throws Exception {
        try (Driver driver = GraphDatabase.driver(neo4j.boltURI(), Config.build().withEncryptionLevel(Config.EncryptionLevel.NONE).toConfig())) {

            Session session = driver.session();

            session.run("CREATE (b:B {id:'iamb'}) CREATE (a:A {id: 'iama' }) ");
            session.run("CREATE (b:Label { name: \"B\"}) CREATE (a:Label { name: \"A\"})-[:SLO]->(b) ");
            StatementResult results = session.run("CALL semantics.inference.getNodesWithLabel('B') YIELD node RETURN count(node) as ct, collect(node.id) as nodes");
            assertEquals(true, results.hasNext());
            Record next = results.next();
            Set<String> expectedNodeIds = new HashSet<String>();
            expectedNodeIds.add("iama");
            expectedNodeIds.add("iamb");
            assertEquals(expectedNodeIds,new HashSet<>(next.get("nodes").asList()));
            assertEquals(2L, next.get("ct").asLong());
            assertEquals(false, results.hasNext());
        }
    }

    @Test
    public void testGetNodesLinkedTo() throws Exception {
        try (Driver driver = GraphDatabase.driver(neo4j.boltURI(), Config.build().withEncryptionLevel(Config.EncryptionLevel.NONE).toConfig())) {

            Session session = driver.session();

            session.run("CREATE (b:Thing {id:'iamb'}) CREATE (a:Thing {id: 'iama' }) ");
            session.run("CREATE (b:Category { name: \"B\"}) CREATE (a:Category { name: \"A\"})-[:SCO]->(b) ");
            session.run("MATCH (b:Thing {id:'iamb'}),(a:Thing {id: 'iama' }),(bcat:Category { name: \"B\"}),(acat:Category { name: \"A\"}) CREATE (a)-[:IN_CAT]->(acat) CREATE (b)-[:IN_CAT]->(bcat)");
            StatementResult results = session.run("MATCH (bcat:Category { name: \"B\"}) CALL semantics.inference.getNodesLinkedTo(bcat) YIELD node RETURN count(node) as ct, collect(node.id) as nodes");
            assertEquals(true, results.hasNext());
            Record next = results.next();
            Set<String> expectedNodeIds = new HashSet<String>();
            expectedNodeIds.add("iama");
            expectedNodeIds.add("iamb");
            assertEquals(expectedNodeIds,new HashSet<>(next.get("nodes").asList()));
            assertEquals(2L, next.get("ct").asLong());
            assertEquals(false, results.hasNext());

            //Non-existent relationship
            results = session.run("MATCH (bcat:Category { name: \"B\"}) CALL semantics.inference.getNodesLinkedTo(bcat, { inCatRelName: 'TYPE' } ) YIELD node RETURN node");
            assertEquals(false, results.hasNext());

            //Custom relationship
            session.run("MATCH (a)-[ic:IN_CAT]->(b) CREATE (a)-[:TYPE]->(b) DELETE ic");
            results = session.run("MATCH (bcat:Category { name: \"B\"}) CALL semantics.inference.getNodesLinkedTo(bcat, { inCatRelName: 'TYPE'} ) YIELD node RETURN count(node) as ct, collect(node.id) as nodes");
            assertEquals(true, results.hasNext());
            next = results.next();
            expectedNodeIds = new HashSet<String>();
            expectedNodeIds.add("iama");
            expectedNodeIds.add("iamb");
            assertEquals(expectedNodeIds,new HashSet<>(next.get("nodes").asList()));
            assertEquals(2L, next.get("ct").asLong());
            assertEquals(false, results.hasNext());
            session.run("MATCH (a)-[sco:SCO]->(b) CREATE (a)-[:SUBCAT_OF]->(b) DELETE sco");

            results = session.run("MATCH (bcat:Category { name: \"B\"}) CALL semantics.inference.getNodesLinkedTo(bcat, { inCatRelName: 'TYPE', subCatRelName: 'SUBCAT_OF'}) YIELD node RETURN count(node) as ct, collect(node.id) as nodes");
            assertEquals(true, results.hasNext());
            next = results.next();
            expectedNodeIds = new HashSet<String>();
            expectedNodeIds.add("iama");
            expectedNodeIds.add("iamb");
            assertEquals(expectedNodeIds,new HashSet<>(next.get("nodes").asList()));
            assertEquals(2L, next.get("ct").asLong());
            assertEquals(false, results.hasNext());
        }
    }

    @Test
    public void testGetRelsNoOnto() throws Exception {
        try (Driver driver = GraphDatabase.driver(neo4j.boltURI(), Config.build().withEncryptionLevel(Config.EncryptionLevel.NONE).toConfig())) {

            Session session = driver.session();

            session.run("CREATE (b:B {id:'iamb'})-[:REL1 { prop: 123 }]->(a:A {id: 'iama' }) CREATE (b)-[:REL2 { prop: 456 }]->(a)");
            StatementResult results = session.run("MATCH (b:B) CALL semantics.inference.getRels(b,'REL2') YIELD rel, node RETURN b.id as source, type(rel) as relType, rel.prop as propVal, node.id as target");
            assertEquals(true, results.hasNext());
            Record next = results.next();
            assertEquals("iamb", next.get("source").asString());
            assertEquals("REL2", next.get("relType").asString());
            assertEquals(456L, next.get("propVal").asLong());
            assertEquals("iama", next.get("target").asString());
            assertEquals(false, results.hasNext());
            assertEquals(false,session.run("MATCH (b:B) CALL semantics.inference.getRels(b,'GENERIC') YIELD rel, node RETURN b.id as source, type(rel) as relType, rel.prop as propVal, node.id as target").hasNext());
        }
    }

    @Test
    public void testGetRels() throws Exception {
        try (Driver driver = GraphDatabase.driver(neo4j.boltURI(), Config.build().withEncryptionLevel(Config.EncryptionLevel.NONE).toConfig())) {

            Session session = driver.session();

            session.run("CREATE (b:B {id:'iamb'})-[:REL1 { prop: 123 }]->(a:A {id: 'iama' }) CREATE (b)-[:REL2 { prop: 456 }]->(a)");
            session.run("CREATE (n:Relationship { name: 'REL1'})-[:SRO]->(:Relationship { name: 'GENERIC'})");
            StatementResult results = session.run("MATCH (b:B) CALL semantics.inference.getRels(b,'GENERIC','>') YIELD rel, node RETURN b.id as source, type(rel) as relType, rel.prop as propVal, node.id as target");
            assertEquals(true, results.hasNext());
            Record next = results.next();
            assertEquals("iamb", next.get("source").asString());
            assertEquals("REL1", next.get("relType").asString());
            assertEquals(123L, next.get("propVal").asLong());
            assertEquals("iama", next.get("target").asString());
            assertEquals(false, results.hasNext());
        }
    }

    @Test
    public void testHasLabelNoOnto() throws Exception {
        try (Driver driver = GraphDatabase.driver(neo4j.boltURI(), Config.build().withEncryptionLevel(Config.EncryptionLevel.NONE).toConfig())) {

            Session session = driver.session();

            session.run("CREATE (:A {id:'iamA1'}) CREATE (:A {id: 'iamA2' }) CREATE (:B {id: 'iamB1' }) ");
            StatementResult results = session.run("MATCH (n) WHERE semantics.inference.hasLabel(n,'A') RETURN count(n) as ct, collect(n.id) as nodes");
            assertEquals(true, results.hasNext());
            Record next = results.next();
            Set<String> expectedNodeIds = new HashSet<String>();
            expectedNodeIds.add("iamA1");
            expectedNodeIds.add("iamA2");
            assertEquals(expectedNodeIds,new HashSet<>(next.get("nodes").asList()));
            assertEquals(2L, next.get("ct").asLong());
            assertEquals(false, results.hasNext());
            assertEquals(false,session.run("MATCH (n:A) WHERE semantics.inference.hasLabel(n,'C') RETURN *").hasNext());
        }
    }

    @Test
    public void testHasLabel() throws Exception {
        try (Driver driver = GraphDatabase.driver(neo4j.boltURI(), Config.build().withEncryptionLevel(Config.EncryptionLevel.NONE).toConfig())) {

            Session session = driver.session();

            session.run("CREATE (:A {id:'iamA1'}) CREATE (:A {id: 'iamA2' }) CREATE (:B {id: 'iamB1' }) ");
            session.run("CREATE (b:Label { name: \"B\"}) CREATE (a:Label { name: \"A\"})-[:SLO]->(b) ");
            StatementResult results = session.run("MATCH (n) WHERE semantics.inference.hasLabel(n,'B') RETURN count(n) as ct, collect(n.id) as nodes");
            assertEquals(true, results.hasNext());
            Record next = results.next();
            Set<String> expectedNodeIds = new HashSet<String>();
            expectedNodeIds.add("iamA1");
            expectedNodeIds.add("iamA2");
            expectedNodeIds.add("iamB1");
            assertEquals(expectedNodeIds,new HashSet<>(next.get("nodes").asList()));
            assertEquals(3L, next.get("ct").asLong());
        }
    }

    @Test
    public void testNodeIsLinkdedTo() throws Exception {
        try (Driver driver = GraphDatabase.driver(neo4j.boltURI(), Config.build().withEncryptionLevel(Config.EncryptionLevel.NONE).toConfig())) {

            Session session = driver.session();

            session.run("CREATE (b:Thing {id:'iamb'}) CREATE (a:Thing {id: 'iama' }) ");
            session.run("CREATE (b:Category { name: \"B\"}) CREATE (a:Category { name: \"A\"})-[:SCO]->(b) ");
            session.run("MATCH (b:Thing {id:'iamb'}),(a:Thing {id: 'iama' }),(bcat:Category { name: \"B\"}),(acat:Category { name: \"A\"}) CREATE (a)-[:IN_CAT]->(acat) CREATE (b)-[:IN_CAT]->(bcat)");
            StatementResult results = session.run("MATCH (bcat:Category)<-[:IN_CAT]-(b:Thing {id:'iamb'}) RETURN bcat.name as inCat");
            assertEquals(true, results.hasNext());
            assertEquals("B",results.next().get("inCat").asString());
            String cypherString = "MATCH (x:Thing {id:$thingId}),(y:Category) WHERE semantics.inference.isNodeLinkedTo(x,y) RETURN collect(y.name) as cats";

            results = session.run(cypherString, new HashMap<String, Object>(){{ put("thingId", "iama"); }});
            assertEquals(true, results.hasNext());
            assertEquals(new HashSet<String>(){{
                add("A");
                add("B");
            }},new HashSet<>(results.next().get("cats").asList()));

            results = session.run(cypherString, new HashMap<String, Object>(){{ put("thingId", "iamb"); }});
            assertEquals(true, results.hasNext());
            assertEquals(new HashSet<String>(){{
                add("B");
            }},new HashSet<>(results.next().get("cats").asList()));

            results = session.run("MATCH (x:Thing {id: 'iama' }),(y:Category { name: \"B\"}) RETURN semantics.inference.isNodeLinkedTo(x,y) as islinked ");
            assertEquals(true, results.hasNext());
            assertEquals(true ,results.next().get("islinked").asBoolean());

            //Non-existent relationship
            results = session.run("MATCH (x:Thing {id: 'iama' } ),(y:Category) WHERE semantics.inference.isNodeLinkedTo(x,y, { inCatRelName: 'TYPE' } ) RETURN collect(y.name) as cats");
            assertEquals(true, results.hasNext());
            assertEquals(new HashSet<String>(),new HashSet<>(results.next().get("cats").asList()));
//
//            //Custom relationship
//            session.run("MATCH (a)-[ic:IN_CAT]->(b) CREATE (a)-[:TYPE]->(b) DELETE ic");
//            results = session.run("MATCH (bcat:Category { name: \"B\"}) CALL semantics.inference.getNodesLinkedTo(bcat, 'TYPE') YIELD node RETURN count(node) as ct, collect(node.id) as nodes");
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
//            results = session.run("MATCH (bcat:Category { name: \"B\"}) CALL semantics.inference.getNodesLinkedTo(bcat, 'TYPE', 'SUBCAT_OF') YIELD node RETURN count(node) as ct, collect(node.id) as nodes");
//            assertEquals(true, results.hasNext());
//            next = results.next();
//            expectedNodeIds = new HashSet<String>();
//            expectedNodeIds.add("iama");
//            expectedNodeIds.add("iamb");
//            assertEquals(expectedNodeIds,new HashSet<>(next.get("nodes").asList()));
//            assertEquals(2L, next.get("ct").asLong());
//            assertEquals(false, results.hasNext());
        }
    }

    //TODO: test modifying the ontology

    //TODO: test relationship with directions

    //TODO: test use of UDF in return expression

}
