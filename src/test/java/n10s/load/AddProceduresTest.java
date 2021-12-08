package n10s.load;

import n10s.RDFProceduresTest;
import n10s.graphconfig.GraphConfigProcedures;
import n10s.nsprefixes.NsPrefixDefProcedures;
import n10s.rdf.load.AddProcedures;
import n10s.rdf.load.RDFLoadProcedures;
import org.junit.Rule;
import org.junit.Test;
import org.neo4j.driver.*;
import org.neo4j.driver.types.Node;
import org.neo4j.driver.types.Point;
import org.neo4j.driver.types.Relationship;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.harness.junit.rule.Neo4jRule;

import static n10s.graphconfig.Params.PREFIX_SEPARATOR;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class AddProceduresTest {

    @Rule
    public Neo4jRule neo4j = new Neo4jRule()
            .withProcedure(RDFLoadProcedures.class)
            .withProcedure(GraphConfigProcedures.class)
            .withProcedure(NsPrefixDefProcedures.class)
            .withProcedure(AddProcedures.class);


    @Test
    public void testAddNodeAfterImport() throws Exception {
        try (Driver driver = GraphDatabase.driver(neo4j.boltURI(),
                Config.builder().withoutEncryption().build()); Session session = driver.session()) {

            initialiseGraphDB(neo4j.defaultDatabaseService(), "{ handleRDFTypes: 'LABELS_AND_NODES'}");

            Result importResults
                    = session.run("CALL n10s.rdf.import.fetch('" +
                    RDFProceduresTest.class.getClassLoader()
                            .getResource("jeu-de-donnees-des-jeux-de-donnees-open-data-paris.rdf")
                            .toURI()
                    + "','RDF/XML',{ commitSize: 500})");
            assertEquals(38L, importResults
                    .next().get("triplesLoaded").asLong());
            assertEquals(7L,
                    session
                            .run("MATCH ()-[r]->(b) WHERE type(r) CONTAINS 'relation' RETURN count(b) as count")
                            .next().get("count").asLong());

            assertEquals(
                    "http://opendata.paris.fr/opendata/jsp/site/Portal.jsp?document_id=109&portlet_id=106",
                    session.run(
                            "MATCH (x:Resource) WHERE x.rdfs" + PREFIX_SEPARATOR + "label = 'harvest_dataset_url'"

                                    + "\nRETURN x.rdf" + PREFIX_SEPARATOR + "value AS datasetUrl").next()
                            .get("datasetUrl").asString());

            assertEquals("ns0",
                    session.run("call n10s.nsprefixes.list() yield prefix, namespace\n"
                            + "with prefix, namespace where namespace = 'http://www.w3.org/ns/dcat#'\n"
                            + "return prefix, namespace").next().get("prefix").asString());

            Result addResult = session.run("call n10s.add.node('http://something/1234'," +
                    "['ns0__Thingamajiggy','ns0__Thingemeebobby']," +
                    "[ { key: 'http://somevocab/v1/one', val: 'one'}])");
                    //"[ { key: 'ns0__one', val: 'one'},{ key: 'ns0__two', val: 2},{ key: 'ns0__three', val: true }])");
            assertTrue(addResult.hasNext());
            Node nodeCreated = addResult.next().get("node").asNode();
            assertEquals("one", nodeCreated.get("ns2__one").asString());

            addResult = session.run("call n10s.add.node('http://something/5678'," +
                    "['ns0__Thingamajiggy','ns0__Thingemeebobby']," +
                    "[ { key: 'http://somevocab/v1/two', val: 'two \"quoted\" '}])");
            //"[ { key: 'ns0__one', val: 'one'},{ key: 'ns0__two', val: 2},{ key: 'ns0__three', val: true }])");
            assertTrue(addResult.hasNext());
            nodeCreated = addResult.next().get("node").asNode();
            assertEquals("two \"quoted\" ", nodeCreated.get("ns2__two").asString());

            addResult = session.run("match (from:Resource{uri:'http://something/1234'}), " +
                    " (to:Resource{uri:'http://something/5678'}) with from, to" +
                    " call n10s.add.relationship(from ," +
                    "'ns0__relationshipppy'," +
                    "[ { key: 'http://somevocab/v1/relone', val: point({ x: 12, y : -33}) }],to) yield rel " +
                    " return rel ");
            //"[ { key: 'ns0__one', val: 'one'},{ key: 'ns0__two', val: 2},{ key: 'ns0__three', val: true }])");
            assertTrue(addResult.hasNext());
            Relationship rel = addResult.next().get("rel").asRelationship();
            Point thepoint = rel.get("ns2__relone").asPoint();
            assertEquals(12, thepoint.x(),0.00005);
            assertEquals(-33, thepoint.y(),0.00005);
            assertEquals(7203, thepoint.srid());
        }
    }

    private void initialiseGraphDB(GraphDatabaseService db, String graphConfigParams) {
        db.executeTransactionally("CREATE CONSTRAINT n10s_unique_uri "
                + "ON (r:Resource) ASSERT r.uri IS UNIQUE");
        db.executeTransactionally("CALL n10s.graphconfig.init(" +
                (graphConfigParams != null ? graphConfigParams : "{}") + ")");
    }
}
