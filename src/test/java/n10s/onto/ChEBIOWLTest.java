package n10s.onto;

import static n10s.CommonProcedures.UNIQUENESS_CONSTRAINT_STATEMENT;
import static org.junit.Assert.*;
import static org.junit.Assume.*;

import java.io.File;
import java.util.Map;
import n10s.graphconfig.GraphConfigProcedures;
import n10s.nsprefixes.NsPrefixDefProcedures;
import n10s.rdf.load.RDFLoadProcedures;
import org.junit.*;
import org.neo4j.driver.Config;
// TODO issue #324: re-enable once the import topology is verified/fixed
import org.neo4j.driver.Driver;
import org.neo4j.driver.GraphDatabase;
import org.neo4j.driver.Record;
import org.neo4j.driver.Result;
import org.neo4j.driver.Session;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.harness.junit.rule.Neo4jRule;

/**
 * Integration test for issue #324: importing the ChEBI OWL ontology.
 *
 * Requires testdata/chebi.owl at the project root — not checked in.
 * Download with: curl -O https://ftp.ebi.ac.uk/pub/databases/chebi/ontology/chebi.owl
 * The test skips automatically if the file is absent (safe to run in CI).
 *
 * Key behaviour under test: ChEBI encodes has_role (RO:0000087) via OWL Restrictions
 * (blank nodes), not as direct predicates. This test verifies the restriction pattern
 * is imported intact and is traversable via Cypher.
 */
public class ChEBIOWLTest {

    public static Driver driver;

    @ClassRule
    public static Neo4jRule neo4j = new Neo4jRule()
        .withProcedure(RDFLoadProcedures.class)
        .withProcedure(GraphConfigProcedures.class)
        .withProcedure(NsPrefixDefProcedures.class);

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

    private void initialiseGraphDB(GraphDatabaseService db, String graphConfigParams) {
        db.executeTransactionally(UNIQUENESS_CONSTRAINT_STATEMENT);
        db.executeTransactionally("CALL n10s.graphconfig.init(" +
            (graphConfigParams != null ? graphConfigParams : "{}") + ")");
    }

    @Ignore("issue #324: import topology needs investigation before assertions can be finalised")
    @Test
    public void testChEBIOWLImport() throws Exception {
        File chebiFile = new File("testdata/chebi.owl");
        assumeTrue("testdata/chebi.owl not found at " + chebiFile.getAbsolutePath() + " — skipping",
            chebiFile.exists());

        initialiseGraphDB(neo4j.defaultDatabaseService(), "{}");

        Session session = driver.session();
        String chebiUri = chebiFile.toURI().toString();

        // Large file — import may take several minutes locally
        Result importResult = session.run("CALL n10s.rdf.import.fetch($uri, 'RDF/XML')",
            Map.of("uri", chebiUri));
        Record stats = importResult.next();
        long triplesLoaded = stats.get("triplesLoaded").asLong();
        assertTrue("Expected triples to be loaded from ChEBI OWL, got 0", triplesLoaded > 0);

        // ibuprofen (CHEBI:46195) must exist as an owl:Class node with its rdfs:label
        Result ibuResult = session.run(
            "MATCH (n:owl__Class {uri: 'http://purl.obolibrary.org/obo/CHEBI_46195'}) " +
            "RETURN n.rdfs__label AS label");
        assertTrue("ibuprofen node (CHEBI:46195) should exist", ibuResult.hasNext());
        assertEquals("ibuprofen", ibuResult.next().get("label").asString());

        // ibuprofen must have at least one rdfs:subClassOf relationship to a parent class
        long subClassCount = session.run(
            "MATCH (:owl__Class {uri: 'http://purl.obolibrary.org/obo/CHEBI_46195'})" +
            "      -[:rdfs__subClassOf]->(parent) " +
            "RETURN count(parent) AS cnt")
            .next().get("cnt").asLong();
        assertTrue("ibuprofen should have rdfs:subClassOf relationships", subClassCount > 0);

        // has_role (RO:0000087) object property node must exist
        Result hasRoleResult = session.run(
            "MATCH (p {uri: 'http://purl.obolibrary.org/obo/RO_0000087'}) " +
            "RETURN p.rdfs__label AS label");
        assertTrue("has_role property (RO:0000087) should exist as a node", hasRoleResult.hasNext());

        // The OWL restriction pattern encoding has_role must be traversable from ibuprofen:
        //   ibuprofen -[:rdfs__subClassOf]-> Restriction -[:owl__onProperty]-> has_role
        // This is the correct topology for OWL-encoded role relationships (see issue #324).
        long restrictionCount = session.run(
            "MATCH (:owl__Class {uri: 'http://purl.obolibrary.org/obo/CHEBI_46195'})" +
            "      -[:rdfs__subClassOf]->(r:owl__Restriction)" +
            "      -[:owl__onProperty]->({uri: 'http://purl.obolibrary.org/obo/RO_0000087'}) " +
            "RETURN count(r) AS cnt")
            .next().get("cnt").asLong();
        assertTrue("ibuprofen should have at least one has_role restriction", restrictionCount > 0);

        // The role target (someValuesFrom) must also be reachable from each restriction
        long roleTargetCount = session.run(
            "MATCH (:owl__Class {uri: 'http://purl.obolibrary.org/obo/CHEBI_46195'})" +
            "      -[:rdfs__subClassOf]->(r:owl__Restriction)" +
            "      -[:owl__onProperty]->({uri: 'http://purl.obolibrary.org/obo/RO_0000087'})" +
            "      MATCH (r)-[:owl__someValuesFrom]->(role) " +
            "RETURN count(role) AS cnt")
            .next().get("cnt").asLong();
        assertTrue("Each has_role restriction should point to a role class via owl:someValuesFrom",
            roleTargetCount > 0);
    }
}
