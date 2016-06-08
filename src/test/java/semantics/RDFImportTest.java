package semantics;

import org.junit.Test;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Result;
import org.neo4j.kernel.impl.proc.Procedures;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.test.TestGraphDatabaseFactory;

import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 * Created by jbarrasa on 21/03/2016.
 */
public class RDFImportTest {

    @Test
    public void testAbortIfNoIndices() throws Exception {
        GraphDatabaseService db = new TestGraphDatabaseFactory().newImpermanentDatabase();
        ((GraphDatabaseAPI)db).getDependencyResolver().resolveDependency(Procedures.class).register(RDFImport.class);

        Result importResults1 = db.execute("CALL semantics.importRDF('" +
                RDFImportTest.class.getClassLoader().getResource("mini-ld.json").toURI() + "','JSON-LD',false,500)");

        Map<String, Object> singleResult = importResults1.next();

        assertEquals(new Long(0), singleResult.get("triplesLoaded"));
        assertEquals("KO", singleResult.get("terminationStatus"));
        assertEquals("At least one of the required indexes was not found [ :Resource(uri), :URI(uri), :BNode(uri), :Class(uri) ]", singleResult.get("extraInfo"));

    }

    @Test
    public void testImportJSONLD() throws Exception {
        GraphDatabaseService db = new TestGraphDatabaseFactory().newImpermanentDatabase();
        ((GraphDatabaseAPI)db).getDependencyResolver().resolveDependency(Procedures.class).register(RDFImport.class);

        createIndices(db);

        Result importResults1 = db.execute("CALL semantics.importRDF('" +
                RDFImportTest.class.getClassLoader().getResource("mini-ld.json").toURI() + "','JSON-LD',false,500)");
        assertEquals(new Long(6), importResults1.next().get("triplesLoaded"));
        assertEquals("http://me.markus-lanthaler.com/",
                db.execute("MATCH (n{`http://xmlns.com/foaf/0.1/name` : 'Markus Lanthaler'}) RETURN n.uri AS uri")
                        .next().get("uri"));
        assertEquals(new Long(1),
                db.execute("MATCH (n) WHERE exists(n.`http://xmlns.com/foaf/0.1/modified`) RETURN count(n) AS count")
                        .next().get("count"));
    }

    @Test
    public void testImportJSONLDShortening() throws Exception {
        GraphDatabaseService db = new TestGraphDatabaseFactory().newImpermanentDatabase();
        ((GraphDatabaseAPI)db).getDependencyResolver().resolveDependency(Procedures.class).register(RDFImport.class);

        createIndices(db);

        Result importResults1 = db.execute("CALL semantics.importRDF('" +
                RDFImportTest.class.getClassLoader().getResource("mini-ld.json").toURI() + "','JSON-LD',true,500)");
        assertEquals(new Long(6), importResults1.next().get("triplesLoaded"));
        assertEquals("http://me.markus-lanthaler.com/",
                db.execute("MATCH (n{ns0_name : 'Markus Lanthaler'}) RETURN n.uri AS uri")
                        .next().get("uri"));
        assertEquals(new Long(1),
                db.execute("MATCH (n) WHERE exists(n.ns0_modified) RETURN count(n) AS count")
                        .next().get("count"));

        HashMap<String, String> expectedNamespaceDefs = new HashMap<String, String>();
        expectedNamespaceDefs.put("baseName", "http://xmlns.com/foaf/0.1/");
        expectedNamespaceDefs.put("prefix", "ns0");
        assertEquals(expectedNamespaceDefs,
                db.execute("MATCH (n:NamespacePrefixDefinition) RETURN { baseName: keys(n)[0] ,  prefix: n[keys(n)[0]]} AS namespaces")
                        .next().get("namespaces"));
    }

    @Test
    public void testImportRDFXML() throws Exception {
        GraphDatabaseService db = new TestGraphDatabaseFactory().newImpermanentDatabase();
        ((GraphDatabaseAPI)db).getDependencyResolver().resolveDependency(Procedures.class).register(RDFImport.class);

        createIndices(db);

        Result importResults1 = db.execute("CALL semantics.importRDF('" +
                RDFImportTest.class.getClassLoader().getResource("jeu-de-donnees-des-jeux-de-donnees-open-data-paris.rdf")
                        .toURI() + "','RDF/XML',false,500)");
        assertEquals(new Long(38), importResults1.next().get("triplesLoaded"));
        assertEquals(new Long(7),
                db.execute("MATCH ()-[r:`http://purl.org/dc/terms/relation`]->(b) RETURN count(b) as count")
                        .next().get("count"));
        assertEquals("http://opendata.paris.fr/opendata/jsp/site/Portal.jsp?document_id=109&portlet_id=106",
                db.execute("MATCH (x:BNode) WHERE x.`http://www.w3.org/2000/01/rdf-schema#label` = 'harvest_dataset_url'" +
                "\nRETURN x.`http://www.w3.org/1999/02/22-rdf-syntax-ns#value` AS datasetUrl").next().get("datasetUrl"));

    }

    @Test
    public void testImportRDFXMLShortening() throws Exception {
        GraphDatabaseService db = new TestGraphDatabaseFactory().newImpermanentDatabase();
        ((GraphDatabaseAPI)db).getDependencyResolver().resolveDependency(Procedures.class).register(RDFImport.class);

        createIndices(db);

        Result importResults1 = db.execute("CALL semantics.importRDF('" +
                RDFImportTest.class.getClassLoader().getResource("jeu-de-donnees-des-jeux-de-donnees-open-data-paris.rdf")
                        .toURI() + "','RDF/XML',true,500)");
        assertEquals(new Long(38), importResults1.next().get("triplesLoaded"));
        assertEquals(new Long(7),
                db.execute("MATCH ()-[r:ns2_relation]->(b) RETURN count(b) as count")
                        .next().get("count"));

        assertEquals("http://opendata.paris.fr/opendata/jsp/site/Portal.jsp?document_id=109&portlet_id=106",
                db.execute("MATCH (x:BNode) WHERE x.ns4_label = 'harvest_dataset_url'" +
                        "\nRETURN x.ns5_value AS datasetUrl").next().get("datasetUrl"));

        assertEquals("ns0",
                db.execute("MATCH (n:NamespacePrefixDefinition) \n" +
                        "RETURN n.`http://www.w3.org/ns/dcat#` as prefix")
                        .next().get("prefix"));

    }

    @Test
    public void testImportRDFXMLShorteningWithPrefixPreDefinition() throws Exception {
        GraphDatabaseService db = new TestGraphDatabaseFactory().newImpermanentDatabase();
        ((GraphDatabaseAPI)db).getDependencyResolver().resolveDependency(Procedures.class).register(RDFImport.class);

        createIndices(db);

        //prefix predefinition by creating the NamespacePrefixDefinitionNode
        db.execute("WITH {`http://purl.org/dc/terms/`:'dc',\n" +
                "`http://www.w3.org/1999/02/22-rdf-syntax-ns#`:'rdf',\n" +
                "`http://www.w3.org/2002/07/owl#`:'owl',\n" +
                "`http://www.w3.org/ns/dcat#`:'dcat',\n" +
                "`http://www.w3.org/2000/01/rdf-schema#`:'rdfs',\n" +
                "`http://xmlns.com/foaf/0.1/`:'foaf'} as nslist\n" +
                "MERGE (n:NamespacePrefixDefinition)\n" +
                "SET n+=nslist");

        Result importResults1 = db.execute("CALL semantics.importRDF('" +
                RDFImportTest.class.getClassLoader().getResource("jeu-de-donnees-des-jeux-de-donnees-open-data-paris.rdf")
                        .toURI() + "','RDF/XML',true,500)");
        assertEquals(new Long(38), importResults1.next().get("triplesLoaded"));
        assertEquals(new Long(7),
                db.execute("MATCH ()-[r:dc_relation]->(b) RETURN count(b) as count")
                        .next().get("count"));

        assertEquals("http://opendata.paris.fr/opendata/jsp/site/Portal.jsp?document_id=109&portlet_id=106",
                db.execute("MATCH (x) WHERE x.rdfs_label = 'harvest_dataset_url'" +
                        "\nRETURN x.rdf_value AS datasetUrl").next().get("datasetUrl"));

        assertEquals("dcat",
                db.execute("MATCH (n:NamespacePrefixDefinition) \n" +
                        "RETURN n.`http://www.w3.org/ns/dcat#` as prefix")
                        .next().get("prefix"));

    }


    @Test
    public void testImportRDFXMLShorteningWithPrefixPreDefinitionOneTriple() throws Exception {
        GraphDatabaseService db = new TestGraphDatabaseFactory().newImpermanentDatabase();
        ((GraphDatabaseAPI)db).getDependencyResolver().resolveDependency(Procedures.class).register(RDFImport.class);

        createIndices(db);

        //prefix predefinition by creating the NamespacePrefixDefinitionNode
        db.execute("WITH {`http://neo4j.com/voc/`:'voc' } as nslist\n" +
                "MERGE (n:NamespacePrefixDefinition)\n" +
                "SET n+=nslist " +
                "RETURN n ");

        Result importResults1 = db.execute("CALL semantics.importRDF('" +
                RDFImportTest.class.getClassLoader().getResource("oneTriple.rdf")
                        .toURI() + "','RDF/XML',true,500)");
        assertEquals(new Long(1), importResults1.next().get("triplesLoaded"));
        assertEquals("JB",
                db.execute("MATCH (jb {uri: 'http://neo4j.com/invividual/JB'}) RETURN jb.voc_name AS name")
                        .next().get("name"));

        assertEquals("voc",
                db.execute("MATCH (n:NamespacePrefixDefinition) \n" +
                        "RETURN n.`http://neo4j.com/voc/` as prefix")
                        .next().get("prefix"));

    }

    @Test
    public void testImportTurtle() throws Exception {
        GraphDatabaseService db = new TestGraphDatabaseFactory().newImpermanentDatabase();
        ((GraphDatabaseAPI)db).getDependencyResolver().resolveDependency(Procedures.class).register(RDFImport.class);

        createIndices(db);

        Result importResults1 = db.execute("CALL semantics.importRDF('" +
                RDFImportTest.class.getClassLoader().getResource("opentox-example.ttl")
                        .toURI() + "','TURTLE',false,500)");
        assertEquals(new Long(157), importResults1.next().get("triplesLoaded"));
        Result algoNames = db.execute("MATCH (n:`http://www.opentox.org/api/1.1#Algorithm`) " +
                "\nRETURN n.`http://purl.org/dc/elements/1.1/title` AS algos ORDER By algos");

        assertEquals("J48", algoNames.next().get("algos"));
        assertEquals("XLogP", algoNames.next().get("algos"));

        Result compounds = db.execute("MATCH ()-[r:`http://www.opentox.org/api/1.1#compound`]->(c) RETURN DISTINCT c.uri AS compound order by compound");
        assertEquals("http://www.opentox.org/example/1.1#benzene", compounds.next().get("compound"));
        assertEquals("http://www.opentox.org/example/1.1#phenol", compounds.next().get("compound"));

    }

    private void createIndices(GraphDatabaseService db) {
        db.execute("CREATE INDEX ON :Resource(uri)");
        db.execute("CREATE INDEX ON :URI(uri)");
        db.execute("CREATE INDEX ON :BNode(uri)");
        db.execute("CREATE INDEX ON :Class(uri)");
    }

}
