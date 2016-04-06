package semantics;

import org.junit.Test;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Result;
import org.neo4j.kernel.impl.proc.Procedures;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.test.TestGraphDatabaseFactory;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 * Created by jbarrasa on 21/03/2016.
 */
public class RDFImportTest {

    @Test
    public void testImportJSONLD() throws Exception {
        GraphDatabaseService db = new TestGraphDatabaseFactory().newImpermanentDatabase();
        ((GraphDatabaseAPI)db).getDependencyResolver().resolveDependency(Procedures.class).register(RDFImport.class);

        Result importResults1 = db.execute("CALL semantics.importRDF('" +
                RDFImportTest.class.getClassLoader().getResource("mini-ld.json").toURI() + "','JSON-LD')");
        assertEquals(new Long(6), importResults1.next().get("triplesLoaded"));
        assertEquals("http://me.markus-lanthaler.com/",
                db.execute("MATCH (n{`http://xmlns.com/foaf/0.1/name` : 'Markus Lanthaler'}) RETURN n.uri AS uri")
                        .next().get("uri"));
        assertEquals(new Long(1),
                db.execute("MATCH (n) WHERE exists(n.`http://xmlns.com/foaf/0.1/modified`) RETURN count(n) AS count")
                        .next().get("count"));
    }

    @Test
    public void testImportRDFXML() throws Exception {
        GraphDatabaseService db = new TestGraphDatabaseFactory().newImpermanentDatabase();
        ((GraphDatabaseAPI)db).getDependencyResolver().resolveDependency(Procedures.class).register(RDFImport.class);

        Result importResults1 = db.execute("CALL semantics.importRDF('" +
                RDFImportTest.class.getClassLoader().getResource("jeu-de-donnees-des-jeux-de-donnees-open-data-paris.rdf")
                        .toURI() + "','RDF/XML')");
        assertEquals(new Long(38), importResults1.next().get("triplesLoaded"));
        assertEquals(new Long(7),
                db.execute("MATCH ()-[r:`http://purl.org/dc/terms/relation`]->(b) RETURN count(b) as count")
                        .next().get("count"));
        assertEquals("http://opendata.paris.fr/opendata/jsp/site/Portal.jsp?document_id=109&portlet_id=106",
                db.execute("MATCH (x:BNode) WHERE x.`http://www.w3.org/2000/01/rdf-schema#label` = 'harvest_dataset_url'" +
                "\nRETURN x.`http://www.w3.org/1999/02/22-rdf-syntax-ns#value` AS datasetUrl").next().get("datasetUrl"));

    }

    @Test
    public void testImportTurtle() throws Exception {
        GraphDatabaseService db = new TestGraphDatabaseFactory().newImpermanentDatabase();
        ((GraphDatabaseAPI)db).getDependencyResolver().resolveDependency(Procedures.class).register(RDFImport.class);

        Result importResults1 = db.execute("CALL semantics.importRDF('" +
                RDFImportTest.class.getClassLoader().getResource("opentox-example.ttl")
                        .toURI() + "','TURTLE')");
        assertEquals(new Long(157), importResults1.next().get("triplesLoaded"));
        Result algoNames = db.execute("MATCH (n:`http://www.opentox.org/api/1.1#Algorithm`) " +
                "\nRETURN n.`http://purl.org/dc/elements/1.1/title` AS algos ORDER By algos");

        assertEquals("J48", algoNames.next().get("algos"));
        assertEquals("XLogP", algoNames.next().get("algos"));

        Result compounds = db.execute("MATCH ()-[r:`http://www.opentox.org/api/1.1#compound`]->(c) RETURN DISTINCT c.uri AS compound order by compound");
        assertEquals("http://www.opentox.org/example/1.1#benzene", compounds.next().get("compound"));
        assertEquals("http://www.opentox.org/example/1.1#phenol", compounds.next().get("compound"));

    }

}
