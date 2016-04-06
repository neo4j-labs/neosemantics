package semantics;

import org.junit.Test;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Result;
import org.neo4j.kernel.impl.proc.Procedures;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.test.TestGraphDatabaseFactory;

import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 * Created by jbarrasa on 21/03/2016.
 */
public class LiteOntologyImporterTest {

    @Test
    public void testImport() throws Exception {
        GraphDatabaseService db = new TestGraphDatabaseFactory().newImpermanentDatabase();
        ((GraphDatabaseAPI)db).getDependencyResolver().resolveDependency(Procedures.class).register(RDFImport.class);
        //db.execute("CREATE INDEX ON :BNode(uri)");
        //db.execute("CREATE INDEX ON :URI(uri)");
        Result res = db.execute("CALL semantics.importRDF('https://publicdata.eu/dataset/jeu-de-donnees-des-jeux-de-donnees-open-data-paris.rdf','RDF/XML')");
        //Result res = db.execute("CALL semantics.importRDF('file:///Users/jbarrasa/Documents/Data/ontodata/data/repository.trig','TriG')");
        // then we expect one result-row with min-degree 0 and max-degree 2
        //assertTrue(res.hasNext());
        //System.out.println(res.next());

        Result res2 = db.execute("CALL semantics.importRDF('file:///Users/jbarrasa/Downloads/mini-ld.json','JSON-LD')");
        assertTrue(res2.hasNext());
        Map<String, Object> nextResult = res2.next();
        System.out.println(nextResult);
        assertEquals(new Long(7), nextResult.get("triplesLoaded"));
        Result res3 = db.execute("MATCH (n:URI) RETURN n");
        assertTrue(res3.hasNext());
        Result res4 = db.execute("MATCH (n{`http://xmlns.com/foaf/0.1/yesorno` : false}) RETURN n");
        assertTrue(res4.hasNext());


//        db.execute("CREATE ({ name: 'An unlabeled node with a name. Inconsistency!!'})");
//        res = db.execute("CALL semantics.importRDF()");
//        assertTrue(res.hasNext());

//        Map<String,Object> row = res.next();
//        System.out.println(row);
//        assertEquals("User", row.get("label"));
//        // Dan has no friends
//        assertEquals(0, row.get("min"));
//        // Alice knows 2 people
//        assertEquals(2, row.get("max"));
//        // We have 4 nodes in our graph
//        assertEquals(4, row.get("count"));
//        // only one result record was produced
//        assertFalse(res.hasNext());
    }

}
