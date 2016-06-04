package semantics;

import org.junit.Test;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Result;
import org.neo4j.kernel.impl.proc.Procedures;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.test.TestGraphDatabaseFactory;

import java.net.URL;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * Created by jbarrasa on 21/03/2016.
 */
public class LiteOntologyImporterTest {

    @Test
    public void LiteOntoImport() throws Exception {
        GraphDatabaseService db = new TestGraphDatabaseFactory().newImpermanentDatabase();
        ((GraphDatabaseAPI)db).getDependencyResolver().resolveDependency(Procedures.class).register(LiteOntologyImporter.class);

        Result importResult = db.execute("CALL semantics.LiteOntoImport('" +
                LiteOntologyImporterTest.class.getClassLoader().getResource("moviesontology.owl").toURI()
                + "','RDF/XML')");

        assertEquals(new Long(16), importResult.next().get("elementsLoaded"));

        assertEquals(new Long(2), db.execute("MATCH (n:Class) RETURN count(n) AS count").next().get("count"));

        assertEquals(new Long(5), db.execute("MATCH (n:DatatypeProperty)-[:DOMAIN]->(:Class)  RETURN count(n) AS count").next().get("count"));

        assertEquals(new Long(3), db.execute("MATCH (n:DatatypeProperty)-[:DOMAIN]->(:ObjectProperty) RETURN count(n) AS count").next().get("count"));

        assertEquals(new Long(6), db.execute("MATCH (n:ObjectProperty) RETURN count(n) AS count").next().get("count"));

    }

}
