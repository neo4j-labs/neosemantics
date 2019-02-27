package semantics;

import org.junit.Test;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Result;
import org.neo4j.kernel.impl.proc.Procedures;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.test.TestGraphDatabaseFactory;

import static org.junit.Assert.assertEquals;

/**
 * Created by jbarrasa on 21/03/2016.
 */
public class LiteOntologyImporterTest {

    @Test
    public void liteOntoImport() throws Exception {
        GraphDatabaseService db = new TestGraphDatabaseFactory().newImpermanentDatabase();
        ((GraphDatabaseAPI)db).getDependencyResolver().resolveDependency(Procedures.class).registerProcedure(LiteOntologyImporter.class);

        Result importResult = db.execute("CALL semantics.liteOntoImport('" +
                LiteOntologyImporterTest.class.getClassLoader().getResource("moviesontology.owl").toURI()
                + "','RDF/XML')");

        assertEquals(new Long(16), importResult.next().get("elementsLoaded"));

        assertEquals(new Long(2), db.execute("MATCH (n:Class) RETURN count(n) AS count").next().get("count"));

        assertEquals(new Long(5), db.execute("MATCH (n:Property)-[:DOMAIN]->(:Class)  RETURN count(n) AS count").next().get("count"));

        assertEquals(new Long(3), db.execute("MATCH (n:Property)-[:DOMAIN]->(:Relationship) RETURN count(n) AS count").next().get("count"));

        assertEquals(new Long(6), db.execute("MATCH (n:Relationship) RETURN count(n) AS count").next().get("count"));

    }
    
    
    @Test
    public void liteOntoImportSchemaOrg() throws Exception {
        GraphDatabaseService db = new TestGraphDatabaseFactory().newImpermanentDatabase();
        ((GraphDatabaseAPI)db).getDependencyResolver().resolveDependency(Procedures.class).registerProcedure(LiteOntologyImporter.class);

        Result importResult = db.execute("CALL semantics.liteOntoImport('" +
                        LiteOntologyImporterTest.class.getClassLoader().getResource("schema.rdf").toURI() +
                "','RDF/XML')");

//        assertEquals(new Long(16), importResult.next().get("elementsLoaded"));

        assertEquals(new Long(596), db.execute("MATCH (n:Class) RETURN count(n) AS count").next().get("count"));

        assertEquals(new Long(371), db.execute("MATCH (n:Property)-[:DOMAIN]->(:Class)  RETURN count(n) AS count").next().get("count"));

        assertEquals(new Long(0), db.execute("MATCH (n:Property)-[:DOMAIN]->(:Relationship) RETURN count(n) AS count").next().get("count"));

        assertEquals(new Long(416), db.execute("MATCH (n:Relationship) RETURN count(n) AS count").next().get("count"));

    }
    @Test
    public void liteOntoImportClassHierarchy() throws Exception{
        GraphDatabaseService db = new TestGraphDatabaseFactory().newImpermanentDatabase();
        ((GraphDatabaseAPI)db).getDependencyResolver().resolveDependency(Procedures.class).registerProcedure(LiteOntologyImporter.class);

        Result importResult = db.execute("CALL semantics.liteOntoImport('" +
                LiteOntologyImporterTest.class.getClassLoader().getResource("class-hierarchy-test.rdf").toURI() +
                "','RDF/XML')");

        assertEquals(new Long(1), db.execute("MATCH p=(:Class{name:'Code'})-[:SCO]->(:Class{name:'Intangible'})" +
                " RETURN count(p) AS count").next().get("count"));
    }

}
