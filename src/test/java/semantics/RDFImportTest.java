package semantics;

import org.junit.Test;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.Result;
import org.neo4j.kernel.impl.proc.Procedures;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.test.TestGraphDatabaseFactory;

import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.neo4j.server.rest.domain.TraverserReturnType.node;

/**
 * Created by jbarrasa on 21/03/2016.
 */
public class RDFImportTest {

    String jsonLdFragment = "{\n" +
            "  \"@context\": {\n" +
            "    \"name\": \"http://xmlns.com/foaf/0.1/name\",\n" +
            "    \"knows\": \"http://xmlns.com/foaf/0.1/knows\",\n" +
            "\t\"modified\": \"http://xmlns.com/foaf/0.1/modified\"\n" +
            "  },\n" +
            "  \"@id\": \"http://me.markus-lanthaler.com/\",\n" +
            "  \"name\": \"Markus Lanthaler\",\n" +
            "  \"knows\": [\n" +
            "    {\n" +
            "      \"@id\": \"http://manu.sporny.org/about#manu\",\n" +
            "      \"name\": \"Manu Sporny\"\n" +
            "    },\n" +
            "    {\n" +
            "      \"name\": \"Dave Longley\",\n" +
            "\t  \"modified\":\n" +
            "\t    {\n" +
            "\t      \"@value\": \"2010-05-29T14:17:39+02:00\",\n" +
            "\t      \"@type\": \"http://www.w3.org/2001/XMLSchema#dateTime\"\n" +
            "\t    }\n" +
            "    }\n" +
            "  ]\n" +
            "}";

    String turtleFragment = "@prefix show: <http://example.org/vocab/show/> .\n" +
            "\n" +
            "show:218 show:localName \"That Seventies Show\"@en .                 # literal with a language tag\n" +
            "show:218 show:localName \"Cette Série des Années Soixante-dix\"@fr . \n" +
            "show:218 show:localName \"Cette Série des Années Septante\"@fr-be .  # literal with a region subtag";

    @Test
    public void testAbortIfNoIndices() throws Exception {
        GraphDatabaseService db = new TestGraphDatabaseFactory().newImpermanentDatabase();
        ((GraphDatabaseAPI)db).getDependencyResolver().resolveDependency(Procedures.class).registerProcedure(RDFImport.class);

        Result importResults1 = db.execute("CALL semantics.importRDF('" +
                RDFImportTest.class.getClassLoader().getResource("mini-ld.json").toURI() + "','JSON-LD',{ shortenUrls: false, typesToLabels: true, commitSize: 500})");

        Map<String, Object> singleResult = importResults1.next();

        assertEquals(0L, singleResult.get("triplesLoaded"));
        assertEquals("KO", singleResult.get("terminationStatus"));
        assertEquals("The required index on :Resource(uri) could not be found", singleResult.get("extraInfo"));

    }

    @Test
    public void testImportJSONLD() throws Exception {
        GraphDatabaseService db = new TestGraphDatabaseFactory().newImpermanentDatabase();
        ((GraphDatabaseAPI)db).getDependencyResolver().resolveDependency(Procedures.class).registerProcedure(RDFImport.class);

        createIndices(db);

        Result importResults1 = db.execute("CALL semantics.importRDF('" +
                RDFImportTest.class.getClassLoader().getResource("mini-ld.json").toURI() + "','JSON-LD',{ shortenUrls: false, typesToLabels: true, commitSize: 500, " +
                "headerParams : { authorization: 'Basic bla bla bla', accept: 'rdf/xml' } })");
        assertEquals(6L, importResults1.next().get("triplesLoaded"));
        assertEquals("http://me.markus-lanthaler.com/",
                db.execute("MATCH (n{`http://xmlns.com/foaf/0.1/name` : 'Markus Lanthaler'}) RETURN n.uri AS uri")
                        .next().get("uri"));
        assertEquals(1L,
                db.execute("MATCH (n) WHERE exists(n.`http://xmlns.com/foaf/0.1/modified`) RETURN count(n) AS count")
                        .next().get("count"));
    }

    @Test
    public void testImportJSONLDShortening() throws Exception {
        GraphDatabaseService db = new TestGraphDatabaseFactory().newImpermanentDatabase();
        ((GraphDatabaseAPI)db).getDependencyResolver().resolveDependency(Procedures.class).registerProcedure(RDFImport.class);

        createIndices(db);

        Result importResults1 = db.execute("CALL semantics.importRDF('" +
                RDFImportTest.class.getClassLoader().getResource("mini-ld.json").toURI() + "','JSON-LD',{ shortenUrls: true, typesToLabels: true, commitSize: 500})");
        assertEquals(6L, importResults1.next().get("triplesLoaded"));
        assertEquals("http://me.markus-lanthaler.com/",
                db.execute("MATCH (n{ns0_name : 'Markus Lanthaler'}) RETURN n.uri AS uri")
                        .next().get("uri"));
        assertEquals(1L,
                db.execute("MATCH (n) WHERE exists(n.ns0_modified) RETURN count(n) AS count")
                        .next().get("count"));

        HashMap<String, String> expectedNamespaceDefs = new HashMap<>();
        expectedNamespaceDefs.put("baseName", "http://xmlns.com/foaf/0.1/");
        expectedNamespaceDefs.put("prefix", "ns0");
        assertEquals(expectedNamespaceDefs,
                db.execute("MATCH (n:NamespacePrefixDefinition) RETURN { baseName: keys(n)[0] ,  prefix: n[keys(n)[0]]} AS namespaces")
                        .next().get("namespaces"));
    }

    @Test
    public void testImportRDFXML() throws Exception {
        GraphDatabaseService db = new TestGraphDatabaseFactory().newImpermanentDatabase();
        ((GraphDatabaseAPI)db).getDependencyResolver().resolveDependency(Procedures.class).registerProcedure(RDFImport.class);

        createIndices(db);

        Result importResults1 = db.execute("CALL semantics.importRDF('" +
                RDFImportTest.class.getClassLoader().getResource("jeu-de-donnees-des-jeux-de-donnees-open-data-paris.rdf")
                        .toURI() + "','RDF/XML',{ shortenUrls: false, typesToLabels: true, commitSize: 500})");
        assertEquals(38L, importResults1.next().get("triplesLoaded"));
        assertEquals(7L,
                db.execute("MATCH ()-[r:`http://purl.org/dc/terms/relation`]->(b) RETURN count(b) as count")
                        .next().get("count"));
        assertEquals("http://opendata.paris.fr/opendata/jsp/site/Portal.jsp?document_id=109&portlet_id=106",
                db.execute("MATCH (x:Resource) WHERE x.`http://www.w3.org/2000/01/rdf-schema#label` = 'harvest_dataset_url'" +
                "\nRETURN x.`http://www.w3.org/1999/02/22-rdf-syntax-ns#value` AS datasetUrl").next().get("datasetUrl"));

    }

    @Test
    public void testImportRDFXMLShortening() throws Exception {
        GraphDatabaseService db = new TestGraphDatabaseFactory().newImpermanentDatabase();
        ((GraphDatabaseAPI)db).getDependencyResolver().resolveDependency(Procedures.class).registerProcedure(RDFImport.class);

        createIndices(db);

        Result importResults1 = db.execute("CALL semantics.importRDF('" +
                RDFImportTest.class.getClassLoader().getResource("jeu-de-donnees-des-jeux-de-donnees-open-data-paris.rdf")
                        .toURI() + "','RDF/XML',{ shortenUrls: true, typesToLabels: true, commitSize: 500})");
        assertEquals(38L, importResults1.next().get("triplesLoaded"));
            assertEquals(7L,
                db.execute("MATCH ()-[r]->(b) WHERE type(r) CONTAINS 'relation' RETURN count(b) as count")
                        .next().get("count"));

        assertEquals("http://opendata.paris.fr/opendata/jsp/site/Portal.jsp?document_id=109&portlet_id=106",
                db.execute("MATCH (x:Resource) WHERE x.ns2_label = 'harvest_dataset_url'" +
                        "\nRETURN x.ns4_value AS datasetUrl").next().get("datasetUrl"));

        assertEquals("ns0",
                db.execute("MATCH (n:NamespacePrefixDefinition) \n" +
                        "RETURN n.`http://www.w3.org/ns/dcat#` as prefix")
                        .next().get("prefix"));

    }

    @Test
    public void testImportRDFXMLShorteningWithPrefixPreDefinition() throws Exception {
        GraphDatabaseService db = new TestGraphDatabaseFactory().newImpermanentDatabase();
        ((GraphDatabaseAPI)db).getDependencyResolver().resolveDependency(Procedures.class).registerProcedure(RDFImport.class);

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
                        .toURI() + "','RDF/XML', { shortenUrls: true, typesToLabels: true, commitSize: 500})");
        assertEquals(38L, importResults1.next().get("triplesLoaded"));
        assertEquals(7L,
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
        ((GraphDatabaseAPI)db).getDependencyResolver().resolveDependency(Procedures.class).registerProcedure(RDFImport.class);

        createIndices(db);

        //prefix predefinition by creating the NamespacePrefixDefinitionNode
        db.execute("WITH {`http://neo4j.com/voc/`:'voc' } as nslist\n" +
                "MERGE (n:NamespacePrefixDefinition)\n" +
                "SET n+=nslist " +
                "RETURN n ");

        Result importResults1 = db.execute("CALL semantics.importRDF('" +
                RDFImportTest.class.getClassLoader().getResource("oneTriple.rdf")
                        .toURI() + "','RDF/XML',{ shortenUrls: true, typesToLabels: true, commitSize: 500})");
        assertEquals(1L, importResults1.next().get("triplesLoaded"));
        assertEquals("JB",
                db.execute("MATCH (jb {uri: 'http://neo4j.com/invividual/JB'}) RETURN jb.voc_name AS name")
                        .next().get("name"));

        assertEquals("voc",
                db.execute("MATCH (n:NamespacePrefixDefinition) \n" +
                        "RETURN n.`http://neo4j.com/voc/` as prefix")
                        .next().get("prefix"));

    }

    @Test
    public void testImportRDFXMLBadUris() throws Exception {
        GraphDatabaseService db = new TestGraphDatabaseFactory().newImpermanentDatabase();
        ((GraphDatabaseAPI)db).getDependencyResolver().resolveDependency(Procedures.class).registerProcedure(RDFImport.class);

        createIndices(db);

        //prefix predefinition by creating the NamespacePrefixDefinitionNode
        db.execute("WITH {`http://neo4j.com/voc/`:'voc' } as nslist\n" +
                "MERGE (n:NamespacePrefixDefinition)\n" +
                "SET n+=nslist " +
                "RETURN n ");

        Result importResults1 = db.execute("CALL semantics.importRDF('" +
                RDFImportTest.class.getClassLoader().getResource("badUris.rdf")
                        .toURI() + "','RDF/XML',{ shortenUrls: true, typesToLabels: true, commitSize: 500})");
        assertEquals(1L, importResults1.next().get("triplesLoaded"));
        assertEquals("JB",
                db.execute("MATCH (jb {uri: 'http://neo4j.com/invividual/JB\\'sUri'}) RETURN jb.voc_name AS name")
                        .next().get("name"));
    }

    @Test
    public void testImportLangFilter() throws Exception {
        GraphDatabaseService db = new TestGraphDatabaseFactory().newImpermanentDatabase();
        ((GraphDatabaseAPI)db).getDependencyResolver().resolveDependency(Procedures.class).registerProcedure(RDFImport.class);

        createIndices(db);

        //prefix predefinition by creating the NamespacePrefixDefinitionNode
        db.execute("WITH {`http://example.org/vocab/show/`:'voc' } as nslist\n" +
                "MERGE (n:NamespacePrefixDefinition)\n" +
                "SET n+=nslist " +
                "RETURN n ");

        Result importResults1 = db.execute("CALL semantics.importRDF('" +
                RDFImportTest.class.getClassLoader().getResource("multilang.ttl")
                        .toURI() + "','Turtle',{ shortenUrls: true, typesToLabels: true, languageFilter: 'en', commitSize: 500})");
        assertEquals(1L, importResults1.next().get("triplesLoaded"));
        assertEquals("That Seventies Show",
                db.execute("MATCH (t {uri: 'http://example.org/vocab/show/218'}) RETURN t.voc_localName AS name")
                        .next().get("name"));

        db.execute("MATCH (t {uri: 'http://example.org/vocab/show/218'}) DETACH DELETE t ");

        importResults1 = db.execute("CALL semantics.importRDF('" +
                RDFImportTest.class.getClassLoader().getResource("multilang.ttl")
                        .toURI() + "','Turtle',{ shortenUrls: true, typesToLabels: true, languageFilter: 'fr', commitSize: 500})");
        assertEquals(1L, importResults1.next().get("triplesLoaded"));
        assertEquals("Cette Série des Années Soixante-dix",
                db.execute("MATCH (t {uri: 'http://example.org/vocab/show/218'}) RETURN t.voc_localName AS name")
                        .next().get("name"));

        db.execute("MATCH (t {uri: 'http://example.org/vocab/show/218'}) DETACH DELETE t ");

        importResults1 = db.execute("CALL semantics.importRDF('" +
                RDFImportTest.class.getClassLoader().getResource("multilang.ttl")
                        .toURI() + "','Turtle',{ shortenUrls: true, typesToLabels: true, languageFilter: 'fr-be', commitSize: 500})");
        assertEquals(1L, importResults1.next().get("triplesLoaded"));
        assertEquals("Cette Série des Années Septante",
                db.execute("MATCH (t {uri: 'http://example.org/vocab/show/218'}) RETURN t.voc_localName AS name")
                        .next().get("name"));

        db.execute("MATCH (t {uri: 'http://example.org/vocab/show/218'}) DETACH DELETE t ");

        importResults1 = db.execute("CALL semantics.importRDF('" +
                RDFImportTest.class.getClassLoader().getResource("multilang.ttl")
                        .toURI() + "','Turtle',{ shortenUrls: true, typesToLabels: true, commitSize: 500})");
        // no language filter means three triples are ingested
        assertEquals(3L, importResults1.next().get("triplesLoaded"));
        //but actually only the last one is kept as they overwrite each other
        //TODO: Find a consistent solution for this problem
        assertEquals("Cette Série des Années Septante",
                db.execute("MATCH (t {uri: 'http://example.org/vocab/show/218'}) RETURN t.voc_localName AS name")
                        .next().get("name"));

    }

    @Test
    public void testImportTurtle() throws Exception {
        GraphDatabaseService db = new TestGraphDatabaseFactory().newImpermanentDatabase();
        ((GraphDatabaseAPI)db).getDependencyResolver().resolveDependency(Procedures.class).registerProcedure(RDFImport.class);

        createIndices(db);

        Result importResults1 = db.execute("CALL semantics.importRDF('" +
                RDFImportTest.class.getClassLoader().getResource("opentox-example.ttl")
                        .toURI() + "','Turtle',{ shortenUrls: false, typesToLabels: true, commitSize: 500})");
        assertEquals(157L, importResults1.next().get("triplesLoaded"));
        Result algoNames = db.execute("MATCH (n:`http://www.opentox.org/api/1.1#Algorithm`) " +
                "\nRETURN n.`http://purl.org/dc/elements/1.1/title` AS algos ORDER By algos");

        assertEquals("J48", algoNames.next().get("algos"));
        assertEquals("XLogP", algoNames.next().get("algos"));

        Result compounds = db.execute("MATCH ()-[r:`http://www.opentox.org/api/1.1#compound`]->(c) RETURN DISTINCT c.uri AS compound order by compound");
        assertEquals("http://www.opentox.org/example/1.1#benzene", compounds.next().get("compound"));
        assertEquals("http://www.opentox.org/example/1.1#phenol", compounds.next().get("compound"));

    }
    
    /**
     * Can we populate the cache correctly when we have a miss?
     */
    @Test
    public void testImportTurtle02() throws Exception {
        GraphDatabaseService db = new TestGraphDatabaseFactory().newImpermanentDatabase();
        ((GraphDatabaseAPI)db).getDependencyResolver().resolveDependency(Procedures.class).registerProcedure(RDFImport.class);

        createIndices(db);
        db.execute("CREATE (rdf:NamespacePrefixDefinition {" +
        		"  `http://www.example.com/ontology/1.0.0#`: 'ex'," +
        		"  `http://www.w3.org/1999/02/22-rdf-syntax-ns#`: 'rdfs'})");
    	Result importResults = db.execute(String.format(
    			"CALL semantics.importRDF('%s','Turtle',{nodeCacheSize: 1})", file("myrdf/testImportTurtle02.ttl")));
    	assertEquals(5L, importResults.next().get("triplesLoaded"));
        
        Result result = db.execute(
        		"MATCH (:ex_DISTANCEVALUE)-[:ex_units]->(mu) " +
        		"RETURN mu.uri AS unitsUri, mu.ex_name as unitsName");
        Map<String, Object> first = result.next();
		assertEquals("http://www.example.com/ontology/1.0.0/common#MEASUREMENTUNIT-T1510615421640", first.get("unitsUri"));
		assertEquals("metres", first.get("unitsName"));
    }

    @Test
    public void testPreviewFromSnippet() throws Exception {
        GraphDatabaseService db = new TestGraphDatabaseFactory().newImpermanentDatabase();
        ((GraphDatabaseAPI)db).getDependencyResolver().resolveDependency(Procedures.class).registerProcedure(RDFImport.class);

        Result importResults1 = db.execute("CALL semantics.previewRDFSnippet('" + jsonLdFragment
                + "','JSON-LD',{ shortenUrls: false, typesToLabels: false})");
        Map<String, Object> next = importResults1.next();
        final ArrayList<Node> nodes = (ArrayList<Node>) next.get("nodes");
        assertEquals(3, nodes.size());
        final ArrayList<Relationship> rels = (ArrayList<Relationship>) next.get("relationships");
        assertEquals(2, rels.size());
    }

    @Test
    public void testPreviewFromSnippetLangFilter() throws Exception {
        GraphDatabaseService db = new TestGraphDatabaseFactory().newImpermanentDatabase();
        ((GraphDatabaseAPI)db).getDependencyResolver().resolveDependency(Procedures.class).registerProcedure(RDFImport.class);

        Result importResults1 = db.execute("CALL semantics.previewRDFSnippet('" + turtleFragment
                + "','Turtle',{ shortenUrls: false, typesToLabels: false, languageFilter: 'fr'})");
        Map<String, Object> next = importResults1.next();
        final ArrayList<Node> nodes = (ArrayList<Node>) next.get("nodes");
        assertEquals(1, nodes.size());
        assertEquals("Cette Série des Années Soixante-dix", nodes.get(0).getProperty("http://example.org/vocab/show/localName"));
        assertEquals(0, ((ArrayList<Relationship>) next.get("relationships")).size());


        importResults1 = db.execute("CALL semantics.previewRDFSnippet('" + turtleFragment
                + "','Turtle',{ shortenUrls: false, typesToLabels: false, languageFilter: 'en'})");
        assertEquals("That Seventies Show", ((ArrayList<Node>)importResults1.next().get("nodes")).get(0).getProperty("http://example.org/vocab/show/localName"));

    }

    @Test
    public void testPreviewFromFile() throws Exception {
        GraphDatabaseService db = new TestGraphDatabaseFactory().newImpermanentDatabase();
        ((GraphDatabaseAPI)db).getDependencyResolver().resolveDependency(Procedures.class).registerProcedure(RDFImport.class);

        Result importResults1 = db.execute("CALL semantics.previewRDF('" +
                RDFImportTest.class.getClassLoader().getResource("jeu-de-donnees-des-jeux-de-donnees-open-data-paris.rdf")
                        .toURI() + "','RDF/XML',{ shortenUrls: false, typesToLabels: false})");
        Map<String, Object> next = importResults1.next();
        final ArrayList<Node> nodes = (ArrayList<Node>) next.get("nodes");
        assertEquals(15, nodes.size());
        final ArrayList<Relationship> rels = (ArrayList<Relationship>) next.get("relationships");
        assertEquals(15, rels.size());
    }

    @Test
    public void testPreviewFromFileLangFilter() throws Exception {
        GraphDatabaseService db = new TestGraphDatabaseFactory().newImpermanentDatabase();
        ((GraphDatabaseAPI)db).getDependencyResolver().resolveDependency(Procedures.class).registerProcedure(RDFImport.class);

        Result importResults1 = db.execute("CALL semantics.previewRDF('" +
                RDFImportTest.class.getClassLoader().getResource("multilang.ttl")
                        .toURI() + "','Turtle',{ shortenUrls: false, typesToLabels: false, languageFilter: 'fr'})");
        Map<String, Object> next = importResults1.next();
        final ArrayList<Node> nodes = (ArrayList<Node>) next.get("nodes");
        assertEquals(1, nodes.size());
        assertEquals("Cette Série des Années Soixante-dix", nodes.get(0).getProperty("http://example.org/vocab/show/localName"));
        assertEquals(0, ((ArrayList<Relationship>) next.get("relationships")).size());


        importResults1 = db.execute("CALL semantics.previewRDF('" +
                RDFImportTest.class.getClassLoader().getResource("multilang.ttl").toURI()
                + "','Turtle',{ shortenUrls: false, typesToLabels: false, languageFilter: 'en'})");
        assertEquals("That Seventies Show", ((ArrayList<Node>)importResults1.next().get("nodes")).get(0).getProperty("http://example.org/vocab/show/localName"));
    }

    private void createIndices(GraphDatabaseService db) {
        db.execute("CREATE INDEX ON :Resource(uri)");
    }

    private static URI file(String path) {
    	try {
			return RDFImportTest.class.getClassLoader().getResource(path).toURI();
		} catch (URISyntaxException e) {
			String msg = String.format("Failed to load the resource with path '%s'", path);
			throw new RuntimeException(msg, e);
		}
    }
}
