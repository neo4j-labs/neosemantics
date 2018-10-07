package semantics.mapping;

import org.junit.Rule;
import org.junit.Test;
import org.neo4j.driver.v1.Config;
import org.neo4j.driver.v1.Driver;
import org.neo4j.driver.v1.GraphDatabase;
import org.neo4j.driver.v1.Session;
import org.neo4j.driver.v1.types.Node;
import org.neo4j.harness.junit.Neo4jRule;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class MappingUtilsTest {
    @Rule
    public Neo4jRule neo4j = new Neo4jRule()
            .withProcedure( MappingUtils.class ).withFunction(MappingUtils.class);


    @Test
    public void testaddSchema() throws Exception {
        try (Driver driver = GraphDatabase.driver(neo4j.boltURI(), Config.build().withEncryptionLevel(Config.EncryptionLevel.NONE).toConfig())) {

            Session session = driver.session();

            //database is empty
            assertFalse(session.run("MATCH (n) RETURN n").hasNext());
            String vocUri = "http://vocabularies.com/vocabulary1/";
            String vocPrefix = "v1";
            Map<String,Object> params = new HashMap<>();
            params.put("ns",vocUri);
            params.put("prefix", vocPrefix);

            //add schema
            String addSchemaCypher = "CALL semantics.mapping.addSchema($ns,$prefix)";
            Node mappingNs = session.run(addSchemaCypher, params).next().get("node").asNode();
            assertEquals(vocPrefix,mappingNs.get("_prefix").asString());
            assertEquals(vocUri,mappingNs.get("_ns").asString());

            //schema has been persisted
            String getSchemaByName = "MATCH (mns:_MapNs { _ns: $ns, _prefix : $prefix }) RETURN count(mns) as ct ";
            assertEquals(1, session.run(getSchemaByName,params).next().get("ct").asInt());

            //add same schema namespace with different prefix
            String alternativeVocPrefix = "v1alt";
            params.put("prefix", alternativeVocPrefix);
            assertFalse(session.run(addSchemaCypher, params).hasNext());

            //schema has not been changed in db
            assertEquals(0, session.run(getSchemaByName,params).next().get("ct").asInt());
            //old one still there
            params.put("prefix", vocPrefix);
            assertEquals(1, session.run(getSchemaByName,params).next().get("ct").asInt());

            //add an alternative schema namespace with a prefix already in use (default, no force overwrite)
            String alternativeVocUri = "http://vocabularies.com/vocabulary1alt/";
            params.put("ns", alternativeVocUri);
            assertFalse(session.run(addSchemaCypher, params).hasNext());

            //schema has not been changed in db
            assertEquals(0, session.run(getSchemaByName,params).next().get("ct").asInt());
            //old one still there
            params.put("ns", vocUri);
            assertEquals(1, session.run(getSchemaByName,params).next().get("ct").asInt());

        }
    }

    @Test
    public void testAddCommonSchemas() throws Exception {
        try (Driver driver = GraphDatabase.driver(neo4j.boltURI(), Config.build().withEncryptionLevel(Config.EncryptionLevel.NONE).toConfig())) {

            Session session = driver.session();

            //when DB is empty
            assertFalse(session.run("MATCH (n) RETURN n").hasNext());
            assertEquals(18,session.run("CALL semantics.mapping.addCommonSchemas() YIELD node RETURN count(node) AS addedCount ").next().get("addedCount").asInt());
            //Check that schema.org is there
            Map<String,Object> params = new HashMap<>();
            params.put("ns","http://schema.org/");
            params.put("prefix", "sch");
            assertEquals(1, session.run("MATCH (mns:_MapNs { _ns: $ns, _prefix : $prefix }) RETURN count(mns) as ct ",params).next().get("ct").asInt());

            //if we run a second time, all schemas are in the DB so no changes
            assertEquals(0,session.run("CALL semantics.mapping.addCommonSchemas() YIELD node RETURN count(node) AS addedCount ").next().get("addedCount").asInt());

            //empty DB again
            session.run("MATCH (n) DETACH DELETE n");

            //and add a custom definition (prefix) for schema.org
            params.put("prefix", "myprefixforschemadotorg");
            String addSchemaCypher = "CALL semantics.mapping.addSchema($ns,$prefix)";
            session.run(addSchemaCypher, params);
            //only those not in use should be added
            assertEquals(17,session.run("CALL semantics.mapping.addCommonSchemas() YIELD node RETURN count(node) AS addedCount ").next().get("addedCount").asInt());
            //and the original custom definition for schema.org should still be in the DB
            assertEquals(1, session.run("MATCH (mns:_MapNs { _ns: $ns, _prefix : $prefix }) RETURN count(mns) as ct ",params).next().get("ct").asInt());
            //and the standard definition from the addcommons should not be there (it was ignored)
            params.put("prefix", "sch");
            assertEquals(0, session.run("MATCH (mns:_MapNs { _ns: $ns, _prefix : $prefix }) RETURN count(mns) as ct ",params).next().get("ct").asInt());
        }
    }

    @Test
    public void testListSchemas() throws Exception {
        try (Driver driver = GraphDatabase.driver(neo4j.boltURI(), Config.build().withEncryptionLevel(Config.EncryptionLevel.NONE).toConfig())) {

            Session session = driver.session();

            //when DB is empty
            assertFalse(session.run("MATCH (n) RETURN n").hasNext());
            assertFalse(session.run("CALL semantics.mapping.listSchemas()").hasNext());
            //add common schemas
            session.run("CALL semantics.mapping.addCommonSchemas()");
            Map<String,Object> params = new HashMap<>();
            params.put("searchString","");
            String getSchemaMatchesQuery = "CALL semantics.mapping.listSchemas($searchString) YIELD node RETURN COUNT(node) AS schemaCount";
            assertEquals(18, session.run(getSchemaMatchesQuery, params).next().get("schemaCount").asInt());
            params.put("searchString","fibo");
            assertEquals(11, session.run(getSchemaMatchesQuery, params).next().get("schemaCount").asInt());
            params.put("searchString","skos");
            assertEquals(1, session.run(getSchemaMatchesQuery, params).next().get("schemaCount").asInt());

        }
    }

    @Test
    public void testAddMappingToSchema() throws Exception {
        try (Driver driver = GraphDatabase.driver(neo4j.boltURI(), Config.build().withEncryptionLevel(Config.EncryptionLevel.NONE).toConfig())) {

            Session session = driver.session();

            // when DB is empty
            assertFalse(session.run("MATCH (n) RETURN n").hasNext());

            String key = "RELATIONSHIP_NAME";
            String localNameInVoc = "relationshipInSchema1";

            Map<String,Object> params = new HashMap<>();
            params.put("ns","http://schemas.com/schema1/");
            params.put("prefix", "sch1");
            params.put("graphElemName", key);
            params.put("localVocElem", localNameInVoc);
            String addMappingAndSchemaCypher = " CALL semantics.mapping.addSchema($ns,$prefix) YIELD node AS sch WITH sch " +
                                              " CALL semantics.mapping.addMappingToSchema(sch,$graphElemName, $localVocElem) YIELD node RETURN node";
            Node mapping = session.run(addMappingAndSchemaCypher, params).next().get("node").asNode();
            assertEquals(key,mapping.get("_key").asString());
            assertEquals(localNameInVoc,mapping.get("_local").asString());
            // check mapping is linked to the schema
            String existMappingAndNs = "MATCH (mns:_MapNs { _ns: $ns } )<-[:_IN]-" +
                    "(elem:_MapDef { _key : $graphElemName, _local: $localVocElem }) RETURN mns, elem ";
            assertTrue(session.run(existMappingAndNs, params).hasNext());

            //overwriting a mapping
            String alternativeLocalNameInVoc = "differentRelationshipInSchema1";
            params.put("localVocElem", alternativeLocalNameInVoc);
            String addMappingToExistingSchemaCypher = " CALL semantics.mapping.listSchemas($ns) YIELD node AS sch WITH sch " +
                    " CALL semantics.mapping.addMappingToSchema(sch,$graphElemName, $localVocElem) YIELD node RETURN node";
            mapping = session.run(addMappingToExistingSchemaCypher, params).next().get("node").asNode();
            assertEquals(key,mapping.get("_key").asString());
            assertEquals(alternativeLocalNameInVoc,mapping.get("_local").asString());
            // check mapping is linked to the schema
            assertTrue(session.run(existMappingAndNs, params).hasNext());
            params.put("localVocElem", localNameInVoc);
            assertFalse(session.run(existMappingAndNs, params).hasNext());
        }
    }

    @Test
    public void testListMappings() throws Exception {
        try (Driver driver = GraphDatabase.driver(neo4j.boltURI(), Config.build().withEncryptionLevel(Config.EncryptionLevel.NONE).toConfig())) {

            Session session = driver.session();

            // when DB is empty
            assertFalse(session.run("MATCH (n) RETURN n").hasNext());

            String addMappingAndSchemaCypher = " call semantics.mapping.addSchema(\"http://schema.org/\",\"sch\") yield node as sch\n" +
                    "call semantics.mapping.addMappingToSchema(sch,\"Movie\",\"Movie\") yield node as mapping1\n" +
                    "call semantics.mapping.addMappingToSchema(sch,\"Person\",\"Person\") yield node as mapping2\n" +
                    "call semantics.mapping.addMappingToSchema(sch,\"name\",\"name\") yield node as mapping3\n" +
                    "return *";
            assertTrue(session.run(addMappingAndSchemaCypher).hasNext());
            assertEquals(3, session.run("CALL semantics.mapping.listMappings() yield elemName RETURN count(elemName) as ct ").next().get("ct").asInt());
            assertEquals(1, session.run("CALL semantics.mapping.listMappings('Person') yield elemName RETURN count(elemName) as ct ").next().get("ct").asInt());
            String updateMappingCypher = " call semantics.mapping.listSchemas('http://schema.org/') yield node as sch\n" +
                    "call semantics.mapping.addMappingToSchema(sch,\"Movie\",\"MovieInSchemaDotOrg\") yield node as mapping\n" +
                    " return * ";
            assertTrue(session.run(updateMappingCypher).hasNext());
            assertEquals(3, session.run("CALL semantics.mapping.listMappings() yield elemName RETURN count(elemName) as ct ").next().get("ct").asInt());
        }
    }

    @Test
    public void testDropMappings() throws Exception {
        try (Driver driver = GraphDatabase.driver(neo4j.boltURI(), Config.build().withEncryptionLevel(Config.EncryptionLevel.NONE).toConfig())) {

            Session session = driver.session();

            // when DB is empty
            assertFalse(session.run("MATCH (n) RETURN n").hasNext());

            String addMappingAndSchemaCypher = " call semantics.mapping.addSchema(\"http://schema.org/\",\"sch\") yield node as sch\n" +
                    "call semantics.mapping.addMappingToSchema(sch,\"Movie\",\"Movie\") yield node as mapping1\n" +
                    "call semantics.mapping.addMappingToSchema(sch,\"Person\",\"Person\") yield node as mapping2\n" +
                    "call semantics.mapping.addMappingToSchema(sch,\"name\",\"name\") yield node as mapping3\n" +
                    "return *";
            assertTrue(session.run(addMappingAndSchemaCypher).hasNext());
            assertEquals("successfully deleted mapping", session.run("CALL semantics.mapping.dropMapping('Movie')").next().get("output").asString());
            assertEquals(2, session.run("CALL semantics.mapping.listMappings() yield elemName RETURN count(elemName) as ct ").next().get("ct").asInt());

        }
    }

    @Test
    public void testDropSchema() throws Exception {
        try (Driver driver = GraphDatabase.driver(neo4j.boltURI(), Config.build().withEncryptionLevel(Config.EncryptionLevel.NONE).toConfig())) {

            Session session = driver.session();

            // when DB is empty
            assertFalse(session.run("MATCH (n) RETURN n").hasNext());

            String addMappingAndSchemaCypher = " call semantics.mapping.addSchema(\"http://schema.org/\",\"sch\") yield node as sch\n" +
                    "call semantics.mapping.addMappingToSchema(sch,\"Movie\",\"Movie\") yield node as mapping1\n" +
                    "call semantics.mapping.addMappingToSchema(sch,\"Person\",\"Person\") yield node as mapping2\n" +
                    "call semantics.mapping.addMappingToSchema(sch,\"name\",\"name\") yield node as mapping3\n" +
                    "return *";
            assertTrue(session.run(addMappingAndSchemaCypher).hasNext());
            assertEquals("schema not found", session.run("CALL semantics.mapping.dropSchema('doesnotexist')").next().get("output").asString());
            assertEquals(3, session.run("CALL semantics.mapping.listMappings() yield elemName RETURN count(elemName) as ct ").next().get("ct").asInt());
            assertEquals("successfully deleted schema with 3 mappings", session.run("CALL semantics.mapping.dropSchema('http://schema.org/')").next().get("output").asString());
            assertEquals(0, session.run("CALL semantics.mapping.listMappings() yield elemName RETURN count(elemName) as ct ").next().get("ct").asInt());
            assertEquals(0, session.run("CALL semantics.mapping.listSchemas() yield node RETURN count(node) as ct ").next().get("ct").asInt());

        }
    }
}
