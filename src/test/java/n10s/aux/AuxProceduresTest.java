package n10s.aux;

import org.eclipse.rdf4j.model.vocabulary.XMLSchema;
import org.junit.Rule;
import org.junit.Test;
import org.neo4j.driver.*;
import org.neo4j.driver.Record;
import org.neo4j.harness.junit.rule.Neo4jRule;
import static n10s.graphconfig.Params.WKTLITERAL_URI;
import static org.junit.Assert.*;

public class AuxProceduresTest {
    @Rule
    public Neo4jRule neo4j = new Neo4jRule().withFunction(AuxProcedures.class);

    @Test
    public void testAddNamespacePrefixInitial() throws Exception {
        try (Driver driver = GraphDatabase.driver(neo4j.boltURI(),
                Config.builder().withoutEncryption().build()); Session session = driver.session()) {

            Result res = session.run("RETURN n10s.aux.dt.check('" + XMLSchema.DATETIME.stringValue() + "',datetime()) as yes," +
                    "n10s.aux.dt.check('"+ WKTLITERAL_URI.stringValue() +"',datetime()) as no1," +
                    "n10s.aux.dt.check('" + XMLSchema.DATE.stringValue() + "',datetime()) as no2," +
                    "n10s.aux.dt.check('" + XMLSchema.TIME.stringValue() +"',datetime()) as no3");
            assertTrue(res.hasNext());
            Record record = res.next();
            assertTrue(record.get("yes").asBoolean());
            assertFalse(record.get("no1").asBoolean());
            assertFalse(record.get("no2").asBoolean());
            assertFalse(record.get("no3").asBoolean());

            res = session.run("RETURN n10s.aux.dt.check('" + WKTLITERAL_URI.stringValue() + "',point({x: -0.1275, y: 51.507222222})) as yes," +
                    "n10s.aux.dt.check('" + XMLSchema.DATETIME.stringValue() + "',point({x: -0.1275, y: 51.507222222})) as no1," +
                    "n10s.aux.dt.check('" + XMLSchema.DATE.stringValue() + "',point({x: -0.1275, y: 51.507222222})) as no2," +
                    "n10s.aux.dt.check('" + XMLSchema.TIME.stringValue() + "',point({x: -0.1275, y: 51.507222222})) as no3");
            assertTrue(res.hasNext());
            record = res.next();
            assertTrue(record.get("yes").asBoolean());
            assertFalse(record.get("no1").asBoolean());
            assertFalse(record.get("no2").asBoolean());
            assertFalse(record.get("no3").asBoolean());

            res = session.run("RETURN n10s.aux.dt.check('" + WKTLITERAL_URI.stringValue() + "',45) as no0," +
                    "n10s.aux.dt.check('" + XMLSchema.DATETIME.stringValue() + "',point({x: -0.1275, y: 51.507222222})) as no1," +
                    "n10s.aux.dt.check('" + XMLSchema.DATE.stringValue() + "',point({x: -0.1275, y: 51.507222222})) as no2," +
                    "n10s.aux.dt.check('" + XMLSchema.TIME.stringValue() + "',point({x: -0.1275, y: 51.507222222})) as no3");
            assertTrue(res.hasNext());
            record = res.next();
            assertFalse(record.get("no0").asBoolean());
            assertFalse(record.get("no1").asBoolean());
            assertFalse(record.get("no2").asBoolean());
            assertFalse(record.get("no3").asBoolean());

            res = session.run("RETURN n10s.aux.dt.check('" + WKTLITERAL_URI.stringValue() + "',true) as no0," +
                    "n10s.aux.dt.check('" + XMLSchema.DATETIME.stringValue() + "',point({x: -0.1275, y: 51.507222222})) as no1," +
                    "n10s.aux.dt.check('" + XMLSchema.DATE.stringValue() + "',point({x: -0.1275, y: 51.507222222})) as no2," +
                    "n10s.aux.dt.check('" + XMLSchema.TIME.stringValue() + "',point({x: -0.1275, y: 51.507222222})) as no3");
            assertTrue(res.hasNext());
            record = res.next();
            assertFalse(record.get("no0").asBoolean());
            assertFalse(record.get("no1").asBoolean());
            assertFalse(record.get("no2").asBoolean());
            assertFalse(record.get("no3").asBoolean());

            res = session.run("RETURN n10s.aux.dt.check('" + WKTLITERAL_URI.stringValue() + "',44.56) as no0," +
                    "n10s.aux.dt.check('" + XMLSchema.DATETIME.stringValue() + "',point({x: -0.1275, y: 51.507222222})) as no1," +
                    "n10s.aux.dt.check('" + XMLSchema.DATE.stringValue() + "',point({x: -0.1275, y: 51.507222222})) as no2," +
                    "n10s.aux.dt.check('" + XMLSchema.TIME.stringValue() + "',point({x: -0.1275, y: 51.507222222})) as no3");
            assertTrue(res.hasNext());
            record = res.next();
            assertFalse(record.get("no0").asBoolean());
            assertFalse(record.get("no1").asBoolean());
            assertFalse(record.get("no2").asBoolean());
            assertFalse(record.get("no3").asBoolean());

            res = session.run("RETURN n10s.aux.dt.check('" + XMLSchema.ANYURI.stringValue() + "',44.56) as no0," +
                    "n10s.aux.dt.check('" + XMLSchema.ANYURI.stringValue() + "',point({x: -0.1275, y: 51.507222222})) as no1," +
                    "n10s.aux.dt.check('" + XMLSchema.ANYURI.stringValue() + "','this is a string') as no2," +
                    "n10s.aux.dt.check('" + XMLSchema.ANYURI.stringValue() + "','neo4j://home.voc/123#something') as yes");
            assertTrue(res.hasNext());
            record = res.next();
            assertFalse(record.get("no0").asBoolean());
            assertFalse(record.get("no1").asBoolean());
            assertFalse(record.get("no2").asBoolean());
            assertTrue(record.get("yes").asBoolean());

            session.run("CREATE (:Thing { dt: date(), dtm: datetime(), zdt: datetime('1956-06-25T09:00:00[Europe/Berlin]')," +
                    "theint: 45, thebo: false, flo: 4.567, po: point({x: -0.1275, y: 51.507222222, z: 44 }), " +
                    "uri: 'mail://somet hing.io#123'})");

            assertEquals(1L,session.run("MATCH (t:Thing) RETURN count(t) as thingcount").next().get("thingcount").asInt());

            res = session.run("MATCH (t:Thing) RETURN n10s.aux.dt.check('" + WKTLITERAL_URI.stringValue() + "',t.zdt) as no1," +
                    "n10s.aux.dt.check('" + XMLSchema.DATETIME.stringValue() + "',t.zdt) as yes," +
                    "n10s.aux.dt.check('" + XMLSchema.DATE.stringValue() + "',t.zdt) as no2," +
                    "n10s.aux.dt.check('" + XMLSchema.TIME.stringValue() + "',t.zdt) as no3," +
                    "n10s.aux.dt.check('" + XMLSchema.ANYURI.stringValue() + "',t.uri) as no4");
            assertTrue(res.hasNext());
            record = res.next();
            assertTrue(record.get("yes").asBoolean());
            assertFalse(record.get("no1").asBoolean());
            assertFalse(record.get("no2").asBoolean());
            assertFalse(record.get("no3").asBoolean());
            assertFalse(record.get("no4").asBoolean());


        }
    }
}
