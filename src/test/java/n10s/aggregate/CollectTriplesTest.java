package n10s.aggregate;

import n10s.ModelTestUtils;
import n10s.rdf.aggregate.CollectTriples;
import n10s.rdf.load.RDFLoadProcedures;
import n10s.rdf.stream.RDFStreamProcedures;
import org.eclipse.rdf4j.rio.RDFFormat;
import org.junit.Rule;
import org.junit.Test;
import org.neo4j.driver.*;
import org.neo4j.harness.junit.rule.Neo4jRule;

import java.nio.file.Files;
import java.nio.file.Paths;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class CollectTriplesTest {

    private static final String TURTLE_FRAGMENT = "@prefix ex: <http://example.org/data#> .\n" +
            "<http://example.org/web-data> ex:title \"Web Data\" ;\n" +
            "                               ex:professor [ ex:fullName \"Alice Carol\" ;\n" +
            "                                              ex:homePage <http://example.net/alice-carol> ] .";

    private static final String TURTLE_FRAGMENT_2 = "@prefix ns0: <http://permid.org/ontology/common/> .\n" +
            "@prefix skos: <http://www.w3.org/2004/02/skos/core#> .\n" +
            "\n" +
            "<https://permid.org/1-10018213>\n" +
            "  ns0:hasPermId \"10018213\" ;\n" +
            "  skos:prefLabel \"Physics, Business Administration and Finance\" ;\n" +
            "  a <http://permid.org/ontology/person/Major> .\n" +
            "\n" +
            "<https://permid.org/1-10017111>\n" +
            "  ns0:hasPermId \"10017111\" ;\n" +
            "  skos:prefLabel \"Economics, Business Administration and Finance\" ;\n" +
            "  a <http://permid.org/ontology/person/Major> .\n" +
            "\n" +
            "<https://permid.org/1-10012482>\n" +
            "  ns0:hasPermId \"10012482\" ;\n" +
            "  skos:prefLabel \"Business Administration and Finance\" ;\n" +
            "  a <http://permid.org/ontology/person/Major> .\n" +
            "\n" +
            "<https://permid.org/1-10012019>\n" +
            "  ns0:hasPermId \"10012019\" ;\n" +
            "  skos:prefLabel \"Accounting, Business Administration and Finance\" ;\n" +
            "  a <http://permid.org/ontology/person/Major> .\n" +
            "\n" +
            "<https://permid.org/1-10012483>\n" +
            "  ns0:hasPermId \"10012483\" ;\n" +
            "  skos:prefLabel \"Business Administration and Finance and Marketing\" ;\n" +
            "  a <http://permid.org/ontology/person/Major> .\n" +
            "\n" +
            "<https://permid.org/1-10012484>\n" +
            "  ns0:hasPermId \"10012484\" ;\n" +
            "  skos:prefLabel \"Business Administration and Finance Theory\" ;\n" +
            "  a <http://permid.org/ontology/person/Major> .";

    @Rule
    public Neo4jRule neo4j = new Neo4jRule()
            .withProcedure(RDFStreamProcedures.class).withProcedure(RDFLoadProcedures.class)
            .withAggregationFunction(CollectTriples.class);

    @Test
    public void testCollectTriplesBasic() throws Exception {
        try (Driver driver = GraphDatabase.driver(neo4j.boltURI(),
                Config.builder().withoutEncryption().build())) {

            Session session = driver.session();
            Result results = session.run("CALL n10s.rdf.stream.inline('"+ TURTLE_FRAGMENT +"', 'Turtle') " +
                    " yield subject, predicate, object, isLiteral, literalType, literalLang "
                    + " return  n10s.rdf.collect(subject, predicate, object, isLiteral, literalType, literalLang) as rdf");
            assertEquals(true, results.hasNext());
            assertTrue(ModelTestUtils
                    .compareModels(results.next().get("rdf").asString(),
                            RDFFormat.NTRIPLES,TURTLE_FRAGMENT,RDFFormat.TURTLE));



        }
    }

    @Test
    public void testCollectTriplesPostFilter() throws Exception {
        try (Driver driver = GraphDatabase.driver(neo4j.boltURI(),
                Config.builder().withoutEncryption().build())) {

            Session session = driver.session();
            Result results = session.run(
                "call n10s.rdf.stream.fetch('" + CollectTriplesTest.class.getClassLoader()
                    .getResource("100k.nt")
                    .toURI() + "',\"N-Triples\" ,{ limit: 999999}) yield subject, predicate, object\n" +
                    "where predicate = \"http://www.w3.org/2004/02/skos/core#prefLabel\" and " +
                    "object contains \"Business Administration and Finance\"\n" +
                    "with collect(subject) as indivList    \n" +
                    "call n10s.rdf.stream.fetch('"+ CollectTriplesTest.class.getClassLoader()
                    .getResource("100k.nt")
                    .toURI() + "',\"N-Triples\" ,{ limit: 999999}) yield subject, predicate, object, isLiteral, literalType, literalLang\n" +
                    "where subject in indivList\n" +
                    "return n10s.rdf.collect(subject, predicate, object, isLiteral, literalType, literalLang) as rdf");
            assertEquals(true, results.hasNext());
            assertTrue(ModelTestUtils
                    .compareModels(results.next().get("rdf").asString(),
                            RDFFormat.NTRIPLES,TURTLE_FRAGMENT_2,RDFFormat.TURTLE));



        }
    }

    @Test
    public void testCollectTriplesDataTypes() throws Exception {
        try (Driver driver = GraphDatabase.driver(neo4j.boltURI(),
                Config.builder().withoutEncryption().build())) {

            Session session = driver.session();
            Result results = session.run("CALL n10s.rdf.stream.fetch('"+ CollectTriplesTest.class.getClassLoader()
                    .getResource("datetime/datetime-and-other.ttl")
                    .toURI() +"', 'Turtle') " +
                    " yield subject, predicate, object, isLiteral, literalType, literalLang "
                    + " return  n10s.rdf.collect(subject, predicate, object, isLiteral, literalType, literalLang) as rdf");
            assertEquals(true, results.hasNext());


            String fileAsString = new String ( Files.readAllBytes( Paths.get(CollectTriplesTest.class.getClassLoader()
                    .getResource("datetime/datetime-and-other.ttl").toURI()) ) );

            assertTrue(ModelTestUtils
                    .compareModels(results.next().get("rdf").asString(),
                            RDFFormat.NTRIPLES, fileAsString,RDFFormat.TURTLE));
        }
    }

    @Test
    public void testCollectTriplesDataTypesTurtle() throws Exception {
        try (Driver driver = GraphDatabase.driver(neo4j.boltURI(),
                Config.builder().withoutEncryption().build())) {

            Session session = driver.session();
            Result results = session.run("CALL n10s.rdf.stream.fetch('"+ CollectTriplesTest.class.getClassLoader()
                    .getResource("datetime/datetime-and-other.ttl")
                    .toURI() +"', 'Turtle') " +
                    " yield subject, predicate, object, isLiteral, literalType, literalLang "
                    + " return  n10s.rdf.collect.ttl(subject, predicate, object, isLiteral, literalType, literalLang) as rdf");
            assertEquals(true, results.hasNext());


            String fileAsString = new String ( Files.readAllBytes( Paths.get(CollectTriplesTest.class.getClassLoader()
                    .getResource("datetime/datetime-and-other.ttl").toURI()) ) );

            assertTrue(ModelTestUtils
                    .compareModels(results.next().get("rdf").asString(),
                            RDFFormat.TURTLE, fileAsString,RDFFormat.TURTLE));
        }
    }

    @Test
    public void testCollectTriplesDataTypesXml() throws Exception {
        try (Driver driver = GraphDatabase.driver(neo4j.boltURI(),
                Config.builder().withoutEncryption().build())) {

            Session session = driver.session();
            Result results = session.run("CALL n10s.rdf.stream.fetch('"+ CollectTriplesTest.class.getClassLoader()
                    .getResource("datetime/datetime-and-other.ttl")
                    .toURI() +"', 'Turtle') " +
                    " yield subject, predicate, object, isLiteral, literalType, literalLang "
                    + " return  n10s.rdf.collect.xml(subject, predicate, object, isLiteral, literalType, literalLang) as rdf");
            assertEquals(true, results.hasNext());


            String fileAsString = new String ( Files.readAllBytes( Paths.get(CollectTriplesTest.class.getClassLoader()
                    .getResource("datetime/datetime-and-other.ttl").toURI()) ) );

            assertTrue(ModelTestUtils
                    .compareModels(results.next().get("rdf").asString(),
                            RDFFormat.RDFXML, fileAsString,RDFFormat.TURTLE));
        }
    }

    @Test
    public void testCollectTriplesDataTypesJson() throws Exception {
        try (Driver driver = GraphDatabase.driver(neo4j.boltURI(),
                Config.builder().withoutEncryption().build())) {

            Session session = driver.session();
            Result results = session.run("CALL n10s.rdf.stream.fetch('"+ CollectTriplesTest.class.getClassLoader()
                    .getResource("datetime/datetime-and-other.ttl")
                    .toURI() +"', 'Turtle') " +
                    " yield subject, predicate, object, isLiteral, literalType, literalLang "
                    + " return  n10s.rdf.collect.json(subject, predicate, object, isLiteral, literalType, literalLang) as rdf");
            assertEquals(true, results.hasNext());


            String fileAsString = new String ( Files.readAllBytes( Paths.get(CollectTriplesTest.class.getClassLoader()
                    .getResource("datetime/datetime-and-other.ttl").toURI()) ) );

            assertTrue(ModelTestUtils
                    .compareModels(results.next().get("rdf").asString(),
                            RDFFormat.JSONLD, fileAsString,RDFFormat.TURTLE));
        }
    }

    @Test
    public void testCollectTriplesDataTypesNtriples() throws Exception {
        try (Driver driver = GraphDatabase.driver(neo4j.boltURI(),
                Config.builder().withoutEncryption().build())) {

            Session session = driver.session();
            Result results = session.run("CALL n10s.rdf.stream.fetch('"+ CollectTriplesTest.class.getClassLoader()
                    .getResource("datetime/datetime-and-other.ttl")
                    .toURI() +"', 'Turtle') " +
                    " yield subject, predicate, object, isLiteral, literalType, literalLang "
                    + " return  n10s.rdf.collect.nt(subject, predicate, object, isLiteral, literalType, literalLang) as rdf");
            assertEquals(true, results.hasNext());


            String fileAsString = new String ( Files.readAllBytes( Paths.get(CollectTriplesTest.class.getClassLoader()
                    .getResource("datetime/datetime-and-other.ttl").toURI()) ) );

            assertTrue(ModelTestUtils
                    .compareModels(results.next().get("rdf").asString(),
                            RDFFormat.NTRIPLES, fileAsString,RDFFormat.TURTLE));
        }
    }


    @Test
    public void testCollectTriplesLang() throws Exception {
        try (Driver driver = GraphDatabase.driver(neo4j.boltURI(),
                Config.builder().withoutEncryption().build())) {

            Session session = driver.session();
            Result results = session.run("CALL n10s.rdf.stream.fetch('"+ CollectTriplesTest.class.getClassLoader()
                    .getResource("multilang.ttl")
                    .toURI() +"', 'Turtle') " +
                    " yield subject, predicate, object, isLiteral, literalType, literalLang "
                    + " return  n10s.rdf.collect(subject, predicate, object, isLiteral, literalType, literalLang) as rdf");
            assertEquals(true, results.hasNext());


            String fileAsString = new String ( Files.readAllBytes( Paths.get(CollectTriplesTest.class.getClassLoader()
                    .getResource("multilang.ttl").toURI()) ) );


            assertTrue(ModelTestUtils
                    .compareModels(results.next().get("rdf").asString(),
                            RDFFormat.NTRIPLES,fileAsString,RDFFormat.TURTLE));

        }
    }

}
