package n10s.validation;

import static n10s.CommonProcedures.UNIQUENESS_CONSTRAINT_ON_URI;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import n10s.graphconfig.GraphConfigProcedures;
import n10s.nsprefixes.NsPrefixDefProcedures;
import n10s.rdf.RDFProcedures;
import n10s.rdf.load.RDFLoadProcedures;
import org.eclipse.rdf4j.model.vocabulary.SHACL;
import org.junit.Rule;
import org.junit.Test;
import org.neo4j.driver.Config;
import org.neo4j.driver.Driver;
import org.neo4j.driver.GraphDatabase;
import org.neo4j.driver.Record;
import org.neo4j.driver.Result;
import org.neo4j.driver.Session;
import org.neo4j.driver.types.Node;
import org.neo4j.harness.junit.rule.Neo4jRule;

public class SHACLValidationProceduresTest {

  final String VAL_RESULTS_QUERY = "MATCH (vr:sh__ValidationResult)\n"
      + "RETURN \n"
      + "       [(vr)-[:sh__sourceConstraintComponent]->(co) | co.uri ][0] as constraint,\n"
      + "       [(vr)-[:sh__resultPath]->()-[:sh__inversePath*0..1]->(p) where not (p)-->() | n10s.rdf.getIRILocalName(p.uri) ][0] as path,\n"
      + "       coalesce([(vr)-[:sh__sourceShape]->()<-[:sh__property*0..1]-()-[:sh__targetClass]->(tc)| n10s.rdf.getIRILocalName(tc.uri) ][0], \n"
      + "       [(vr)-[:sh__sourceShape]->()<-[:sh__property*0..1]-(tc:rdfs__Class)| n10s.rdf.getIRILocalName(tc.uri) ][0]) as targetClass,\n"
      + "       [(vr)-[:sh__focusNode]->(f) | id(f) ][0] as focus,\n"
      + "       [(vr)-[:sh__resultSeverity]->(sev) | sev.uri ][0]  as sev, "
      + "       toString(([(vr)-[:sh__value]->(x)| x.uri] + ([] + coalesce(vr.sh__value,[])))[0]) as offendingValue ";


  final String VAL_RESULTS_QUERY_AS_RDF = "MATCH (vr:sh__ValidationResult)\n"
      + "RETURN \n"
      + "       [(vr)-[:sh__sourceConstraintComponent]->(co) | co.uri ][0] as constraint,\n"
      + "       [(vr)-[:sh__resultPath]->()-[:sh__inversePath*0..1]->(p) where not (p)-->() | p.uri ][0] as path,\n"
      + "       coalesce([(vr)-[:sh__sourceShape]->()<-[:sh__property*0..1]-()-[:sh__targetClass]->(tc)| tc.uri ][0], \n"
      + "       [(vr)-[:sh__sourceShape]->()<-[:sh__property*0..1]-(tc:rdfs__Class)| tc.uri ][0]) as targetClass,\n"
      + "       [(vr)-[:sh__focusNode]->(f) | f.uri ][0] as focus,\n"
      + "       [(vr)-[:sh__resultSeverity]->(sev) | sev.uri ][0]  as sev, "
      + "       toString(([(vr)-[:sh__value]->(x)| x.uri] + ([] + coalesce(vr.sh__value,[])))[0]) as offendingValue ";

  @Rule
  public Neo4jRule neo4j = new Neo4jRule()
      .withProcedure(ValidationProcedures.class).withProcedure(GraphConfigProcedures.class)
      .withProcedure(RDFLoadProcedures.class).withFunction(RDFProcedures.class).withProcedure(
          NsPrefixDefProcedures.class);


  @Test
  public void testCompiledValidatorIsPersisted() throws Exception {
    try (Driver driver = GraphDatabase.driver(neo4j.boltURI(),
        Config.builder().withoutEncryption().build())) {

      Session session = driver.session();

      assertFalse(session.run("MATCH (n) RETURN n").hasNext());

      Result loadDataResults = session.run(
          "CREATE (SleeplessInSeattle:Movie {title:'Sleepless in Seattle', released:1993, tagline:'What if someone you never met, someone you never saw, someone you never knew was the only someone for you?'})\n"
              +
              "CREATE (NoraE:Person {name:'Nora Ephron', born:1941})\n" +
              "CREATE (RitaW:Person {name:'Rita Wilson', born:1956})\n" +
              "CREATE (BillPull:Person {name:['Bill Pullman','B.P.'], born:1953})\n" +
              "CREATE (VictorG:Person {name:'Victor Garber', born:1949})\n" +
              "CREATE (RosieO:Person {name:\"Rosie O'Donnell\", born:1962})\n" +
              "CREATE\n" +
              "  (RitaW)-[:ACTED_IN {roles:['Suzy']}]->(SleeplessInSeattle),\n" +
              "  (BillPull)-[:ACTED_IN {roles:['Walter']}]->(SleeplessInSeattle),\n" +
              "  (VictorG)-[:ACTED_IN {roles:['Greg']}]->(SleeplessInSeattle),\n" +
              "  (RosieO)-[:ACTED_IN {roles:['Becky']}]->(SleeplessInSeattle),\n" +
              "  (NoraE)-[:DIRECTED]->(SleeplessInSeattle) ");

      loadDataResults.hasNext();

      String SHACL_SNIPPET = "@prefix ex: <http://example/> .\n"
          + "@prefix neo4j: <neo4j://voc#> .\n"
          + "@prefix sh: <http://www.w3.org/ns/shacl#> .\n"
          + "\n"
          + "ex:PersonShape\n"
          + "\ta sh:NodeShape ;\n"
          + "\tsh:targetClass neo4j:Person ;    # Applies to all Person nodes in Neo4j\n"
          + "\tsh:property [\n"
          + "\t\tsh:path neo4j:name ;           # constrains the values of neo4j:name\n"
          + "\t\tsh:maxCount 1 ;                # cardinality\n"
          + "\t\tsh:datatype xsd:string ;       # data type\n"
          + "\t] ;\n"
          + "\tsh:property [\n"
          + "\t\tsh:path neo4j:ACTED_IN ;       # constrains the values of neo4j:ACTED_IN\n"
          + "\t\tsh:class neo4j:Movie ;         # range\n"
          + "\t\tsh:nodeKind sh:IRI ;           # type of property\n"
          + "\t\tsh:severity sh:Warning ;\n"
          + "\t] ;\n"
          + "\tsh:closed true ;\n"
          + "\tsh:ignoredProperties ( neo4j:born neo4j:DIRECTED neo4j:FOLLOWS neo4j:REVIEWED neo4j:PRODUCED neo4j:WROTE ) .\n"
          + "\n"
          + "\n"
          + "neo4j:Movie\n"
          + "\ta sh:NodeShape , rdfs:Class;\n"
          + "\tsh:property [\n"
          + "\t\tsh:path neo4j:title ;           # constrains the values of neo4j:title\n"
          + "\t\tsh:maxCount 1 ;                 # cardinality\n"
          + "\t\tsh:datatype xsd:string ;        # data type\n"
          + "\t\tsh:minLength 10 ;               # string length\n"
          + "\t\tsh:maxLength 18 ;               # string length\n"
          + "\t] ;\n"
          + "\tsh:property [\n"
          + "\t\tsh:path neo4j:released ;        # constrains the values of neo4j:title\n"
          + "\t\tsh:datatype xsd:integer ;       # data type\n"
          + "\t\tsh:nodeKind sh:Literal ;        # type of property\n"
          + "        sh:minInclusive 2000 ;      # numeric range\n"
          + "        sh:maxInclusive 2019 ;      # numeric range\n"
          + "\t] ;\n"
          + "\tsh:closed true ;\n"
          + "\tsh:ignoredProperties ( neo4j:tagline ) .";

      Result results = session
          .run(
              "CALL n10s.validation.shacl.import.inline(\"" + SHACL_SNIPPET + "\",\"Turtle\", {})");

      assertTrue(results.hasNext());

      results = session.run("MATCH (vc:_n10sValidatorConfig) RETURN vc ");
      assertTrue(results.hasNext());
      Node vc = results.next().get("vc").asNode();
      assertFalse(vc.get("_gq").isNull());

      Result loadCompiled = session.run("call n10s.validation.shacl.validate()");
      assertTrue(loadCompiled.hasNext());
    }
  }

  @Test
  public void testLargeShapesFile() throws Exception {
    try (Driver driver = GraphDatabase.driver(neo4j.boltURI(),
        Config.builder().withoutEncryption().build())) {

      Session session = driver.session();

      assertFalse(session.run("MATCH (n) RETURN n").hasNext());

      String data_input = " CREATE (:Lead { annualRevenue: '345', leadScore: 1.1, leadRating: 0 })-[:FOR_CO]->(:Company { id: 'co.234'}) ;";

      session.run(data_input);

      Result results = session
          .run("CALL n10s.validation.shacl.import.fetch(\"" + SHACLValidationProceduresTest.class
              .getClassLoader()
              .getResource("shacl/cim.ttl")
              .toURI() + "\",\"Turtle\", {})");

      assertTrue(results.hasNext());

      results = session.run("MATCH (vc:_n10sValidatorConfig) RETURN vc ");
      assertTrue(results.hasNext());
      Node vc = results.next().get("vc").asNode();
      assertFalse(vc.get("_gq").isNull());

      Result result = session.run("call n10s.validation.shacl.validate()");
      assertTrue(result.hasNext());
      int minCountCount = 0;
      int datatypeConstCount = 0;
      while (result.hasNext()) {
        Record next = result.next();
        if (next.get("propertyShape").asString()
            .equals(SHACL.MIN_COUNT_CONSTRAINT_COMPONENT.stringValue())) {
          minCountCount++;
        }
        if (next.get("propertyShape").asString()
            .equals(SHACL.DATATYPE_CONSTRAINT_COMPONENT.stringValue())) {
          datatypeConstCount++;
        }
      }
      assertEquals(3, minCountCount);
      assertEquals(3, datatypeConstCount);
    }
  }

  @Test
  public void tesMusicExample() throws Exception {
    try (Driver driver = GraphDatabase.driver(neo4j.boltURI(),
        Config.builder().withoutEncryption().build()); Session session = driver.session()) {
      assertFalse(session.run("MATCH (n) RETURN n").hasNext());
      session.run("CREATE CONSTRAINT ON ( resource:Resource ) ASSERT (resource.uri) IS UNIQUE ");
      session.run("CALL n10s.graphconfig.init( { handleMultival: 'ARRAY', "
          + "multivalPropList: [ 'http://stardog.com/tutorial/date'] })");
      session.run("CALL n10s.nsprefixes.add('tut','http://stardog.com/tutorial/')");
      session.close();
    }

    try (Driver driver = GraphDatabase.driver(neo4j.boltURI(),
          Config.builder().withoutEncryption().build()); Session session = driver.session()) {

      Result loadShapes = session.run(
          "CALL n10s.validation.shacl.import.fetch(\"" + SHACLValidationProceduresTest.class
              .getClassLoader()
              .getResource("shacl/musictest/music.shacl")
              .toURI() + "\",\"Turtle\", {})");
      assertTrue(loadShapes.hasNext());

      Result loadData = session.run(
          "CALL n10s.rdf.import.fetch(\"" + SHACLValidationProceduresTest.class
              .getClassLoader()
              .getResource("shacl/musictest/data.ttl")
              .toURI() + "\",\"Turtle\", {})");

      assertTrue(loadData.hasNext());

      Result result = session.run("call n10s.validation.shacl.validate()");
//      "call n10s.validation.shacl.validate() yield nodeType,  propertyShape, offendingValue, resultPath, severity\n"
//          + "return collect({  nt: nodeType,  ps: propertyShape, ov: offendingValue, rp: resultPath, s: severity}) as resultList"
      assertTrue(result.hasNext());
      int minCountCount = 0;
      int maxCountCount = 0;
      int datatypeConstCount = 0;
      while (result.hasNext()) {
        Record next = result.next();
        assertEquals("http://stardog.com/tutorial/Album",next.get("nodeType").asString());
        if (next.get("propertyShape").asString()
            .equals(SHACL.MIN_COUNT_CONSTRAINT_COMPONENT.stringValue())) {
          assertEquals("http://stardog.com/tutorial/track",next.get("resultPath").asString());
          minCountCount++;
        }
        if (next.get("propertyShape").asString()
            .equals(SHACL.DATATYPE_CONSTRAINT_COMPONENT.stringValue())) {
          assertEquals("http://stardog.com/tutorial/date",next.get("resultPath").asString());
          datatypeConstCount++;
        }
        if (next.get("propertyShape").asString()
            .equals(SHACL.MAX_COUNT_CONSTRAINT_COMPONENT.stringValue())) {
          assertEquals("http://stardog.com/tutorial/date",next.get("resultPath").asString());
          maxCountCount++;
        }
      }
      assertEquals(1, minCountCount);
      assertEquals(1, datatypeConstCount);
      assertEquals(1, maxCountCount);
      }
  }

  @Test
  public void testRegexValidationOnMovieDB() throws Exception {
    try (Driver driver = GraphDatabase.driver(neo4j.boltURI(),
        Config.builder().withoutEncryption().build())) {

      Session session = driver.session();

      assertFalse(session.run("MATCH (n) RETURN n").hasNext());

      session.run(
          "CREATE (SleeplessInSeattle:Movie {title:'Sleepless in Seattle', released:1993, tagline:'What if someone you never met, someone you never saw, someone you never knew was the only someone for you?'})\n"
              +
              "CREATE (NoraE:Person:Animal {name:'Nora Ephron', born:1941})\n" +
              "CREATE (RitaW:Person:Thing {name:'Rita Wilson', born:1956})\n" +
              "CREATE (BillPull:Person {name:'Bill Pullman', born:1953})\n" +
              "CREATE (VictorG:Person {name:'Victor Garber', born:1949})\n" +
              "CREATE (RosieO:Person {name:\"Rosie O'Donnell\", born:1962})\n" +
              "CREATE\n" +
              "  (RitaW)-[:ACTED_IN {roles:['Suzy']}]->(SleeplessInSeattle),\n" +
              "  (BillPull)-[:ACTED_IN {roles:['Walter']}]->(SleeplessInSeattle),\n" +
              "  (VictorG)-[:ACTED_IN {roles:['Greg']}]->(SleeplessInSeattle),\n" +
              "  (RosieO)-[:ACTED_IN {roles:['Becky']}]->(SleeplessInSeattle),\n" +
              "  (NoraE)-[:DIRECTED]->(SleeplessInSeattle) ");

      session.run("CREATE CONSTRAINT ON ( resource:Resource ) ASSERT (resource.uri) IS UNIQUE ");

      session.run("CALL n10s.validation.shacl.import.fetch(\"" + SHACLValidationProceduresTest.class
          .getClassLoader()
          .getResource("shacl/person2-shacl.ttl")
          .toURI() + "\",\"Turtle\", {})");

      Result validationResults = session.run("CALL n10s.validation.shacl.validate() ");

      assertEquals(true, validationResults.hasNext());

      while (validationResults.hasNext()) {
        Record next = validationResults.next();
        if (next.get("nodeType").equals("Person")) {
          assertEquals("Rosie O'Donnell", next.get("offendingValue").asString());
          assertEquals("http://www.w3.org/ns/shacl#PatternConstraintComponent",
              next.get("propertyShape").asString());
        } else if (next.get("nodeType").equals("Movie")) {
          assertEquals(1993, next.get("offendingValue").asInt());
          assertEquals("http://www.w3.org/ns/shacl#MinExclusiveConstraintComponent",
              next.get("propertyShape").asString());
        }
      }

    }
  }


  @Test
    public void testValidationBeforeNsDefined() throws Exception {
    try (Driver driver = GraphDatabase.driver(neo4j.boltURI(),
        Config.builder().withoutEncryption().build())) {

      Session session = driver.session();

      assertFalse(session.run("MATCH (n) RETURN n").hasNext());
      session.run("CREATE CONSTRAINT ON ( resource:Resource ) ASSERT (resource.uri) IS UNIQUE ");
      session.run("CALL n10s.graphconfig.init()");
      Result result = session.run(
          "CALL n10s.validation.shacl.import.fetch(\"" + SHACLValidationProceduresTest.class
              .getClassLoader()
              .getResource("shacl/person2-shacl.ttl")
              .toURI() + "\",\"Turtle\", {})");

      try {
        result.hasNext();
        assertFalse(true); //should not get here
      } catch (Exception e) {
        assertEquals("Failed to invoke procedure `n10s.validation.shacl.import.fetch`: Caused by: n10s.utils.UriUtils$UriNamespaceHasNoAssociatedPrefix: Prefix Undefined: No prefix defined for namespace <neo4j://voc#Movie>. Use n10s.nsprefixes.add(...) procedure.",e.getMessage());
      }
      session.run("CALL n10s.nsprefixes.add('neo','neo4j://voc#')");
      session.run("CALL n10s.nsprefixes.add('hello','http://example/')");
      result = session.run(
          "CALL n10s.validation.shacl.import.fetch(\"" + SHACLValidationProceduresTest.class
              .getClassLoader()
              .getResource("shacl/person2-shacl.ttl")
              .toURI() + "\",\"Turtle\", {})");
      assertTrue(result.hasNext());
    }
  }

  @Test
  public void testLoadShapesOutput() throws Exception {
    try (Driver driver = GraphDatabase.driver(neo4j.boltURI(),
        Config.builder().withoutEncryption().build())) {

      Session session = driver.session();

      assertFalse(session.run("MATCH (n) RETURN n").hasNext());

      //On pure LPG
      Result result = session.run(
          "CALL n10s.validation.shacl.import.fetch(\"" + SHACLValidationProceduresTest.class
              .getClassLoader()
              .getResource("shacl/person2-shacl.ttl")
              .toURI() + "\",\"Turtle\", {})");

      int matches = 0;
      while (result.hasNext()) {
        Record next = result.next();
        if ((next.get("target").asString().equals("Person") &&
            next.get("propertyOrRelationshipPath").asString().equals("ACTED_IN") &&
            next.get("param").asString().equals("class") &&
            next.get("value").asString().equals("Movie"))
            ||
            (next.get("target").asString().equals("Movie") &&
                next.get("propertyOrRelationshipPath").asString().equals("released") &&
                next.get("param").asString().equals("datatype") &&
                next.get("value").asString().equals("integer"))
            ||
            (next.get("target").asString().equals("Movie") &&
                next.get("propertyOrRelationshipPath").asString().equals("released") &&
                next.get("param").asString().equals("minInclusive") &&
                next.get("value").asInt() == 2000)
            ||
            (next.get("target").asString().equals("Person") &&
                next.get("propertyOrRelationshipPath").isNull() &&
                next.get("param").asString().equals("ignoredProperties") &&
                next.get("value").asList(x -> x.asString()).equals(
                    List.of("born", "DIRECTED", "FOLLOWS", "REVIEWED", "PRODUCED", "WROTE")))) {
          matches++;
        }
      }
      assertEquals(4, matches);

      session.run("MATCH (n) DETACH DELETE n");
      assertFalse(session.run("MATCH (n) RETURN n").hasNext());
      //RDF SHORTEN GRAPH
      session.run("CREATE CONSTRAINT ON ( resource:Resource ) ASSERT (resource.uri) IS UNIQUE ");
      session.run("CALL n10s.graphconfig.init()");
      session.run("CALL n10s.nsprefixes.add('neo','neo4j://voc#')");
      session.run("CALL n10s.nsprefixes.add('ex','http://example/')");

      result = session.run(
          "CALL n10s.validation.shacl.import.fetch(\"" + SHACLValidationProceduresTest.class
              .getClassLoader()
              .getResource("shacl/person2-shacl.ttl")
              .toURI() + "\",\"Turtle\", {})");

      matches = 0;
      while (result.hasNext()) {
        Record next = result.next();
        if ((next.get("target").asString().equals("neo__Person") &&
            next.get("propertyOrRelationshipPath").asString().equals("neo__ACTED_IN") &&
            next.get("param").asString().equals("sh:class") &&
            next.get("value").asString().equals("neo__Movie"))
            ||
            (next.get("target").asString().equals("neo__Movie") &&
                next.get("propertyOrRelationshipPath").asString().equals("neo__released") &&
                next.get("param").asString().equals("sh:minInclusive") &&
                next.get("value").asInt() == 2000)
            ||
            (next.get("target").asString().equals("neo__Movie") &&
                next.get("propertyOrRelationshipPath").asString().equals("neo__released") &&
                next.get("param").asString().equals("sh:datatype") &&
                next.get("value").asString().equals("http://www.w3.org/2001/XMLSchema#integer"))
            ||
            (next.get("target").asString().equals("neo__Person") &&
                next.get("propertyOrRelationshipPath").isNull() &&
                next.get("param").asString().equals("sh:ignoredProperties") &&
                next.get("value").asList(x -> x.asString()).equals(
                    List.of("neo__born", "neo__DIRECTED", "neo__FOLLOWS", "neo__REVIEWED",
                        "neo__PRODUCED", "neo__WROTE")))) {
          matches++;
        }
      }
      assertEquals(4, matches);

      session.run("MATCH (n) DETACH DELETE n");
      assertFalse(session.run("MATCH (n) RETURN n").hasNext());
      //RDF IGNORE GRAPH
      session.run("CALL n10s.graphconfig.init({ handleVocabUris: 'IGNORE' })");

      result = session.run(
          "CALL n10s.validation.shacl.import.fetch(\"" + SHACLValidationProceduresTest.class
              .getClassLoader()
              .getResource("shacl/person2-shacl.ttl")
              .toURI() + "\",\"Turtle\", {})");

      matches = 0;
      while (result.hasNext()) {
        Record next = result.next();
        if ((next.get("target").asString().equals("Person") &&
            next.get("propertyOrRelationshipPath").asString().equals("ACTED_IN") &&
            next.get("param").asString().equals("class") &&
            next.get("value").asString().equals("Movie"))
            ||
            (next.get("target").asString().equals("Movie") &&
                next.get("propertyOrRelationshipPath").asString().equals("released") &&
                next.get("param").asString().equals("datatype") &&
                next.get("value").asString().equals("integer"))
            ||
            (next.get("target").asString().equals("Movie") &&
                next.get("propertyOrRelationshipPath").asString().equals("released") &&
                next.get("param").asString().equals("minInclusive") &&
                next.get("value").asInt() == 2000)
            ||
            (next.get("target").asString().equals("Person") &&
                next.get("propertyOrRelationshipPath").isNull() &&
                next.get("param").asString().equals("ignoredProperties") &&
                next.get("value").asList(x -> x.asString()).equals(
                    List.of("born", "DIRECTED", "FOLLOWS", "REVIEWED", "PRODUCED", "WROTE")))) {
          matches++;
        }
      }
      assertEquals(4, matches);

      session.run("MATCH (n) DETACH DELETE n");
      assertFalse(session.run("MATCH (n) RETURN n").hasNext());
      //RDF KEEP GRAPH
      session.run("CALL n10s.graphconfig.init({ handleVocabUris: 'KEEP' })");

      result = session.run(
          "CALL n10s.validation.shacl.import.fetch(\"" + SHACLValidationProceduresTest.class
              .getClassLoader()
              .getResource("shacl/person2-shacl.ttl")
              .toURI() + "\",\"Turtle\", {})");

      matches = 0;
      while (result.hasNext()) {
        Record next = result.next();
        if ((next.get("target").asString().equals("neo4j://voc#Person") &&
            next.get("propertyOrRelationshipPath").asString().equals("neo4j://voc#ACTED_IN") &&
            next.get("param").asString().equals("sh:class") &&
            next.get("value").asString().equals("neo4j://voc#Movie"))
            ||
            (next.get("target").asString().equals("neo4j://voc#Movie") &&
                next.get("propertyOrRelationshipPath").asString().equals("neo4j://voc#released") &&
                next.get("param").asString().equals("sh:minInclusive") &&
                next.get("value").asInt() == 2000)
            ||
            (next.get("target").asString().equals("neo4j://voc#Movie") &&
                next.get("propertyOrRelationshipPath").asString().equals("neo4j://voc#released") &&
                next.get("param").asString().equals("sh:datatype") &&
                next.get("value").asString().equals("http://www.w3.org/2001/XMLSchema#integer"))
            ||
            (next.get("target").asString().equals("neo4j://voc#Person") &&
                next.get("propertyOrRelationshipPath").isNull() &&
                next.get("param").asString().equals("sh:ignoredProperties") &&
                next.get("value").asList(x -> x.asString()).equals(
                    List.of("neo4j://voc#born", "neo4j://voc#DIRECTED", "neo4j://voc#FOLLOWS",
                        "neo4j://voc#REVIEWED",
                        "neo4j://voc#PRODUCED", "neo4j://voc#WROTE")))) {
          matches++;
        }
      }
      assertEquals(4, matches);
    }
  }

  @Test
  public void testListShapesInRDFIgnoreGraph() throws Exception {
    try (Driver driver = GraphDatabase.driver(neo4j.boltURI(),
        Config.builder().withoutEncryption().build())) {

      Session session = driver.session();

      session.run("CALL n10s.graphconfig.init({ handleVocabUris: 'IGNORE' })");

      session.run("CREATE CONSTRAINT ON ( resource:Resource ) ASSERT (resource.uri) IS UNIQUE ");

      session.run("CALL n10s.validation.shacl.import.fetch(\"" + SHACLValidationProceduresTest.class
          .getClassLoader()
          .getResource("shacl/person2-shacl.ttl")
          .toURI() + "\",\"Turtle\", {})");

      Result shapesResults = session.run("CALL n10s.validation.shacl.listShapes() ");

      assertEquals(true, shapesResults.hasNext());

      while (shapesResults.hasNext()) {
        Record next = shapesResults.next();
        assertTrue(next.get("target").asString().equals("Movie") || next.get("target").asString()
            .equals("Person"));
        if (next.get("target").asString().equals("Movie") && next.get("propertyOrRelationshipPath")
            .asString().equals("released")
            && next.get("param").asString().equals("maxInclusive")) {
          assertEquals(2019, next.get("value").asInt());
        }
        if (next.get("target").equals("Person") && next.get("propertyOrRelationshipPath").isNull()
            && next.get("param").equals("ignoredProperties")) {
          List<Object> expected = new ArrayList<>();
          expected.add("WROTE");
          expected.add("PRODUCED");
          expected.add("REVIEWED");
          expected.add("FOLLOWS");
          expected.add("DIRECTED");
          expected.add("born");
          assertEquals(expected, next.get("value").asList());
        }
      }

    }
  }

  @Test
  public void testListShapesInRDFShortenGraph() throws Exception {
    try (Driver driver = GraphDatabase.driver(neo4j.boltURI(),
        Config.builder().withoutEncryption().build())) {

      Session session = driver.session();

      session.run("CALL n10s.graphconfig.init()");

      session.run("CREATE CONSTRAINT ON ( resource:Resource ) ASSERT (resource.uri) IS UNIQUE ");

      String turtleNsDefinition = "@prefix ex: <http://example/> .\n"
          + "@prefix neo4j: <neo4j://voc#> .\n"
          + "@prefix sh: <http://www.w3.org/ns/shacl#> .";

      session.run("CALL n10s.nsprefixes.addFromText(' " + turtleNsDefinition + " ')");

      session.run("CALL n10s.validation.shacl.import.fetch(\"" + SHACLValidationProceduresTest.class
          .getClassLoader()
          .getResource("shacl/person2-shacl.ttl")
          .toURI() + "\",\"Turtle\", {})");

      Result shapesResults = session.run("CALL n10s.validation.shacl.listShapes() ");

      assertEquals(true, shapesResults.hasNext());

      while (shapesResults.hasNext()) {
        Record next = shapesResults.next();
        assertTrue(
            next.get("target").asString().equals("neo4j__Movie") || next.get("target").asString()
                .equals("neo4j__Person"));
        if (next.get("target").asString().equals("neo4j__Movie") && next
            .get("propertyOrRelationshipPath").asString().equals("neo4j__released")
            && next.get("param").asString().equals("sh:maxInclusive")) {
          assertEquals(2019, next.get("value").asInt());
        }
        if (next.get("target").equals("neo4j__Person") && next.get("propertyOrRelationshipPath")
            .isNull()
            && next.get("param").equals("sh:ignoredProperties")) {
          List<Object> expected = new ArrayList<>();
          expected.add("neo4j__WROTE");
          expected.add("neo4j__PRODUCED");
          expected.add("neo4j__REVIEWED");
          expected.add("neo4j__FOLLOWS");
          expected.add("neo4j__DIRECTED");
          expected.add("neo4j__born");
          assertEquals(expected, next.get("value").asList());
        }
      }

    }
  }

  @Test
  public void testTxTriggerValidation() throws Exception {
    try (Driver driver = GraphDatabase.driver(neo4j.boltURI(),
        Config.builder().withoutEncryption().build())) {

      Session session = driver.session();

      assertFalse(session.run("MATCH (n) RETURN n").hasNext());

      session.run(
          "CREATE (SleeplessInSeattle:Movie {title:'Sleepless in Seattle', released:1993, tagline:'What if someone you never met, someone you never saw, someone you never knew was the only someone for you?'})\n"
              +
              "CREATE (NoraE:Person {name:'Nora Ephron', born:1941})\n" +
              "CREATE (RitaW:Person {name:'Rita Wilson', born:1956})\n" +
              "CREATE (BillPull:Person {name:'Ice-T', born:1953})\n" +
              "CREATE (VictorG:Person {name:'Victor Garber', born:1949})\n" +
              "CREATE (RosieO:Person {name:\"Rosie O'Donnell\", born:1962})\n" +
              "CREATE\n" +
              "  (RitaW)-[:ACTED_IN {roles:['Suzy']}]->(SleeplessInSeattle),\n" +
              "  (BillPull)-[:ACTED_IN {roles:['Walter']}]->(SleeplessInSeattle),\n" +
              "  (VictorG)-[:ACTED_IN {roles:['Greg']}]->(SleeplessInSeattle),\n" +
              "  (RosieO)-[:ACTED_IN {roles:['Becky']}]->(SleeplessInSeattle),\n" +
              "  (NoraE)-[:DIRECTED]->(SleeplessInSeattle) ");

      session.run("CREATE CONSTRAINT " + UNIQUENESS_CONSTRAINT_ON_URI
          + " ON ( resource:Resource ) ASSERT (resource.uri) IS UNIQUE ");
      assertTrue(session.run("call db.schemaStatements").hasNext());

      Result loadShapesResult = session.run(
          "CALL n10s.validation.shacl.import.fetch(\"" + SHACLValidationProceduresTest.class
              .getClassLoader()
              .getResource("shacl/person2-shacl.ttl")
              .toURI() + "\",\"Turtle\", {})");

      Result validationResults = session.run("MATCH (p:Person) WITH collect(p) as nodes "
          + "call n10s.validation.shacl.validateTransaction(nodes,[], {}, {}, {}, {}) "
          + "yield focusNode, nodeType, shapeId, propertyShape, offendingValue, resultPath, severity, resultMessage "
          + "RETURN focusNode, nodeType, shapeId, propertyShape, offendingValue, resultPath, severity, resultMessage");

      try {
        assertEquals(true, validationResults.hasNext());

        //Should not get here
        assertTrue(false);

      } catch (Exception e) {
        //This is expected
        assertTrue(e.getMessage().contains("SHACLValidationException"));
      }

    }
  }


  @Test
  public void testRunTestSuite1() throws Exception {
    runIndividualTest("core/complex", "personexample", null, "IGNORE");
    runIndividualTest("core/complex", "personexample", null, "SHORTEN");
    runIndividualTest("core/complex", "personexample", null, "KEEP");
  }

  @Test
  public void testRunTestSuite2() throws Exception {
    runIndividualTest("core/path", "path-inverse-001", null, "IGNORE");
    runIndividualTest("core/path", "path-inverse-001", null, "SHORTEN");
    runIndividualTest("core/path", "path-inverse-001", null, "KEEP");
  }

  @Test
  public void testRunTestSuite3() throws Exception {
    runIndividualTest("core/property", "datatype-001", null, "IGNORE");
    runIndividualTest("core/property", "datatype-001", null, "SHORTEN");
    runIndividualTest("core/property", "datatype-001", null, "KEEP");
  }

  @Test
  public void testRunTestSuite4() throws Exception {
    runIndividualTest("core/property", "datatype-002", null, "IGNORE");
    runIndividualTest("core/property", "datatype-002", null, "SHORTEN");
    runIndividualTest("core/property", "datatype-002", null, "KEEP");
  }

  @Test
  public void testRunTestSuite5() throws Exception {
    runIndividualTest("core/property", "maxCount-001", null, "IGNORE");
    runIndividualTest("core/property", "maxCount-001", null, "SHORTEN");
    runIndividualTest("core/property", "maxCount-001", null, "KEEP");
  }

  @Test
  public void testRunTestSuite6() throws Exception {
    runIndividualTest("core/property", "minExclussive-001", null, "IGNORE");
    runIndividualTest("core/property", "minExclussive-001", null, "SHORTEN");
    runIndividualTest("core/property", "minExclussive-001", null, "KEEP");
  }

  @Test
  public void testRunTestSuite7() throws Exception {
    runIndividualTest("core/property", "hasValue-001", null, "IGNORE");
    runIndividualTest("core/property", "hasValue-001", null, "SHORTEN");
    runIndividualTest("core/property", "hasValue-001", null, "KEEP");
  }

  @Test
  public void testRunTestSuite7b() throws Exception {
    // unclear what would that mean on a pure LPG. How to identify a node? By id maybe?
    // runIndividualTest("core/property", "hasValue-001b", null, "IGNORE");
    runIndividualTest("core/property", "hasValue-001b", null, "SHORTEN");
    runIndividualTest("core/property", "hasValue-001b", null, "KEEP");
  }

  @Test
  public void testRunTestSuite8() throws Exception {
    runIndividualTest("core/property", "in-001", null, "IGNORE");
    runIndividualTest("core/property", "in-001", null, "SHORTEN");
    runIndividualTest("core/property", "in-001", null, "KEEP");
  }

  @Test
  public void testRunTestSuite8b() throws Exception {
    runIndividualTest("core/property", "in-001b", null, "IGNORE");
    runIndividualTest("core/property", "in-001b", null, "SHORTEN");
    runIndividualTest("core/property", "in-001b", null, "KEEP");
  }


  @Test
  public void testRunTestSuite9() throws Exception {
    runIndividualTest("core/property", "maxLength-001", null, "IGNORE");
    runIndividualTest("core/property", "maxLength-001", null, "SHORTEN");
    runIndividualTest("core/property", "maxLength-001", null, "KEEP");
  }

  @Test
  public void testRunTestSuite10() throws Exception {
    runIndividualTest("core/property", "minCount-001", null, "IGNORE");
    runIndividualTest("core/property", "minCount-001", null, "SHORTEN");
    runIndividualTest("core/property", "minCount-001", null, "KEEP");
  }

  @Test
  public void testRunTestSuite11() throws Exception {
    runIndividualTest("core/property", "nodeKind-001", null, "IGNORE");
    runIndividualTest("core/property", "nodeKind-001", null, "SHORTEN");
    runIndividualTest("core/property", "nodeKind-001", null, "KEEP");
  }

  public void runIndividualTest(String testGroupName, String testName,
      String cypherScript, String handleVocabUris) throws Exception {
    try (Driver driver = GraphDatabase.driver(neo4j.boltURI(),
        Config.builder().withoutEncryption().build())) {

      Session session = driver.session();
      Result getschemastatementsResults = session
          .run("call db.schemaStatements() yield name return name");
      if (getschemastatementsResults.hasNext() &&
          getschemastatementsResults.next().get("name").asString()
              .equals(UNIQUENESS_CONSTRAINT_ON_URI)) {
        //constraint exists. do nothing.
      } else {
        session.run("CREATE CONSTRAINT " + UNIQUENESS_CONSTRAINT_ON_URI
            + " ON ( resource:Resource ) ASSERT (resource.uri) IS UNIQUE ");
        assertTrue(session.run("call db.schemaStatements").hasNext());
      }

      //db is empty
      assertFalse(session.run("MATCH (n) RETURN n").hasNext());

      session.run("CALL n10s.graphconfig.init({ handleMultival: 'ARRAY'" +
          ", handleVocabUris: '" + handleVocabUris + "' })");

      //load data
      session.run(
          "CALL n10s.rdf.import.fetch(\"" + SHACLValidationProceduresTest.class.getClassLoader()
              .getResource("shacl/w3ctestsuite/" + testGroupName + "/" + testName + "-data.ttl")
              .toURI() + "\",\"Turtle\")");

      //load shapes
      Result results = session
          .run("CALL n10s.validation.shacl.import.fetch(\"" + SHACLValidationProceduresTest.class
              .getClassLoader()
              .getResource("shacl/w3ctestsuite/" + testGroupName + "/" + testName + "-shapes.ttl")
              .toURI() + "\",\"Turtle\", {})");

      //load shapes for test completeness
      session
          .run("CALL n10s.validation.shacl.import.fetch(\"" + SHACLValidationProceduresTest.class
              .getClassLoader()
              .getResource("shacl/w3ctestsuite/" + testGroupName + "/" + testName + "-shapes.ttl")
              .toURI() + "\",\"Turtle\", {})");
      //load expected results
      session.run("call n10s.validation.shacl.import.fetch('" + SHACLValidationProceduresTest.class
          .getClassLoader()
          .getResource("shacl/w3ctestsuite/" + testGroupName + "/" + testName + "-results.ttl")
          .toURI() + "','Turtle')");

      // query them in the graph and flatten the list
      Result expectedValidationResults = session.run(
          (handleVocabUris.equals("SHORTEN") || handleVocabUris.equals("KEEP"))
              ? VAL_RESULTS_QUERY_AS_RDF : VAL_RESULTS_QUERY);

      //print them out
      //System.out.println("expected: ");
      Set<ValidationResult> expectedResults = new HashSet<ValidationResult>();
      while (expectedValidationResults.hasNext()) {
        Record validationResult = expectedValidationResults.next();
        Object focusNode = ((handleVocabUris.equals("SHORTEN") || handleVocabUris.equals("KEEP"))
            ? validationResult.get("focus").asString() : validationResult.get("focus").asLong());
        String nodeType = validationResult.get("targetClass").asString();
        String propertyName = validationResult.get("path").asString();
        String severity = validationResult.get("sev").asString();
        String constraint = validationResult.get("constraint").asString();
        String message = validationResult.get("messge").asString();
        String shapeId = validationResult.get("shapeId").asString();
        String offendingValue = validationResult.get("offendingValue").asString();

        //TODO:  add the value to the results query and complete  below
        expectedResults
            .add(new ValidationResult(focusNode, nodeType, propertyName, severity, constraint,
                shapeId, message, offendingValue));

//        System.out.println("focusNode: " + focusNode + ", nodeType: " + nodeType + ",  propertyName: " +
//            propertyName + ", severity: " + severity + ", constraint: " + constraint
//            + ", offendingValue: " + offendingValue  + ", message: " + message);
      }

      // run validation
      Result actualValidationResults = session
          .run("call n10s.validation.shacl.validate() ");

      // print out validation results
      //System.out.println("actual: ");
      Set<ValidationResult> actualResults = new HashSet<ValidationResult>();
      while (actualValidationResults.hasNext()) {
        Record validationResult = actualValidationResults.next();
        Object focusNode = validationResult.get("focusNode").asObject();
        String nodeType = validationResult.get("nodeType").asString();
        String propertyName = validationResult.get("resultPath").asString();
        String severity = validationResult.get("severity").asString();
        Object offendingValue = validationResult.get("offendingValue").asObject();
        String constraint = validationResult.get("propertyShape").asString();
        String message = validationResult.get("resultMessage").asString();
        String shapeId = validationResult.get("shapeId").asString();
        actualResults
            .add(new ValidationResult(focusNode, nodeType, propertyName, severity, constraint,
                shapeId, message, offendingValue));

//        System.out.println("focusNode: " + focusNode + ", nodeType: " + nodeType + ",  propertyName: " +
//            propertyName + ", severity: " + severity + ", constraint: " + constraint
//            + ", offendingValue: " + offendingValue  + ", message: " + message);

      }

      //System.out.println("expected results size: " + expectedResults.size() +  " / " + "actual results size: " + actualResults.size() );
      assertEquals(expectedResults.size(), actualResults.size());

      for (ValidationResult x : expectedResults) {
        assertTrue(contains(actualResults, x));
      }

      for (ValidationResult x : actualResults) {
        assertTrue(contains(expectedResults, x));
      }

      //re-run it on set of nodes

      // run validation
      actualValidationResults = session
          .run("MATCH (n) with collect(n) as nodelist "
              + "call n10s.validation.shacl.validateSet(nodelist)"
              + " yield focusNode, nodeType, shapeId, propertyShape, offendingValue, resultPath, severity, resultMessage "
              + " return focusNode, nodeType, shapeId, propertyShape, offendingValue, resultPath, severity, resultMessage ");

      // print out validation results
      //System.out.println("actual on set of nodes: ");
      actualResults = new HashSet<ValidationResult>();
      while (actualValidationResults.hasNext()) {
        Record validationResult = actualValidationResults.next();
        Object focusNode = validationResult.get("focusNode").asObject();
        String nodeType = validationResult.get("nodeType").asString();
        String propertyName = validationResult.get("resultPath").asString();
        String severity = validationResult.get("severity").asString();
        Object offendingValue = validationResult.get("offendingValue").asObject();
        String constraint = validationResult.get("propertyShape").asString();
        String message = validationResult.get("resultMessage").asString();
        String shapeId = validationResult.get("shapeId").asString();
        actualResults
            .add(new ValidationResult(focusNode, nodeType, propertyName, severity, constraint,
                shapeId, message, offendingValue));

//        System.out.println("focusNode: " + focusNode + ", nodeType: " + nodeType + ",  propertyName: " +
//            propertyName + ", severity: " + severity + ", constraint: " + constraint
//            + ", offendingValue: " + offendingValue  + ", message: " + message);

      }

      //System.out.println("expected results size: " + expectedResults.size() +  " / " + "actual results size: " + actualResults.size() );
      assertEquals(expectedResults.size(), actualResults.size());

      for (ValidationResult x : expectedResults) {
        assertTrue(contains(actualResults, x));
      }

      for (ValidationResult x : actualResults) {
        assertTrue(contains(expectedResults, x));
      }

      session.run("MATCH (n) DETACH DELETE n ").hasNext();

    }


  }

  private boolean contains(Set<ValidationResult> set, ValidationResult res) {
    boolean contained = false;
    for (ValidationResult vr : set) {
      contained |= equivalentValidationResult(vr, res);
    }
//    if (!contained) {
//      System.out.println("Validation Result: " + res + "\nnot found in oposite set: " + set);
//    }
    return contained;
  }

  private boolean equivalentValidationResult(ValidationResult x, ValidationResult res) {
    return x.focusNode.equals(res.focusNode) && x.severity.equals(res.severity) && x.nodeType
        .equals(res.nodeType) && x.propertyShape.equals(res.propertyShape) && x.resultPath
        .equals(res.resultPath);
  }


}




