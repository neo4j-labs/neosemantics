package semantics.validation;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import org.junit.Rule;
import org.junit.Test;
import org.neo4j.driver.v1.Config;
import org.neo4j.driver.v1.Driver;
import org.neo4j.driver.v1.GraphDatabase;
import org.neo4j.driver.v1.Record;
import org.neo4j.driver.v1.Session;
import org.neo4j.driver.v1.StatementResult;
import org.neo4j.harness.junit.Neo4jRule;
import semantics.RDFImport;

public class SHACLValidationTest {

  final String VAL_RESULTS_QUERY = "MATCH (vr:sh__ValidationResult)\n"
      + "RETURN \n"
      + "       [(vr)-[:sh__sourceConstraintComponent]->(co) | co.uri ][0] as constraint,\n"
      + "       [(vr)-[:sh__resultPath]->()-[:sh__inversePath*0..1]->(p) where not (p)-->() | semantics.getIRILocalName(p.uri) ][0] as path,\n"
      + "       coalesce([(vr)-[:sh__sourceShape]->()<-[:sh__property*0..1]-()-[:sh__targetClass]->(tc)| semantics.getIRILocalName(tc.uri) ][0], \n"
      + "       [(vr)-[:sh__sourceShape]->()<-[:sh__property*0..1]-(tc:rdfs__Class)| semantics.getIRILocalName(tc.uri) ][0]) as targetClass,\n"
      + "       [(vr)-[:sh__focusNode]->(f) | f.uri ][0] as focus,\n"
      + "       [(vr)-[:sh__resultSeverity]->(sev) | sev.uri ][0]  as sev";

  //@Rule
  public Neo4jRule neo4j = new Neo4jRule()
      .withProcedure(SHACLValidation.class).withFunction(SHACLValidation.class)
      .withProcedure(RDFImport.class).withFunction(RDFImport.class);


  //@Test
  public void testRegexValidationOnMovieDB() throws Exception {
    try (Driver driver = GraphDatabase.driver(neo4j.boltURI(),
        Config.build().withEncryptionLevel(Config.EncryptionLevel.NONE).toConfig())) {

      Session session = driver.session();

      assertFalse(session.run("MATCH (n) RETURN n").hasNext());

      session.run(
          "CREATE (SleeplessInSeattle:Movie {title:'Sleepless in Seattle', released:1993, tagline:'What if someone you never met, someone you never saw, someone you never knew was the only someone for you?'})\n"
              +
              "CREATE (NoraE:Person {name:'Nora Ephron', born:1941})\n" +
              "CREATE (RitaW:Person {name:'Rita Wilson', born:1956})\n" +
              "CREATE (BillPull:Person {name:'Bill Pullman', born:1953})\n" +
              "CREATE (VictorG:Person {name:'Victor Garber', born:1949})\n" +
              "CREATE (RosieO:Person {name:\"Rosie O'Donnell\", born:1962})\n" +
              "CREATE\n" +
              "  (RitaW)-[:ACTED_IN {roles:['Suzy']}]->(SleeplessInSeattle),\n" +
              "  (BillPull)-[:ACTED_IN {roles:['Walter']}]->(SleeplessInSeattle),\n" +
              "  (VictorG)-[:ACTED_IN {roles:['Greg']}]->(SleeplessInSeattle),\n" +
              "  (RosieO)-[:ACTED_IN {roles:['Becky']}]->(SleeplessInSeattle),\n" +
              "  (NoraE)-[:DIRECTED]->(SleeplessInSeattle) ");

      session.run("CREATE (:NamespacePrefixDefinition {\n" +
          "  `http://www.w3.org/1999/02/22-rdf-syntax-ns#`: \"rdf\",\n" +
          "  `http://www.w3.org/2002/07/owl#`: \"owl\",\n" +
          "  `http://www.w3.org/ns/shacl#`: \"sh\",\n" +
          "  `http://www.w3.org/2000/01/rdf-schema#`: \"rdfs\"\n" +
          "})");

      session.run("CREATE INDEX ON :Resource(uri) ");

      session.run("CALL semantics.importRDF(\"" + SHACLValidationTest.class.getClassLoader()
          .getResource("shacl/person2-shacl.ttl")
          .toURI() + "\",\"Turtle\", {})");

      //StatementResult validationResults = session.run("CALL semantics.validation.shaclValidate() ");

      StatementResult validationResults = session.run("MATCH (p:Person) WITH collect(p) as nodes "
          + "CALL semantics.validation.shaclValidateTx(nodes) yield nodeId, nodeType, shapeId, propertyShape, offendingValue, propertyName  "
          + "RETURN nodeId, nodeType, shapeId, propertyShape, offendingValue, propertyName ");

      assertEquals(true, validationResults.hasNext());

      //TODO: complete this with additional checks

      assertEquals("Rosie O'Donnell", validationResults.next().get("offendingValue").asString());

      assertEquals(false, validationResults.hasNext());

    }
  }

  //@Test
  public void testTxTriggerValidation() throws Exception {
    try (Driver driver = GraphDatabase.driver(neo4j.boltURI(),
        Config.build().withEncryptionLevel(Config.EncryptionLevel.NONE).toConfig())) {

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

      session.run("CREATE (:NamespacePrefixDefinition {\n" +
          "  `http://www.w3.org/1999/02/22-rdf-syntax-ns#`: \"rdf\",\n" +
          "  `http://www.w3.org/2002/07/owl#`: \"owl\",\n" +
          "  `http://www.w3.org/ns/shacl#`: \"sh\",\n" +
          "  `http://www.w3.org/2000/01/rdf-schema#`: \"rdfs\"\n" +
          "})");

      session.run("CREATE INDEX ON :Resource(uri) ");

      session.run("CALL semantics.importRDF(\"" + SHACLValidationTest.class.getClassLoader()
          .getResource("shacl/person2-shacl.ttl")
          .toURI() + "\",\"Turtle\", {})");

      //StatementResult validationResults = session.run("CALL semantics.validation.shaclValidate() ");

      StatementResult validationResults = session.run("MATCH (p:Person) WITH collect(p) as nodes "
          + "call semantics.validation.shaclValidateTxForTrigger(nodes,[], {}, {}, {}) "
          + "yield nodeId, nodeType, shapeId, propertyShape, offendingValue, propertyName  "
              + "RETURN nodeId, nodeType, shapeId, propertyShape, offendingValue, propertyName");

      try {
        assertEquals(true, validationResults.hasNext());

        //Should not get here
        assertTrue(false);

      } catch (Exception e){
        //This is expected
        assertTrue(e.getMessage().contains("SHACLValidationException"));
      }

    }
  }


  //@Test
  public void testRunTestSuite() throws Exception{
    testRunIndividualTestInTestSuite("core/complex", "personexample", null);
    testRunIndividualTestInTestSuite("core/path", "path-inverse-001", null);
    testRunIndividualTestInTestSuite("core/property", "datatype-001",
        "MATCH (n { uri: 'http://datashapes.org/sh/tests/core/property/datatype-001.test#ValidResource'})"
            + "SET n.dateProperty = [date(n.dateProperty[0])]");
    //testRunIndividualTestInTestSuite("core/property", "datatype-002", null);
    testRunIndividualTestInTestSuite("core/property", "maxCount-001", null);
    testRunIndividualTestInTestSuite("core/property", "minExclussive-001", null);
  }


  public void testRunIndividualTestInTestSuite(String testGroupName, String testName,
      String cypherScript ) throws Exception{
    try (Driver driver = GraphDatabase.driver(neo4j.boltURI(),
        Config.build().withEncryptionLevel(Config.EncryptionLevel.NONE).toConfig())) {


      Session session = driver.session();

      //db is empty
      assertFalse(session.run("MATCH (n) RETURN n").hasNext());

      //load shapes
      session.run("CREATE INDEX ON :Resource(uri) ");
      session.run("call semantics.importRDF('" + SHACLValidationTest.class.getClassLoader()
          .getResource("shacl/w3ctestsuite/" + testGroupName +"/" + testName + "-data.ttl")
          .toURI() + "','Turtle', { handleVocabUris: 'IGNORE', handleMultival: 'ARRAY' })");

      //load data
      session.run("call semantics.importRDF('" + SHACLValidationTest.class.getClassLoader()
          .getResource("shacl/w3ctestsuite/" + testGroupName +"/" + testName + "-shapes.ttl")
          .toURI() + "','Turtle')");

      //Run any additional change to modify the imported RDF into LPG
      if (cypherScript != null ) {
        session.run(cypherScript);
      }

      // run validation
      StatementResult actualValidationResults = session.run("CALL semantics.validation.shaclValidate() ");

      // print out validation results
      //System.out.println("actual: ");
      Set<ValidationResult> actualResults = new HashSet<ValidationResult>();
      while(actualValidationResults.hasNext()){
        Record validationResult = actualValidationResults.next();
        Map<String, Object> params = new HashMap<String, Object>();
        params.put("id",validationResult.get("nodeId"));
        String focusNode = session.run("MATCH (n) WHERE ID(n) = $id RETURN n.uri as uri ", params).next()
            .get("uri").asString();
        String nodeType = validationResult.get("nodeType").asString();
        String propertyName = validationResult.get("propertyName").asString();
        String severity = validationResult.get("severity").asString();
        String constraint = validationResult.get("propertyShape").asString();
        actualResults.add(new ValidationResult(focusNode, nodeType, propertyName, severity, constraint));

//        System.out.println("focusNode: " + focusNode + ", nodeType: " + nodeType + ",  propertyName: " +
//            propertyName + ", severity: " + severity + ", constraint: " + constraint);

      }

      //load expected results
      session.run("call semantics.importRDF('" + SHACLValidationTest.class.getClassLoader()
          .getResource("shacl/w3ctestsuite/" + testGroupName +"/" + testName + "-results.ttl")
          .toURI() + "','Turtle')");

      // query them in the graph and flatten the list
      StatementResult expectedValidationResults = session.run(VAL_RESULTS_QUERY);

      //print them out
      //System.out.println("expected: ");
      Set<ValidationResult> expectedResults = new HashSet<ValidationResult>();
      while(expectedValidationResults.hasNext()){
        Record validationResult = expectedValidationResults.next();
        String focusNode = validationResult.get("focus").asString();
        String nodeType = validationResult.get("targetClass").asString();
        String propertyName = validationResult.get("path").asString();
        String severity = validationResult.get("sev").asString();
        String constraint = validationResult.get("constraint").asString();

        expectedResults.add(new ValidationResult(focusNode, nodeType, propertyName, severity, constraint));

//        System.out.println("focusNode: " + focusNode + ", nodeType: " + nodeType + ",  propertyName: " +
//            propertyName + ", severity: " + severity + ", constraint: " + constraint);
      }

      assertEquals(expectedResults.size(),actualResults.size());

      for (ValidationResult x:expectedResults) {
        //System.out.println("about to compare: " + x );
        assertTrue(contains(actualResults,x));
      }

      for (ValidationResult x:actualResults) {
        //System.out.println("about to compare: " + x );
        assertTrue(contains(expectedResults,x));
      }

      session.run("MATCH (n) DETACH DELETE n ").hasNext();

    }



  }

  private boolean contains(Set<ValidationResult> set, ValidationResult res) {
    boolean contained = false;
    for (ValidationResult vr:set) {
      contained |= reasonablyEqual(vr,res);
    }
    if (!contained) {
      System.out.println("Validation Result: " + res + "\nnot found in oposite set: " + set );
    }
    return contained;
  }

  private boolean reasonablyEqual(ValidationResult x, ValidationResult res) {
    return x.nodeId.equals(res.nodeId) && x.severity.equals(res.severity) && x.nodeType.equals(res.nodeType) && x.propertyShape.equals(res.propertyShape);
  }


}




