package n10s.validation;

import static n10s.CommonProcedures.UNIQUENESS_CONSTRAINT_ON_URI;
import static n10s.CommonProcedures.UNIQUENESS_CONSTRAINT_STATEMENT;
import static n10s.validation.SHACLValidator.SHACL_COUNT_CONSTRAINT_COMPONENT;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static n10s.graphconfig.Params.WKTLITERAL_URI;

import java.time.LocalDate;
import java.time.ZonedDateTime;
import java.util.*;

import n10s.aux.AuxProcedures;
import n10s.graphconfig.GraphConfigProcedures;
import n10s.nsprefixes.NsPrefixDefProcedures;
import n10s.rdf.RDFProcedures;
import n10s.rdf.load.RDFLoadProcedures;
import org.eclipse.rdf4j.model.impl.SimpleValueFactory;
import org.eclipse.rdf4j.model.vocabulary.SHACL;
import org.junit.*;
import org.neo4j.driver.*;
import org.neo4j.driver.Record;
import org.neo4j.driver.types.Node;
import org.neo4j.harness.junit.rule.Neo4jRule;

public class SHACLTestSuiteCTest {

  final String VAL_RESULTS_QUERY_ON_IGNORE_GRAPH = "MATCH (vr:ValidationResult)\n" +
          "RETURN\n" +
          "        [(vr)-[:sourceConstraintComponent]->(co) | co.uri ][0] as constraint,\n" +
          "       [(vr)-[:resultPath]->()-[:inversePath*0..1]->(p) where not (p)-->() | n10s.rdf.getIRILocalName(p.uri) ][0] as path,\n" +
          "coalesce([(vr)-[:sourceShape]->()<-[:property*0..1]-()-[:targetClass]->(tc)| n10s.rdf.getIRILocalName(tc.uri) ][0], \n" +
          "       [(vr)-[:sourceShape]->()<-[:property*0..1]-(tc:Class)| n10s.rdf.getIRILocalName(tc.uri) ][0]) as targetClass,\n" +
          "       [(vr)-[:sourceShape]->(ss)| ss.uri ][0] as shapeId,\n" +
          "       [(vr)-[:focusNode]->(f) | f.uri ][0] as focus,\n" +
          "       [(vr)-[:resultSeverity]->(sev) | sev.uri ][0]  as sev,toString(([(vr)-[:value]->(x)| x.uri] + ([] + coalesce(vr.value,[])))[0]) as offendingValue , " +
          "      vr.resultMessage as message";


  final String VAL_RESULTS_QUERY_ON_SHORTEN_GRAPH = "MATCH (vr:sh__ValidationResult)\n"
      + "RETURN \n"
      + "       [(vr)-[:sh__sourceConstraintComponent]->(co) | co.uri ][0] as constraint,\n"
      + "       [(vr)-[:sh__resultPath]->()-[:sh__inversePath*0..1]->(p) where not (p)-->() | p.uri ][0] as path,\n"
      + "       coalesce([(vr)-[:sh__sourceShape]->()<-[:sh__property*0..1]-()-[:sh__targetClass]->(tc)| tc.uri ][0], \n"
      + "       [(vr)-[:sh__sourceShape]->()<-[:sh__property*0..1]-(tc:rdfs__Class)| tc.uri ][0]) as targetClass,\n"
      + "       [(vr)-[:sh__sourceShape]->(ss)| ss.uri ][0] as shapeId,\n"
      + "       [(vr)-[:sh__focusNode]->(f) | f.uri ][0] as focus,\n"
      + "       [(vr)-[:sh__resultSeverity]->(sev) | sev.uri ][0]  as sev, "
      + "       toString(([(vr)-[:sh__value]->(x)| x.uri] + ([] + coalesce(vr.sh__value,[])))[0]) as offendingValue, "
          + "      vr.sh__resultMessage as message";

  final String VAL_RESULTS_QUERY_ON_KEEP_GRAPH = "MATCH (vr:`http://www.w3.org/ns/shacl#ValidationResult`)\n"
          + "RETURN \n"
          + "       [(vr)-[:`http://www.w3.org/ns/shacl#sourceConstraintComponent`]->(co) | co.uri ][0] as constraint,\n"
          + "       [(vr)-[:`http://www.w3.org/ns/shacl#resultPath`]->()-[:`http://www.w3.org/ns/shacl#inversePath`*0..1]->(p) where not (p)-->() | p.uri ][0] as path,\n"
          + "       coalesce([(vr)-[:`http://www.w3.org/ns/shacl#sourceShape`]->()<-[:`http://www.w3.org/ns/shacl#property`*0..1]-()-[:`http://www.w3.org/ns/shacl#targetClass`]->(tc)| tc.uri ][0], \n"
          + "       [(vr)-[:`http://www.w3.org/ns/shacl#sourceShape`]->()<-[:`http://www.w3.org/ns/shacl#property`*0..1]-(tc:`http://www.w3.org/2000/01/rdf-schema#Class`)| tc.uri ][0]) as targetClass,\n"
          + "       [(vr)-[:`http://www.w3.org/ns/shacl#sourceShape`]->(ss)| ss.uri ][0] as shapeId,\n"
          + "       [(vr)-[:`http://www.w3.org/ns/shacl#focusNode`]->(f) | f.uri ][0] as focus,\n"
          + "       [(vr)-[:`http://www.w3.org/ns/shacl#resultSeverity`]->(sev) | sev.uri ][0]  as sev, "
          + "       toString(([(vr)-[:`http://www.w3.org/ns/shacl#value`]->(x)| x.uri] + ([] + coalesce(vr.`http://www.w3.org/ns/shacl#value`,[])))[0]) as offendingValue, "
          + "      vr.`http://www.w3.org/ns/shacl#resultMessage` as message";

  public static Driver driver;

  @ClassRule
  public static Neo4jRule neo4j = new Neo4jRule()
          .withProcedure(ValidationProcedures.class)
          .withProcedure(GraphConfigProcedures.class)
          .withProcedure(RDFLoadProcedures.class)
          .withFunction(RDFProcedures.class)
          .withProcedure(NsPrefixDefProcedures.class)
          .withFunction(AuxProcedures.class);

  @BeforeClass
  public static void init() {
    driver = GraphDatabase.driver(neo4j.boltURI(),
            Config.builder().withoutEncryption().build());
  }

  @Before
  public void cleanDatabase() {
    driver.session().run("match (n) detach delete n").consume();
    driver.session().run("drop constraint n10s_unique_uri if exists").consume();
  }

  final String CREATE_N10S_CONSTRAINT = "CREATE CONSTRAINT n10s_unique_uri FOR ( resource:Resource ) REQUIRE (resource.uri) IS UNIQUE";

  @Test
  public void testRunTestSuite12() throws Exception {
    runIndividualTest("core/property", "pattern-query-001", null, "IGNORE");
    runIndividualTest("core/property", "pattern-query-001", null, "SHORTEN","pattern-query-001-shorten");
    runIndividualTest("core/property", "pattern-query-001", null, "KEEP","pattern-query-001-keep");
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
  public void testRunTestSuite10b() throws Exception {
    runIndividualTest("core/property", "minCount-001b", null, "IGNORE");
    runIndividualTest("core/property", "minCount-001b", null, "SHORTEN");
    runIndividualTest("core/property", "minCount-001b", null, "KEEP");
  }

  @Test
  public void testRunTestSuiteCountQueryBased() throws Exception {
    runIndividualTest("core/property", "count-query-001", null, "IGNORE");
    runIndividualTest("core/property", "count-query-001", null, "SHORTEN", "count-query-001-shorten");
    runIndividualTest("core/property", "count-query-001", null, "KEEP", "count-query-001-keep");
  }

  @Test
  public void testRunTestSuite11() throws Exception {
    runIndividualTest("core/property", "nodeKind-001", null, "IGNORE");
    runIndividualTest("core/property", "nodeKind-001", null, "SHORTEN");
    runIndividualTest("core/property", "nodeKind-001", null, "KEEP");
  }

  @Test
  public void testRunTestSuite13() throws Exception {
    runIndividualTest("core/other", "closed-shape-query-001", null, "IGNORE");
    runIndividualTest("core/other", "closed-shape-query-001", null, "SHORTEN","closed-shape-query-001-shorten");
    runIndividualTest("core/other", "closed-shape-query-001", null, "KEEP","closed-shape-query-001-keep");
  }

  @Test
  public void testRunTestSuite14() throws Exception {
    runIndividualTest("core/other", "required-excluded-query-001", null, "IGNORE");
    runIndividualTest("core/other", "required-excluded-query-001", null, "SHORTEN","required-excluded-query-001-shorten");
    runIndividualTest("core/other", "required-excluded-query-001", null, "KEEP","required-excluded-query-001-keep");
  }

  @Test
  public void testRunTestSuite15() throws Exception {
    runIndividualTest("core/property", "disjoint-001", null, "IGNORE");
    runIndividualTest("core/property", "disjoint-001", null, "SHORTEN");
    runIndividualTest("core/property", "disjoint-001", null, "KEEP");
  }

  @Test
  public void testRunTestSuite16() throws Exception {
    runIndividualTest("core/property", "disjoint-002", null, "IGNORE");
    runIndividualTest("core/property", "disjoint-002", null, "SHORTEN");
    runIndividualTest("core/property", "disjoint-002", null, "KEEP");
  }

  @Test
  public void testRunTestSuite17() throws Exception {
    runIndividualTest("core/property", "disjoint-query-001", null, "IGNORE");
    runIndividualTest("core/property", "disjoint-query-001", null, "SHORTEN", "disjoint-query-001-shorten");
    runIndividualTest("core/property", "disjoint-query-001", null, "KEEP", "disjoint-query-001-keep");
  }

  public void runIndividualTest(String testGroupName, String testName,
      String cypherScript, String handleVocabUris, String ... overrideShapesFileName) throws Exception {

      Session session = driver.session();
      Result getschemastatementsResults = session
          .run("show unique constraints yield name");
      if (getschemastatementsResults.hasNext() &&
          getschemastatementsResults.next().get("name").asString()
              .equals(UNIQUENESS_CONSTRAINT_ON_URI)) {
        //constraint exists. do nothing.
      } else {
        session.run(UNIQUENESS_CONSTRAINT_STATEMENT);
        assertTrue(session.run("show unique constraints yield name").hasNext());
      }

      //db is empty
      assertFalse(session.run("MATCH (n) RETURN n").hasNext());

      session.run("CALL n10s.graphconfig.init({ handleRDFTypes: 'LABELS_AND_NODES', handleMultival: 'ARRAY'" +
          ", handleVocabUris: '" + handleVocabUris + "' })");

      //load data
      Result dataImportResults = session.run(
              "CALL n10s.rdf.import.fetch(\"" + SHACLTestSuiteCTest.class.getClassLoader()
                      .getResource("shacl/w3ctestsuite/" + testGroupName + "/" + testName + "-data.ttl")
                      .toURI() + "\",\"Turtle\")");

      assertTrue(dataImportResults.hasNext());

      assertTrue(dataImportResults.next().get("triplesLoaded").asLong() > 0);

      //load shapes
      Result shapesLoadResults = session
          .run("CALL n10s.validation.shacl.import.fetch(\"" + SHACLTestSuiteCTest.class
              .getClassLoader()
              .getResource("shacl/w3ctestsuite/" + testGroupName + "/" + (overrideShapesFileName.length>0?overrideShapesFileName[0]:testName) + "-shapes.ttl")
              .toURI() + "\",\"Turtle\", {})");


      assertTrue(shapesLoadResults.hasNext());

      //load shapes for test completeness
      Result loadShapesAsNodes = session
          .run("CALL n10s.rdf.import.fetch(\"" + SHACLTestSuiteCTest.class
              .getClassLoader()
              .getResource("shacl/w3ctestsuite/" + testGroupName + "/" + testName + "-shapes.ttl")
              .toURI() + "\",\"Turtle\", {})");

      assertTrue(loadShapesAsNodes.hasNext());

      assertTrue(loadShapesAsNodes.next().get("triplesLoaded").asLong() > 0);

      //load expected results
      Result resultsLoadResult = session.run("call n10s.rdf.import.fetch('" + SHACLTestSuiteCTest.class
              .getClassLoader()
              .getResource("shacl/w3ctestsuite/" + testGroupName + "/" + testName + "-results.ttl")
              .toURI() + "','Turtle')");

      assertTrue(resultsLoadResult.hasNext());

      assertTrue(resultsLoadResult.next().get("triplesLoaded").asLong() > 0);

      // query them in the graph and flatten the list
      Result expectedValidationResults = session.run(selectQuery(handleVocabUris));

      assertTrue(expectedValidationResults.hasNext());


      Set<ValidationResult> expectedResults = new HashSet<ValidationResult>();
      while (expectedValidationResults.hasNext()) {
        Record validationResult = expectedValidationResults.next();
        Object focusNode = ((handleVocabUris.equals("SHORTEN") || handleVocabUris.equals("KEEP"))
            ? validationResult.get("focus").asString() : validationResult.get("focus").asString());
        String nodeType = validationResult.get("targetClass").isNull()?"":validationResult.get("targetClass").asString();
        String propertyName = validationResult.get("path").asString();
        String severity = validationResult.get("sev").asString();
        String constraint = validationResult.get("constraint").asString();
        String message = validationResult.get("message").isNull()? "": validationResult.get("message").asList().iterator().next().toString();
        String shapeId = validationResult.get("shapeId").asString();
        Object offendingValue = validationResult.get("offendingValue").asObject();
        String customMsg = validationResult.get("customMsg").isNull()? "": validationResult.get("customMsg").asList().iterator().next().toString();

        //TODO: Remove this
        expectedResults
            .add(new ValidationResult(focusNode, nodeType, propertyName, severity, constraint,
                shapeId, message, customMsg, offendingValue));
      }

      // run validation
      Result actualValidationResults = session
          .run("call n10s.validation.shacl.validate() ");

      assertTrue(actualValidationResults.hasNext());

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
        String customMsg = validationResult.get("customMsg").isNull()? "": validationResult.get("customMsg").asList().iterator().next().toString();
        //TODO: remove this
        actualResults
            .add(new ValidationResult(focusNode, nodeType, propertyName, severity, constraint,
                shapeId, message, customMsg, offendingValue));
      }

      //when using labels_and_nodes there might be "duplicates" in the results (one for the label and one for the type)
      assertEquals(expectedResults.size() , actualResults.size());

      for (ValidationResult x : expectedResults) {
        assertTrue(contains(actualResults, x));
      }

      for (ValidationResult x : actualResults) {
        assertTrue(contains(expectedResults, x));
      }

      //re-run it on set of nodes
      actualValidationResults = session
          .run("MATCH (n) with collect(n) as nodelist "
              + "call n10s.validation.shacl.validateSet(nodelist)"
              + " yield focusNode, nodeType, shapeId, propertyShape, offendingValue, resultPath, severity, resultMessage "
              + " return focusNode, nodeType, shapeId, propertyShape, offendingValue, resultPath, severity, resultMessage ");

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
        String customMsg = validationResult.get("customMsg").isNull()? "": validationResult.get("customMsg").asList().iterator().next().toString();
        actualResults
            .add(new ValidationResult(focusNode, nodeType, propertyName, severity, constraint,
                shapeId, message, customMsg, offendingValue));

      }

      //when using labels_and_nodes there might be "duplicates" in the results (one for the label and one for the type)
      assertEquals(expectedResults.size(), actualResults.size());

      for (ValidationResult x : expectedResults) {
        assertTrue(contains(actualResults, x));
      }

      for (ValidationResult x : actualResults) {
        assertTrue(contains(expectedResults, x));
      }

    //re-run it on empty set of nodes
    actualValidationResults = session
            .run("MATCH (n:NonExistingNodes) with collect(n) as nodelist "
                    + "call n10s.validation.shacl.validateSet(nodelist)"
                    + " yield focusNode, nodeType, shapeId, propertyShape, offendingValue, resultPath, severity, resultMessage "
                    + " return focusNode, nodeType, shapeId, propertyShape, offendingValue, resultPath, severity, resultMessage ");

    //no results expected as running on empty set of nodes
    assertFalse(actualValidationResults.hasNext());

      session.run("MATCH (n) DETACH DELETE n ").hasNext();
  }

  private String selectQuery(String handleVocabUris) {
    if(handleVocabUris.equals("SHORTEN")) {
      return VAL_RESULTS_QUERY_ON_SHORTEN_GRAPH ;
    } else if (handleVocabUris.equals("KEEP")){
      return VAL_RESULTS_QUERY_ON_KEEP_GRAPH ;
    } else {
      // "IGNORE"
      return VAL_RESULTS_QUERY_ON_IGNORE_GRAPH ;
    }
  }

  private boolean contains(Set<ValidationResult> set, ValidationResult res) {
    boolean contained = false;
    for (ValidationResult vr : set) {
      contained |= equivalentValidationResult(vr, res);
    }
    return contained;
  }

  private boolean equivalentValidationResult(ValidationResult x, ValidationResult res) {
    return x.focusNode.equals(res.focusNode) && x.severity.equals(res.severity) &&
            equivalentNodeTypes(x.nodeType,res.nodeType) && x.propertyShape.equals(res.propertyShape) && x.resultPath
        .equals(res.resultPath) && equivalentOffendingValues(x.offendingValue, res.offendingValue);
  }

  private boolean equivalentNodeTypes(String a, String b) {
    if ((a.equals("[all nodes]") || a.equals("[query-based selection]")) && b.equals("")) {
      return true ;
    } else if ((b.equals("[all nodes]") || b.equals("[query-based selection]")) && a.equals("")){
      return true;
    }
    return a.equals(b);
  }

  private boolean equivalentOffendingValues(Object a, Object b) {
    if(a==null && b==null){
      return true;
    } else if (a!=null && b!=null) {
      if (a instanceof Collection<?>){
        a = ((Collection<?>) a).iterator().next();
      }
      if (b instanceof Collection<?>){
        b = ((Collection<?>) b).iterator().next();
      }
      return getLocalPart(a.toString()).equals(getLocalPart(b.toString()));
    } else {
      return false;
    }
  }

  private String getLocalPart(String str) {
    if (str.indexOf(58) < 0) {
      int index = str.indexOf("__");
      if( index < 0 ){
        return str;
      } else{
        return str.substring(index + 2);
      }
    } else {
      return SimpleValueFactory.getInstance().createIRI(str).getLocalName();
    }
  }

}
