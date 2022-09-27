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
import org.junit.Rule;
import org.junit.Test;
import org.neo4j.driver.*;
import org.neo4j.driver.Record;
import org.neo4j.driver.types.Node;
import org.neo4j.harness.junit.rule.Neo4jRule;

public class SHACLValidationProceduresTest {

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

  @Rule
  public Neo4jRule neo4j = new Neo4jRule()
      .withProcedure(ValidationProcedures.class).withProcedure(GraphConfigProcedures.class)
      .withProcedure(RDFLoadProcedures.class).withFunction(RDFProcedures.class).withProcedure(
          NsPrefixDefProcedures.class).withFunction(AuxProcedures.class);


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
              + "@prefix n4sch: <neo4j://graph.schema#> .\n"
              + "@prefix sh: <http://www.w3.org/ns/shacl#> .\n"
              + "\n"
              + "ex:PersonShape\n"
              + "\ta sh:NodeShape ;\n"
              + "\tsh:targetClass n4sch:Person ;    # Applies to all Person nodes in Neo4j\n"
              + "\tsh:property [\n"
              + "\t\tsh:path n4sch:name ;           # constrains the values of n4sch:name\n"
              + "\t\tsh:maxCount 1 ;                # cardinality\n"
              + "\t\tsh:datatype xsd:string ;       # data type\n"
              + "\t] ;\n"
              + "\tsh:property [\n"
              + "\t\tsh:path n4sch:ACTED_IN ;       # constrains the values of n4sch:ACTED_IN\n"
              + "\t\tsh:class n4sch:Movie ;         # range\n"
              + "\t\tsh:nodeKind sh:IRI ;           # type of property\n"
              + "\t\tsh:severity sh:Warning ;\n"
              + "\t] ;\n"
              + "\tsh:closed true ;\n"
              + "\tsh:ignoredProperties ( n4sch:born n4sch:DIRECTED n4sch:FOLLOWS n4sch:REVIEWED n4sch:PRODUCED n4sch:WROTE ) .\n"
              + "\n"
              + "\n"
              + "n4sch:Movie\n"
              + "\ta sh:NodeShape , rdfs:Class;\n"
              + "\tsh:property [\n"
              + "\t\tsh:path n4sch:title ;           # constrains the values of n4sch:title\n"
              + "\t\tsh:maxCount 1 ;                 # cardinality\n"
              + "\t\tsh:datatype xsd:string ;        # data type\n"
              + "\t\tsh:minLength 10 ;               # string length\n"
              + "\t\tsh:maxLength 18 ;               # string length\n"
              + "\t] ;\n"
              + "\tsh:property [\n"
              + "\t\tsh:path n4sch:released ;        # constrains the values of n4sch:title\n"
              + "\t\tsh:datatype xsd:integer ;       # data type\n"
              + "\t\tsh:nodeKind sh:Literal ;        # type of property\n"
              + "        sh:minInclusive 2000 ;      # numeric range\n"
              + "        sh:maxInclusive 2019 ;      # numeric range\n"
              + "\t] ;\n"
              + "\tsh:closed true ;\n"
              + "\tsh:ignoredProperties ( n4sch:tagline ) .";

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
  public void testUriWithWhitespaces() throws Exception {
    try (Driver driver = GraphDatabase.driver(neo4j.boltURI(),
            Config.builder().withoutEncryption().build())) {

      Session session = driver.session();

      String SHACL_URI_WHITESPACES = "@prefix neo4j: <neo4j://graph.schema#> .\n" +
              "  @prefix sh: <http://www.w3.org/ns/shacl#> .\n" +
              "  @prefix xsd: <http://www.w3.org/2001/XMLSchema> .\n" +
              "  @prefix rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#> .\n" +
              " @prefix sp: <neo4j://graph.schema#Soccer > .\n" +
              "\n" +
              "  neo4j:PersonFooShape a sh:NodeShape ;\n" +
              "    sh:targetClass sp:Player ;\n" +
              "    sh:property [\n" +
              "      sh:path neo4j:gender ;\n" +
              "      sh:datatype xsd:string ;\n" +
              "      sh:not [ sh:hasValue rdf:nil ];\n" +
              "      sh:in (\"female\" \"male\");\n" +
              "    ];\n" +
              "." ;

      Result results = session
              .run(
                      "CALL n10s.validation.shacl.import.inline('" + SHACL_URI_WHITESPACES + "',\"Turtle\", {" +
                              "verifyUriSyntax: false })");

      assertTrue(results.hasNext());

      results = session.run("call n10s.validation.shacl.listShapes() yield target return collect(distinct target) as t");
      assertTrue(results.hasNext());
      List<Object> targetClasses = results.next().get("t").asList();
      assertTrue(targetClasses.size()==1);
      assertEquals("Soccer Player", targetClasses.get(0));
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
            .equals(SHACL.MIN_COUNT_CONSTRAINT_COMPONENT.stringValue())||
                next.get("propertyShape").asString()
                        .equals(SHACL_COUNT_CONSTRAINT_COMPONENT)) {
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

  String SHAPES_CLOSED_NO_EXCLUSION = "@prefix rdf:   <http://www.w3.org/1999/02/22-rdf-syntax-ns#> .\n" +
          "@prefix sh:    <http://www.w3.org/ns/shacl#> .\n" +
          "@prefix xsd:   <http://www.w3.org/2001/XMLSchema#> .\n" +
          "@prefix rdfs:  <http://www.w3.org/2000/01/rdf-schema#> .\n" +
          "@prefix ex:    <http://www.example.com/device#> .\n" +
          "@prefix owl:   <http://www.w3.org/2002/07/owl#> .\n" +
          "\n" +
          "\n" +
          "ex:SoftwareShape\n" +
          "    a sh:NodeShape ;\n" +
          "    sh:targetClass ex:Software ;\n" +
          "    sh:property [             \n" +
          "        sh:path ex:id;     \n" +
          "        sh:datatype xsd:string ;\n" +
          "        sh:maxCount 1 ;\n" +
          "        sh:minCount 1 ;\n" +
          "        sh:maxLength 255;\n" +
          "    ] ;\n" +
          "    sh:property [              \n" +
          "        sh:path ex:type;     \n" +
          "        sh:datatype xsd:string ;\n" +
          "        sh:maxCount 1 ;\n" +
          "        sh:minCount 1 ;\n" +
          "    ] ;\n" +
          "    sh:property [              \n" +
          "        sh:path ex:status;     \n" +
          "        sh:datatype xsd:string ;\n" +
          "        sh:maxCount 1 ;\n" +
          "        sh:minCount 1 ;\n" +
          "    ] ;\n" +
          "    sh:closed true ;\n" +
          "    .";

  String SHAPES_CLOSED_DATA = "<?xml version=\"1.0\"?>\n" +
          "<rdf:RDF xmlns=\"http://www.example.com/device#\"\n" +
          "     xml:base=\"http://www.example.com/device\"\n" +
          "     xmlns:owl=\"http://www.w3.org/2002/07/owl#\"\n" +
          "     xmlns:rdf=\"http://www.w3.org/1999/02/22-rdf-syntax-ns#\"\n" +
          "     xmlns:xml=\"http://www.w3.org/XML/1998/namespace\"\n" +
          "     xmlns:xsd=\"http://www.w3.org/2001/XMLSchema#\"\n" +
          "     xmlns:rdfs=\"http://www.w3.org/2000/01/rdf-schema#\"\n" +
          "     xmlns:device=\"http://www.example.com/device#\">\n" +
          " \n" +
          "\n" +
          "    <!-- http://ww.example.com/device#68288e7d-5e92-46e4-ac6b-7ce4710739d4 -->\n" +
          "\n" +
          "    <owl:NamedIndividual rdf:about=\"http://www.example.com/device#68288e7d-5e92-46e4-ac6b-7ce4710739d4\">\n" +
          "        <rdf:type rdf:resource=\"http://www.example.com/device#Software\"/>\n" +
          "        <id rdf:datatype=\"http://www.w3.org/2001/XMLSchema#string\">2</id>\n" +
          "        <status rdf:datatype=\"http://www.w3.org/2001/XMLSchema#string\">NEW</status>\n" +
          "        <type rdf:datatype=\"http://www.w3.org/2001/XMLSchema#string\">SCRIPT1</type>\n" +
          "    </owl:NamedIndividual>\n" +
          "</rdf:RDF>";

  @Test
  public void testClosedShapeIgnore() throws Exception {
    try (Driver driver = GraphDatabase.driver(neo4j.boltURI(),
            Config.builder().withoutEncryption().build()); Session session = driver.session()) {
      assertFalse(session.run("MATCH (n) RETURN n").hasNext());
      session.run("CREATE CONSTRAINT ON ( resource:Resource ) ASSERT (resource.uri) IS UNIQUE ");
      session.run("call n10s.graphconfig.init({handleRDFTypes:\"LABELS_AND_NODES\",handleMultival:\"ARRAY\"});\n");
      session.run("call n10s.nsprefixes.add(\"ex\", \"http://www.example.com/device#\");");
      session.run("CALL n10s.rdf.import.inline('" + SHAPES_CLOSED_DATA + "',\"RDF/XML\")");

    }
    try (Driver driver = GraphDatabase.driver(neo4j.boltURI(),
            Config.builder().withoutEncryption().build()); Session session = driver.session()) {

      Result results = session
              .run("CALL n10s.validation.shacl.import.inline(\"" + SHAPES_CLOSED_NO_EXCLUSION + "\",\"Turtle\")");

      assertTrue(results.hasNext());

      Result result = session.run("call n10s.validation.shacl.validate()");
      assertFalse(result.hasNext());
    }
  }

  @Test
  public void testClosedShapeNoExclusion() throws Exception {
    try (Driver driver = GraphDatabase.driver(neo4j.boltURI(),
            Config.builder().withoutEncryption().build()); Session session = driver.session()) {
      assertFalse(session.run("MATCH (n) RETURN n").hasNext());
      session.run("CREATE CONSTRAINT ON ( resource:Resource ) ASSERT (resource.uri) IS UNIQUE ");
      session.run("call n10s.graphconfig.init({handleRDFTypes:\"LABELS_AND_NODES\",handleMultival:\"ARRAY\"});\n");
      session.run("call n10s.nsprefixes.add(\"ex\", \"http://www.example.com/device#\");");
      session.run("CALL n10s.rdf.import.inline('" + SHAPES_CLOSED_DATA + "',\"RDF/XML\")");

    }
    try (Driver driver = GraphDatabase.driver(neo4j.boltURI(),
            Config.builder().withoutEncryption().build()); Session session = driver.session()) {

      Result results = session
              .run("CALL n10s.validation.shacl.import.inline(\"" + SHAPES_CLOSED_NO_EXCLUSION + "\",\"Turtle\")");

      assertTrue(results.hasNext());

      Result result = session.run("call n10s.validation.shacl.validate()");
      assertFalse(result.hasNext());
    }
  }

  String DATE_TYPE_CONSTRAINT = "@prefix neo4j: <http://adaptive.accenture.com/ontologies/o1#> .\n" +
          "  @prefix sh: <http://www.w3.org/ns/shacl#> .\n" +
          "\n" +
          "  neo4j:myShape a sh:NodeShape ;\n" +
          "    sh:targetClass neo4j:TestEntitype ;\n" +
          "    sh:property [            \n" +
          "        sh:path neo4j:testDate;   \n" +
          "        sh:datatype xsd:dateTime ;\n" +
          "        sh:maxCount 1 ;\n" +
          "        sh:minCount 1 ;\n" +
          "    ] ;\n" +
          ".";

  String DATE_DATA_1 =
          "<http://adaptive.accenture.com/ind#testIndividual> <http://adaptive.accenture.com/ontologies/o1#testDate> " +
                  "\"1956-06-25T10:00:00\"^^<http://www.w3.org/2001/XMLSchema#dateTime> . " +
          "\\n<http://adaptive.accenture.com/ind#testIndividual> <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> " +
                  "<http://adaptive.accenture.com/ontologies/o1#TestEntitype> .";

  String DATE_DATA_2 =
          "<http://adaptive.accenture.com/ind#testIndividual2> <http://adaptive.accenture.com/ontologies/o1#testDate> " +
                  "\"1956-06-25T10:00:00[Europe/Berlin]\"^^<http://www.w3.org/2001/XMLSchema#dateTime> . " +
          "\\n<http://adaptive.accenture.com/ind#testIndividual2> <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> " +
                  "<http://adaptive.accenture.com/ontologies/o1#TestEntitype> ." +
                  "\\n<http://adaptive.accenture.com/ind#testIndividual3> <http://adaptive.accenture.com/ontologies/o1#testDate> " +
                  "\"353\"^^<http://www.w3.org/2001/XMLSchema#integer> . " +
                  "\\n<http://adaptive.accenture.com/ind#testIndividual3> <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> " +
                  "<http://adaptive.accenture.com/ontologies/o1#TestEntitype> .";

  @Test
  public void testDataTypeShape() throws Exception {
    try (Driver driver = GraphDatabase.driver(neo4j.boltURI(),
            Config.builder().withoutEncryption().build()); Session session = driver.session()) {
      assertFalse(session.run("MATCH (n) RETURN n").hasNext());
      session.run("CREATE CONSTRAINT ON ( resource:Resource ) ASSERT (resource.uri) IS UNIQUE ");
      session.run("CALL n10s.graphconfig.init()");
      session.run("call n10s.nsprefixes.add(\"o1\",\"http://adaptive.accenture.com/ontologies/o1#\")");
      session.run("call n10s.nsprefixes.add(\"ind\",\"http://adaptive.accenture.com/ind#\")");
      session.run("CALL n10s.rdf.import.inline('" + DATE_DATA_1 + "',\"N-Triples\")");

    }
    try (Driver driver = GraphDatabase.driver(neo4j.boltURI(),
            Config.builder().withoutEncryption().build()); Session session = driver.session()) {

      Result results = session
              .run("CALL n10s.validation.shacl.import.inline(\"" + DATE_TYPE_CONSTRAINT + "\",\"Turtle\", {})");

      assertTrue(results.hasNext());

      Result result = session.run("call n10s.validation.shacl.validate()");
      assertFalse(result.hasNext());


      results = session.run("CALL n10s.rdf.import.inline('" + DATE_DATA_2 + "',\"N-Triples\")");
      assertTrue(results.hasNext());

      result = session.run("call n10s.validation.shacl.validate()");
      assertTrue(result.hasNext());

      int totalCount = 0;
      int datatypeConstCount = 0;
      while (result.hasNext()) {
        Record next = result.next();
        if (next.get("propertyShape").asString()
                .equals(SHACL.DATATYPE_CONSTRAINT_COMPONENT.stringValue())) {
          datatypeConstCount++;
          assertEquals(353,next.get("offendingValue").asInt());
          assertEquals("http://adaptive.accenture.com/ind#testIndividual3",next.get("focusNode").asString());
        }
        totalCount++;
      }
      assertEquals(1, totalCount);
      assertEquals(1, datatypeConstCount);
    }
  }

  String DATE_DATA_WHERE_PROP_IS_USED_AS_REL =
          "<http://adaptive.accenture.com/ind#testIndividual2> <http://adaptive.accenture.com/ontologies/o1#testDate> " +
                  "\"1956-06-25T10:00:00[Europe/Berlin]\"^^<http://www.w3.org/2001/XMLSchema#dateTime> . " +
                  "\\n<http://adaptive.accenture.com/ind#testIndividual2> <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> " +
                  "<http://adaptive.accenture.com/ontologies/o1#TestEntitype> ." +
                  "\\n<http://adaptive.accenture.com/ind#testIndividual3> <http://adaptive.accenture.com/ontologies/o1#testDate> " +
                  "<http://adaptive.accenture.com/ind#ADateForTestIndividual3> . " +
                  "\\n<http://adaptive.accenture.com/ind#testIndividual3> <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> " +
                  "<http://adaptive.accenture.com/ontologies/o1#TestEntitype> .";

  @Test
  public void testDataTypeShapeDataTypeRestrictedPropUsedAsRel() throws Exception {
    try (Driver driver = GraphDatabase.driver(neo4j.boltURI(),
            Config.builder().withoutEncryption().build()); Session session = driver.session()) {
      assertFalse(session.run("MATCH (n) RETURN n").hasNext());
      session.run("CREATE CONSTRAINT ON ( resource:Resource ) ASSERT (resource.uri) IS UNIQUE ");
      session.run("CALL n10s.graphconfig.init()");
      session.run("call n10s.nsprefixes.add(\"o1\",\"http://adaptive.accenture.com/ontologies/o1#\")");
      session.run("call n10s.nsprefixes.add(\"ind\",\"http://adaptive.accenture.com/ind#\")");
      session.run("CALL n10s.rdf.import.inline('" + DATE_DATA_WHERE_PROP_IS_USED_AS_REL + "',\"N-Triples\")");

    }
    try (Driver driver = GraphDatabase.driver(neo4j.boltURI(),
            Config.builder().withoutEncryption().build()); Session session = driver.session()) {

      Result results = session
              .run("CALL n10s.validation.shacl.import.inline(\"" + DATE_TYPE_CONSTRAINT + "\",\"Turtle\", {})");

      assertTrue(results.hasNext());

      Result result = session.run("call n10s.validation.shacl.validate()");

      int totalCount = 0;
      int datatypeConstCount = 0;
      while (result.hasNext()) {
        Record next = result.next();
        if (next.get("propertyShape").asString()
                .equals(SHACL.DATATYPE_CONSTRAINT_COMPONENT.stringValue())) {
          datatypeConstCount++;
          assertEquals("http://adaptive.accenture.com/ind#ADateForTestIndividual3",next.get("offendingValue").asString());
          assertEquals("http://adaptive.accenture.com/ind#testIndividual3",next.get("focusNode").asString());
        }
        totalCount++;
      }
      assertEquals(1, totalCount);
      assertEquals(1, datatypeConstCount);
    }
  }

  String DATE_TYPE_CONSTRAINT_DATE_AND_POINT = "@prefix neo4j: <http://adaptive.accenture.com/ontologies/o1#> .\n" +
          "  @prefix sh: <http://www.w3.org/ns/shacl#> .\n" +
          "\n" +
          "  neo4j:myShape a sh:NodeShape ;\n" +
          "    sh:targetClass neo4j:TestEntitype ;\n" +
          "    sh:property [            \n" +
          "        sh:path neo4j:testDate;   \n" +
          "        sh:datatype xsd:date ;\n" +
          "        sh:maxCount 1 ;\n" +
          "        sh:minCount 1 ;\n" +
          "    ] ;\n" +
          "    sh:property [            \n" +
          "        sh:path neo4j:testPoint;   \n" +
          "        sh:datatype <" + WKTLITERAL_URI.stringValue()+ "> ;\n" +
          "    ] ;\n" +
          ".";

  String DATE_DATA_POINT_AND_TYPE =
          "<http://adaptive.accenture.com/ind#testIndividual> <http://adaptive.accenture.com/ontologies/o1#testDate> " +
          "\"1956-06-25\"^^<http://www.w3.org/2001/XMLSchema#date> . " +
          "\\n<http://adaptive.accenture.com/ind#testIndividual> <http://adaptive.accenture.com/ontologies/o1#testPoint> " +
          "\"Point(-1.324 -5.354)\"^^<" + WKTLITERAL_URI.stringValue() + "> . " +
          "\\n<http://adaptive.accenture.com/ind#testIndividual> <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> " +
          "<http://adaptive.accenture.com/ontologies/o1#TestEntitype> ." +
          "\\n<http://adaptive.accenture.com/ind#testIndividual2> <http://adaptive.accenture.com/ontologies/o1#testDate> " +
          "\"1956-02-25T10:00:00.00[Europe/Berlin]\"^^<http://www.w3.org/2001/XMLSchema#dateTime> . " +
          "\\n<http://adaptive.accenture.com/ind#testIndividual2> <http://adaptive.accenture.com/ontologies/o1#testPoint> " +
          "\"1956-02-25T10:00:00.0000+01:00[Europe/Berlin]\"^^<http://www.w3.org/2001/XMLSchema#dateTime> . " +
          "\\n<http://adaptive.accenture.com/ind#testIndividual2> <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> " +
          "<http://adaptive.accenture.com/ontologies/o1#TestEntitype> ." +
          "\\n<http://adaptive.accenture.com/ind#testIndividual3> <http://adaptive.accenture.com/ontologies/o1#testDate> " +
          "\"353\"^^<http://www.w3.org/2001/XMLSchema#integer> . " +
          "\\n<http://adaptive.accenture.com/ind#testIndividual3> <http://adaptive.accenture.com/ontologies/o1#testPoint> " +
          "\"353\"^^<http://www.w3.org/2001/XMLSchema#integer> . " +
          "\\n<http://adaptive.accenture.com/ind#testIndividual3> <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> " +
          "<http://adaptive.accenture.com/ontologies/o1#TestEntitype> .";

  @Test
  public void testDataTypeShapeDataPointAndDate() throws Exception {
    try (Driver driver = GraphDatabase.driver(neo4j.boltURI(),
            Config.builder().withoutEncryption().build()); Session session = driver.session()) {
      assertFalse(session.run("MATCH (n) RETURN n").hasNext());
      session.run("CREATE CONSTRAINT ON ( resource:Resource ) ASSERT (resource.uri) IS UNIQUE ");
      session.run("CALL n10s.graphconfig.init()");
      session.run("call n10s.nsprefixes.add(\"o1\",\"http://adaptive.accenture.com/ontologies/o1#\")");
      session.run("call n10s.nsprefixes.add(\"ind\",\"http://adaptive.accenture.com/ind#\")");
      session.run("CALL n10s.rdf.import.inline('" + DATE_DATA_POINT_AND_TYPE + "',\"N-Triples\")");

    }
    try (Driver driver = GraphDatabase.driver(neo4j.boltURI(),
            Config.builder().withoutEncryption().build()); Session session = driver.session()) {


      Result results = session
              .run("CALL n10s.validation.shacl.import.inline(\"" + DATE_TYPE_CONSTRAINT_DATE_AND_POINT + "\",\"Turtle\", {})");

      assertTrue(results.hasNext());

      Result result = session.run("call n10s.validation.shacl.validate()");

      int totalCount = 0;
      int datatypeConstCount = 0;
      while (result.hasNext()) {
        Record next = result.next();
        if (next.get("propertyShape").asString()
                .equals(SHACL.DATATYPE_CONSTRAINT_COMPONENT.stringValue())) {
          datatypeConstCount++;
          if(next.get("focusNode").asString().equals("http://adaptive.accenture.com/ind#testIndividual2")) {
            if (next.get("resultPath").asString().equals("http://adaptive.accenture.com/ontologies/o1#testDate")||
                    next.get("resultPath").asString().equals("http://adaptive.accenture.com/ontologies/o1#testPoint")){
              assertEquals(ZonedDateTime.parse("1956-02-25T10:00:00.0000+01:00[Europe/Berlin]"), next.get("offendingValue").asZonedDateTime());
            } else {
              assertFalse(true); //we should not get here
            }
          } else if (next.get("focusNode").asString().equals("http://adaptive.accenture.com/ind#testIndividual3")) {
            if (next.get("resultPath").asString().equals("http://adaptive.accenture.com/ontologies/o1#testDate")||
                    next.get("resultPath").asString().equals("http://adaptive.accenture.com/ontologies/o1#testPoint")){
              assertEquals(353, next.get("offendingValue").asInt());
            } else {
              assertFalse(true); //we should not get here
            }
          } else {
            assertFalse(true); //we should not get here
          }
        }
        totalCount++;
      }
      assertEquals(4, totalCount);
      assertEquals(4, datatypeConstCount);
    }
  }

  String DATE_TYPE_CONSTRAINT_ANYURI = "@prefix neo4j: <http://adaptive.accenture.com/ontologies/o1#> .\n" +
          "  @prefix sh: <http://www.w3.org/ns/shacl#> .\n" +
          "\n" +
          "  neo4j:myShape a sh:NodeShape ;\n" +
          "    sh:targetClass neo4j:TestEntitype ;\n" +
          "    sh:property [            \n" +
          "        sh:path neo4j:testUri ;   \n" +
          "        sh:datatype xsd:anyURI ;\n" +
          "        sh:minCount 1 ;\n" +
          "    ] ;\n" +
          ".";

  String DATE_DATA_ANYURI =
          "<http://adaptive.accenture.com/ind#testIndividual> <http://adaptive.accenture.com/ontologies/o1#testUri> " +
                  "\"1956-06-25\"^^<http://www.w3.org/2001/XMLSchema#date> . " +
                  "\\n<http://adaptive.accenture.com/ind#testIndividual> <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> " +
                  "<http://adaptive.accenture.com/ontologies/o1#TestEntitype> ." +
                  "\\n<http://adaptive.accenture.com/ind#testIndividual2> <http://adaptive.accenture.com/ontologies/o1#testUri> " +
                  "\"http://www.example.com\" . " +
                  "\\n<http://adaptive.accenture.com/ind#testIndividual2> <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> " +
                  "<http://adaptive.accenture.com/ontologies/o1#TestEntitype> ." +
                  "\\n<http://adaptive.accenture.com/ind#testIndividual3> <http://adaptive.accenture.com/ontologies/o1#testUri> " +
                  "\"http://www.example with.whitespaces.com\" . " +
                  "\\n<http://adaptive.accenture.com/ind#testIndividual3> <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> " +
                  "<http://adaptive.accenture.com/ontologies/o1#TestEntitype> ." ;

  @Test
  public void testDataTypeShapeAnyUri() throws Exception {
    try (Driver driver = GraphDatabase.driver(neo4j.boltURI(),
            Config.builder().withoutEncryption().build()); Session session = driver.session()) {
      assertFalse(session.run("MATCH (n) RETURN n").hasNext());
      session.run("CREATE CONSTRAINT ON ( resource:Resource ) ASSERT (resource.uri) IS UNIQUE ");
      session.run("CALL n10s.graphconfig.init()");
      session.run("call n10s.nsprefixes.add(\"o1\",\"http://adaptive.accenture.com/ontologies/o1#\")");
      session.run("call n10s.nsprefixes.add(\"ind\",\"http://adaptive.accenture.com/ind#\")");
      session.run("CALL n10s.rdf.import.inline('" + DATE_DATA_ANYURI + "',\"N-Triples\")");

    }
    try (Driver driver = GraphDatabase.driver(neo4j.boltURI(),
            Config.builder().withoutEncryption().build()); Session session = driver.session()) {


      Result results = session
              .run("CALL n10s.validation.shacl.import.inline(\"" + DATE_TYPE_CONSTRAINT_ANYURI + "\",\"Turtle\", {})");

      assertTrue(results.hasNext());

      Result result = session.run("call n10s.validation.shacl.validate()");

      int totalCount = 0;
      int datatypeConstCount = 0;
      while (result.hasNext()) {
        Record next = result.next();
        if (next.get("propertyShape").asString()
                .equals(SHACL.DATATYPE_CONSTRAINT_COMPONENT.stringValue())) {
          datatypeConstCount++;
          if(next.get("resultPath").asString().equals("http://adaptive.accenture.com/ontologies/o1#testUri")) {
            if (next.get("focusNode").asString().equals("http://adaptive.accenture.com/ind#testIndividual")){
              assertEquals(LocalDate.parse("1956-06-25"), next.get("offendingValue").asLocalDate());
            } else if (next.get("focusNode").asString().equals("http://adaptive.accenture.com/ind#testIndividual3")){
              assertEquals("http://www.example with.whitespaces.com", next.get("offendingValue").asString());
            } else {
              assertFalse(true); //we should not get here
            }
          } else {
            assertFalse(true); //we should not get here
          }
        }
        totalCount++;
      }
      assertEquals(2, totalCount);
      assertEquals(2, datatypeConstCount);
    }
  }

  @Test
  public void testMusicExample() throws Exception {
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
            .equals(SHACL.MAX_COUNT_CONSTRAINT_COMPONENT.stringValue())||
                next.get("propertyShape").asString()
                        .equals(SHACL_COUNT_CONSTRAINT_COMPONENT)) {
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
          .getResource("shacl/person2lpg-shacl.ttl")
          .toURI() + "\",\"Turtle\", {})");

      Result validationResults = session.run("CALL n10s.validation.shacl.validate() ");

      assertEquals(true, validationResults.hasNext());

      while (validationResults.hasNext()) {
        Record next = validationResults.next();
        if (next.get("nodeType").asString().equals("Person")) {
          assertEquals("Rosie O'Donnell", next.get("offendingValue").asString());
          assertEquals("http://www.w3.org/ns/shacl#PatternConstraintComponent",
              next.get("propertyShape").asString());
        } else if (next.get("nodeType").asString().equals("Movie")) {
          assertEquals(1993, next.get("offendingValue").asInt());
          assertEquals("http://www.w3.org/ns/shacl#ValueRangeConstraintComponent",
              next.get("propertyShape").asString());
        }
      }

    }
  }

  @Test
  public void testBug213() throws Exception {
    try (Driver driver = GraphDatabase.driver(neo4j.boltURI(),
            Config.builder().withoutEncryption().build())) {

      Session session = driver.session();

      assertFalse(session.run("MATCH (n) RETURN n").hasNext());

      session.run("CREATE(s:Object) SET s.minVal = \"200\";");

      session.run("call n10s.validation.shacl.import.inline('\n" +
              "\n" +
              "@prefix dtc: <http://ttt/#> .\n" +
              "@prefix vs: <neo4j://graph.schema#>.\n" +
              "@prefix sh: <http://www.w3.org/ns/shacl#> .\n" +
              "\n" +
              "dtc:ObjectShape\n" +
              "    a sh:NodeShape;\n" +
              "    sh:targetClass vs:Object;    \n" +
              "    sh:property [            \n" +
              "        sh:path vs:minVal;\n" +
              "                sh:datatype xsd:integer ;\n" +
              "            sh:minInclusive 0;\n" +
              "            sh:maxInclusive 100;\n" +
              "    ] ;\n" +
              ".\n" +
              "\n" +
              "','Turtle')");

      Result validationResults = session.run("CALL n10s.validation.shacl.validate() ");

      assertEquals(true, validationResults.hasNext());

      Record next = validationResults.next();
      assertTrue(next.get("nodeType").asString().equals("Object"));
      assertTrue(next.get("resultPath").asString().equals("minVal"));
      assertEquals("200", next.get("offendingValue").asString());
      assertEquals("http://www.w3.org/ns/shacl#DatatypeConstraintComponent",
              next.get("propertyShape").asString());
      assertEquals("property value should be of type integer",
              next.get("resultMessage").asString());

      assertEquals(false, validationResults.hasNext());

      session.run("MATCH(s:Object) SET s.minVal = \"hello-world\";");

      validationResults = session.run("CALL n10s.validation.shacl.validate() ");

      assertEquals(true, validationResults.hasNext());

      next = validationResults.next();
      assertTrue(next.get("nodeType").asString().equals("Object"));
      assertTrue(next.get("resultPath").asString().equals("minVal"));
      assertEquals("hello-world", next.get("offendingValue").asString());
      assertEquals("http://www.w3.org/ns/shacl#DatatypeConstraintComponent",
              next.get("propertyShape").asString());
      assertEquals("property value should be of type integer",
              next.get("resultMessage").asString());

      assertEquals(false, validationResults.hasNext());

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
        assertTrue(e.getMessage().startsWith("Failed to invoke procedure `n10s.validation.shacl.import.fetch`: Caused by: n10s.utils.UriUtils$UriNamespaceHasNoAssociatedPrefix: Prefix Undefined: No prefix defined for namespace <neo4j://graph.schema#"));
      }
      session.run("CALL n10s.nsprefixes.add('neo','neo4j://graph.schema#')");
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
              .getResource("shacl/person2lpg-shacl.ttl")
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
      session.run("CALL n10s.nsprefixes.add('neo','neo4j://graph.schema#')");
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

      assertTrue(session.run("CALL n10s.graphconfig.init({ handleVocabUris: 'IGNORE' })").hasNext());
      result = session.run(
          "CALL n10s.validation.shacl.import.fetch(\"" + SHACLValidationProceduresTest.class
              .getClassLoader()
              .getResource("shacl/person2lpg-shacl.ttl")
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
        if ((next.get("target").asString().equals("neo4j://graph.schema#Person") &&
            next.get("propertyOrRelationshipPath").asString().equals("neo4j://graph.schema#ACTED_IN") &&
            next.get("param").asString().equals("sh:class") &&
            next.get("value").asString().equals("neo4j://graph.schema#Movie"))
            ||
            (next.get("target").asString().equals("neo4j://graph.schema#Movie") &&
                next.get("propertyOrRelationshipPath").asString().equals("neo4j://graph.schema#released") &&
                next.get("param").asString().equals("sh:minInclusive") &&
                next.get("value").asInt() == 2000)
            ||
            (next.get("target").asString().equals("neo4j://graph.schema#Movie") &&
                next.get("propertyOrRelationshipPath").asString().equals("neo4j://graph.schema#released") &&
                next.get("param").asString().equals("sh:datatype") &&
                next.get("value").asString().equals("http://www.w3.org/2001/XMLSchema#integer"))
            ||
            (next.get("target").asString().equals("neo4j://graph.schema#Person") &&
                next.get("propertyOrRelationshipPath").isNull() &&
                next.get("param").asString().equals("sh:ignoredProperties") &&
                next.get("value").asList(x -> x.asString()).equals(
                    List.of("neo4j://graph.schema#born", "neo4j://graph.schema#DIRECTED", "neo4j://graph.schema#FOLLOWS",
                        "neo4j://graph.schema#REVIEWED",
                        "neo4j://graph.schema#PRODUCED", "neo4j://graph.schema#WROTE")))) {
          matches++;
        }
      }
      assertEquals(4, matches);
    }
  }

  @Test
  public void testListAndDropShapesInRDFIgnoreGraph() throws Exception {
    try (Driver driver = GraphDatabase.driver(neo4j.boltURI(),
        Config.builder().withoutEncryption().build())) {

      Session session = driver.session();

      session.run("CALL n10s.graphconfig.init({ handleVocabUris: 'IGNORE' })");

      session.run("CREATE CONSTRAINT ON ( resource:Resource ) ASSERT (resource.uri) IS UNIQUE ");

      session.run("CALL n10s.validation.shacl.import.fetch(\"" + SHACLValidationProceduresTest.class
          .getClassLoader()
          .getResource("shacl/person2lpg-shacl.ttl")
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
        if (next.get("target").asString().equals("Person") && next.get("propertyOrRelationshipPath").isNull()
            && next.get("param").asString().equals("ignoredProperties")) {
          List<Object> expected = new ArrayList<>();
          expected.add("WROTE");
          expected.add("PRODUCED");
          expected.add("REVIEWED");
          expected.add("FOLLOWS");
          expected.add("DIRECTED");
          expected.add("born");
          List<Object> actual = next.get("value").asList();
          assertTrue(expected.containsAll(actual) && actual.containsAll(expected));
        }
      }

      shapesResults = session.run("CALL n10s.validation.shacl.dropShapes() ");
      assertFalse(shapesResults.hasNext());

      try{
        shapesResults = session.run("CALL n10s.validation.shacl.listShapes() ");
        shapesResults.hasNext();
        //execution should break here
        assertFalse(true);

      } catch (Exception e){
        //Expected
        assertTrue(e.getMessage().contains("n10s.validation.SHACLValidationException: No shapes compiled"));
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
          + "@prefix neo4j: <neo4j://graph.schema#> .\n"
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
        if (next.get("target").asString().equals("neo4j__Person") && next.get("propertyOrRelationshipPath")
            .isNull()
            && next.get("param").asString().equals("sh:ignoredProperties")) {
          List<Object> expected = new ArrayList<>();
          expected.add("neo4j__WROTE");
          expected.add("neo4j__PRODUCED");
          expected.add("neo4j__REVIEWED");
          expected.add("neo4j__FOLLOWS");
          expected.add("neo4j__DIRECTED");
          expected.add("neo4j__born");
          List<Object> actual = next.get("value").asList();
          assertTrue(expected.containsAll(actual) && actual.containsAll(expected));
        }
      }

      shapesResults = session.run("CALL n10s.validation.shacl.dropShapes() ");
      assertFalse(shapesResults.hasNext());

      try{
        shapesResults = session.run("CALL n10s.validation.shacl.listShapes() ");
        shapesResults.hasNext();
        //execution should break here
        assertFalse(true);

      } catch (Exception e){
        //Expected
        assertTrue(e.getMessage().contains("n10s.validation.SHACLValidationException: No shapes compiled"));
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

      session.run(UNIQUENESS_CONSTRAINT_STATEMENT);
      assertTrue(session.run("call db.schemaStatements").hasNext());

      Result loadShapesResult = session.run(
          "CALL n10s.validation.shacl.import.fetch(\"" + SHACLValidationProceduresTest.class
              .getClassLoader()
              .getResource("shacl/person2lpg-shacl.ttl")
              .toURI() + "\",\"Turtle\", {})");

      Result validationResults = session.run("MATCH (p:Person) WITH collect(p) as nodes "
          + "call n10s.validation.shacl.validateTransaction(nodes,[], {}, {}, {}, {}, [], []) "
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
  public void testRunTestSuite0() throws Exception {
    runIndividualTest("core/complex", "personexample", null, "IGNORE");
    runIndividualTest("core/complex", "personexample", null, "SHORTEN");
    runIndividualTest("core/complex", "personexample", null, "KEEP");
  }

  @Test
  public void testRunTestSuite1() throws Exception {
    runIndividualTest("core/other", "rangeType-001", null, "IGNORE");
    runIndividualTest("core/other", "rangeType-001", null, "SHORTEN");
    runIndividualTest("core/other", "rangeType-001", null, "KEEP");
  }

  @Test
  public void testRunTestSuite1b() throws Exception {
    runIndividualTest("core/other", "rangeType-query-001", null, "IGNORE");
    runIndividualTest("core/other", "rangeType-query-001", null, "SHORTEN", "rangeType-query-001-shorten");
    runIndividualTest("core/other", "rangeType-query-001", null, "KEEP","rangeType-query-001-keep");
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
  public void testRunTestSuite3queryBased() throws Exception {
    runIndividualTest("core/property", "datatype-query-001", null, "IGNORE");
    runIndividualTest("core/property", "datatype-query-001", null, "SHORTEN", "datatype-query-001-shorten");
    runIndividualTest("core/property", "datatype-query-001", null, "KEEP", "datatype-query-001-keep");
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
  public void testRunTestSuite5b() throws Exception {
    runIndividualTest("core/property", "maxCount-001b", null, "IGNORE");
    runIndividualTest("core/property", "maxCount-001b", null, "SHORTEN");
    runIndividualTest("core/property", "maxCount-001b", null, "KEEP");
  }

  @Test
  public void testRunTestSuite5c() throws Exception {
    runIndividualTest("core/property", "maxCount-001c", null, "IGNORE");
    runIndividualTest("core/property", "maxCount-001c", null, "SHORTEN");
    runIndividualTest("core/property", "maxCount-001c", null, "KEEP");
  }

  @Test
  public void testRunTestSuite6() throws Exception {
    runIndividualTest("core/property", "minExclussive-001", null, "IGNORE");
    runIndividualTest("core/property", "minExclussive-001", null, "SHORTEN");
    runIndividualTest("core/property", "minExclussive-001", null, "KEEP");
  }

  @Test
  public void testRunTestSuite6QueryBased() throws Exception {
    runIndividualTest("core/property", "minMax-query-001", null, "IGNORE");
    runIndividualTest("core/property", "minMax-query-001", null, "SHORTEN", "minMax-query-001-shorten");
    runIndividualTest("core/property", "minMax-query-001", null, "KEEP","minMax-query-001-keep");
  }

  @Test
  public void testRunTestSuite7() throws Exception {
    runIndividualTest("core/property", "hasValue-001", null, "IGNORE");
    runIndividualTest("core/property", "hasValue-001", null, "SHORTEN");
    runIndividualTest("core/property", "hasValue-001", null, "KEEP");
  }

  @Test
  public void testRunTestSuite7QueryBased() throws Exception {
    runIndividualTest("core/property", "hasValue-query-001", null, "IGNORE");
    runIndividualTest("core/property", "hasValue-query-001", null, "SHORTEN", "hasValue-query-001-shorten");
    runIndividualTest("core/property", "hasValue-query-001", null, "KEEP","hasValue-query-001-keep");
  }

  @Test
  public void testRunTestSuite7b() throws Exception {
    // unclear what would that mean on a pure LPG. How to identify a node? By id maybe?
    runIndividualTest("core/property", "hasValue-001b", null, "IGNORE");
    runIndividualTest("core/property", "hasValue-001b", null, "SHORTEN");
    runIndividualTest("core/property", "hasValue-001b", null, "KEEP");
  }

  @Test
  public void testRunTestSuite7bQueryBased() throws Exception {
    // unclear what would that mean on a pure LPG. How to identify a node? By id maybe?
    runIndividualTest("core/property", "hasValue-query-001b", null, "IGNORE");
    runIndividualTest("core/property", "hasValue-query-001b", null, "SHORTEN", "hasValue-query-001b-shorten");
    runIndividualTest("core/property", "hasValue-query-001b", null, "KEEP", "hasValue-query-001b-keep");
  }

  @Test
  public void testRunTestSuite7c() throws Exception {
    runIndividualTest("core/property", "hasValue-001c", null, "IGNORE");
    runIndividualTest("core/property", "hasValue-001c", null, "SHORTEN");
    runIndividualTest("core/property", "hasValue-001c", null, "KEEP");
  }

  @Test
  public void testRunTestSuite7cQueryBased() throws Exception {
    runIndividualTest("core/property", "hasValue-query-001c", null, "IGNORE");
    runIndividualTest("core/property", "hasValue-query-001c", null, "SHORTEN", "hasValue-query-001c-shorten");
    runIndividualTest("core/property", "hasValue-query-001c", null, "KEEP", "hasValue-query-001c-keep");
  }

  @Test
  public void testRunTestSuite8() throws Exception {
    runIndividualTest("core/property", "in-001", null, "IGNORE");
    runIndividualTest("core/property", "in-001", null, "SHORTEN");
    runIndividualTest("core/property", "in-001", null, "KEEP");
  }

  @Test
  public void testRunTestSuite8QueryBased() throws Exception {
    runIndividualTest("core/property", "in-query-001", null, "IGNORE");
    runIndividualTest("core/property", "in-query-001", null, "SHORTEN", "in-query-001-shorten");
    runIndividualTest("core/property", "in-query-001", null, "KEEP", "in-query-001-keep");
  }

  @Test
  public void testRunTestSuite8not() throws Exception {
    runIndividualTest("core/property", "in-not-001", null, "IGNORE");
    runIndividualTest("core/property", "in-not-001", null, "SHORTEN");
    runIndividualTest("core/property", "in-not-001", null, "KEEP");
  }

  @Test
  public void testRunTestSuite8notQueryBased() throws Exception {
    runIndividualTest("core/property", "in-not-query-001", null, "IGNORE");
    runIndividualTest("core/property", "in-not-query-001", null, "SHORTEN","in-not-query-001-shorten");
    runIndividualTest("core/property", "in-not-query-001", null, "KEEP","in-not-query-001-keep");
  }

  @Test
  public void testRunTestSuite8b() throws Exception {
    runIndividualTest("core/property", "in-001b", null, "IGNORE");
    runIndividualTest("core/property", "in-001b", null, "SHORTEN");
    runIndividualTest("core/property", "in-001b", null, "KEEP");
  }

  @Test
  public void testRunTestSuite8bQueryBased() throws Exception {
    runIndividualTest("core/property", "in-query-001b", null, "IGNORE");
    runIndividualTest("core/property", "in-query-001b", null, "SHORTEN","in-query-001b-shorten");
    runIndividualTest("core/property", "in-query-001b", null, "KEEP", "in-query-001b-keep");
  }

  @Test
  public void testRunTestSuite8bnot() throws Exception {
    runIndividualTest("core/property", "in-not-001b", null, "IGNORE");
    runIndividualTest("core/property", "in-not-001b", null, "SHORTEN");
    runIndividualTest("core/property", "in-not-001b", null, "KEEP");
  }

  @Test
  public void testRunTestSuite8bnotQueryBased() throws Exception {
    runIndividualTest("core/property", "in-not-query-001b", null, "IGNORE");
    runIndividualTest("core/property", "in-not-query-001b", null, "SHORTEN","in-not-query-001b-shorten");
    runIndividualTest("core/property", "in-not-query-001b", null, "KEEP", "in-not-query-001b-keep");
  }

  @Test
  public void testRunTestSuite8c() throws Exception {
    runIndividualTest("core/property", "in-001c", null, "IGNORE");
    runIndividualTest("core/property", "in-001c", null, "SHORTEN");
    runIndividualTest("core/property", "in-001c", null, "KEEP");
  }

  @Test
  public void testRunTestSuite8cQueryBased() throws Exception {
    runIndividualTest("core/property", "in-query-001c", null, "IGNORE");
    runIndividualTest("core/property", "in-query-001c", null, "SHORTEN", "in-query-001c-shorten");
    runIndividualTest("core/property", "in-query-001c", null, "KEEP","in-query-001c-keep");
  }


  @Test
  public void testRunTestSuite8cnot() throws Exception {
    runIndividualTest("core/property", "in-not-001c", null, "IGNORE");
    runIndividualTest("core/property", "in-not-001c", null, "SHORTEN");
    runIndividualTest("core/property", "in-not-001c", null, "KEEP");
  }

  @Test
  public void testRunTestSuite8cnotQueryBased() throws Exception {
    runIndividualTest("core/property", "in-not-query-001c", null, "IGNORE");
    runIndividualTest("core/property", "in-not-query-001c", null, "SHORTEN", "in-not-query-001c-shorten");
    runIndividualTest("core/property", "in-not-query-001c", null, "KEEP","in-not-query-001c-keep");
  }

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

  public void runIndividualTest(String testGroupName, String testName,
      String cypherScript, String handleVocabUris, String ... overrideShapesFileName) throws Exception {
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
        session.run(UNIQUENESS_CONSTRAINT_STATEMENT);
        assertTrue(session.run("call db.schemaStatements").hasNext());
      }

      //db is empty
      assertFalse(session.run("MATCH (n) RETURN n").hasNext());

      session.run("CALL n10s.graphconfig.init({ handleRDFTypes: 'LABELS_AND_NODES', handleMultival: 'ARRAY'" +
          ", handleVocabUris: '" + handleVocabUris + "' })");

      //load data
      Result dataImportResults = session.run(
              "CALL n10s.rdf.import.fetch(\"" + SHACLValidationProceduresTest.class.getClassLoader()
                      .getResource("shacl/w3ctestsuite/" + testGroupName + "/" + testName + "-data.ttl")
                      .toURI() + "\",\"Turtle\")");

      assertTrue(dataImportResults.hasNext());

      assertTrue(dataImportResults.next().get("triplesLoaded").asLong() > 0);

      //load shapes
      Result shapesLoadResults = session
          .run("CALL n10s.validation.shacl.import.fetch(\"" + SHACLValidationProceduresTest.class
              .getClassLoader()
              .getResource("shacl/w3ctestsuite/" + testGroupName + "/" + (overrideShapesFileName.length>0?overrideShapesFileName[0]:testName) + "-shapes.ttl")
              .toURI() + "\",\"Turtle\", {})");


      assertTrue(shapesLoadResults.hasNext());

      //load shapes for test completeness
      Result loadShapesAsNodes = session
          .run("CALL n10s.rdf.import.fetch(\"" + SHACLValidationProceduresTest.class
              .getClassLoader()
              .getResource("shacl/w3ctestsuite/" + testGroupName + "/" + testName + "-shapes.ttl")
              .toURI() + "\",\"Turtle\", {})");

      assertTrue(loadShapesAsNodes.hasNext());

      assertTrue(loadShapesAsNodes.next().get("triplesLoaded").asLong() > 0);

      //load expected results
      Result resultsLoadResult = session.run("call n10s.rdf.import.fetch('" + SHACLValidationProceduresTest.class
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

      session.run("MATCH (n) DETACH DELETE n ").hasNext();

    } catch (Exception e) {
      assertTrue("Failure due to exception raised: " + e.getMessage(), false);
    }


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


  @Test
  public void testHasTypeValidationOnMovieDB() throws Exception {
    try (Driver driver = GraphDatabase.driver(neo4j.boltURI(),
            Config.builder().withoutEncryption().build())) {

      Session session = driver.session();

      assertFalse(session.run("MATCH (n) RETURN n").hasNext());

      List<Long> invalidNodes = session.run(
              "CREATE (SleeplessInSeattle:Movie {title:'Sleepless in Seattle', released:1993, tagline:'What if someone you never met, someone you never saw, someone you never knew was the only someone for you?'})\n"
                      +
                      "CREATE (NoraE:Person:Individual {name:'Nora Ephron', born:1941})\n" +
                      "CREATE (RitaW:Person:Thing {name:'Rita Wilson', born:1956})\n" +
                      "CREATE (BillPull:Person:Entity {name:'Bill Pullman', born:1953})\n" +
                      "CREATE (VictorG:Person:Entity {name:'Victor Garber', born:1949})\n" +
                      "CREATE (RosieO:Person:Individual {name:\"Rosie O'Donnell\", born:1962})\n" +
                      "CREATE\n" +
                      "  (RitaW)-[:ACTED_IN {roles:['Suzy']}]->(SleeplessInSeattle),\n" +
                      "  (BillPull)-[:ACTED_IN {roles:['Walter']}]->(SleeplessInSeattle),\n" +
                      "  (VictorG)-[:ACTED_IN {roles:['Greg']}]->(SleeplessInSeattle),\n" +
                      "  (RosieO)-[:ACTED_IN {roles:['Becky']}]->(SleeplessInSeattle),\n" +
                      "  (NoraE)-[:DIRECTED]->(SleeplessInSeattle) " +
                      " RETURN [id(RitaW), id(BillPull), id(VictorG)] as invalidNodes").next().get("invalidNodes").asList(Value::asLong);


      session.run("CALL n10s.validation.shacl.import.fetch(\"" + SHACLValidationProceduresTest.class
              .getClassLoader()
              .getResource("shacl/typerestriction-shacl.ttl")
              .toURI() + "\",\"Turtle\", {})");

      Result validationResults = session.run("CALL n10s.validation.shacl.validate() ");

      assertEquals(true, validationResults.hasNext());

      int count = 0;
      while (validationResults.hasNext()) {
        Record next = validationResults.next();
        count++;
        assertTrue(invalidNodes.contains(next.get("focusNode").asLong()));
        assertEquals(SHACL.HAS_VALUE_CONSTRAINT_COMPONENT.stringValue(),
                  next.get("propertyShape").asString());
      }
      assertEquals(3,count);




      session.run("CALL n10s.validation.shacl.import.fetch(\"" + SHACLValidationProceduresTest.class
              .getClassLoader()
              .getResource("shacl/typerestriction-enumerated-shacl.ttl")
              .toURI() + "\",\"Turtle\", {})");

      validationResults = session.run("CALL n10s.validation.shacl.validate() ");

      assertEquals(true, validationResults.hasNext());

      count = 0;
      while (validationResults.hasNext()) {
        Record next = validationResults.next();
        count++;
        assertTrue(invalidNodes.contains(next.get("focusNode").asLong()));
        assertEquals(SHACL.IN_CONSTRAINT_COMPONENT.stringValue(),
                next.get("propertyShape").asString());
      }
      assertEquals(3,count);


      long tooMany = session.run("CREATE (p:Person:Individual:Thing:Human {name:\"Mr. Toomany Types\", born:1902}) " +
              "return id(p) as id\n").next().get("id").asLong();
      long tooFew = session.run("CREATE (p:Person {name:\"Mr. Toofew Types\", born:1922}) " +
              "return id(p) as id\n").next().get("id").asLong();
      session.run("CALL n10s.validation.shacl.import.fetch(\"" + SHACLValidationProceduresTest.class
              .getClassLoader()
              .getResource("shacl/typerestriction-count-shacl.ttl")
              .toURI() + "\",\"Turtle\", {})");

      validationResults = session.run("CALL n10s.validation.shacl.validate() ");

      assertEquals(true, validationResults.hasNext());

      count = 0;
      while (validationResults.hasNext()) {
        Record next = validationResults.next();
        count++;
        if(next.get("propertyShape").asString().equals(SHACL_COUNT_CONSTRAINT_COMPONENT)){
            assertTrue(next.get("focusNode").asLong() == tooFew || next.get("focusNode").asLong() == tooMany);
        } else {
          assertFalse(true); //there should not be violations of other types
        }
      }
      assertEquals(2,count);
    }
  }

  String SHAPES_REQUIRED_EXCLUDED_TYPES = "@prefix ex: <http://example.neo4j.com/graphvalidation#> .\n" +
          "@prefix sh: <http://www.w3.org/ns/shacl#> .\n" +
          "@prefix neo4j: <neo4j://graph.schema#> .\n" +
          "@prefix xsd: <http://www.w3.org/2001/XMLSchema#> .\n" +
          "@prefix rdfs: <http://www.w3.org/2000/01/rdf-schema#> .\n" +
          "\n" +
          "ex:womanShape a sh:NodeShape ;\n" +
          "  sh:targetClass neo4j:Woman ;\n" +
          "  sh:property [\n" +
          "    sh:path neo4j:name ;\n" +
          "    sh:pattern \".*\" ;\n" +
          "    sh:maxCount 1 ;\n" +
          "    sh:datatype xsd:string ;\n" +
          "  ];\n" +
          "  sh:class neo4j:Person ;\n" +
          "  sh:not [ sh:class neo4j:Man ] ;\n" +
          ".\n" +
          "ex:manShape a sh:NodeShape ;\n" +
          "  sh:targetClass neo4j:Man ;\n" +
          "  sh:property [\n" +
          "    sh:path neo4j:name ;\n" +
          "    sh:pattern \".*\" ;\n" +
          "    sh:maxCount 1 ;\n" +
          "    sh:datatype xsd:string ;\n" +
          "  ];\n" +
          "  sh:class neo4j:Person ;\n" +
          ".\n" ;

  String SHAPES_REQUIRED_EXCLUDED_TYPES_QUERY = "@prefix ex: <http://example.neo4j.com/graphvalidation#> .\n" +
          "@prefix sh: <http://www.w3.org/ns/shacl#> .\n" +
          "@prefix neo4j: <neo4j://graph.schema#> .\n" +
          "@prefix xsd: <http://www.w3.org/2001/XMLSchema#> .\n" +
          "@prefix rdfs: <http://www.w3.org/2000/01/rdf-schema#> .\n" +
          "\n" +
          "ex:womanShape a sh:NodeShape ;\n" +
          "  sh:targetQuery \" (focus)-[:daughter_of]->() \" ;\n" +
          "  sh:class neo4j:Woman ;\n" +
          "  sh:not [ sh:class neo4j:Man ] ;\n" +
          ".\n" +
          "ex:manShape a sh:NodeShape ;\n" +
          "  sh:targetQuery \" (focus)-[:son_of]->() \" ;\n" +
          "  sh:class neo4j:Man ;\n" +
          "  sh:not [ sh:class neo4j:Woman ] ;\n" +
          ".\n" ;

  @Test
  public void testRequiredAndExcludedTypes() throws Exception {
    try (Driver driver = GraphDatabase.driver(neo4j.boltURI(),
            Config.builder().withoutEncryption().build()); Session session = driver.session()) {
      assertFalse(session.run("MATCH (n) RETURN n").hasNext());
      session.run("create (:Man { name: 'JB'}) ");
      session.run("create (:Person:Woman:Man { name: 'Carol'}) ");
    }
    try (Driver driver = GraphDatabase.driver(neo4j.boltURI(),
            Config.builder().withoutEncryption().build()); Session session = driver.session()) {

      Result results = session
              .run("CALL n10s.validation.shacl.import.inline('" + SHAPES_REQUIRED_EXCLUDED_TYPES + "',\"Turtle\")");

      assertTrue(results.hasNext());

      Result result = session.run("call n10s.validation.shacl.validate()");
      int resultcount = 0;
      while(result.hasNext()){
        result.next();
        resultcount++;
      }
      assertTrue(resultcount==2);

      results = session
              .run("CALL n10s.validation.shacl.import.inline('" + SHAPES_REQUIRED_EXCLUDED_TYPES_QUERY + "',\"Turtle\")");

      assertTrue(results.hasNext());

      assertFalse(session.run("call n10s.validation.shacl.validate()").hasNext());

    }
    try (Driver driver = GraphDatabase.driver(neo4j.boltURI(),
            Config.builder().withoutEncryption().build()); Session session = driver.session()) {
      session.run("create (:Woman { name: 'Bill'})-[:son_of]->() ");
      session.run("create (:Man:Woman { name: 'Kat'})-[:daughter_of]->() ");
    }
    try (Driver driver = GraphDatabase.driver(neo4j.boltURI(),
            Config.builder().withoutEncryption().build()); Session session = driver.session()) {
      
      Result result = session.run("call n10s.validation.shacl.validate()");
      int resultcount = 0;
      while(result.hasNext()){
        result.next();
        resultcount++;
      }
      assertTrue(resultcount==3);

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




