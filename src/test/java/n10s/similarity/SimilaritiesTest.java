package n10s.similarity;

import n10s.graphconfig.GraphConfigProcedures;
import n10s.onto.OntoProceduresTest;
import n10s.onto.load.OntoLoadProcedures;
import n10s.onto.preview.OntoPreviewProcedures;
import n10s.rdf.RDFProcedures;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.neo4j.driver.Record;
import org.neo4j.driver.*;
import org.neo4j.driver.types.Node;
import org.neo4j.driver.types.Path;
import org.neo4j.driver.types.Relationship;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.harness.junit.rule.Neo4jRule;

import java.net.URI;
import java.net.URISyntaxException;
import java.util.*;

import static n10s.CommonProcedures.UNIQUENESS_CONSTRAINT_STATEMENT;
import static org.junit.Assert.*;

/**
 * Created by jbarrasa on 21/03/2016.
 */
public class SimilaritiesTest {
  public static Driver driver;

  @ClassRule
  public static Neo4jRule neo4j = new Neo4jRule()
          .withFunction(Similarities.class)
          .withProcedure(Similarities.class)
          .withProcedure(OntoLoadProcedures.class)
          .withProcedure(GraphConfigProcedures.class);

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


  private static URI file(String path) {
    try {
      return SimilaritiesTest.class.getClassLoader().getResource(path).toURI();
    } catch (URISyntaxException e) {
      String msg = String.format("Failed to load the resource with path '%s'", path);
      throw new RuntimeException(msg, e);
    }
  }

  @Test
  public void testSimFunctions() throws Exception {
    Session session = driver.session();

      initialiseGraphDB(neo4j.defaultDatabaseService(),
          "{ handleVocabUris :'IGNORE'}");



      Result importResults
          = session.run("CALL n10s.onto.import.fetch('" + SimilaritiesTest.class.getClassLoader()
              .getResource("vw.owl")
              .toURI() + "','Turtle')");

      Map<String, Object> next = importResults
          .next().asMap();

      assertEquals(75L, next.get("triplesLoaded"));
      assertEquals(168L, next.get("triplesParsed"));

    session.run("create (a:Class:Resource { uri: \"http://adisconnectednode.com/1234\", name: \"Disconnected\" }) ");

    double sim = session.run("match (a:Class {name:\t\"Atlas\"}),(b:Class {name:\t\"Tiguan\"}) \n" +
            "return n10s.sim.wupsim.value(a,b) as sim").next().get("sim").asDouble();
    assertEquals(0.75D, sim, 0.0001);

    sim = session.run("match (a:Class {name:\t\"Atlas\"}),(b:Class {name:\t\"Golf\"}) \n" +
            "return n10s.sim.wupsim.value(a,b) as sim").next().get("sim").asDouble();
    assertEquals(0.5D, sim, 0.0001);

    sim = session.run("match (a:Class {name:\t\"Atlas\"}),(b:Class {name:\t\"Electric\"}) \n" +
            "return n10s.sim.wupsim.value(a,b) as sim").next().get("sim").asDouble();
    assertEquals(0.2857D, sim, 0.001);

    sim = session.run("match (a:Class {name:\t\"Atlas\"}),(b:Class {name:\t\"Golf\"}) \n" +
            "return n10s.sim.lchsim.value(a,b) as sim").next().get("sim").asDouble();
    assertEquals(0.6931D, sim, 0.0001);

    sim = session.run("match (a:Class {name:\t\"Atlas\"}),(b:Class {name:\t\"Electric\"}) \n" +
            "return n10s.sim.lchsim.value(a,b) as sim").next().get("sim").asDouble();
    assertEquals(0.13353D, sim, 0.001);

    sim = session.run("match (a:Class {name:\t\"Atlas\"}),(b:Class {name:\t\"Disconnected\"}) \n" +
            "return n10s.sim.lchsim.value(a,b, { simulateRoot: false}) as sim").next().get("sim").asDouble();
    assertEquals(0.0, sim, 0.001);

    sim = session.run("match (a:Class {name:\t\"Atlas\"}),(b:Class {name:\t\"Disconnected\"}) \n" +
            "return n10s.sim.lchsim.value(a,b) as sim").next().get("sim").asDouble();
    assertEquals(0.47, sim, 0.001);

    sim = session.run("match (a:Class {name:\t\"Atlas\"}),(b:Class {name:\t\"Disconnected\"}) \n" +
            "return n10s.sim.pathsim.value(a,b) as sim").next().get("sim").asDouble();
    assertEquals(0.1666D, sim, 0.001);

    sim = session.run("match (a:Class {name:\t\"Atlas\"}),(b:Class {name:\t\"Disconnected\"}) \n" +
            "return n10s.sim.pathsim.value(a,b, { simulateRoot: false}) as sim").next().get("sim").asDouble();
    assertEquals(0.0D, sim, 0.0000001);

    Path p = session.run("match (a:Class {name:\t\"Atlas\"}),(b:Class {name:\t\"Tiguan\"}) \n" +
            "return n10s.sim.pathsim.path(a,b) as p").next().get("p").asPath();

    assertEquals(2, p.length());

    assertTrue(session.run("match (a:Class {name:\t\"Atlas\"}),(b:Class {name:\t\"Disconnected\"}) \n" +
            "return n10s.sim.pathsim.path(a,b, { simulateRoot: false}) as p").next().get("p").isNull());

  }

  @Test
  public void testSimProcs() throws Exception {
    Session session = driver.session();

    initialiseGraphDB(neo4j.defaultDatabaseService(),
            "{ handleVocabUris :'IGNORE'}");

    Result importResults
            = session.run("CALL n10s.onto.import.fetch('" + SimilaritiesTest.class.getClassLoader()
            .getResource("vw.owl")
            .toURI() + "','Turtle')");

    Map<String, Object> next = importResults
            .next().asMap();

    assertEquals(75L, next.get("triplesLoaded"));
    assertEquals(168L, next.get("triplesParsed"));


    Record record = session.run("match (a:Class {name:\t\"Tiguan\"}) with a \n" +
            " call n10s.sim.pathsim.search(a,0.3) yield node, similarity \n" +
            " return count(node) as ct , collect(node.name) as details").next();
    
    assertEquals(3, record.get("ct").asLong());

    assertEquals(new HashSet<>(Arrays.asList("SUV", "Atlas","Volkswagen")),new HashSet(record.get("details").asList()));

    record = session.run("match (a:Class {name:\t\"Tiguan\"}) with a \n" +
            " call n10s.sim.lchsim.search(a,0.5) yield node, similarity \n" +
            " return count(node) as ct , collect(node.name) as details").next();

    assertEquals(3, record.get("ct").asLong());

    assertEquals(new HashSet<>(Arrays.asList("SUV", "Atlas","Volkswagen")),new HashSet(record.get("details").asList()));
  }


  @Test
  public void testSimProcsNoRDFImport() throws Exception {
    Session session = driver.session();

    session.run("create (a:Category { name: 'A'})\n" +
            "create (b:Category { name: 'B'})\n" +
            "create (c:Category { name: 'C'})\n" +
            "create (d:Category { name: 'D'})\n" +
            "create (e:Category { name: 'E'})\n" +
            "create (f:Category { name: 'F'})\n" +
            "create (g:Category { name: 'G'})\n" +
            "create (a)-[:has_parent]->(c)\n" +
            "create (b)-[:has_parent]->(c)\n" +
            "create (d)-[:has_parent]->(f)\n" +
            "create (e)-[:has_parent]->(f)\n" +
            "create (f)-[:has_parent]->(g)\n" +
            "create (c)-[:has_parent]->(g)");


    Record record = session.run("match (a:Category {name: \"B\"}) with a \n" +
            " call n10s.sim.pathsim.search(a,0.3, { classLabel: 'Category', subClassOfRel: 'has_parent' }) yield node, similarity \n" +
            " return count(node) as ct , collect(node.name) as details").next();

    assertEquals(3, record.get("ct").asLong());

    assertEquals(new HashSet<>(Arrays.asList("A", "C","G")),new HashSet(record.get("details").asList()));

    record = session.run("match (a:Category {name: \"C\"}) with a \n" +
            " call n10s.sim.pathsim.search(a,0.3, { classLabel: 'Category', subClassOfRel: 'has_parent' }) yield node, similarity \n" +
            " return count(node) as ct , collect(node.name) as details").next();

    assertEquals(4, record.get("ct").asLong());

    assertEquals(new HashSet<>(Arrays.asList("A", "B","F","G")),new HashSet(record.get("details").asList()));

    try {
      Result result = session.run("match (a:Category {name: \"A\"}) \n" +
              " call n10s.sim.pathsim.search(a,0.3) yield node, similarity \n" +
              " return count(node) as ct , collect(node.name) as details");
      result.next();
      //should not get here
      assertTrue(false);
    }catch(Exception e){
      assertTrue(true);
        System.out.println(e.getMessage());
      }



  }


  public void testSimFunctionsNoRDFImport() throws Exception {
    Session session = driver.session();

    session.run("create (a:Category { name: 'A'})\n" +
            "create (b:Category { name: 'B'})\n" +
            "create (c:Category { name: 'C'})\n" +
            "create (d:Category { name: 'D'})\n" +
            "create (e:Category { name: 'E'})\n" +
            "create (f:Category { name: 'F'})\n" +
            "create (g:Category { name: 'G'})\n" +
            "create (a)-[:has_parent]->(c)\n" +
            "create (b)-[:has_parent]->(c)\n" +
            "create (d)-[:has_parent]->(f)\n" +
            "create (e)-[:has_parent]->(f)\n" +
            "create (f)-[:has_parent]->(g)\n" +
            "create (c)-[:has_parent]->(g)");


    double sim = session.run("match (a:Category {name:\t\"A\"}),(b:Category {name:\t\"B\"}) \n" +
            "return n10s.sim.wupsim.value(a,b, { classLabel: 'Category', subClassOfRel: 'has_parent' }) as sim").next().get("sim").asDouble();
    assertEquals(0.66D, sim, 0.01);

    try {
      Result run = session.run("match (a:Category {name:\t\"A\"}),(b:Category {name:\t\"C\"}) \n" +
              "return n10s.sim.wupsim.value(a,b) as sim");
      run.next();
      //should not get here
      assertTrue(false);
    }catch(Exception e){
      assertTrue(true);
      System.out.println(e.getMessage());
    }

    sim = session.run("match (a:Category {name:\t\"A\"}),(b:Category {name:\t\"E\"}) \n" +
            "return n10s.sim.wupsim.value(a,b, { classLabel: 'Category', subClassOfRel: 'has_parent' }) as sim").next().get("sim").asDouble();
    assertEquals(0.33D, sim, 0.01);

    sim = session.run("match (a:Category {name:\t\"A\"}),(b:Category {name:\t\"E\"}) \n" +
            "return n10s.sim.lchsim.value(a,b, { classLabel: 'Category', subClassOfRel: 'has_parent' }) as sim").next().get("sim").asDouble();
    assertEquals(0.33D, sim, 0.01);

    sim = session.run("match (a:Category {name:\t\"A\"}),(b:Category {name:\t\"E\"}) \n" +
            "return n10s.sim.pathsim.value(a,b, { classLabel: 'Category', subClassOfRel: 'has_parent' }) as sim").next().get("sim").asDouble();
    assertEquals(0.33D, sim, 0.01);

    Path p = session.run("match (a:Category {name:\t\"A\"}),(b:Category {name:\t\"E\"}) \n" +
            "return n10s.sim.pathsim.path(a,b, { classLabel: 'Category', subClassOfRel: 'has_parent' }) as sim").next().get("sim").asPath();

    assertEquals(0.33D, sim, 0.01);

  }



  private void initialiseGraphDB(GraphDatabaseService db, String graphConfigParams) {
    db.executeTransactionally(UNIQUENESS_CONSTRAINT_STATEMENT);
    db.executeTransactionally("CALL n10s.graphconfig.init(" +
        (graphConfigParams != null ? graphConfigParams : "{}") + ")");
  }
}
