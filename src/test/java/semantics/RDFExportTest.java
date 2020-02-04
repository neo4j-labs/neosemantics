package semantics;

import static org.junit.Assert.assertTrue;

import org.junit.Rule;
import org.junit.Test;
import org.neo4j.driver.*;
import org.neo4j.harness.junit.rule.Neo4jRule;
import semantics.mapping.MappingUtils;

public class RDFExportTest {

  @Rule
  public Neo4jRule neo4j = new Neo4jRule()
      .withProcedure(RDFExport.class)
      .withProcedure(MappingUtils.class);


  @Test
  public void testStreamAsRDF() throws Exception {
    try (Driver driver = GraphDatabase.driver(neo4j.boltURI(),
            Config.builder().withoutEncryption().build()); Session session = driver.session()) {

      session
          .run(
              "CREATE (n:Node { a: 1, b: 'hello' })-[:CONNECTED_TO]->(:Node {  a:2, b2:'bye@en'})");

      Result res
          = session
          .run(" CALL semantics.cypherResultsAsRDF(' MATCH path = (n)-[r]->(m) RETURN path ', {}) ");
      assertTrue(res.hasNext());
      while (res.hasNext()) {
        System.out.println(res.next());
      }

    }
  }

}
