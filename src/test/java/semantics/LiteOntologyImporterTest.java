package semantics;

import static org.junit.Assert.assertEquals;

import org.junit.Rule;
import org.junit.Test;
import org.neo4j.driver.Config;
import org.neo4j.driver.Driver;
import org.neo4j.driver.GraphDatabase;
import org.neo4j.driver.*;
import org.neo4j.harness.junit.rule.Neo4jRule;


/**
 * Created by jbarrasa on 21/03/2016.
 */
public class LiteOntologyImporterTest {

  @Rule
  public Neo4jRule neo4j = new Neo4jRule()
      .withProcedure(LiteOntologyImporter.class);

  @Test
  public void liteOntoImport() throws Exception {
    try (Driver driver = GraphDatabase.driver(neo4j.boltURI(),
            Config.builder().withoutEncryption().build())) {

      Session session = driver.session();

      Result importResults = session.run("CALL semantics.liteOntoImport('" +
          LiteOntologyImporterTest.class.getClassLoader().getResource("moviesontology.owl").toURI()
          + "','RDF/XML')");

      assertEquals(13L, importResults.next().get("elementsLoaded").asLong());

      assertEquals(2L,
          session.run("MATCH (n:Class) RETURN count(n) AS count").next().get("count").asLong());

      assertEquals(5L,
          session.run("MATCH (n:Property)-[:DOMAIN]->(:Class)  RETURN count(n) AS count").next()
              .get("count").asLong());

      assertEquals(0L,
          session.run("MATCH (n:Property)-[:DOMAIN]->(:Relationship) RETURN count(n) AS count")
              .next().get("count").asLong());

      assertEquals(6L,
          session.run("MATCH (n:Relationship) RETURN count(n) AS count").next().get("count")
              .asLong());
    }

  }

  @Test
  public void liteOntoImportWithCustomNames() throws Exception {
    try (Driver driver = GraphDatabase.driver(neo4j.boltURI(),
            Config.builder().withoutEncryption().build())) {

      Session session = driver.session();

      Result importResults = session.run("CALL semantics.liteOntoImport('" +
          LiteOntologyImporterTest.class.getClassLoader().getResource("moviesontology.owl").toURI()
          + "','RDF/XML', { classLabel : 'Category', objectPropertyLabel: 'Rel', dataTypePropertyLabel: 'Prop'})");

      assertEquals(13L, importResults.next().get("elementsLoaded").asLong());

      assertEquals(0L,
          session.run("MATCH (n:Class) RETURN count(n) AS count").next().get("count").asLong());

      assertEquals(2L,
          session.run("MATCH (n:Category) RETURN count(n) AS count").next().get("count").asLong());

      assertEquals(0L,
          session.run("MATCH (n:Property) RETURN count(n) AS count").next().get("count").asLong());

      assertEquals(5L,
          session.run("MATCH (n:Prop)-[:DOMAIN]->(:Category)  RETURN count(n) AS count").next()
              .get("count").asLong());

      assertEquals(0L,
          session.run("MATCH (n:Relationship) RETURN count(n) AS count").next().get("count")
              .asLong());

      assertEquals(6L,
          session.run("MATCH (n:Rel) RETURN count(n) AS count").next().get("count").asLong());
    }

  }


  @Test
  public void liteOntoImportWithCustomNamesAndResourceLabels() throws Exception {
    try (Driver driver = GraphDatabase.driver(neo4j.boltURI(),
            Config.builder().withoutEncryption().build())) {

      Session session = driver.session();

      Result importResults = session.run("CALL semantics.liteOntoImport('" +
          LiteOntologyImporterTest.class.getClassLoader().getResource("moviesontology.owl").toURI()
          + "','RDF/XML', { addResourceLabels: true, classLabel : 'Category', objectPropertyLabel: 'Rel', dataTypePropertyLabel: 'Prop'})");

      assertEquals(13L, importResults.next().get("elementsLoaded").asLong());

      assertEquals(0L,
          session.run("MATCH (n:Class) RETURN count(n) AS count").next().get("count").asLong());

      assertEquals(2L,
          session.run("MATCH (n:Category:Resource) RETURN count(n) AS count").next().get("count")
              .asLong());

      assertEquals(0L,
          session.run("MATCH (n:Property) RETURN count(n) AS count").next().get("count").asLong());

      assertEquals(5L,
          session.run("MATCH (n:Prop:Resource)-[:DOMAIN]->(:Category)  RETURN count(n) AS count")
              .next()
              .get("count").asLong());

      assertEquals(0L,
          session.run("MATCH (n:Relationship) RETURN count(n) AS count").next().get("count")
              .asLong());

      assertEquals(6L,
          session.run("MATCH (n:Rel) RETURN count(n) AS count").next().get("count").asLong());
    }

  }

  @Test
  public void liteOntoImportSchemaOrg() throws Exception {
    try (Driver driver = GraphDatabase.driver(neo4j.boltURI(),
            Config.builder().withoutEncryption().build())) {

      Session session = driver.session();

      Result importResults = session.run("CALL semantics.liteOntoImport('" +
          LiteOntologyImporterTest.class.getClassLoader().getResource("schema.rdf").toURI() +
          "','RDF/XML')");

      assertEquals(596L,
          session.run("MATCH (n:Class) RETURN count(n) AS count").next().get("count").asLong());

      assertEquals(348L,
          session.run("MATCH (n:Property)-[:DOMAIN]->(:Class)  RETURN count(n) AS count").next()
              .get("count").asLong());

      assertEquals(293L,
          session.run("MATCH (n:Relationship)-[:DOMAIN]->(:Class)  RETURN count(n) AS count").next()
              .get("count").asLong());

      assertEquals(0L,
          session.run("MATCH (n:Property)-[:DOMAIN]->(:Relationship) RETURN count(n) AS count")
              .next().get("count").asLong());

      assertEquals(416L,
          session.run("MATCH (n:Relationship) RETURN count(n) AS count").next().get("count")
              .asLong());
    }

  }

  @Test
  public void liteOntoImportClassHierarchy() throws Exception {
    try (Driver driver = GraphDatabase.driver(neo4j.boltURI(),
            Config.builder().withoutEncryption().build())) {

      Session session = driver.session();

      Result importResults = session.run("CALL semantics.liteOntoImport('" +
          LiteOntologyImporterTest.class.getClassLoader().getResource("class-hierarchy-test.rdf")
              .toURI() +
          "','RDF/XML')");

      assertEquals(1L,
          session.run("MATCH p=(:Class{name:'Code'})-[:SCO]->(:Class{name:'Intangible'})" +
              " RETURN count(p) AS count").next().get("count").asLong());
    }
  }


  @Test
  public void liteOntoImportPropHierarchy() throws Exception {
    try (Driver driver = GraphDatabase.driver(neo4j.boltURI(),
            Config.builder().withoutEncryption().build())) {

      Session session = driver.session();

      Result importResults = session.run("CALL semantics.liteOntoImport('" +
          LiteOntologyImporterTest.class.getClassLoader().getResource("SPOTest.owl").toURI() +
          "','RDF/XML')");

      assertEquals(1L,
          session.run("MATCH p=(:Property{name:'prop1'})-[:SPO]->(:Property{name:'superprop'})" +
              " RETURN count(p) AS count").next().get("count").asLong());
    }
  }

}
