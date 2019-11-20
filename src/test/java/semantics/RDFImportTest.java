package semantics;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.neo4j.driver.v1.Values.NULL;
import static org.neo4j.driver.v1.Values.ofNode;
import static semantics.Params.PREFIX_SEPARATOR;


import java.net.URI;
import java.net.URISyntaxException;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.eclipse.rdf4j.model.vocabulary.XMLSchema;

import org.junit.Rule;
import org.junit.Test;
import org.neo4j.driver.internal.value.IntegerValue;
import org.neo4j.driver.internal.value.ListValue;
import org.neo4j.driver.v1.Config;
import org.neo4j.driver.v1.Driver;
import org.neo4j.driver.v1.GraphDatabase;
import org.neo4j.driver.v1.Record;
import org.neo4j.driver.v1.Session;
import org.neo4j.driver.v1.StatementResult;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.harness.junit.Neo4jRule;
import semantics.mapping.MappingUtils;

/**
 * Created by jbarrasa on 21/03/2016.
 */
public class RDFImportTest {

  @Rule
  public Neo4jRule neo4j = new Neo4jRule()
      .withProcedure(RDFImport.class).withFunction(RDFImport.class)
      .withProcedure(MappingUtils.class);
  private String jsonLdFragment = "{\n" +
      "  \"@context\": {\n" +
      "    \"name\": \"http://xmlns.com/foaf/0.1/name\",\n" +
      "    \"knows\": \"http://xmlns.com/foaf/0.1/knows\",\n" +
      "\t\"modified\": \"http://xmlns.com/foaf/0.1/modified\"\n" +
      "  },\n" +
      "  \"@id\": \"http://me.markus-lanthaler.com/\",\n" +
      "  \"name\": \"Markus Lanthaler\",\n" +
      "  \"knows\": [\n" +
      "    {\n" +
      "      \"@id\": \"http://manu.sporny.org/about#manu\",\n" +
      "      \"name\": \"Manu Sporny\"\n" +
      "    },\n" +
      "    {\n" +
      "      \"name\": \"Dave Longley\",\n" +
      "\t  \"modified\":\n" +
      "\t    {\n" +
      "\t      \"@value\": \"2010-05-29T14:17:39+02:00\",\n" +
      "\t      \"@type\": \"http://www.w3.org/2001/XMLSchema#dateTime\"\n" +
      "\t    }\n" +
      "    }\n" +
      "  ]\n" +
      "}";

  private String turtleFragment = "@prefix show: <http://example.org/vocab/show/> .\n" +
      "\n" +
      "show:218 show:localName \"That Seventies Show\"@en .                 # literal with a language tag\n"
      +
      "show:218 show:localName \"Cette Série des Années Soixante-dix\"@fr . \n" +
      "show:218 show:localName \"Cette Série des Années Septante\"@fr-be .  # literal with a region subtag";

  private String wrongUriTtl = "@prefix pr: <http://example.org/vocab/show/> .\n" +
      "pr:ent" +
      "      pr:P854 <https://suasprod.noc-science.at/XLCubedWeb/WebForm/ShowReport.aspx?rep=004+studierende%2f001+universit%u00e4ten%2f003+studierende+nach+universit%u00e4ten.xml&toolbar=true> ;\n"
      +
      "      pr:P813 \"2017-10-11T00:00:00Z\"^^xsd:dateTime .\n";

  private String turtleOntology = "@prefix owl: <http://www.w3.org/2002/07/owl#> .\n"
      + "@prefix rdfs: <http://www.w3.org/2000/01/rdf-schema#> .\n"
      + "\n"
      + "<http://neo4j.com/voc/movies>\n"
      + "  a owl:Ontology ;\n"
      + "  rdfs:comment \"A basic OWL ontology for Neo4j's movie database\", \"\"\"Simple ontology providing basic vocabulary and domain+range axioms\n"
      + "            for the movie database.\"\"\" ;\n"
      + "  rdfs:label \"Neo4j's Movie Ontology\" .\n"
      + "\n"
      + "<http://neo4j.com/voc/movies#Person>\n"
      + "  a owl:Class ;\n"
      + "  rdfs:label \"Person\"@en ;\n"
      + "  rdfs:comment \"Individual involved in the film industry\"@en .\n"
      + "\n"
      + "<http://neo4j.com/voc/movies#Movie>\n"
      + "  a owl:Class ;\n"
      + "  rdfs:label \"Movie\"@en ;\n"
      + "  rdfs:comment \"A film\"@en .\n"
      + "\n"
      + "<http://neo4j.com/voc/movies#name>\n"
      + "  a owl:DatatypeProperty ;\n"
      + "  rdfs:label \"name\"@en ;\n"
      + "  rdfs:comment \"A person's name\"@en ;\n"
      + "  rdfs:domain <http://neo4j.com/voc/movies#Person> .\n"
      + "\n"
      + "<http://neo4j.com/voc/movies#born>\n"
      + "  a owl:DatatypeProperty ;\n"
      + "  rdfs:label \"born\"@en ;\n"
      + "  rdfs:comment \"A person's date of birth\"@en ;\n"
      + "  rdfs:domain <http://neo4j.com/voc/movies#Person> .\n"
      + "\n"
      + "<http://neo4j.com/voc/movies#title>\n"
      + "  a owl:DatatypeProperty ;\n"
      + "  rdfs:label \"title\"@en ;\n"
      + "  rdfs:comment \"The title of a film\"@en ;\n"
      + "  rdfs:domain <http://neo4j.com/voc/movies#Movie> .\n"
      + "\n"
      + "<http://neo4j.com/voc/movies#released>\n"
      + "  a owl:DatatypeProperty ;\n"
      + "  rdfs:label \"released\"@en ;\n"
      + "  rdfs:comment \"A film's release date\"@en ;\n"
      + "  rdfs:domain <http://neo4j.com/voc/movies#Movie> .\n"
      + "\n"
      + "<http://neo4j.com/voc/movies#tagline>\n"
      + "  a owl:DatatypeProperty ;\n"
      + "  rdfs:label \"tagline\"@en ;\n"
      + "  rdfs:comment \"Tagline for a film\"@en ;\n"
      + "  rdfs:domain <http://neo4j.com/voc/movies#Movie> .\n"
      + "\n"
      + "<http://neo4j.com/voc/movies#ACTED_IN>\n"
      + "  a owl:ObjectProperty ;\n"
      + "  rdfs:label \"ACTED_IN\"@en ;\n"
      + "  rdfs:comment \"Actor had a role in film\"@en ;\n"
      + "  rdfs:domain <http://neo4j.com/voc/movies#Person> ;\n"
      + "  rdfs:range <http://neo4j.com/voc/movies#Movie> .\n"
      + "\n"
      + "<http://neo4j.com/voc/movies#DIRECTED>\n"
      + "  a owl:ObjectProperty ;\n"
      + "  rdfs:label \"DIRECTED\"@en ;\n"
      + "  rdfs:comment \"Director directed film\"@en ;\n"
      + "  rdfs:domain <http://neo4j.com/voc/movies#Person> ;\n"
      + "  rdfs:range <http://neo4j.com/voc/movies#Movie> .\n"
      + "\n"
      + "<http://neo4j.com/voc/movies#PRODUCED>\n"
      + "  a owl:ObjectProperty ;\n"
      + "  rdfs:label \"PRODUCED\"@en ;\n"
      + "  rdfs:comment \"Producer produced film\"@en ;\n"
      + "  rdfs:domain <http://neo4j.com/voc/movies#Person> ;\n"
      + "  rdfs:range <http://neo4j.com/voc/movies#Movie> .\n"
      + "\n"
      + "<http://neo4j.com/voc/movies#REVIEWED>\n"
      + "  a owl:ObjectProperty ;\n"
      + "  rdfs:label \"REVIEWED\"@en ;\n"
      + "  rdfs:comment \"Critic reviewed film\"@en ;\n"
      + "  rdfs:domain <http://neo4j.com/voc/movies#Person> ;\n"
      + "  rdfs:range <http://neo4j.com/voc/movies#Movie> .\n"
      + "\n"
      + "<http://neo4j.com/voc/movies#FOLLOWS>\n"
      + "  a owl:ObjectProperty ;\n"
      + "  rdfs:label \"FOLLOWS\"@en ;\n"
      + "  rdfs:comment \"Critic follows another critic\"@en ;\n"
      + "  rdfs:domain <http://neo4j.com/voc/movies#Person> ;\n"
      + "  rdfs:range <http://neo4j.com/voc/movies#Person> .\n"
      + "\n"
      + "<http://neo4j.com/voc/movies#WROTE>\n"
      + "  a owl:ObjectProperty ;\n"
      + "  rdfs:label \"WROTE\"@en ;\n"
      + "  rdfs:comment \"Screenwriter wrote screenplay of\"@en ;\n"
      + "  rdfs:domain <http://neo4j.com/voc/movies#Person> ;\n"
      + "  rdfs:range <http://neo4j.com/voc/movies#Movie> .";

  private String turtleMultilangOntology = "@prefix owl: <http://www.w3.org/2002/07/owl#> .\n"
      + "@prefix rdfs: <http://www.w3.org/2000/01/rdf-schema#> .\n"
      + "\n"
      + "<http://neo4j.com/voc/movies>\n"
      + "  a owl:Ontology ;\n"
      + "  rdfs:comment \"A basic OWL ontology for Neo4j's movie database\", \"\"\"Simple ontology providing basic vocabulary and domain+range axioms\n"
      + "            for the movie database.\"\"\" ;\n"
      + "  rdfs:label \"Neo4j's Movie Ontology\" .\n"
      + "\n"
      + "<http://neo4j.com/voc/movies#Person>\n"
      + "  a owl:Class ;\n"
      + "  rdfs:label \"Person\"@en, \"Persona\"@es ;\n"
      + "  rdfs:comment \"Individual involved in the film industry\"@en, \"Individuo relacionado con la industria del cine\"@es .\n"
      + "\n"
      + "<http://neo4j.com/voc/movies#Movie>\n"
      + "  a owl:Class ;\n"
      + "  rdfs:label \"Movie\"@en, \"Pelicula\"@es ;\n"
      + "  rdfs:comment \"A film\"@en, \"Una pelicula\"@es .\n"
      + "\n"
      + "<http://neo4j.com/voc/movies#name>\n"
      + "  a owl:DatatypeProperty ;\n"
      + "  rdfs:label \"name\"@en, \"nombre\"@es ;\n"
      + "  rdfs:comment \"A person's name\"@en, \"El nombre de una persona\"@es ;\n"
      + "  rdfs:domain <http://neo4j.com/voc/movies#Person> .\n"
      + "\n"
      + "<http://neo4j.com/voc/movies#born>\n"
      + "  a owl:DatatypeProperty ;\n"
      + "  rdfs:label \"born\"@en, \"fechaNacimiento\"@es ;\n"
      + "  rdfs:comment \"A person's date of birth\"@en, \"La fecha de nacimiento de una persona\"@es ;\n"
      + "  rdfs:domain <http://neo4j.com/voc/movies#Person> .\n"
      + "\n"
      + "<http://neo4j.com/voc/movies#title>\n"
      + "  a owl:DatatypeProperty ;\n"
      + "  rdfs:label \"title\"@en, \"titulo\"@es ;\n"
      + "  rdfs:comment \"The title of a film\"@en, \"El titulo de una pelicula\"@es ;\n"
      + "  rdfs:domain <http://neo4j.com/voc/movies#Movie> .\n"
      + "\n"
      + "<http://neo4j.com/voc/movies#released>\n"
      + "  a owl:DatatypeProperty ;\n"
      + "  rdfs:label \"released\"@en, \"estreno\"@es ;\n"
      + "  rdfs:comment \"A film's release date\"@en, \"La fecha de estreno de una pelicula\"@es ;\n"
      + "  rdfs:domain <http://neo4j.com/voc/movies#Movie> .\n"
      + "\n"
      + "<http://neo4j.com/voc/movies#tagline>\n"
      + "  a owl:DatatypeProperty ;\n"
      + "  rdfs:label \"tagline\"@en, \"lema\"@es ;\n"
      + "  rdfs:comment \"Tagline for a film\"@en, \"El lema o eslogan de una pelicula\"@es ;\n"
      + "  rdfs:domain <http://neo4j.com/voc/movies#Movie> .\n"
      + "\n"
      + "<http://neo4j.com/voc/movies#ACTED_IN>\n"
      + "  a owl:ObjectProperty ;\n"
      + "  rdfs:label \"ACTED_IN\"@en, \"ACTUA_EN\"@es ;\n"
      + "  rdfs:comment \"Actor had a role in film\"@en, \"Actor con un papel en una pelicula a role in film\"@es ;\n"
      + "  rdfs:domain <http://neo4j.com/voc/movies#Person> ;\n"
      + "  rdfs:range <http://neo4j.com/voc/movies#Movie> .\n"
      + "\n"
      + "<http://neo4j.com/voc/movies#DIRECTED>\n"
      + "  a owl:ObjectProperty ;\n"
      + "  rdfs:label \"DIRECTED\"@en, \"DIRIGE\"@es ;\n"
      + "  rdfs:comment \"Director directed film\"@en ;\n"
      + "  rdfs:domain <http://neo4j.com/voc/movies#Person> ;\n"
      + "  rdfs:range <http://neo4j.com/voc/movies#Movie> .\n"
      + "\n"
      + "<http://neo4j.com/voc/movies#PRODUCED>\n"
      + "  a owl:ObjectProperty ;\n"
      + "  rdfs:label \"PRODUCED\"@en, \"PRODUCE\"@es ;\n"
      + "  rdfs:comment \"Producer produced film\"@en, \"productor de una pelicula\"@es ;\n"
      + "  rdfs:domain <http://neo4j.com/voc/movies#Person> ;\n"
      + "  rdfs:range <http://neo4j.com/voc/movies#Movie> .\n"
      + "\n"
      + "<http://neo4j.com/voc/movies#REVIEWED>\n"
      + "  a owl:ObjectProperty ;\n"
      + "  rdfs:label \"REVIEWED\"@en, \"HACE_CRITICA\"@es ;\n"
      + "  rdfs:comment \"Critic reviewed film\"@en, \"critico que publica una critica sobre una pelicula\"@es ;\n"
      + "  rdfs:domain <http://neo4j.com/voc/movies#Person> ;\n"
      + "  rdfs:range <http://neo4j.com/voc/movies#Movie> .\n"
      + "\n"
      + "<http://neo4j.com/voc/movies#FOLLOWS>\n"
      + "  a owl:ObjectProperty ;\n"
      + "  rdfs:label \"FOLLOWS\"@en, \"SIGUE\"@es ;\n"
      + "  rdfs:comment \"Critic follows another critic\"@en, \"Un critico que sigue a otro (en redes sociales)\"@es ;\n"
      + "  rdfs:domain <http://neo4j.com/voc/movies#Person> ;\n"
      + "  rdfs:range <http://neo4j.com/voc/movies#Person> .\n"
      + "\n"
      + "<http://neo4j.com/voc/movies#WROTE>\n"
      + "  a owl:ObjectProperty ;\n"
      + "  rdfs:label \"WROTE\"@en, \"ESCRIBE\"@es ;\n"
      + "  rdfs:comment \"Screenwriter wrote screenplay of\"@en, \"escribe el guion de una pelicula\"@es ;\n"
      + "  rdfs:domain <http://neo4j.com/voc/movies#Person> ;\n"
      + "  rdfs:range <http://neo4j.com/voc/movies#Movie> .";

  private static URI file(String path) {
    try {
      return RDFImportTest.class.getClassLoader().getResource(path).toURI();
    } catch (URISyntaxException e) {
      String msg = String.format("Failed to load the resource with path '%s'", path);
      throw new RuntimeException(msg, e);
    }
  }

  @Test
  public void testAbortIfNoIndices() throws Exception {
    try (Driver driver = GraphDatabase.driver(neo4j.boltURI(),
        Config.build().toConfig()); Session session = driver.session()) {

      StatementResult importResults
          = session.run("CALL semantics.importRDF('" +
          RDFImportTest.class.getClassLoader().getResource("mini-ld.json").toURI() +
          "','JSON-LD',{ handleVocabUris: 'SHORTEN', typesToLabels: true, commitSize: 500})");

      Map<String, Object> singleResult = importResults
          .single().asMap();

      assertEquals(0L, singleResult.get("triplesLoaded"));
      assertEquals("KO", singleResult.get("terminationStatus"));
      assertEquals("The following index is required for importing RDF. Please run "
              + "'CREATE INDEX ON :Resource(uri)' and try again.",
          singleResult.get("extraInfo"));
    }
  }

  @Test
  public void testFullTextIndexesPresent() throws Exception {
    try (Driver driver = GraphDatabase.driver(neo4j.boltURI(),
        Config.build().toConfig()); Session session = driver.session()) {

      session.run("CALL db.index.fulltext.createNodeIndex(\"multiLabelIndex\","
          + "[\"Movie\", \"Book\"],[\"title\", \"description\"])");

      StatementResult importResults
          = session.run("CALL semantics.importRDF('file:///fileDoesnotExist.txt','JSON-LD',{})");

      Map<String, Object> singleResult = importResults
          .single().asMap();

      assertEquals(0L, singleResult.get("triplesLoaded"));
      assertEquals("KO", singleResult.get("terminationStatus"));
      assertEquals("The following index is required for importing RDF. Please run "
              + "'CREATE INDEX ON :Resource(uri)' and try again.",
          singleResult.get("extraInfo"));
    }
  }

  @Test
  public void testCompositeIndexesPresent() throws Exception {
    try (Driver driver = GraphDatabase.driver(neo4j.boltURI(),
        Config.build().toConfig()); Session session = driver.session()) {


      session.run("CREATE INDEX ON :Person(age, country)");

      StatementResult importResults
          = session.run("CALL semantics.importRDF('file:///fileDoesnotExist.txt','JSON-LD',{})");

      Map<String, Object> singleResult = importResults
          .single().asMap();

      assertEquals(0L, singleResult.get("triplesLoaded"));
      assertEquals("KO", singleResult.get("terminationStatus"));
      assertEquals("The following index is required for importing RDF. Please run "
              + "'CREATE INDEX ON :Resource(uri)' and try again.",
          singleResult.get("extraInfo"));
    }
  }

  @Test
  public void testAbortIfNoIndicesImportSnippet() throws Exception {
    try (Driver driver = GraphDatabase.driver(neo4j.boltURI(),
        Config.build().withEncryptionLevel(Config.EncryptionLevel.NONE).toConfig())) {

      Session session = driver.session();

      StatementResult importResults1 = session.run("CALL semantics.importRDFSnippet('" +
          turtleFragment +
          "','Turtle')");

      Map<String, Object> singleResult = importResults1.single().asMap();

      assertEquals(0L, singleResult.get("triplesLoaded"));
      assertEquals("KO", singleResult.get("terminationStatus"));
      assertEquals("The following index is required for importing RDF. Please run "
              + "'CREATE INDEX ON :Resource(uri)' and try again.",
          singleResult.get("extraInfo"));
    }
  }

  @Test
  public void testImportJSONLD() throws Exception {
    try (Driver driver = GraphDatabase.driver(neo4j.boltURI(),
        Config.build().toConfig()); Session session = driver.session()) {

      createIndices(neo4j.getGraphDatabaseService());

      StatementResult importResults
          = session.run("CALL semantics.importRDF('" +
          RDFImportTest.class.getClassLoader().getResource("mini-ld.json").toURI() + "','JSON-LD',"
          +
          "{ handleVocabUris: 'KEEP', typesToLabels: true, commitSize: 500, " +
          "headerParams : { authorization: 'Basic bla bla bla', accept: 'rdf/xml' } })");
      assertEquals(6L, importResults
          .single().get("triplesLoaded").asLong());
      assertEquals("http://me.markus-lanthaler.com/",
          session.run(
              "MATCH (n{`http://xmlns.com/foaf/0.1/name` : 'Markus Lanthaler'}) RETURN n.uri AS uri")
              .next().get("uri").asString());
      assertEquals(1L,
          session.run(
              "MATCH (n) WHERE exists(n.`http://xmlns.com/foaf/0.1/modified`) RETURN count(n) AS count")
              .next().get("count").asLong());
    }
  }

  @Test
  public void testImportJSONLDImportSnippet() throws Exception {
    try (Driver driver = GraphDatabase.driver(neo4j.boltURI(),
        Config.build().withEncryptionLevel(Config.EncryptionLevel.NONE).toConfig())) {

      Session session = driver.session();

      createIndices(neo4j.getGraphDatabaseService());

      StatementResult importResults1 = session.run("CALL semantics.importRDFSnippet('" +
          jsonLdFragment + "','JSON-LD',"
          +
          "{ handleVocabUris: 'KEEP', typesToLabels: true, commitSize: 500, " +
          "headerParams : { authorization: 'Basic bla bla bla', accept: 'rdf/xml' } })");
      assertEquals(6L, importResults1.single().get("triplesLoaded").asLong());
      assertEquals("http://me.markus-lanthaler.com/",
          session.run(
              "MATCH (n{`http://xmlns.com/foaf/0.1/name` : 'Markus Lanthaler'}) RETURN n.uri AS uri")
              .next().get("uri").asString());
      assertEquals(1L,
          session.run(
              "MATCH (n) WHERE exists(n.`http://xmlns.com/foaf/0.1/modified`) RETURN count(n) AS count")
              .next().get("count").asLong());
    }
  }

  @Test
  public void testImportJSONLDShortening() throws Exception {
    try (Driver driver = GraphDatabase.driver(neo4j.boltURI(),
        Config.build().toConfig()); Session session = driver.session()) {

      createIndices(neo4j.getGraphDatabaseService());

      StatementResult importResults
          = session.run("CALL semantics.importRDF('" +
          RDFImportTest.class.getClassLoader().getResource("mini-ld.json").toURI() + "','JSON-LD',"
          +
          "{ handleVocabUris: 'SHORTEN', typesToLabels: true, commitSize: 500})");
      assertEquals(6L, importResults
          .next().get("triplesLoaded").asLong());
      assertEquals("http://me.markus-lanthaler.com/",
          session.run(
              "MATCH (n{ns0" + PREFIX_SEPARATOR + "name : 'Markus Lanthaler'}) RETURN n.uri AS uri")
              .next().get("uri").asString());
      assertEquals(1L,
          session.run("MATCH (n) WHERE exists(n.ns0" + PREFIX_SEPARATOR
              + "modified) RETURN count(n) AS count")
              .next().get("count").asLong());

      assertEquals("ns0",
          session.run(
              "MATCH (n:NamespacePrefixDefinition) RETURN n.`http://xmlns.com/foaf/0.1/` AS prefix")
              .next().get("prefix").asString());

      session.run("MATCH (n) DETACH DELETE n ;");

      importResults = session.run("CALL semantics.importRDFSnippet('" +
          jsonLdFragment + "','JSON-LD',"
          +
          "{ handleVocabUris: 'SHORTEN', typesToLabels: true, commitSize: 500})");
      assertEquals(6L, importResults.next().get("triplesLoaded").asLong());
      assertEquals("http://me.markus-lanthaler.com/",
          session.run(
              "MATCH (n{ns0" + PREFIX_SEPARATOR + "name : 'Markus Lanthaler'}) RETURN n.uri AS uri")
              .next().get("uri").asString());
      assertEquals(1L,
          session.run("MATCH (n) WHERE exists(n.ns0" + PREFIX_SEPARATOR
              + "modified) RETURN count(n) AS count")
              .next().get("count").asLong());

      assertEquals("ns0",
          session.run(
              "MATCH (n:NamespacePrefixDefinition) RETURN n.`http://xmlns.com/foaf/0.1/` AS prefix")
              .next().get("prefix").asString());
    }

  }

  @Test
  public void testImportRDFXML() throws Exception {
    try (Driver driver = GraphDatabase.driver(neo4j.boltURI(),
        Config.build().toConfig()); Session session = driver.session()) {

      createIndices(neo4j.getGraphDatabaseService());

      StatementResult importResults
          = session.run("CALL semantics.importRDF('" +
          RDFImportTest.class.getClassLoader()
              .getResource("jeu-de-donnees-des-jeux-de-donnees-open-data-paris.rdf")
              .toURI()
          + "','RDF/XML',{ handleVocabUris: 'KEEP', typesToLabels: true, commitSize: 500})");
      assertEquals(38L, importResults
          .next().get("triplesLoaded").asLong());
      assertEquals(7L,
          session
              .run("MATCH ()-[r:`http://purl.org/dc/terms/relation`]->(b) RETURN count(b) as count")
              .next().get("count").asLong());
      assertEquals(
          "http://opendata.paris.fr/opendata/jsp/site/Portal.jsp?document_id=109&portlet_id=106",
          session.run(
              "MATCH (x:Resource) WHERE x.`http://www.w3.org/2000/01/rdf-schema#label` = 'harvest_dataset_url'"
                  +
                  "\nRETURN x.`http://www.w3.org/1999/02/22-rdf-syntax-ns#value` AS datasetUrl")
              .next().get("datasetUrl").asString());

    }
  }

  @Test
  public void testImportRDFXMLShortening() throws Exception {
    try (Driver driver = GraphDatabase.driver(neo4j.boltURI(),
        Config.build().toConfig()); Session session = driver.session()) {

      createIndices(neo4j.getGraphDatabaseService());

      StatementResult importResults
          = session.run("CALL semantics.importRDF('" +
          RDFImportTest.class.getClassLoader()
              .getResource("jeu-de-donnees-des-jeux-de-donnees-open-data-paris.rdf")
              .toURI()
          + "','RDF/XML',{ handleVocabUris: 'SHORTEN', typesToLabels: true, commitSize: 500})");
      assertEquals(38L, importResults
          .next().get("triplesLoaded").asLong());
      assertEquals(7L,
          session
              .run("MATCH ()-[r]->(b) WHERE type(r) CONTAINS 'relation' RETURN count(b) as count")
              .next().get("count").asLong());

      assertEquals(
          "http://opendata.paris.fr/opendata/jsp/site/Portal.jsp?document_id=109&portlet_id=106",
          session.run(
              "MATCH (x:Resource) WHERE x.rdfs" + PREFIX_SEPARATOR + "label = 'harvest_dataset_url'"

                  + "\nRETURN x.rdf" + PREFIX_SEPARATOR + "value AS datasetUrl").next()
              .get("datasetUrl").asString());

      assertEquals("ns0",
          session.run("MATCH (n:NamespacePrefixDefinition) \n" +
              "RETURN n.`http://www.w3.org/ns/dcat#` as prefix")
              .next().get("prefix").asString());

    }
  }

  @Test
  public void testImportRDFXMLShorteningWithPrefixPreDefinition() throws Exception {
    try (Driver driver = GraphDatabase.driver(neo4j.boltURI(),
        Config.build().toConfig()); Session session = driver.session()) {

      createIndices(neo4j.getGraphDatabaseService());

      session.run("WITH {`http://purl.org/dc/terms/`:'dc',\n" +
          "`http://www.w3.org/1999/02/22-rdf-syntax-ns#`:'rdf',\n" +
          "`http://www.w3.org/2002/07/owl#`:'owl',\n" +
          "`http://www.w3.org/ns/dcat#`:'dcat',\n" +
          "`http://www.w3.org/2000/01/rdf-schema#`:'rdfs',\n" +
          "`http://xmlns.com/foaf/0.1/`:'foaf'} as nslist\n" +
          "MERGE (n:NamespacePrefixDefinition)\n" +
          "SET n+=nslist");

      StatementResult importResults
          = session.run("CALL semantics.importRDF('" +
          RDFImportTest.class.getClassLoader()
              .getResource("jeu-de-donnees-des-jeux-de-donnees-open-data-paris.rdf")
              .toURI()
          + "','RDF/XML', { handleVocabUris: 'SHORTEN', typesToLabels: true, commitSize: 500})");
      assertEquals(38L, importResults
          .next().get("triplesLoaded").asLong());
      assertEquals(7L,
          session
              .run("MATCH ()-[r:dc" + PREFIX_SEPARATOR + "relation]->(b) RETURN count(b) as count")
              .next().get("count").asLong());

      assertEquals(
          "http://opendata.paris.fr/opendata/jsp/site/Portal.jsp?document_id=109&portlet_id=106",
          session
              .run("MATCH (x) WHERE x.rdfs" + PREFIX_SEPARATOR + "label = 'harvest_dataset_url'" +
                  "\nRETURN x.rdf" + PREFIX_SEPARATOR + "value AS datasetUrl").next()
              .get("datasetUrl").asString());

      assertEquals("dcat",
          session.run("MATCH (n:NamespacePrefixDefinition) \n" +
              "RETURN n.`http://www.w3.org/ns/dcat#` as prefix")
              .next().get("prefix").asString());

    }
  }

  @Test
  public void testImportRDFXMLShorteningWithPrefixPreDefinitionOneTriple() throws Exception {
    try (Driver driver = GraphDatabase.driver(neo4j.boltURI(),
        Config.build().toConfig()); Session session = driver.session()) {

      createIndices(neo4j.getGraphDatabaseService());

      session.run("WITH {`http://neo4j.com/voc/`:'voc' } as nslist\n" +
          "MERGE (n:NamespacePrefixDefinition)\n" +
          "SET n+=nslist " +
          "RETURN n ");

      StatementResult importResults = session.run("CALL semantics.importRDF('" +
          RDFImportTest.class.getClassLoader().getResource("oneTriple.rdf")
              .toURI()
          + "','RDF/XML',{ handleVocabUris: 'SHORTEN', typesToLabels: true, commitSize: 500})");
      assertEquals(1L, importResults.next().get("triplesLoaded").asLong());
      assertEquals("JB",
          session.run(
              "MATCH (jb {uri: 'http://neo4j.com/invividual/JB'}) RETURN jb.voc" + PREFIX_SEPARATOR
                  + "name AS name")
              .next().get("name").asString());

      assertEquals("voc",
          session.run("MATCH (n:NamespacePrefixDefinition) \n" +
              "RETURN n.`http://neo4j.com/voc/` as prefix")
              .next().get("prefix").asString());

    }
  }

  @Test
  public void testImportBadUrisTtl() throws Exception {
    try (Driver driver = GraphDatabase.driver(neo4j.boltURI(),
        Config.build().toConfig()); Session session = driver.session()) {

      createIndices(neo4j.getGraphDatabaseService());

      session.run("WITH {`http://example.org/vocab/show/`:'pr' } as nslist\n" +
          "MERGE (n:NamespacePrefixDefinition)\n" +
          "SET n+=nslist " +
          "RETURN n ");

      StatementResult importResults
          = session.run("CALL semantics.importRDF('" +
          RDFImportTest.class.getClassLoader().getResource("badUri.ttl")
              .toURI()
          + "','Turtle',{ handleVocabUris: 'SHORTEN', typesToLabels: true, commitSize: 500, verifyUriSyntax: false})");
      assertEquals(2L, importResults
          .next().get("triplesLoaded").asLong());
      assertEquals("test name",
          session.run("MATCH (jb {uri: 'http://example.org/vocab/show/ent'}) RETURN jb.pr"
              + PREFIX_SEPARATOR + "name AS name")
              .next().get("name").asString());
    }
  }

  @Test
  public void testImportTtlBadUrisException() throws Exception {
    try (Driver driver = GraphDatabase.driver(neo4j.boltURI(),
        Config.build().toConfig()); Session session = driver.session()) {

      createIndices(neo4j.getGraphDatabaseService());

      session.run("WITH {`http://example.org/vocab/show/`:'pr' } as nslist\n" +
          "MERGE (n:NamespacePrefixDefinition)\n" +
          "SET n+=nslist " +
          "RETURN n ");

      StatementResult importResults
          = session.run("CALL semantics.importRDF('" +
          RDFImportTest.class.getClassLoader().getResource("badUri.ttl")
              .toURI()
          + "','Turtle',{ handleVocabUris: 'SHORTEN', typesToLabels: true, commitSize: 500})");
      assertEquals(0, importResults
          .next().get("triplesLoaded").asLong());
      assertFalse(session.run("MATCH (jb {uri: 'http://example.org/vocab/show/ent'}) RETURN jb.pr"
          + PREFIX_SEPARATOR + "name AS name")
          .hasNext());
    }
  }

  @Test
  public void testImportRDFXMLBadUris() throws Exception {
    try (Driver driver = GraphDatabase.driver(neo4j.boltURI(),
        Config.build().toConfig()); Session session = driver.session()) {

      createIndices(neo4j.getGraphDatabaseService());

      session.run("WITH {`http://neo4j.com/voc/`:'voc' } as nslist\n" +
          "MERGE (n:NamespacePrefixDefinition)\n" +
          "SET n+=nslist " +
          "RETURN n ");

      StatementResult importResults
          = session.run("CALL semantics.importRDF('" +
          RDFImportTest.class.getClassLoader().getResource("badUris.rdf")
              .toURI()
          + "','RDF/XML',{ handleVocabUris: 'SHORTEN', typesToLabels: true, commitSize: 500})");
      assertEquals(1L, importResults
          .next().get("triplesLoaded").asLong());
      assertEquals("JB",
          session.run("MATCH (jb {uri: 'http://neo4j.com/invividual/JB\\'sUri'}) RETURN jb.voc"
              + PREFIX_SEPARATOR + "name AS name")
              .next().get("name").asString());
    }
  }

  @Test
  public void testImportLangFilter() throws Exception {
    try (Driver driver = GraphDatabase.driver(neo4j.boltURI(),
        Config.build().toConfig()); Session session = driver.session()) {

      createIndices(neo4j.getGraphDatabaseService());

      session.run("WITH {`http://example.org/vocab/show/`:'voc' } as nslist\n" +
          "MERGE (n:NamespacePrefixDefinition)\n" +
          "SET n+=nslist " +
          "RETURN n ");

      StatementResult importResults
          = session.run("CALL semantics.importRDF('" +
          RDFImportTest.class.getClassLoader().getResource("multilang.ttl")
              .toURI()
          + "','Turtle',{ handleVocabUris: 'SHORTEN', typesToLabels: true, languageFilter: 'en', commitSize: 500})");
      assertEquals(1L, importResults
          .next().get("triplesLoaded").asLong());
      assertEquals("That Seventies Show",
          session.run(
              "MATCH (t {uri: 'http://example.org/vocab/show/218'}) RETURN t.voc" + PREFIX_SEPARATOR
                  + "localName AS name")
              .next().get("name").asString());

      session.run("MATCH (t {uri: 'http://example.org/vocab/show/218'}) DETACH DELETE t ");

      importResults
          = session.run("CALL semantics.importRDF('" +
          RDFImportTest.class.getClassLoader().getResource("multilang.ttl")
              .toURI()
          + "','Turtle',{ handleVocabUris: 'SHORTEN', typesToLabels: true, languageFilter: 'fr', commitSize: 500})");
      assertEquals(1L, importResults
          .next().get("triplesLoaded").asLong());
      assertEquals("Cette Série des Années Soixante-dix",
          session.run(
              "MATCH (t {uri: 'http://example.org/vocab/show/218'}) RETURN t.voc" + PREFIX_SEPARATOR
                  + "localName AS name")
              .next().get("name").asString());

      session.run("MATCH (t {uri: 'http://example.org/vocab/show/218'}) DETACH DELETE t ");

      importResults
          = session.run("CALL semantics.importRDF('" +
          RDFImportTest.class.getClassLoader().getResource("multilang.ttl")
              .toURI()
          + "','Turtle',{ handleVocabUris: 'SHORTEN', typesToLabels: true, languageFilter: 'fr-be', commitSize: 500})");
      assertEquals(1L, importResults
          .next().get("triplesLoaded").asLong());
      assertEquals("Cette Série des Années Septante",
          session.run(
              "MATCH (t {uri: 'http://example.org/vocab/show/218'}) RETURN t.voc" + PREFIX_SEPARATOR
                  + "localName AS name")
              .next().get("name").asString());

      session.run("MATCH (t {uri: 'http://example.org/vocab/show/218'}) DETACH DELETE t ");

      importResults
          = session.run("CALL semantics.importRDF('" +
          RDFImportTest.class.getClassLoader().getResource("multilang.ttl")
              .toURI()
          + "','Turtle',{ handleVocabUris: 'SHORTEN', typesToLabels: true, commitSize: 500})");
      // no language filter means three triples are ingested
      assertEquals(3L, importResults
          .next().get("triplesLoaded").asLong());
      //default option is overwrite, so only the last value is kept
      assertEquals("Cette Série des Années Septante",
          session.run(
              "MATCH (t {uri: 'http://example.org/vocab/show/218'}) RETURN t.voc" + PREFIX_SEPARATOR
                  + "localName AS name")
              .next().get("name").asString());

    }
  }

  @Test
  public void testImportMultivalLangTag() throws Exception {
    try (Driver driver = GraphDatabase.driver(neo4j.boltURI(),
        Config.build().toConfig()); Session session = driver.session()) {

      createIndices(neo4j.getGraphDatabaseService());
      String importCypher = "CALL semantics.importRDF('" +
          RDFImportTest.class.getClassLoader().getResource("multilang.ttl")
              .toURI() + "','Turtle',{ keepLangTag : true, handleMultival: 'ARRAY'})";
      StatementResult importResults
          = session.run(importCypher);
      Record next = importResults
          .next();
      assertEquals(3, next.get("triplesLoaded").asInt());

      importResults
          = session.run(
          "match (n:Resource) return n.ns0__localName as all, semantics.getLangValue('en',n.ns0__localName) as en_name, "
              +
              "semantics.getLangValue('fr',n.ns0__localName) as fr_name, semantics.getLangValue('fr-be',n.ns0__localName) as frbe_name");
      next = importResults
          .next();
      assertEquals("That Seventies Show", next.get("en_name").asString());
      assertEquals("Cette Série des Années Soixante-dix", next.get("fr_name").asString());
      assertEquals("Cette Série des Années Septante", next.get("frbe_name").asString());
    }
  }

  @Test
  public void testImportMultivalWithMultivalList() throws Exception {
    try (Driver driver = GraphDatabase.driver(neo4j.boltURI(),
        Config.build().toConfig()); Session session = driver.session()) {

      createIndices(neo4j.getGraphDatabaseService());
      String importCypher = "CALL semantics.importRDF('" +
          RDFImportTest.class.getClassLoader().getResource("multival.ttl")
              .toURI()
          + "','Turtle',{ handleMultival: 'ARRAY', multivalPropList : ['http://example.org/vocab/show/availableInLang','http://example.org/vocab/show/localName'] })";
      StatementResult importResults
          = session.run(importCypher);
      Record next = importResults
          .next();

      assertEquals(9, next.get("triplesLoaded").asInt());

      importResults
          = session.run(
          "match (n:Resource) return n.ns0__localName as all, n.ns0__availableInLang as ail, n.ns0__showId as sid, n.ns0__producer as prod ");
      next = importResults
          .next();
      List<String> localNames = new ArrayList<>();
      localNames.add("That Seventies Show");
      localNames.add("Cette Série des Années Soixante-dix");
      localNames.add("Cette Série des Années Septante");
      assertEquals(localNames, next.get("all").asList());
      List<String> availableInLang = new ArrayList<>();
      availableInLang.add("EN");
      availableInLang.add("FR");
      availableInLang.add("ES");
      assertEquals(availableInLang, next.get("ail").asList());
      assertEquals(218, next.get("sid").asLong());
      assertEquals("Joanna Smith", next.get("prod").asString());
    }
  }

  @Test
  public void testImportMultivalWithExclusionList() throws Exception {
    try (Driver driver = GraphDatabase.driver(neo4j.boltURI(),
        Config.build().toConfig()); Session session = driver.session()) {

      createIndices(neo4j.getGraphDatabaseService());
      String importCypher = "CALL semantics.importRDF('" +
          RDFImportTest.class.getClassLoader().getResource("multival.ttl")
              .toURI()
          + "','Turtle',{ handleMultival: 'ARRAY', predicateExclusionList : ['http://example.org/vocab/show/availableInLang','http://example.org/vocab/show/localName'] })";
      StatementResult importResults
          = session.run(importCypher);
      Record next = importResults
          .next();

      assertEquals(3, next.get("triplesLoaded").asInt());

      importResults
          = session.run(
          "match (n:Resource) return n.ns0__localName as all, n.ns0__availableInLang as ail, n.ns0__showId as sid, n.ns0__producer as prod ");
      next = importResults
          .next();
      assertTrue(next.get("all").isNull());
      assertTrue(next.get("ail").isNull());
      List<Long> sids = new ArrayList<Long>();
      sids.add(218L);
      assertEquals(sids, next.get("sid").asList());
      List<String> prod = new ArrayList<String>();
      prod.add("John Smith");
      prod.add("Joanna Smith");
      assertEquals(prod, next.get("prod").asList());
    }
  }

  @Test
  public void testImportTurtle() throws Exception {
    try (Driver driver = GraphDatabase.driver(neo4j.boltURI(),
        Config.build().toConfig()); Session session = driver.session()) {

      createIndices(neo4j.getGraphDatabaseService());

      StatementResult importResults
          = session.run("CALL semantics.importRDF('" +
          RDFImportTest.class.getClassLoader().getResource("opentox-example.ttl")
              .toURI()
          + "','Turtle',{ handleVocabUris: 'KEEP', typesToLabels: true, commitSize: 500})");
      assertEquals(157L, importResults
          .next().get("triplesLoaded").asLong());
      StatementResult algoNames = session
          .run("MATCH (n:`http://www.opentox.org/api/1.1#Algorithm`) " +
              "\nRETURN n.`http://purl.org/dc/elements/1.1/title` AS algos ORDER By algos");

      assertEquals("J48", algoNames.next().get("algos").asString());
      assertEquals("XLogP", algoNames.next().get("algos").asString());

      StatementResult compounds = session.run(
          "MATCH ()-[r:`http://www.opentox.org/api/1.1#compound`]->(c) RETURN DISTINCT c.uri AS compound order by compound");
      assertEquals("http://www.opentox.org/example/1.1#benzene",
          compounds.next().get("compound").asString());
      assertEquals("http://www.opentox.org/example/1.1#phenol",
          compounds.next().get("compound").asString());

    }
  }

  /**
   * Can we populate the cache correctly when we have a miss?
   */
  @Test
  public void testImportTurtle02() throws Exception {
    try (Driver driver = GraphDatabase.driver(neo4j.boltURI(),
        Config.build().toConfig()); Session session = driver.session()) {

      createIndices(neo4j.getGraphDatabaseService());
      session.run("CREATE (rdf:NamespacePrefixDefinition {" +
          "  `http://www.example.com/ontology/1.0.0#`: 'ex'," +
          "  `http://www.w3.org/1999/02/22-rdf-syntax-ns#`: 'rdfs'})");
      StatementResult importResults = session.run(String.format(
          "CALL semantics.importRDF('%s','Turtle',{nodeCacheSize: 1})",
          file("myrdf/testImportTurtle02.ttl")));
      assertEquals(5, importResults.next().get("triplesLoaded").asInt());

      StatementResult result = session.run(
          "MATCH (:ex" + PREFIX_SEPARATOR + "DISTANCEVALUE)-[:ex" + PREFIX_SEPARATOR
              + "units]->(mu) " +
              "RETURN mu.uri AS unitsUri, mu.ex" + PREFIX_SEPARATOR + "name as unitsName");
      Record first = result.next();
      assertEquals("http://www.example.com/ontology/1.0.0/common#MEASUREMENTUNIT-T1510615421640",
          first.get("unitsUri").asString());
      assertEquals("metres", first.get("unitsName").asString());
    }
  }

  @Test
  public void testPreviewFromSnippetPassWrongUri() throws Exception {
    try (Driver driver = GraphDatabase.driver(neo4j.boltURI(),
        Config.build().toConfig()); Session session = driver.session()) {

      createIndices(neo4j.getGraphDatabaseService());

      StatementResult importResults
          = session
          .run("CALL semantics.previewRDFSnippet('" + wrongUriTtl
              + "','Turtle',{ handleVocabUris: 'KEEP', typesToLabels: false, verifyUriSyntax: false})");
      Map<String, Object> next = importResults
          .next().asMap();
      final List<Node> nodes = (List<Node>) next.get("nodes");
      assertEquals(2, nodes.size());
      final List<Relationship> rels = (List<Relationship>) next.get("relationships");
      assertEquals(1, rels.size());
    }
  }

  @Test
  public void testPreviewFromSnippetFailWrongUri() throws Exception {
    try (Driver driver = GraphDatabase.driver(neo4j.boltURI(),
        Config.build().toConfig()); Session session = driver.session()) {

      createIndices(neo4j.getGraphDatabaseService());

      StatementResult importResults
          = session
          .run("CALL semantics.previewRDFSnippet('" + wrongUriTtl
              + "','Turtle',{ handleVocabUris: 'KEEP', typesToLabels: false})");
      Map<String, Object> next = importResults
          .next().asMap();
      final List<Node> nodes = (List<Node>) next.get("nodes");
      assertEquals(0, nodes.size());
      final List<Relationship> rels = (List<Relationship>) next.get("relationships");
      assertEquals(0, rels.size());
    }
  }

  @Test
  public void testPreviewFromSnippet() throws Exception {
    try (Driver driver = GraphDatabase.driver(neo4j.boltURI(),
        Config.build().toConfig()); Session session = driver.session()) {

      createIndices(neo4j.getGraphDatabaseService());
      StatementResult importResults
          = session
          .run("CALL semantics.previewRDFSnippet('" + jsonLdFragment
              + "','JSON-LD',{ handleVocabUris: 'KEEP', typesToLabels: false})");
      Map<String, Object> next = importResults
          .next().asMap();
      final List<Node> nodes = (List<Node>) next.get("nodes");
      assertEquals(3, nodes.size());
      final List<Relationship> rels = (List<Relationship>) next.get("relationships");
      assertEquals(2, rels.size());
    }
  }

  @Test
  public void testPreviewFromSnippetLangFilter() throws Exception {
    try (Driver driver = GraphDatabase.driver(neo4j.boltURI(),
        Config.build().toConfig()); Session session = driver.session()) {

      createIndices(neo4j.getGraphDatabaseService());

      String turtleFragment = "@prefix show: <http://example.org/vocab/show/> .\n" +
          "\n" +
          "show:218 show:localName \"That Seventies Show\"@en .                 # literal with a language tag\n"
          +
          "show:218 show:localName \"Cette Série des Années Soixante-dix\"@fr . \n" +
          "show:218 show:localName \"Cette Série des Années Septante\"@fr-be .  # literal with a region subtag";
      StatementResult importResults
          = session
          .run("CALL semantics.previewRDFSnippet('" + turtleFragment
              + "','Turtle',{ handleVocabUris: 'KEEP', typesToLabels: false, languageFilter: 'fr'})");
      Record next = importResults
          .next();
      assertEquals(1, next.get("nodes").size());
      assertEquals("Cette Série des Années Soixante-dix",
          next.get("nodes").asList(ofNode()).get(0).get("http://example.org/vocab/show/localName")
              .asString());
      assertEquals(0, next.get("relationships").size());

      importResults
          = session.run("CALL semantics.previewRDFSnippet('" + turtleFragment
          + "','Turtle',{ handleVocabUris: 'KEEP', typesToLabels: false, languageFilter: 'en'})");
      assertEquals("That Seventies Show", importResults
          .next().get("nodes").asList(ofNode()).get(0)
          .get("http://example.org/vocab/show/localName").asString());

    }
  }

  @Test
  public void testPreviewFromFile() throws Exception {
    try (Driver driver = GraphDatabase.driver(neo4j.boltURI(),
        Config.build().toConfig()); Session session = driver.session()) {

      createIndices(neo4j.getGraphDatabaseService());

      StatementResult importResults
          = session.run("CALL semantics.previewRDF('" +
          RDFImportTest.class.getClassLoader()
              .getResource("jeu-de-donnees-des-jeux-de-donnees-open-data-paris.rdf")
              .toURI() + "','RDF/XML',{ handleVocabUris: 'KEEP', typesToLabels: false})");
      Map<String, Object> next = importResults
          .next().asMap();
      final List<Node> nodes = (List<Node>) next.get("nodes");
      assertEquals(15, nodes.size());
      final List<Relationship> rels = (List<Relationship>) next.get("relationships");
      assertEquals(15, rels.size());
    }
  }

  @Test
  public void testPreviewFromBadUriFile() throws Exception {
    try (Driver driver = GraphDatabase.driver(neo4j.boltURI(),
        Config.build().toConfig()); Session session = driver.session()) {

      createIndices(neo4j.getGraphDatabaseService());

      StatementResult importResults
          = session.run("CALL semantics.previewRDF('" +
          RDFImportTest.class.getClassLoader()
              .getResource("badUri.ttl")
              .toURI()
          + "','Turtle',{ handleVocabUris: 'KEEP', typesToLabels: false, verifyUriSyntax: false})");
      Map<String, Object> next = importResults
          .next().asMap();
      final List<Node> nodes = (List<Node>) next.get("nodes");
      assertEquals(2, nodes.size());
      final List<Relationship> rels = (List<Relationship>) next.get("relationships");
      assertEquals(1, rels.size());
    }
  }

  @Test
  public void testPreviewFromBadUriFileFail() throws Exception {
    try (Driver driver = GraphDatabase.driver(neo4j.boltURI(),
        Config.build().toConfig()); Session session = driver.session()) {

      createIndices(neo4j.getGraphDatabaseService());

      StatementResult importResults
          = session.run("CALL semantics.previewRDF('" +
          RDFImportTest.class.getClassLoader()
              .getResource("badUri.ttl")
              .toURI() + "','Turtle',{ handleVocabUris: 'KEEP', typesToLabels: false})");
      Map<String, Object> next = importResults
          .next().asMap();
      final List<Node> nodes = (List<Node>) next.get("nodes");
      assertEquals(0, nodes.size());
      final List<Relationship> rels = (List<Relationship>) next.get("relationships");
      assertEquals(0, rels.size());
    }
  }

  @Test
  public void testPreviewFromFileLangFilter() throws Exception {
    try (Driver driver = GraphDatabase.driver(neo4j.boltURI(),
        Config.build().toConfig()); Session session = driver.session()) {

      createIndices(neo4j.getGraphDatabaseService());

      StatementResult importResults
          = session.run("CALL semantics.previewRDF('" +
          RDFImportTest.class.getClassLoader().getResource("multilang.ttl")
              .toURI()
          + "','Turtle',{ handleVocabUris: 'KEEP', typesToLabels: false, languageFilter: 'fr', keepLangTag : false })");
      Record next = importResults
          .next();

      assertEquals(1, next.get("nodes").size());
      assertEquals("Cette Série des Années Soixante-dix",
          next.get("nodes").asList(ofNode()).get(0).get("http://example.org/vocab/show/localName")
              .asString());
      assertEquals(0, (next.get("relationships")).size());

      importResults
          = session.run("CALL semantics.previewRDF('" +
          RDFImportTest.class.getClassLoader().getResource("multilang.ttl").toURI()
          + "','Turtle',{ handleVocabUris: 'KEEP', typesToLabels: false, languageFilter: 'en', keepLangTag : false })");
      assertEquals("That Seventies Show", importResults
          .next().get("nodes").asList(ofNode()).get(0)
          .get("http://example.org/vocab/show/localName").asString());
    }
  }

  @Test
  public void testImportFromFileWithMapping() throws Exception {
    try (Driver driver = GraphDatabase.driver(neo4j.boltURI(),
        Config.build().toConfig()); Session session = driver.session()) {

      createIndices(neo4j.getGraphDatabaseService());

      String addMapping1 =
          " call semantics.mapping.addSchema(\"http://neo4j.com/voc/\",\"voc\") yield namespace as sch\n"
              +
              "call semantics.mapping.addMappingToSchema(sch,\"uniqueName\",\"name\") yield elemName as mapping1\n"
              +
              "return *";
      session.run(addMapping1);
      String addMapping2 =
          " call semantics.mapping.addSchema(\"http://neo4j.com/category/\",\"cats\") yield namespace as sch\n"
              +
              "call semantics.mapping.addMappingToSchema(sch,\"Media\",\"Publication\") yield elemName as mapping1\n"
              +
              "return *";
      session.run(addMapping2);

      StatementResult importResults
          = session.run("CALL semantics.importRDF('" +
          RDFImportTest.class.getClassLoader().getResource("myrdf/three.rdf")
              .toURI() + "','RDF/XML',{ handleVocabUris: 'MAP'})");
      assertEquals(6L, importResults
          .next().get("triplesLoaded").asLong());
      StatementResult mediaNames = session.run("MATCH (m:Media) " +
          "\nRETURN m.uniqueName AS nm, m.uri AS uri");

      Record next = mediaNames.next();
      assertEquals("The Financial Times", next.get("nm").asString());
      assertEquals("http://neo4j.com/invividual/FT", next.get("uri").asString());

      StatementResult personNames = session.run("MATCH (m { PersonName : 'JC'}) " +
          "\nRETURN m.LivesIn AS li, m.uri AS uri");

      next = personNames.next();
      assertEquals("Chesham", next.get("li").asString());
      assertEquals("http://neo4j.com/invividual/JC", next.get("uri").asString());
    }
  }

  @Test
  public void testImportFromFileIgnoreNs() throws Exception {
    try (Driver driver = GraphDatabase.driver(neo4j.boltURI(),
        Config.build().toConfig()); Session session = driver.session()) {

      createIndices(neo4j.getGraphDatabaseService());

      StatementResult importResults
          = session.run("CALL semantics.importRDF('" +
          RDFImportTest.class.getClassLoader().getResource("myrdf/three.rdf")
              .toURI() + "','RDF/XML',{ handleVocabUris: 'IGNORE'})");
      assertEquals(6L, importResults
          .next().get("triplesLoaded").asLong());
      StatementResult mediaNames = session.run("MATCH (m:Publication) " +
          "\nRETURN m.name AS nm, m.uri AS uri");

      Record next = mediaNames.next();
      assertEquals("The Financial Times", next.get("nm").asString());
      assertEquals("http://neo4j.com/invividual/FT", next.get("uri").asString());

      StatementResult rels = session.run(
          "MATCH ({ PersonName: 'JC'})-[r:reads]-(:Publication { name: 'The Financial Times'}) " +
              "\nRETURN count(r) as ct");

      next = rels.next();
      assertEquals(1L, next.get("ct").asLong());

    }
  }

  @Test
  public void testImportFromFileIgnoreNsApplyNeoNaming() throws Exception {
    try (Driver driver = GraphDatabase.driver(neo4j.boltURI(),
        Config.build().toConfig()); Session session = driver.session()) {

      createIndices(neo4j.getGraphDatabaseService());

      StatementResult importResults
          = session.run("CALL semantics.importRDF('" +
          RDFImportTest.class.getClassLoader().getResource("myrdf/three.rdf")
              .toURI() + "','RDF/XML',{ handleVocabUris: 'IGNORE', applyNeo4jNaming: true })");
      assertEquals(6L, importResults
          .next().get("triplesLoaded").asLong());
      StatementResult mediaNames = session.run("MATCH (m:Publication) " +
          "\nRETURN m.name AS nm, m.uri AS uri");

      Record next = mediaNames.next();
      assertEquals("The Financial Times", next.get("nm").asString());
      assertEquals("http://neo4j.com/invividual/FT", next.get("uri").asString());

      StatementResult rels = session.run(
          "MATCH ({ personName: 'JC'})-[r:READS]-(:Publication { name: 'The Financial Times'}) " +
              "\nRETURN count(r) as ct");

      next = rels.next();
      assertEquals(1L, next.get("ct").asLong());

    }
  }

  @Test
  public void testImportFromFileWithPredFilter() throws Exception {
    try (Driver driver = GraphDatabase.driver(neo4j.boltURI(),
        Config.build().toConfig()); Session session = driver.session()) {

      createIndices(neo4j.getGraphDatabaseService());

      String addMapping1 =
          " call semantics.mapping.addSchema(\"http://schema.org/\",\"sch\") yield namespace as sch\n"
              +
              "call semantics.mapping.addMappingToSchema(sch,\"WHERE\",\"location\") yield elemName as mapping1\n"
              +
              "call semantics.mapping.addMappingToSchema(sch,\"desc\",\"description\") yield elemName as mapping2\n"
              +
              "return *";
      session.run(addMapping1);

      StatementResult importResults
          = session.run("CALL semantics.importRDF('" +
          RDFImportTest.class.getClassLoader().getResource("event.json")
              .toURI()
          + "','JSON-LD',{ handleVocabUris: 'MAP', predicateExclusionList: ['http://schema.org/price','http://schema.org/priceCurrency'] })");
      assertEquals(26L, importResults
          .next().get("triplesLoaded").asLong());

      StatementResult postalAddresses = session.run("MATCH (m:PostalAddress) " +
          "\nRETURN m.postalCode as zip");

      Record next = postalAddresses.next();
      assertEquals("95051", next.get("zip").asString());

      StatementResult whereRels = session.run("MATCH (e:Event)-[:WHERE]->(p:Place) " +
          "\nRETURN p.name as placeName, e.desc as desc ");

      next = whereRels.next();
      assertEquals(
          "Join us for an afternoon of Jazz with Santa Clara resident and pianist Andy Lagunoff. Complimentary food and beverages will be served.",
          next.get("desc").asString());
      assertEquals("Santa Clara City Library, Central Park Library",
          next.get("placeName").asString());

    }
  }

  @Test
  public void testStreamFromFile() throws Exception {
    try (Driver driver = GraphDatabase.driver(neo4j.boltURI(),
        Config.build().toConfig()); Session session = driver.session()) {

      createIndices(neo4j.getGraphDatabaseService());

      StatementResult importResults
          = session.run("CALL semantics.streamRDF('" +
          RDFImportTest.class.getClassLoader().getResource("oneTriple.rdf")
              .toURI() + "','RDF/XML',{})");
      Map<String, Object> next = importResults
          .next().asMap();
      assertEquals("http://neo4j.com/invividual/JB", next.get("subject"));
      assertEquals("http://neo4j.com/voc/name", next.get("predicate"));
      assertEquals("JB", next.get("object"));
      assertEquals(true, next.get("isLiteral"));
      assertEquals("http://www.w3.org/2001/XMLSchema#string", next.get("literalType"));
      assertNull(next.get("literalLang"));
    }
  }

  @Test
  public void testStreamFromString() throws Exception {
    try (Driver driver = GraphDatabase.driver(neo4j.boltURI(),
        Config.build().toConfig()); Session session = driver.session()) {

      createIndices(neo4j.getGraphDatabaseService());

      String rdf = "<rdf:RDF xmlns:owl=\"http://www.w3.org/2002/07/owl#\"\n"
          + "         xmlns:rdfs=\"http://www.w3.org/2000/01/rdf-schema#\"\n"
          + "         xmlns:rdf=\"http://www.w3.org/1999/02/22-rdf-syntax-ns#\"\n"
          + "         xmlns:voc=\"http://neo4j.com/voc/\">\n"
          + "         <rdf:Description rdf:about=\"http://neo4j.com/invividual/JB\">\n"
          + "            <voc:name>JB</voc:name>\n"
          + "         </rdf:Description>\n"
          + "</rdf:RDF>";

      StatementResult importResults
          = session.run("CALL semantics.streamRDFSnippet('" + rdf + "','RDF/XML',{})");
      Map<String, Object> next = importResults
          .next().asMap();
      assertEquals("http://neo4j.com/invividual/JB", next.get("subject"));
      assertEquals("http://neo4j.com/voc/name", next.get("predicate"));
      assertEquals("JB", next.get("object"));
      assertEquals(true, next.get("isLiteral"));
      assertEquals("http://www.w3.org/2001/XMLSchema#string", next.get("literalType"));
      assertNull(next.get("literalLang"));
    }
  }

  @Test
  public void testStreamFromBadUriFile() throws Exception {
    try (Driver driver = GraphDatabase.driver(neo4j.boltURI(),
        Config.build().toConfig()); Session session = driver.session()) {

      createIndices(neo4j.getGraphDatabaseService());

      StatementResult importResults
          = session.run("CALL semantics.streamRDF('" +
          RDFImportTest.class.getClassLoader().getResource("badUri.ttl")
              .toURI() + "','Turtle',{verifyUriSyntax: false})");
      Map<String, Object> next = importResults
          .next().asMap();
      assertEquals("http://example.org/vocab/show/ent", next.get("subject"));
      assertEquals("http://example.org/vocab/show/P854", next.get("predicate"));
      assertEquals(
          "https://suasprod.noc-science.at/XLCubedWeb/WebForm/ShowReport.aspx?rep=004+studierende%2f001+universit%u00e4",
          next.get("object"));
      assertEquals(false, next.get("isLiteral"));
    }
  }

  @Test
  public void testStreamFromBadUriString() throws Exception {
    try (Driver driver = GraphDatabase.driver(neo4j.boltURI(),
        Config.build().toConfig()); Session session = driver.session()) {

      createIndices(neo4j.getGraphDatabaseService());

      String rdf = "@prefix pr: <http://example.org/vocab/show/> .\n"
          + "pr:ent\n"
          + "      pr:P854 <https://suasprod.noc-science.at/XLCubedWeb/WebForm/ShowReport.aspx?rep=004+studierende%2f001+universit%u00e4> ;\n"
          + "      pr:name \"test name\" .";
      StatementResult importResults
          = session.run("CALL semantics.streamRDFSnippet('" + rdf + "','Turtle',{verifyUriSyntax: false})");
      Map<String, Object> next = importResults
          .next().asMap();
      assertEquals("http://example.org/vocab/show/ent", next.get("subject"));
      assertEquals("http://example.org/vocab/show/P854", next.get("predicate"));
      assertEquals(
          "https://suasprod.noc-science.at/XLCubedWeb/WebForm/ShowReport.aspx?rep=004+studierende%2f001+universit%u00e4",
          next.get("object"));
      assertEquals(false, next.get("isLiteral"));
    }
  }

  @Test
  public void testStreamFromBadUriFileFail() throws Exception {
    try (Driver driver = GraphDatabase.driver(neo4j.boltURI(),
        Config.build().toConfig()); Session session = driver.session()) {

      createIndices(neo4j.getGraphDatabaseService());

      StatementResult importResults
          = session.run("CALL semantics.streamRDF('" +
          RDFImportTest.class.getClassLoader().getResource("badUri.ttl")
              .toURI() + "','Turtle',{})");
      assertTrue(importResults
          .hasNext());
      Record next = importResults.next();
      assertEquals("neo4j://error", next.get("subject").asString());
      assertEquals("neo4j://message", next.get("predicate").asString());
      assertTrue(next.get("object").asString().startsWith("Illegal percent encoding"));
      assertEquals(true, next.get("isLiteral").asBoolean());
    }
  }

  @Test
  public void testGetLangValUDF() throws Exception {
    try (Driver driver = GraphDatabase.driver(neo4j.boltURI(),
        Config.build().toConfig()); Session session = driver.session()) {

      createIndices(neo4j.getGraphDatabaseService());

      StatementResult importResults
          = session.run(
          "return semantics.getLangValue('fr',[\"The Hague@en\", \"Den Haag@nl\", \"La Haye@fr\"]) as val");
      Map<String, Object> next = importResults
          .next().asMap();
      assertEquals("La Haye", next.get("val"));

      importResults
          = session.run(
          "return semantics.getLangValue('es',[\"The Hague@en\", \"Den Haag@nl\", \"La Haye@fr\"]) as val");
      next = importResults
          .next().asMap();
      assertNull(next.get("val"));

      importResults
          = session.run("return semantics.getLangValue('fr','La Haye@fr') as val");
      next = importResults
          .next().asMap();
      assertEquals("La Haye", next.get("val"));

      importResults
          = session.run("return semantics.getLangValue('es','La Haye@fr') as val");
      next = importResults
          .next().asMap();
      assertNull(next.get("val"));

      importResults
          = session.run("return semantics.getLangValue('es',[2, 45, 3]) as val");
      next = importResults
          .next().asMap();
      assertNull(next.get("val"));

      session.run(
          "create (n:Thing { prop: [\"That Seventies Show@en\", \"Cette Série des Années Soixante-dix@fr\", \"Cette Série des Années Septante@fr-be\"] })");
      importResults
          = session.run(
          "match (n:Thing) return semantics.getLangValue('en',n.prop) as en_name, semantics.getLangValue('fr',n.prop) as fr_name, semantics.getLangValue('fr-be',n.prop) as frbe_name");
      next = importResults
          .next().asMap();
      assertEquals("Cette Série des Années Soixante-dix", next.get("fr_name"));
      assertEquals("That Seventies Show", next.get("en_name"));
      assertEquals("Cette Série des Années Septante", next.get("frbe_name"));

      session.run("match (x:Thing) delete x");
      session.run(
          "create (n:Thing { prop: [\"That Seventies Show@en-US\", \"Cette Série des Années Soixante-dix@fr-custom-tag\", \"你好@zh-Hans-CN\"] })");
      importResults
          = session.run(
          "match (n:Thing) return semantics.getLangValue('en-US',n.prop) as enus_name, semantics.getLangValue('fr-custom-tag',n.prop) as frcust_name, semantics.getLangValue('zh-Hans-CN',n.prop) as cn_name");
      next = importResults
          .next().asMap();
      assertEquals("Cette Série des Années Soixante-dix", next.get("frcust_name"));
      assertEquals("That Seventies Show", next.get("enus_name"));
      assertEquals("你好", next.get("cn_name"));
    }
  }

  @Test
  public void testGetLangTagUDF() throws Exception {
    try (Driver driver = GraphDatabase.driver(neo4j.boltURI(),
        Config.build().toConfig()); Session session = driver.session()) {

      createIndices(neo4j.getGraphDatabaseService());

      StatementResult importResults
          = session.run("return semantics.getLangTag('The Hague@en') as val_en,"
              + "semantics.getLangTag('Den Haag@nl') as val_nl, "
              + "semantics.getLangTag('La Haye@fr') as val_fr,"
          + "semantics.getLangTag('That Seventies Show@en-US') as val_us,"
          + "semantics.getLangTag([2, 45, 3]) as val_array,"
          + "semantics.getLangTag('hello') as val_no_tag");
      Map<String, Object> next = importResults
          .next().asMap();
      assertEquals("en", next.get("val_en"));
      assertEquals("fr", next.get("val_fr"));
      assertEquals("nl", next.get("val_nl"));
      assertEquals("en-US", next.get("val_us"));
      assertNull(next.get("val_array"));
      assertNull(next.get("val_no_tag"));


      session.run(
          "create (n:Thing { prop: [\"That Seventies Show@en-US\", \"Cette Série des Années Soixante-dix@fr-custom-tag\", \"你好@zh-Hans-CN\"] })");
      importResults
          = session.run(
          "match (n:Thing) return semantics.getLangTag(n.prop[0]) as enus_tag, "
              + "semantics.getLangTag(n.prop[1]) as frcust_tag, semantics.getLangTag(n.prop[2]) as cn_tag");
      next = importResults
          .next().asMap();
      assertEquals("fr-custom-tag", next.get("frcust_tag"));
      assertEquals("en-US", next.get("enus_tag"));
      assertEquals("zh-Hans-CN", next.get("cn_tag"));
    }
  }


  @Test
  public void testHasLangTagUDF() throws Exception {
    try (Driver driver = GraphDatabase.driver(neo4j.boltURI(),
        Config.build().toConfig()); Session session = driver.session()) {

      createIndices(neo4j.getGraphDatabaseService());

      StatementResult importResults
          = session.run("return semantics.hasLangTag('en','The Hague@en') as val_en,"
          + "semantics.hasLangTag('nl','Den Haag@nl') as val_nl, "
          + "semantics.hasLangTag('en','La Haye@fr') as val_fr_no,"
          + "semantics.hasLangTag('en-US','That Seventies Show@en-US') as val_us,"
          + "semantics.hasLangTag('it',[2, 45, 3]) as val_array,"
          + "semantics.hasLangTag('ru','hello') as val_no_tag");
      Map<String, Object> next = importResults
          .next().asMap();
      assertEquals(true, next.get("val_en"));
      assertEquals(false, next.get("val_fr_no"));
      assertEquals(true, next.get("val_nl"));
      assertEquals(true, next.get("val_us"));
      assertEquals(false, next.get("val_array"));
      assertEquals(false, next.get("val_no_tag"));


      session.run(
          "create (n:Thing { prop: [\"That Seventies Show@en-US\", \"Cette Série des Années Soixante-dix@fr-custom-tag\", \"你好@zh-Hans-CN\"] })");
      importResults
          = session.run(
          "match (n:Thing) return semantics.hasLangTag('en-US',n.prop[0]) as enus_tag, "
              + "semantics.hasLangTag('fr-custom-tag',n.prop[1]) as frcust_tag, "
              + "semantics.hasLangTag('es',n.prop[1]) as frcust_tag_no, "
              + "semantics.hasLangTag('zh-Hans-CN',n.prop[2]) as cn_tag");
      next = importResults
          .next().asMap();
      assertEquals(true, next.get("frcust_tag"));
      assertEquals(false, next.get("frcust_tag_no"));
      assertEquals(true, next.get("enus_tag"));
      assertEquals(true, next.get("cn_tag"));
    }
  }

  @Test
  public void testGetUriFromShortAndShortFromUri() throws Exception {
    try (Driver driver = GraphDatabase.driver(neo4j.boltURI(),
        Config.build().toConfig()); Session session = driver.session()) {

      createIndices(neo4j.getGraphDatabaseService());

      StatementResult importResults
          = session.run("CALL semantics.importRDF('" +
          RDFImportTest.class.getClassLoader().getResource("mini-ld.json").toURI() + "','JSON-LD',"
          +
          "{ handleVocabUris: 'SHORTEN', typesToLabels: true, commitSize: 500})");
      assertEquals(6L, importResults
          .next().get("triplesLoaded").asLong());
      assertEquals("http://xmlns.com/foaf/0.1/knows",
          session.run("MATCH (n{ns0" + PREFIX_SEPARATOR + "name : 'Markus Lanthaler'})-[r]-() " +
              " RETURN semantics.uriFromShort(type(r)) AS uri")
              .next().get("uri").asString());

      assertEquals("ns0" + PREFIX_SEPARATOR + "knows",
          session.run("RETURN semantics.shortFromUri('http://xmlns.com/foaf/0.1/knows') AS uri")
              .next().get("uri").asString());
    }
  }

  @Test
  public void testAddNamespacePrefixInitial() throws Exception {
    try (Driver driver = GraphDatabase.driver(neo4j.boltURI(),
        Config.build().toConfig()); Session session = driver.session()) {

      StatementResult res = session.run("CALL semantics.addNamespacePrefix('abc','http://myvoc#')");
      assertTrue(res.hasNext());
      Record next = res.next();
      assertEquals("abc", next.get("prefix").asString());
      assertEquals("http://myvoc#", next.get("namespace").asString());
      assertFalse(res.hasNext());
    }
  }

  @Test
  public void testAddNamespacePrefixExisting() throws Exception {
    try (Driver driver = GraphDatabase.driver(neo4j.boltURI(),
        Config.build().toConfig()); Session session = driver.session()) {

      createIndices(neo4j.getGraphDatabaseService());
      StatementResult res1 = session.run("CALL semantics.importRDF('" +
          RDFImportTest.class.getClassLoader().getResource("mini-ld.json").toURI()
          + "','JSON-LD')");
      assertTrue(res1.hasNext());
      Map<String, Object> preAddition = res1.next().get("namespaces").asMap();
      StatementResult res2 = session
          .run("CALL semantics.addNamespacePrefix('abc','http://myvoc#')");
      assertTrue(res2.hasNext());
      StatementResult res3 = session.run("MATCH (n:NamespacePrefixDefinition) RETURN n");
      assertTrue(res3.hasNext());
      Map<String, Object> postAddition = res3.next().get("n").asNode().asMap();
      assertFalse(res3.hasNext());
      Set<String> keys = new HashSet<>(postAddition.keySet());
      keys.removeAll(preAddition.keySet());
      assertEquals(1, keys.size());
      assertEquals("http://myvoc#", keys.iterator().next());
    }
  }

  @Test
  public void testAddNamespacePrefixFromText() throws Exception {
    try (Driver driver = GraphDatabase.driver(neo4j.boltURI(),
        Config.build().toConfig()); Session session = driver.session()) {

      String turtle = "@prefix tr-common: <http://permid.org/ontology/common/> . \n"
          + "@prefix fibo-be-le-cb: <http://www.omg.org/spec/EDMC-FIBO/BE/LegalEntities/CorporateBodies/> . \n"
          + "@prefix xsd:   <http://www.w3.org/2001/XMLSchema#> . \n"
          + "@prefix vcard: <http://www.w3.org/2006/vcard/ns#> . \n"
          + "@prefix tr-org: <http://permid.org/ontology/organization/> . \n";

      Map <String, String> validPairs = new HashMap<>();
      validPairs.put("tr_common","http://permid.org/ontology/common/");
      validPairs.put("fibo_be_le_cb","http://www.omg.org/spec/EDMC-FIBO/BE/LegalEntities/CorporateBodies/");
      validPairs.put("xsd","http://www.w3.org/2001/XMLSchema#");
      validPairs.put("vcard","http://www.w3.org/2006/vcard/ns#");
      validPairs.put("tr_org","http://permid.org/ontology/organization/");


      checkValidPrefixesCreated(session, validPairs, turtle);

      session.run("match  (n) detach delete n");

      String rdfxml = "<rdf:RDF xml:base=\"https://spec.edmcouncil.org/fibo/ontology/BE/Corporations/Corporations/\"\n"
          + "         xmlns:afn=\"http://jena.apache.org/ARQ/function#\"\n"
          + "         xmlns:dc=\"http://purl.org/dc/elements/1.1/\"\n"
          + "         xmlns:fibo-be-corp-corp=\"https://spec.edmcouncil.org/fibo/ontology/BE/Corporations/Corporations/\"\n"
          + "         xmlns:fibo-be-le-cb=\"https://spec.edmcouncil.org/fibo/ontology/BE/LegalEntities/CorporateBodies/\"\n"
          + "         xmlns:fibo-be-le-fbo=\"https://spec.edmcouncil.org/fibo/ontology/BE/LegalEntities/FormalBusinessOrganizations/\"\n"
          + "         xmlns:grddl=\"http://www.w3.org/2003/g/data-view#\"\n"
          + "         xmlns:sm=\"http://www.omg.org/techprocess/ab/SpecificationMetadata/\"\n"
          + "         xmlns:xsd=\"http://www.w3.org/2001/XMLSchema#\">\n";

      validPairs.clear();
      validPairs.put("afn","http://jena.apache.org/ARQ/function#");
      validPairs.put("dc","http://purl.org/dc/elements/1.1/");
      validPairs.put("fibo_be_corp_corp","https://spec.edmcouncil.org/fibo/ontology/BE/Corporations/Corporations/");
      validPairs.put("fibo_be_le_cb","https://spec.edmcouncil.org/fibo/ontology/BE/LegalEntities/CorporateBodies/");
      validPairs.put("fibo_be_le_fbo","https://spec.edmcouncil.org/fibo/ontology/BE/LegalEntities/FormalBusinessOrganizations/");
      validPairs.put("grddl","http://www.w3.org/2003/g/data-view#");
      validPairs.put("sm","http://www.omg.org/techprocess/ab/SpecificationMetadata/");
      validPairs.put("xsd","http://www.w3.org/2001/XMLSchema#");

      checkValidPrefixesCreated(session, validPairs, rdfxml);

      session.run("match  (n) detach delete n");

      String sparql = "PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>        \n"
          + "PREFIX type: <http://dbpedia.org/class/yago/>\n"
          + "PREFIX prop: <http://dbpedia.org/property/>\n";

      validPairs.clear();
      validPairs.put("rdfs","http://www.w3.org/2000/01/rdf-schema#");
      validPairs.put("type","http://dbpedia.org/class/yago/");
      validPairs.put("prop","http://dbpedia.org/property/");

      checkValidPrefixesCreated(session, validPairs, sparql);

    }
  }

  private void checkValidPrefixesCreated(Session session, Map<String, String> validPairs,
      String textFragment) {
    StatementResult res;
    int resultCount;
    res = session.run("CALL semantics.addNamespacePrefixesFromText('" + textFragment + "')");
    assertTrue(res.hasNext());
    resultCount = 0;
    while(res.hasNext()) {
      Record next = res.next();
      assertTrue(isExpectedPair(validPairs, next.get("prefix").asString(),
          next.get("namespace").asString()));
      resultCount++;
    }
    assertEquals(validPairs.size(), resultCount);
  }

  private boolean isExpectedPair(Map <String, String> validPairs, String prefix, String namespace) {
    return validPairs.get(prefix).equals(namespace);
  }

  @Test
  public void testGetDataType() throws Exception {
    try (Driver driver = GraphDatabase.driver(neo4j.boltURI(),
        Config.build().toConfig()); Session session = driver.session()) {

      createIndices(neo4j.getGraphDatabaseService());

      StatementResult importResults = session
          .run("return semantics.getDataType('2008-04-17^^ns1__date') AS val");
      Map<String, Object> next = importResults.next().asMap();
      assertEquals("ns1__date", next.get("val"));

      importResults = session
          .run("return semantics.getDataType('10000^^http://example.org/USD') AS val");
      next = importResults.next().asMap();
      assertEquals("http://example.org/USD", next.get("val"));

      importResults = session.run("return semantics.getDataType('10000') AS val");
      next = importResults.next().asMap();
      assertEquals(XMLSchema.STRING.stringValue(), next.get("val"));

      importResults = session.run("return semantics.getDataType(10000) AS val");
      next = importResults.next().asMap();
      assertEquals(XMLSchema.LONG.stringValue(), next.get("val"));

      importResults = session.run("return semantics.getDataType(10000.0) AS val");
      next = importResults.next().asMap();
      assertEquals(XMLSchema.DOUBLE.stringValue(), next.get("val"));

      importResults = session.run("return semantics.getDataType(true) AS val");
      next = importResults.next().asMap();
      assertEquals(XMLSchema.BOOLEAN.stringValue(), next.get("val"));

      importResults = session.run("return semantics.getDataType(date('1986-07-19')) AS val");
      next = importResults.next().asMap();
      assertEquals(XMLSchema.DATE.stringValue(), next.get("val"));

      importResults = session
          .run("return semantics.getDataType(localdatetime('1986-07-09T18:06:36')) AS val");
      next = importResults.next().asMap();
      assertEquals(XMLSchema.DATETIME.stringValue(), next.get("val"));

    }
  }

  @Test
  public void testGetValue() throws Exception {
    try (Driver driver = GraphDatabase.driver(neo4j.boltURI(),
        Config.build().toConfig()); Session session = driver.session()) {

      createIndices(neo4j.getGraphDatabaseService());

      StatementResult importResults = session
          .run("return semantics.getValue('2008-04-17^^ns1__date') AS val");
      Map<String, Object> next = importResults.next().asMap();
      assertEquals("2008-04-17", next.get("val"));

      importResults = session.run(
          "return semantics.getValue('10000^^http://example.org/USD') AS val");
      next = importResults.next().asMap();
      assertEquals("10000", next.get("val"));

      importResults = session.run("return semantics.getValue('10000') AS val");
      next = importResults.next().asMap();
      assertEquals("10000", next.get("val"));

      importResults = session.run("return semantics.getValue('This is a test@en') AS val");
      next = importResults.next().asMap();
      assertEquals("This is a test", next.get("val"));
    }
  }

  @Test
  public void testCustomDataTypesKeepURIs() throws Exception {
    try (Driver driver = GraphDatabase.driver(neo4j.boltURI(),
        Config.build().toConfig()); Session session = driver.session()) {

      createIndices(neo4j.getGraphDatabaseService());

      StatementResult importResults
          = session.run("CALL semantics.importRDF('" +
          RDFImportTest.class.getClassLoader().getResource("customDataTypes.ttl")
              .toURI() + "','Turtle',{keepLangTag: true, handleMultival: 'ARRAY', " +
          "multivalPropList: ['http://example.com/price', 'http://example.com/power', 'http://example.com/class'], keepCustomDataTypes: true, "
          +
          "customDataTypedPropList: ['http://example.com/price', 'http://example.com/color', 'http://example.com/power'], "
          +
          "handleVocabUris: 'KEEP', typesToLabels: true, commitSize: 500})");
      assertEquals(10L, importResults
          .next().get("triplesLoaded").asLong());
      StatementResult cars = session.run("MATCH (n:`http://example.com/Car`) " +
          "\nRETURN n.`http://example.com/price` AS price," +
          "n.`http://example.com/power` AS power, " +
          "n.`http://example.com/color` AS color, " +
          "n.`http://example.com/class` AS class, n.`http://example.com/released` AS released, " +
          "n.`http://example.com/type` AS type ORDER BY price");

      Record car = cars.next();
      List price = car.get("price").asList();
      assertEquals(2, price.size());
      assertEquals("10000^^http://example.com/EUR", price.get(0));
      assertEquals("11000^^http://example.com/USD", price.get(1));
      assertEquals("300^^http://example.com/HP", car.get("power").get(0).asString());
      assertEquals("223,71^^http://example.com/kW", car.get("power").get(1).asString());
      assertEquals("red^^http://example.com/Color", car.get("color").asString());
      assertEquals("A-Klasse@de", car.get("class").asList().get(0));
      assertEquals("A-Class@en", car.get("class").asList().get(1));
      assertEquals(2019, car.get("released").asLong());
      assertEquals("Cabrio", car.get("type").asString());
    }
  }

  @Test
  public void testCustomDataTypesShortenURIs() throws Exception {
    try (Driver driver = GraphDatabase.driver(neo4j.boltURI(),
        Config.build().toConfig()); Session session = driver.session()) {

      createIndices(neo4j.getGraphDatabaseService());

      StatementResult importResults
          = session.run("CALL semantics.importRDF('" +
          RDFImportTest.class.getClassLoader().getResource("customDataTypes.ttl")
              .toURI() + "','Turtle',{keepLangTag: true, handleMultival: 'ARRAY', " +
          "multivalPropList: ['http://example.com/price', 'http://example.com/power', 'http://example.com/class'], keepCustomDataTypes: true, "
          +
          "customDataTypedPropList: ['http://example.com/price', 'http://example.com/color', 'http://example.com/power'], "
          +
          "handleVocabUris: 'SHORTEN', typesToLabels: true, commitSize: 500})");
      assertEquals(10L, importResults
          .next().get("triplesLoaded").asLong());
      StatementResult cars = session.run("MATCH (n:ns0__Car) " +
          "\nRETURN n.ns0__price AS price," +
          "n.ns0__power AS power, " +
          "n.ns0__color AS color, " +
          "n.ns0__class AS class, n.ns0__released AS released, " +
          "n.ns0__type AS type ORDER BY price");

      Record car = cars.next();
      List price = car.get("price").asList();
      assertEquals(2, price.size());
      assertEquals("10000^^ns0__EUR", price.get(0));
      assertEquals("11000^^ns0__USD", price.get(1));
      assertEquals("300^^ns0__HP", car.get("power").get(0).asString());
      assertEquals("223,71^^ns0__kW", car.get("power").get(1).asString());
      assertEquals("red^^ns0__Color", car.get("color").asString());
      assertEquals("A-Klasse@de", car.get("class").asList().get(0));
      assertEquals("A-Class@en", car.get("class").asList().get(1));
      assertEquals(2019, car.get("released").asLong());
      assertEquals("Cabrio", car.get("type").asString());
    }
  }

  @Test
  public void testImportMultiValAfterImportSingelVal() throws Exception {
    try (Driver driver = GraphDatabase.driver(neo4j.boltURI(),
        Config.build().toConfig()); Session session = driver.session()) {

      createIndices(neo4j.getGraphDatabaseService());
      String importCypher = "CALL semantics.importRDF('" +
          RDFImportTest.class.getClassLoader()
              .getResource("testImportMultiValAfterImportSingelVal.ttl")
              .toURI() + "','Turtle',{handleMultival: 'OVERWRITE', handleVocabUris: 'KEEP'})";
      StatementResult importResults = session.run(importCypher);
      Record next = importResults.next();
      assertEquals(3, next.get("triplesLoaded").asInt());
      StatementResult queryResults = session
          .run("MATCH (n:Resource) RETURN n.`http://example.com/price` AS price");
      Object imports = queryResults.next().get("price");
      assertEquals(IntegerValue.class, imports.getClass());

      importCypher = "CALL semantics.importRDF('" +
          RDFImportTest.class.getClassLoader()
              .getResource("testImportMultiValAfterImportSingelVal.ttl")
              .toURI() + "','Turtle',{handleMultival: 'ARRAY', handleVocabUris: 'KEEP'})";

      importResults = session.run(importCypher);

      next = importResults.next();
      assertEquals(3, next.get("triplesLoaded").asInt());

      queryResults = session.run("MATCH (n:Resource) RETURN n.`http://example.com/price` AS price");
      imports = queryResults.next().get("price");
      assertEquals(ListValue.class, imports.getClass());
    }
  }

  @Test
  public void testReificationImport() throws Exception {
    try (Driver driver = GraphDatabase.driver(neo4j.boltURI(),
        Config.build().toConfig()); Session session = driver.session()) {

      createIndices(neo4j.getGraphDatabaseService());

      StatementResult importResults
          = session.run("CALL semantics.importRDF('" +
          RDFImportTest.class.getClassLoader().getResource("reification.ttl")
              .toURI()
          + "','Turtle',{ handleVocabUris: 'KEEP', typesToLabels: true, commitSize: 500})");
      assertEquals(25L, importResults
          .next().get("triplesLoaded").asLong());
      StatementResult dates = session
          .run("MATCH (n:`http://www.w3.org/1999/02/22-rdf-syntax-ns#Statement`) " +
              "\nRETURN n.`http://example.com/from` AS fromDates ORDER BY fromDates DESC");

      assertEquals(LocalDate.parse("2019-09-01"), dates.next().get("fromDates").asLocalDate());
      assertEquals(LocalDate.parse("2016-09-01"), dates.next().get("fromDates").asLocalDate());

      StatementResult statements = session.run("MATCH (statement)\n" +
          "WHERE (statement)-[:`http://www.w3.org/1999/02/22-rdf-syntax-ns#subject`]->()\n" +
          "AND (statement)-[:`http://www.w3.org/1999/02/22-rdf-syntax-ns#predicate`]->()\n" +
          "AND (statement)-[:`http://www.w3.org/1999/02/22-rdf-syntax-ns#object`]->()\n" +
          "RETURN statement.uri AS statement ORDER BY statement");

      assertEquals("http://example.com/studyInformation1",
          statements.next().get("statement").asString());
      assertEquals("http://example.com/studyInformation2",
          statements.next().get("statement").asString());
    }
  }

  @Test
  public void testIncrementalLoadMultivaluesInArray() throws Exception {
    try (Driver driver = GraphDatabase.driver(neo4j.boltURI(),
        Config.build().toConfig()); Session session = driver.session()) {

      createIndices(neo4j.getGraphDatabaseService());

      StatementResult importResults
          = session.run("CALL semantics.importRDF('" +
          RDFImportTest.class.getClassLoader().getResource("incremental/step1.ttl")
              .toURI() + "','Turtle',{ handleMultival: 'ARRAY' })");
      assertEquals(2L, importResults
          .next().get("triplesLoaded").asLong());
      importResults
          = session.run("CALL semantics.importRDF('" +
          RDFImportTest.class.getClassLoader().getResource("incremental/step2.ttl")
              .toURI() + "','Turtle',{ handleMultival: 'ARRAY' })");
      assertEquals(2L, importResults
          .next().get("triplesLoaded").asLong());

      StatementResult result = session.run("MATCH (n:ns0__Thing) " +
          "\nRETURN n.ns0__prop as multival ");

      List<String> vals = new ArrayList<>();
      vals.add("one");
      vals.add("two");
      assertEquals(vals, result.next().get("multival").asList());


    }
  }

  @Test
  public void testIncrementalLoadNamespaces() throws Exception {
    try (Driver driver = GraphDatabase.driver(neo4j.boltURI(),
        Config.build().toConfig()); Session session = driver.session()) {

      createIndices(neo4j.getGraphDatabaseService());

      StatementResult importResults
          = session.run("CALL semantics.importRDF('" +
          RDFImportTest.class.getClassLoader().getResource("event.json")
              .toURI() + "','JSON-LD')");
      assertEquals(28L, importResults
          .next().get("triplesLoaded").asLong());
      StatementResult nsDefResult = session.run("MATCH (n:NamespacePrefixDefinition) "
          + "RETURN properties(n) as defs");
      assertTrue(nsDefResult.hasNext());
      Map<String, Object> defsPre = nsDefResult.next().get("defs").asMap();
      assertFalse(nsDefResult.hasNext());
      importResults
          = session.run("CALL semantics.importRDF('" +
          RDFImportTest.class.getClassLoader().getResource("fibo-fragment.rdf")
              .toURI() + "','RDF/XML')");
      assertEquals(171L, importResults
          .next().get("triplesLoaded").asLong());
      nsDefResult = session.run("MATCH (n:NamespacePrefixDefinition) "
          + "RETURN properties(n) as defs");
      assertTrue(nsDefResult.hasNext());
      Map<String, Object> defsPost = nsDefResult.next().get("defs").asMap();
      assertFalse(nsDefResult.hasNext());
      assertTrue(getPrePostDelta(defsPre, defsPost).isEmpty());
      importResults
          = session.run("CALL semantics.importRDF('" +
          RDFImportTest.class.getClassLoader().getResource("oneTriple.rdf")
              .toURI() + "','RDF/XML')");
      assertEquals(1L, importResults
          .next().get("triplesLoaded").asLong());
      nsDefResult = session.run("MATCH (n:NamespacePrefixDefinition) "
          + "RETURN properties(n) as defs");
      assertTrue(nsDefResult.hasNext());
      Map<String, Object> defsPost2 = nsDefResult.next().get("defs").asMap();
      assertFalse(nsDefResult.hasNext());
      assertTrue(getPrePostDelta(defsPost, defsPost2).isEmpty());

    }
  }

  @Test
  public void testLoadNamespacesWithCustomPredefined() throws Exception {
    try (Driver driver = GraphDatabase.driver(neo4j.boltURI(),
        Config.build().toConfig()); Session session = driver.session()) {

      createIndices(neo4j.getGraphDatabaseService());

      session.run("CREATE (:NamespacePrefixDefinition {\n"
          + "  `http://www.w3.org/2000/01/rdf-schema#`: 'myschema',\n"
          + "  `http://www.w3.org/1999/02/22-rdf-syntax-ns#`: 'myrdf'})");
      StatementResult nsDefResult = session.run("MATCH (n:NamespacePrefixDefinition) "
          + "RETURN properties(n) as defs");
      assertTrue(nsDefResult.hasNext());
      Map<String, Object> defsPre = nsDefResult.next().get("defs").asMap();
      assertFalse(nsDefResult.hasNext());
      StatementResult importResults = session.run("CALL semantics.importRDF('" +
          RDFImportTest.class.getClassLoader().getResource("fibo-fragment.rdf")
              .toURI() + "','RDF/XML')");
      assertEquals(171L, importResults.next().get("triplesLoaded").asLong());
      nsDefResult = session.run("MATCH (n:NamespacePrefixDefinition) "
          + "RETURN properties(n) as defs");
      assertTrue(nsDefResult.hasNext());
      Map<String, Object> defsPost = nsDefResult.next().get("defs").asMap();
      assertFalse(nsDefResult.hasNext());
    }
  }

  private Map<String, Object> getPrePostDelta(Map<String, Object> defsPre,
      Map<String, Object> defsPost) {
    Map<String, Object> delta = new HashMap<>();
    defsPre.forEach((k, v) -> {
      if (!defsPost.containsKey(k) || !defsPost.get(k).equals(v)) {
        delta.put(k, v);
      }
    });
    return delta;
  }

  @Test
  public void testIncrementalLoadArrayOnPreviouslyAtomicValue() throws Exception {
    try (Driver driver = GraphDatabase.driver(neo4j.boltURI(),
        Config.build().toConfig()); Session session = driver.session()) {

      createIndices(neo4j.getGraphDatabaseService());

      StatementResult importResults
          = session.run("CALL semantics.importRDF('" +
          RDFImportTest.class.getClassLoader().getResource("incremental/step1.ttl")
              .toURI() + "','Turtle')");
      assertEquals(2L, importResults
          .next().get("triplesLoaded").asLong());
      importResults
          = session.run("CALL semantics.importRDF('" +
          RDFImportTest.class.getClassLoader().getResource("incremental/step2.ttl")
              .toURI() + "','Turtle',{ handleMultival: 'ARRAY' })");
      assertEquals(2L, importResults
          .next().get("triplesLoaded").asLong());

      StatementResult result = session.run("MATCH (n:ns0__Thing) " +
          "\nRETURN n.ns0__prop as multival ");

      List<String> vals = new ArrayList<String>();
      vals.add("one");
      vals.add("two");
      assertEquals(vals, result.next().get("multival").asList());


    }
  }

  @Test
  public void testIncrementalLoadAtomicValueOnPreviouslyArray() throws Exception {
    try (Driver driver = GraphDatabase.driver(neo4j.boltURI(),
        Config.build().toConfig()); Session session = driver.session()) {

      createIndices(neo4j.getGraphDatabaseService());

      StatementResult importResults
          = session.run("CALL semantics.importRDF('" +
          RDFImportTest.class.getClassLoader().getResource("incremental/step1.ttl")
              .toURI() + "','Turtle',{ handleMultival: 'ARRAY' })");
      assertEquals(2L, importResults
          .next().get("triplesLoaded").asLong());
      importResults
          = session.run("CALL semantics.importRDF('" +
          RDFImportTest.class.getClassLoader().getResource("incremental/step3.ttl")
              .toURI() + "','Turtle')");
      assertEquals(2L, importResults
          .next().get("triplesLoaded").asLong());

      StatementResult result = session.run("MATCH (n:ns0__Thing) " +
          "\nRETURN n.ns0__prop as singleVal ");

      assertEquals(230L, result.next().get("singleVal").asLong());


    }
  }

  @Test
  public void testLargerFileManyTransactions() throws Exception {
    try (Driver driver = GraphDatabase.driver(neo4j.boltURI(),
        Config.build().toConfig()); Session session = driver.session()) {

      createIndices(neo4j.getGraphDatabaseService());

      StatementResult importResults
          = session.run("CALL semantics.importRDF('" +
          RDFImportTest.class.getClassLoader().getResource("100k.nt").toURI() + "','N-Triples',"
          + "{ commitSize: 5 , predicateExclusionList: ['http://www.w3.org/2004/02/skos/core#prefLabel']})");
      assertEquals(92712L, importResults
          .next().get("triplesLoaded").asLong());
    }

  }

  @Test
  public void testDeleteRelationshipKeepURIs() throws Exception {
    try (Driver driver = GraphDatabase.driver(neo4j.boltURI(),
        Config.build().toConfig()); Session session = driver.session()) {

      createIndices(neo4j.getGraphDatabaseService());

      StatementResult importResults = session.run("CALL semantics.importRDF('" +
          RDFImportTest.class.getClassLoader().getResource("deleteRDF/dataset1.ttl")
              .toURI()
          + "','Turtle',{ handleVocabUris: 'KEEP', typesToLabels: true, commitSize: 500, keepCustomDataTypes: true, handleMultival: 'ARRAY'})");

      assertEquals(15L, importResults.next().get("triplesLoaded").asLong());
      StatementResult result = session.run("MATCH (n {uri: 'http://example.org/Resource1'}),"
          + "(m {uri: 'http://example.org/Resource2'})"
          + "OPTIONAL MATCH (n)-[r]->(m) "
          + "RETURN n.uri AS nUri, type(r) AS type, m.uri AS mUri");
      Record record = result.next();
      assertEquals("http://example.org/Predicate3", record.get("type").asString());
      assertEquals("http://example.org/Resource1", record.get("nUri").asString());
      assertEquals("http://example.org/Resource2", record.get("mUri").asString());

      StatementResult deleteResults = session.run("CALL semantics.deleteRDF('" +
          RDFImportTest.class.getClassLoader().getResource("deleteRDF/dataset1Delete1.ttl").toURI()
          + "', 'Turtle', {handleVocabUris: 'KEEP', typesToLabels: true, commitSize: 500, keepCustomDataTypes: true})");

      assertEquals(1L, deleteResults.next().get("triplesDeleted").asLong());

      result = session.run("MATCH (n {uri: 'http://example.org/Resource1'}),"
          + "(m {uri: 'http://example.org/Resource2'})"
          + "OPTIONAL MATCH (n)-[r]->(m) "
          + "RETURN n.uri AS nUri, type(r) AS type, m.uri AS mUri");
      record = result.next();
      assertEquals(NULL, record.get("type"));
      assertEquals("http://example.org/Resource1", record.get("nUri").asString());
      assertEquals("http://example.org/Resource2", record.get("mUri").asString());

    }
  }

  @Test
  public void testDeleteRelationshipShortenURIs() throws Exception {
    try (Driver driver = GraphDatabase.driver(neo4j.boltURI(),
        Config.build().toConfig()); Session session = driver.session()) {

      createIndices(neo4j.getGraphDatabaseService());

      StatementResult importResults = session.run("CALL semantics.importRDF('" +
          RDFImportTest.class.getClassLoader().getResource("deleteRDF/dataset1.ttl")
              .toURI()
          + "','Turtle',{ handleVocabUris: 'SHORTEN', typesToLabels: true, commitSize: 500, keepCustomDataTypes: true, handleMultival: 'ARRAY'})");

      assertEquals(15L, importResults.next().get("triplesLoaded").asLong());
      StatementResult result = session.run("MATCH (n {uri: 'http://example.org/Resource1'}),"
          + "(m {uri: 'http://example.org/Resource2'})"
          + "OPTIONAL MATCH (n)-[r]->(m) "
          + "RETURN n.uri AS nUri, type(r) AS type, m.uri AS mUri");
      Record record = result.next();
      assertEquals("ns0__Predicate3", record.get("type").asString());
      assertEquals("http://example.org/Resource1", record.get("nUri").asString());
      assertEquals("http://example.org/Resource2", record.get("mUri").asString());

      StatementResult deleteResults = session.run("CALL semantics.deleteRDF('" +
          RDFImportTest.class.getClassLoader().getResource("deleteRDF/dataset1Delete1.ttl").toURI()
          + "', 'Turtle', {handleVocabUris: 'SHORTEN', typesToLabels: true, commitSize: 500, keepCustomDataTypes: true})");

      assertEquals(1L, deleteResults.next().get("triplesDeleted").asLong());

      result = session.run("MATCH (n {uri: 'http://example.org/Resource1'}),"
          + "(m {uri: 'http://example.org/Resource2'})"
          + "OPTIONAL MATCH (n)-[r]->(m) "
          + "RETURN n.uri AS nUri, type(r) AS type, m.uri AS mUri");
      record = result.next();
      assertEquals(NULL, record.get("type"));
      assertEquals("http://example.org/Resource1", record.get("nUri").asString());
      assertEquals("http://example.org/Resource2", record.get("mUri").asString());

    }
  }


  @Test
  public void testDeleteRelationshipShortenURIsFromString() throws Exception {
    try (Driver driver = GraphDatabase.driver(neo4j.boltURI(),
        Config.build().toConfig()); Session session = driver.session()) {

      createIndices(neo4j.getGraphDatabaseService());

      String rdf = "@prefix ex: <http://example.org/> .\n"
          + "@prefix xsd: <http://www.w3.org/2001/XMLSchema#> .\n"
          + "\n"
          + "ex:Resource1\n"
          + "  a ex:TestResource ;\n"
          + "  ex:Predicate1 \"100\"^^ex:CDT ;\n"
          + "  ex:Predicate2 \"test\";\n"
          + "  ex:Predicate3 ex:Resource2 ;\n"
          + "  ex:Predicate4 \"val1\" ;\n"
          + "  ex:Predicate4 \"val2\" ;\n"
          + "  ex:Predicate4 \"val3\" ;\n"
          + "  ex:Predicate4 \"val4\" .\n"
          + "\n"
          + "ex:Resource2\n"
          + "  a ex:TestResource ;\n"
          + "  ex:Predicate1 \"test\";\n"
          + "  ex:Predicate2 ex:Resource3 ;\n"
          + "  ex:Predicate3 \"100\"^^xsd:long ;\n"
          + "  ex:Predicate3 \"200\"^^xsd:long ;\n"
          + "  ex:Predicate4 \"300.0\"^^xsd:double ;\n"
          + "  ex:Predicate4 \"400.0\"^^xsd:double .\n"
          + "\n";

      StatementResult importResults = session.run("CALL semantics.importRDFSnippet('" +
          rdf
          + "','Turtle',{ handleVocabUris: 'SHORTEN', typesToLabels: true, commitSize: 500, keepCustomDataTypes: true, handleMultival: 'ARRAY'})");

      assertEquals(15L, importResults.next().get("triplesLoaded").asLong());
      StatementResult result = session.run("MATCH (n {uri: 'http://example.org/Resource1'}),"
          + "(m {uri: 'http://example.org/Resource2'})"
          + "OPTIONAL MATCH (n)-[r]->(m) "
          + "RETURN n.uri AS nUri, type(r) AS type, m.uri AS mUri");
      Record record = result.next();
      assertEquals("ns0__Predicate3", record.get("type").asString());
      assertEquals("http://example.org/Resource1", record.get("nUri").asString());
      assertEquals("http://example.org/Resource2", record.get("mUri").asString());

      String deleteRdf1 = "@prefix ex: <http://example.org/> .\n"
          + "\n"
          + "ex:Resource1\n"
          + "  ex:Predicate3 ex:Resource2 .\n";
      StatementResult deleteResults = session.run("CALL semantics.deleteRDFSnippet('" +
          deleteRdf1
          + "', 'Turtle', {handleVocabUris: 'SHORTEN', typesToLabels: true, commitSize: 500, keepCustomDataTypes: true})");

      assertEquals(1L, deleteResults.next().get("triplesDeleted").asLong());

      result = session.run("MATCH (n {uri: 'http://example.org/Resource1'}),"
          + "(m {uri: 'http://example.org/Resource2'})"
          + "OPTIONAL MATCH (n)-[r]->(m) "
          + "RETURN n.uri AS nUri, type(r) AS type, m.uri AS mUri");
      record = result.next();
      assertEquals(NULL, record.get("type"));
      assertEquals("http://example.org/Resource1", record.get("nUri").asString());
      assertEquals("http://example.org/Resource2", record.get("mUri").asString());

    }
  }

  @Test
  public void testDeleteLiteralKeepURIs() throws Exception {
    try (Driver driver = GraphDatabase.driver(neo4j.boltURI(),
        Config.build().toConfig()); Session session = driver.session()) {

      createIndices(neo4j.getGraphDatabaseService());

      StatementResult importResults = session.run("CALL semantics.importRDF('" +
          RDFImportTest.class.getClassLoader().getResource("deleteRDF/dataset1.ttl")
              .toURI()
          + "','Turtle',{ handleVocabUris: 'KEEP', typesToLabels: true, commitSize: 500, keepCustomDataTypes: true, handleMultival: 'ARRAY'})");

      assertEquals(15L, importResults.next().get("triplesLoaded").asLong());
      StatementResult result = session.run("MATCH (n {uri: 'http://example.org/Resource1'})"
          + "RETURN n.`http://example.org/Predicate2` AS nP2");

      Record record = result.next();
      assertEquals(1, record.get("nP2").asList().size());
      assertTrue(record.get("nP2").asList().contains("test"));

      StatementResult deleteResults = session.run("CALL semantics.deleteRDF('" +
          RDFImportTest.class.getClassLoader().getResource("deleteRDF/dataset1Delete2.ttl").toURI()
          + "', 'Turtle', {handleVocabUris: 'KEEP', typesToLabels: true, commitSize: 500, keepCustomDataTypes: true})");

      assertEquals(1L, deleteResults.next().get("triplesDeleted").asLong());

      result = session.run("MATCH (n {uri: 'http://example.org/Resource1'})"
          + "RETURN n.`http://example.org/Predicate2` AS nP2");
      record = result.next();
      assertEquals(NULL, record.get("nP2"));

    }
  }

  @Test
  public void testDeleteLiteralShortenURIs() throws Exception {
    try (Driver driver = GraphDatabase.driver(neo4j.boltURI(),
        Config.build().toConfig()); Session session = driver.session()) {

      createIndices(neo4j.getGraphDatabaseService());

      StatementResult importResults = session.run("CALL semantics.importRDF('" +
          RDFImportTest.class.getClassLoader().getResource("deleteRDF/dataset1.ttl")
              .toURI()
          + "','Turtle',{ handleVocabUris: 'SHORTEN', typesToLabels: true, commitSize: 500, keepCustomDataTypes: true, handleMultival: 'ARRAY'})");

      assertEquals(15L, importResults.next().get("triplesLoaded").asLong());
      StatementResult result = session.run("MATCH (n {uri: 'http://example.org/Resource1'})"
          + "RETURN n.ns0__Predicate2 AS nP2");

      Record record = result.next();
      assertEquals(1, record.get("nP2").asList().size());
      assertTrue(record.get("nP2").asList().contains("test"));

      StatementResult deleteResults = session.run("CALL semantics.deleteRDF('" +
          RDFImportTest.class.getClassLoader().getResource("deleteRDF/dataset1Delete2.ttl").toURI()
          + "', 'Turtle', {handleVocabUris: 'SHORTEN', typesToLabels: true, commitSize: 500, keepCustomDataTypes: true})");

      assertEquals(1L, deleteResults.next().get("triplesDeleted").asLong());

      result = session.run("MATCH (n {uri: 'http://example.org/Resource1'})"
          + "RETURN n.ns0__Predicate2 AS nP2");
      record = result.next();
      assertEquals(NULL, record.get("nP2"));

    }
  }

  @Test
  public void testDeleteLiteralShortenURIsFromString() throws Exception {
    try (Driver driver = GraphDatabase.driver(neo4j.boltURI(),
        Config.build().toConfig()); Session session = driver.session()) {

      createIndices(neo4j.getGraphDatabaseService());

      String rdf = "@prefix ex: <http://example.org/> .\n"
          + "@prefix xsd: <http://www.w3.org/2001/XMLSchema#> .\n"
          + "\n"
          + "ex:Resource1\n"
          + "  a ex:TestResource ;\n"
          + "  ex:Predicate1 \"100\"^^ex:CDT ;\n"
          + "  ex:Predicate2 \"test\";\n"
          + "  ex:Predicate3 ex:Resource2 ;\n"
          + "  ex:Predicate4 \"val1\" ;\n"
          + "  ex:Predicate4 \"val2\" ;\n"
          + "  ex:Predicate4 \"val3\" ;\n"
          + "  ex:Predicate4 \"val4\" .\n"
          + "\n"
          + "ex:Resource2\n"
          + "  a ex:TestResource ;\n"
          + "  ex:Predicate1 \"test\";\n"
          + "  ex:Predicate2 ex:Resource3 ;\n"
          + "  ex:Predicate3 \"100\"^^xsd:long ;\n"
          + "  ex:Predicate3 \"200\"^^xsd:long ;\n"
          + "  ex:Predicate4 \"300.0\"^^xsd:double ;\n"
          + "  ex:Predicate4 \"400.0\"^^xsd:double .\n"
          + "\n";

      StatementResult importResults = session.run("CALL semantics.importRDFSnippet('" +
          rdf
          + "','Turtle',{ handleVocabUris: 'SHORTEN', typesToLabels: true, commitSize: 500, keepCustomDataTypes: true, handleMultival: 'ARRAY'})");

      assertEquals(15L, importResults.next().get("triplesLoaded").asLong());
      StatementResult result = session.run("MATCH (n {uri: 'http://example.org/Resource1'})"
          + "RETURN n.ns0__Predicate2 AS nP2");

      Record record = result.next();
      assertEquals(1, record.get("nP2").asList().size());
      assertTrue(record.get("nP2").asList().contains("test"));

      String deleteRdf = "@prefix ex: <http://example.org/> .\n"
          + "\n"
          + "ex:Resource1\n"
          + "  ex:Predicate2 \"test\" .\n";

      StatementResult deleteResults = session.run("CALL semantics.deleteRDFSnippet('" +
          deleteRdf
          + "', 'Turtle', {handleVocabUris: 'SHORTEN', typesToLabels: true, commitSize: 500, keepCustomDataTypes: true})");

      assertEquals(1L, deleteResults.next().get("triplesDeleted").asLong());

      result = session.run("MATCH (n {uri: 'http://example.org/Resource1'})"
          + "RETURN n.ns0__Predicate2 AS nP2");
      record = result.next();
      assertEquals(NULL, record.get("nP2"));

    }
  }

  @Test
  public void testDeleteTypeFromResource() throws Exception {
    try (Driver driver = GraphDatabase.driver(neo4j.boltURI(),
        Config.build().toConfig()); Session session = driver.session()) {

      createIndices(neo4j.getGraphDatabaseService());

      StatementResult importResults = session.run("CALL semantics.importRDF('" +
          RDFImportTest.class.getClassLoader().getResource("deleteRDF/dataset1.ttl")
              .toURI()
          + "','Turtle',{ handleVocabUris: 'KEEP', typesToLabels: true, commitSize: 500, keepCustomDataTypes: true, handleMultival: 'ARRAY'})");

      assertEquals(15L, importResults.next().get("triplesLoaded").asLong());
      StatementResult result = session.run("MATCH (n:Resource)"
          + "RETURN n");
      assertEquals(3, result.list().size());
      result = session.run("MATCH (n {uri: 'http://example.org/Resource2'})"
          + "RETURN labels(n) AS labels");
      Record record = result.next();
      assertEquals(2, record.get("labels").asList().size());

      StatementResult deleteResult = session.run("CALL semantics.deleteRDF('" +
          RDFImportTest.class.getClassLoader().getResource("deleteRDF/dataset1Delete3.ttl").toURI()
          + "', 'Turtle', {handleVocabUris: 'KEEP', typesToLabels: true, commitSize: 500, keepCustomDataTypes: true, handleMultival: 'ARRAY'})");

      assertEquals(1L, deleteResult.next().get("triplesDeleted").asLong());

      result = session.run("MATCH (n {uri: 'http://example.org/Resource2'})"
          + "RETURN labels(n) AS labels");
      record = result.next();
      assertEquals(1, record.get("labels").asList().size());

    }
  }

  @Test
  public void testDeleteAllTriplesRelatedToResource() throws Exception {
    try (Driver driver = GraphDatabase.driver(neo4j.boltURI(),
        Config.build().toConfig()); Session session = driver.session()) {

      createIndices(neo4j.getGraphDatabaseService());

      StatementResult importResults = session.run("CALL semantics.importRDF('" +
          RDFImportTest.class.getClassLoader().getResource("deleteRDF/dataset1.ttl")
              .toURI()
          + "','Turtle',{ handleVocabUris: 'KEEP', typesToLabels: true, commitSize: 500, keepCustomDataTypes: true, handleMultival: 'ARRAY'})");

      assertEquals(15L, importResults.next().get("triplesLoaded").asLong());
      StatementResult result = session.run("MATCH (n:Resource)"
          + "RETURN n");
      assertEquals(3, result.list().size());

      StatementResult deleteResult = session.run("CALL semantics.deleteRDF('" +
          RDFImportTest.class.getClassLoader().getResource("deleteRDF/dataset1Delete4.ttl").toURI()
          + "', 'Turtle', {handleVocabUris: 'KEEP', typesToLabels: true, commitSize: 500, keepCustomDataTypes: true, handleMultival: 'ARRAY'})");

      assertEquals(8L, deleteResult.next().get("triplesDeleted").asLong());

      result = session.run("MATCH (n:Resource)"
          + "RETURN n");
      assertEquals(1, result.list().size());

    }
  }

  @Test
  public void testDeleteMultiLiteral() throws Exception {
    try (Driver driver = GraphDatabase.driver(neo4j.boltURI(),
        Config.build().toConfig()); Session session = driver.session()) {

      createIndices(neo4j.getGraphDatabaseService());

      StatementResult importResults = session.run("CALL semantics.importRDF('" +
          RDFImportTest.class.getClassLoader().getResource("deleteRDF/dataset1.ttl")
              .toURI()
          + "','Turtle',{ handleVocabUris: 'KEEP', typesToLabels: true, commitSize: 500, keepCustomDataTypes: true, handleMultival: 'ARRAY'})");

      assertEquals(15L, importResults.next().get("triplesLoaded").asLong());
      StatementResult result = session.run("MATCH (n {uri: 'http://example.org/Resource1'}), "
          + "(m {uri: 'http://example.org/Resource2'})"
          + "RETURN n.`http://example.org/Predicate4` AS nP4, "
          + "m.`http://example.org/Predicate3` AS mP3, "
          + "m.`http://example.org/Predicate4` AS mP4");

      Record record = result.next();
      assertArrayEquals(new String[]{"val1", "val2", "val3", "val4"},
          record.get("nP4").asList().toArray());
      assertTrue(record.get("mP3").asList().contains(100L));
      assertTrue(record.get("mP3").asList().contains(200L));
      assertTrue(record.get("mP4").asList().contains(300.0));
      assertTrue(record.get("mP4").asList().contains(400.0));

      StatementResult deleteResult = session.run("CALL semantics.deleteRDF('" +
          RDFImportTest.class.getClassLoader().getResource("deleteRDF/dataset1Delete5.ttl").toURI()
          + "', 'Turtle', {handleVocabUris: 'KEEP', typesToLabels: true, commitSize: 500, keepCustomDataTypes: true, handleMultival: 'ARRAY'})");

      assertEquals(3L, deleteResult.next().get("triplesDeleted").asLong());

      result = session.run("MATCH (n {uri: 'http://example.org/Resource1'})"
          + "RETURN n.`http://example.org/Predicate4` AS nP4");
      record = result.next();
      assertArrayEquals(new String[]{"val2"}, record.get("nP4").asList().toArray());

      deleteResult = session.run("CALL semantics.deleteRDF('" +
          RDFImportTest.class.getClassLoader().getResource("deleteRDF/dataset1Delete6.ttl").toURI()
          + "', 'Turtle', {handleVocabUris: 'KEEP', typesToLabels: true, commitSize: 500, keepCustomDataTypes: true, handleMultival: 'ARRAY'})");

      assertEquals(2L, deleteResult.next().get("triplesDeleted").asLong());

      result = session.run("MATCH (n {uri: 'http://example.org/Resource2'})"
          + "RETURN n.`http://example.org/Predicate3` AS nP3, n.`http://example.org/Predicate4` AS nP4");
      record = result.next();
      assertFalse(record.get("nP3").asList().contains(100L));
      assertFalse(record.get("nP4").asList().contains(400.0));

    }
  }

  @Test
  public void testDeleteSubjectNode() throws Exception {
    try (Driver driver = GraphDatabase.driver(neo4j.boltURI(),
        Config.build().toConfig()); Session session = driver.session()) {

      createIndices(neo4j.getGraphDatabaseService());

      StatementResult importResults = session.run("CALL semantics.importRDF('" +
          RDFImportTest.class.getClassLoader().getResource("deleteRDF/dataset1.ttl")
              .toURI()
          + "','Turtle',{ handleVocabUris: 'KEEP', typesToLabels: true, commitSize: 500, keepCustomDataTypes: true, handleMultival: 'ARRAY'})");

      assertEquals(15L, importResults.next().get("triplesLoaded").asLong());
      StatementResult result = session.run("MATCH (n:Resource)"
          + "RETURN n");
      assertEquals(3, result.list().size());
      result = session.run("MATCH (n {uri: 'http://example.org/Resource3'})"
          + "RETURN n.uri");
      Record record = result.next();
      assertEquals("http://example.org/Resource3", record.get("n.uri").asString());

      StatementResult deleteResult = session.run("CALL semantics.deleteRDF('" +
          RDFImportTest.class.getClassLoader().getResource("deleteRDF/dataset1Delete7.ttl").toURI()
          + "', 'Turtle', {handleVocabUris: 'KEEP', typesToLabels: true, commitSize: 500, keepCustomDataTypes: true, handleMultival: 'ARRAY'})");

      assertEquals(1L, deleteResult.next().get("triplesDeleted").asLong());

      result = session.run("MATCH (n {uri: 'http://example.org/Resource3'})"
          + "RETURN n.uri");
      assertFalse(result.hasNext());

    }
  }

  @Test
  public void testRepetitiveDeletion() throws Exception {
    try (Driver driver = GraphDatabase.driver(neo4j.boltURI(),
        Config.build().toConfig()); Session session = driver.session()) {

      createIndices(neo4j.getGraphDatabaseService());

      StatementResult importResults = session.run("CALL semantics.importRDF('" +
          RDFImportTest.class.getClassLoader().getResource("deleteRDF/dataset1.ttl")
              .toURI()
          + "','Turtle',{ handleVocabUris: 'KEEP', typesToLabels: true, commitSize: 500, keepCustomDataTypes: true, handleMultival: 'ARRAY'})");

      assertEquals(15L, importResults.next().get("triplesLoaded").asLong());
      StatementResult result = session.run("MATCH (n:Resource)"
          + "RETURN n");
      assertEquals(3, result.list().size());
      result = session.run("MATCH (n {uri: 'http://example.org/Resource3'})"
          + "RETURN n.uri");
      Record record = result.next();
      assertEquals("http://example.org/Resource3", record.get("n.uri").asString());

      StatementResult deleteResult = session.run("CALL semantics.deleteRDF('" +
          RDFImportTest.class.getClassLoader().getResource("deleteRDF/dataset1Delete4.ttl").toURI()
          + "', 'Turtle', {handleVocabUris: 'KEEP', typesToLabels: true, commitSize: 500, keepCustomDataTypes: true, handleMultival: 'ARRAY'})");

      assertEquals(8L, deleteResult.next().get("triplesDeleted").asLong());

      deleteResult = session.run("CALL semantics.deleteRDF('" +
          RDFImportTest.class.getClassLoader().getResource("deleteRDF/dataset1Delete4.ttl").toURI()
          + "', 'Turtle', {handleVocabUris: 'KEEP', typesToLabels: true, commitSize: 500, keepCustomDataTypes: true, handleMultival: 'ARRAY'})");

      assertEquals(0L, deleteResult.next().get("triplesDeleted").asLong());

    }
  }

  @Test
  public void ontoImportTest() throws Exception {
    try (Driver driver = GraphDatabase.driver(neo4j.boltURI(),
        Config.build().withEncryptionLevel(Config.EncryptionLevel.NONE).toConfig())) {

      createIndices(neo4j.getGraphDatabaseService());
      Session session = driver.session();

      StatementResult importResults = session.run("CALL semantics.importOntology('" +
          LiteOntologyImporterTest.class.getClassLoader().getResource("moviesontology.owl").toURI()
          + "','RDF/XML')");

      assertEquals(57L, importResults.next().get("triplesLoaded").asLong());

      assertEquals(2L,
          session.run("MATCH (n:Class) RETURN count(n) AS count").next().get("count").asLong());

      assertEquals(5L,
          session.run("MATCH (n:Property)-[:DOMAIN]->(:Class)  RETURN count(n) AS count").next()
              .get("count").asLong());

      assertEquals(6L,
          session.run("MATCH (n:Relationship) RETURN count(n) AS count").next().get("count")
              .asLong());
    }

  }

  @Test
  public void ontoSnippetImportTest() throws Exception {
    try (Driver driver = GraphDatabase.driver(neo4j.boltURI(),
        Config.build().withEncryptionLevel(Config.EncryptionLevel.NONE).toConfig())) {

      createIndices(neo4j.getGraphDatabaseService());
      Session session = driver.session();

      Map<String,Object> params = new HashMap<>();
      params.put("rdf",this.turtleOntology);

      StatementResult importResults = session.run("CALL semantics.importOntologySnippet($rdf,'Turtle')",
          params);

      assertEquals(57L, importResults.next().get("triplesLoaded").asLong());

      assertEquals(2L,
          session.run("MATCH (n:Class) RETURN count(n) AS count").next().get("count").asLong());

      assertEquals(5L,
          session.run("MATCH (n:Property)-[:DOMAIN]->(:Class)  RETURN count(n) AS count").next()
              .get("count").asLong());

      assertEquals(6L,
          session.run("MATCH (n:Relationship) RETURN count(n) AS count").next().get("count")
              .asLong());
    }

  }

  @Test
  public void ontoImportWithCustomNames() throws Exception {
    try (Driver driver = GraphDatabase.driver(neo4j.boltURI(),
        Config.build().withEncryptionLevel(Config.EncryptionLevel.NONE).toConfig())) {

      createIndices(neo4j.getGraphDatabaseService());
      Session session = driver.session();

      StatementResult importResults = session.run("CALL semantics.importOntology('" +
          LiteOntologyImporterTest.class.getClassLoader().getResource("moviesontology.owl").toURI()
          + "','RDF/XML', { classLabel : 'Category', objectPropertyLabel: 'Rel', dataTypePropertyLabel: 'Prop'})");

      assertEquals(57L, importResults.next().get("triplesLoaded").asLong());

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

      assertEquals(13L,
          session.run("MATCH (n:Resource) RETURN count(distinct n.label) AS count")
              .next().get("count").asLong());

      assertEquals(13L,
          session.run("MATCH (n:Resource) RETURN count(distinct n.comment) AS count")
              .next().get("count").asLong());

    }

  }

  @Test
  public void ontoSnippetImportWithCustomNames() throws Exception {
    try (Driver driver = GraphDatabase.driver(neo4j.boltURI(),
        Config.build().withEncryptionLevel(Config.EncryptionLevel.NONE).toConfig())) {

      createIndices(neo4j.getGraphDatabaseService());
      Session session = driver.session();

      Map<String,Object> params = new HashMap<>();
      params.put("rdf",this.turtleOntology);

      StatementResult importResults = session.run("CALL semantics.importOntologySnippet("
          + "$rdf, 'Turtle', { classLabel : 'Category', objectPropertyLabel: 'Rel', "
          + "dataTypePropertyLabel: 'Prop'})",  params );

      assertEquals(57L, importResults.next().get("triplesLoaded").asLong());

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

      assertEquals(13L,
          session.run("MATCH (n:Resource) RETURN count(distinct n.label) AS count")
              .next().get("count").asLong());

      assertEquals(13L,
          session.run("MATCH (n:Resource) RETURN count(distinct n.comment) AS count")
              .next().get("count").asLong());

    }

  }

  @Test
  public void ontoImportWithCustomNamesFilterLabels() throws Exception {
    try (Driver driver = GraphDatabase.driver(neo4j.boltURI(),
        Config.build().withEncryptionLevel(Config.EncryptionLevel.NONE).toConfig())) {

      createIndices(neo4j.getGraphDatabaseService());
      Session session = driver.session();

      StatementResult importResults = session.run("CALL semantics.importOntology('" +
          LiteOntologyImporterTest.class.getClassLoader().getResource("moviesontology.owl").toURI()
          + "','RDF/XML', { predicateExclusionList: ['http://www.w3.org/2000/01/rdf-schema#label',"
          + "'http://www.w3.org/2000/01/rdf-schema#comment'], classLabel : 'Category', "
          + " objectPropertyLabel: 'Rel', dataTypePropertyLabel: 'Prop'})");

      assertEquals(30L, importResults.next().get("triplesLoaded").asLong());

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
          session.run("MATCH (n:Rel:Resource) RETURN count(n) AS count").next().get("count")
              .asLong());

      assertEquals(0L,
          session.run("MATCH (n:Resource) RETURN count(distinct n.label) AS count")
              .next().get("count").asLong());

      assertEquals(0L,
          session.run("MATCH (n:Resource) RETURN count(distinct n.comment) AS count")
              .next().get("count").asLong());

    }

  }

  @Test
  public void ontoSnippetImportWithCustomNamesFilterLabels() throws Exception {
    try (Driver driver = GraphDatabase.driver(neo4j.boltURI(),
        Config.build().withEncryptionLevel(Config.EncryptionLevel.NONE).toConfig())) {

      createIndices(neo4j.getGraphDatabaseService());
      Session session = driver.session();

      Map<String,Object> params = new HashMap<>();
      params.put("rdf",this.turtleOntology);

      StatementResult importResults = session.run("CALL semantics.importOntologySnippet($rdf,"
          + "'Turtle', { predicateExclusionList: ['http://www.w3.org/2000/01/rdf-schema#label',"
          + "'http://www.w3.org/2000/01/rdf-schema#comment'], classLabel : 'Category', "
          + " objectPropertyLabel: 'Rel', dataTypePropertyLabel: 'Prop'})", params);

      assertEquals(30L, importResults.next().get("triplesLoaded").asLong());

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
          session.run("MATCH (n:Rel:Resource) RETURN count(n) AS count").next().get("count")
              .asLong());

      assertEquals(0L,
          session.run("MATCH (n:Resource) RETURN count(distinct n.label) AS count")
              .next().get("count").asLong());

      assertEquals(0L,
          session.run("MATCH (n:Resource) RETURN count(distinct n.comment) AS count")
              .next().get("count").asLong());

    }

  }

  @Test
  public void ontoImportSchemaOrg() throws Exception {
    try (Driver driver = GraphDatabase.driver(neo4j.boltURI(),
        Config.build().withEncryptionLevel(Config.EncryptionLevel.NONE).toConfig())) {

      createIndices(neo4j.getGraphDatabaseService());
      Session session = driver.session();

      session.run("CALL semantics.importOntology('" +
          LiteOntologyImporterTest.class.getClassLoader().getResource("schema.rdf").toURI() +
          "','RDF/XML')");

      assertEquals(592L,
          session.run("MATCH (n:Class) RETURN count(n) AS count").next().get("count").asLong());

      assertEquals(343L,
          session.run("MATCH (n:Property)-[:DOMAIN]->(:Class)  RETURN count(n) AS count").next()
              .get("count").asLong());

      assertEquals(292L,
          session.run("MATCH (n:Relationship)-[:DOMAIN]->(:Class)  RETURN count(n) AS count").next()
              .get("count").asLong());

      assertEquals(0L,
          session.run("MATCH (n:Property)-[:DOMAIN]->(:Relationship) RETURN count(n) AS count")
              .next().get("count").asLong());

      assertEquals(416L,
          session.run("MATCH (n:Relationship) RETURN count(n) AS count").next().get("count")
              .asLong());
      session.close();
    }

  }

  @Test
  public void ontoImportClassHierarchy() throws Exception {
    try (Driver driver = GraphDatabase.driver(neo4j.boltURI(),
        Config.build().withEncryptionLevel(Config.EncryptionLevel.NONE).toConfig())) {

      createIndices(neo4j.getGraphDatabaseService());
      Session session = driver.session();

      StatementResult importResults = session.run("CALL semantics.importOntology('" +
          LiteOntologyImporterTest.class.getClassLoader().getResource("class-hierarchy-test.rdf")
              .toURI() +
          "','RDF/XML')");

      assertEquals(1L,
          session.run("MATCH p=(:Class{name:'Code'})-[:SCO]->(:Class{name:'Intangible'})" +
              " RETURN count(p) AS count").next().get("count").asLong());
      session.close();
    }
  }

  @Test
  public void ontoImportPropHierarchy() throws Exception {
    try (Driver driver = GraphDatabase.driver(neo4j.boltURI(),
        Config.build().withEncryptionLevel(Config.EncryptionLevel.NONE).toConfig())) {

      createIndices(neo4j.getGraphDatabaseService());
      Session session = driver.session();

      StatementResult importResults = session.run("CALL semantics.importOntology('" +
          LiteOntologyImporterTest.class.getClassLoader().getResource("SPOTest.owl").toURI() +
          "','RDF/XML')");

      assertEquals(1L,
          session.run("MATCH p=(:Property{name:'prop1'})-[:SPO]->(:Property{name:'superprop'})" +
              " RETURN count(p) AS count").next().get("count").asLong());
      session.close();
    }
  }

  @Test
  public void ontoImportMultilabel() throws Exception {
    try (Driver driver = GraphDatabase.driver(neo4j.boltURI(),
        Config.build().withEncryptionLevel(Config.EncryptionLevel.NONE).toConfig())) {

      createIndices(neo4j.getGraphDatabaseService());
      Session session = driver.session();

      StatementResult importResults = session.run("CALL semantics.importOntology('" +
          LiteOntologyImporterTest.class.getClassLoader().getResource("moviesontologyMultilabel.owl").toURI()
          + "','RDF/XML', { keepLangTag: true, handleMultival: 'ARRAY' } )");

      assertEquals(81L, importResults.next().get("triplesLoaded").asLong());

      assertEquals(2L,
          session.run("MATCH (n:Class) RETURN count(n) AS count").next().get("count").asLong());

      assertEquals(5L,
          session.run("MATCH (n:Property)-[:DOMAIN]->(:Class)  RETURN count(n) AS count").next()
              .get("count").asLong());

      assertEquals(6L,
          session.run("MATCH (n:Relationship) RETURN count(n) AS count").next().get("count")
              .asLong());

      Record singleRecord = session.run("MATCH (n:Class { uri: 'http://neo4j.com/voc/movies#Movie'}) "
          + " RETURN n.label as label, n.comment as comment, "
          + " semantics.getLangValue('en',n.label) as label_en, "
          + " semantics.getLangValue('es',n.label) as label_es, "
          + " semantics.getLangValue('en',n.comment) as comment_en, "
          + " semantics.getLangValue('es',n.comment) as comment_es").next();
      assertTrue(singleRecord.get("label").asList().contains("Movie@en") &&
          singleRecord.get("label").asList().contains("Pelicula@es"));
      assertTrue( singleRecord.get("comment").asList().contains("A film@en") &&
          singleRecord.get("comment").asList().contains("Una pelicula@es"));
      assertEquals("Movie", singleRecord.get("label_en").asString());
      assertEquals("Pelicula", singleRecord.get("label_es").asString());
      assertEquals("A film", singleRecord.get("comment_en").asString());
      assertEquals("Una pelicula", singleRecord.get("comment_es").asString());


      singleRecord = session.run("MATCH (n:Relationship { uri: 'http://neo4j.com/voc/movies#PRODUCED'}) "
          + " RETURN n.label as label, n.comment as comment, "
          + " semantics.getLangValue('en',n.label) as label_en, "
          + " semantics.getLangValue('es',n.label) as label_es, "
          + " semantics.getLangValue('en',n.comment) as comment_en, "
          + " semantics.getLangValue('es',n.comment) as comment_es").next();
      assertTrue(singleRecord.get("label").asList().contains("PRODUCED@en") &&
          singleRecord.get("label").asList().contains("PRODUCE@es"));
      assertTrue( singleRecord.get("comment").asList().contains("Producer produced film@en") &&
          singleRecord.get("comment").asList().contains("productor de una pelicula@es"));
      assertEquals("PRODUCED", singleRecord.get("label_en").asString());
      assertEquals("PRODUCE", singleRecord.get("label_es").asString());
      assertEquals("Producer produced film", singleRecord.get("comment_en").asString());
      assertEquals("productor de una pelicula", singleRecord.get("comment_es").asString());


      singleRecord = session.run("MATCH (n:Property { uri: 'http://neo4j.com/voc/movies#title'}) "
          + " RETURN n.label as label, n.comment as comment, "
          + " semantics.getLangValue('en',n.label) as label_en, "
          + " semantics.getLangValue('es',n.label) as label_es, "
          + " semantics.getLangValue('en',n.comment) as comment_en, "
          + " semantics.getLangValue('es',n.comment) as comment_es").next();
      assertTrue(singleRecord.get("label").asList().contains("title@en") &&
          singleRecord.get("label").asList().contains("titulo@es"));
      assertTrue( singleRecord.get("comment").asList().contains("The title of a film@en") &&
          singleRecord.get("comment").asList().contains("El titulo de una pelicula@es"));
      assertEquals("title", singleRecord.get("label_en").asString());
      assertEquals("titulo", singleRecord.get("label_es").asString());
      assertEquals("The title of a film", singleRecord.get("comment_en").asString());
      assertEquals("El titulo de una pelicula", singleRecord.get("comment_es").asString());

    }

  }


  @Test
  public void ontoSnippetImportMultilabel() throws Exception {
    try (Driver driver = GraphDatabase.driver(neo4j.boltURI(),
        Config.build().withEncryptionLevel(Config.EncryptionLevel.NONE).toConfig())) {

      createIndices(neo4j.getGraphDatabaseService());
      Session session = driver.session();

      Map<String,Object> params = new HashMap<>();
      params.put("rdf",this.turtleMultilangOntology);

      StatementResult importResults = session.run("CALL semantics.importOntologySnippet($rdf,'Turtle',"
          + " { keepLangTag: true, handleMultival: 'ARRAY' } )", params);

      assertEquals(81L, importResults.next().get("triplesLoaded").asLong());

      assertEquals(2L,
          session.run("MATCH (n:Class) RETURN count(n) AS count").next().get("count").asLong());

      assertEquals(5L,
          session.run("MATCH (n:Property)-[:DOMAIN]->(:Class)  RETURN count(n) AS count").next()
              .get("count").asLong());

      assertEquals(6L,
          session.run("MATCH (n:Relationship) RETURN count(n) AS count").next().get("count")
              .asLong());

      Record singleRecord = session.run("MATCH (n:Class { uri: 'http://neo4j.com/voc/movies#Movie'}) "
          + " RETURN n.label as label, n.comment as comment, "
          + " semantics.getLangValue('en',n.label) as label_en, "
          + " semantics.getLangValue('es',n.label) as label_es, "
          + " semantics.getLangValue('en',n.comment) as comment_en, "
          + " semantics.getLangValue('es',n.comment) as comment_es").next();
      assertTrue(singleRecord.get("label").asList().contains("Movie@en") &&
          singleRecord.get("label").asList().contains("Pelicula@es"));
      assertTrue( singleRecord.get("comment").asList().contains("A film@en") &&
          singleRecord.get("comment").asList().contains("Una pelicula@es"));
      assertEquals("Movie", singleRecord.get("label_en").asString());
      assertEquals("Pelicula", singleRecord.get("label_es").asString());
      assertEquals("A film", singleRecord.get("comment_en").asString());
      assertEquals("Una pelicula", singleRecord.get("comment_es").asString());


      singleRecord = session.run("MATCH (n:Relationship { uri: 'http://neo4j.com/voc/movies#PRODUCED'}) "
          + " RETURN n.label as label, n.comment as comment, "
          + " semantics.getLangValue('en',n.label) as label_en, "
          + " semantics.getLangValue('es',n.label) as label_es, "
          + " semantics.getLangValue('en',n.comment) as comment_en, "
          + " semantics.getLangValue('es',n.comment) as comment_es").next();
      assertTrue(singleRecord.get("label").asList().contains("PRODUCED@en") &&
          singleRecord.get("label").asList().contains("PRODUCE@es"));
      assertTrue( singleRecord.get("comment").asList().contains("Producer produced film@en") &&
          singleRecord.get("comment").asList().contains("productor de una pelicula@es"));
      assertEquals("PRODUCED", singleRecord.get("label_en").asString());
      assertEquals("PRODUCE", singleRecord.get("label_es").asString());
      assertEquals("Producer produced film", singleRecord.get("comment_en").asString());
      assertEquals("productor de una pelicula", singleRecord.get("comment_es").asString());


      singleRecord = session.run("MATCH (n:Property { uri: 'http://neo4j.com/voc/movies#title'}) "
          + " RETURN n.label as label, n.comment as comment, "
          + " semantics.getLangValue('en',n.label) as label_en, "
          + " semantics.getLangValue('es',n.label) as label_es, "
          + " semantics.getLangValue('en',n.comment) as comment_en, "
          + " semantics.getLangValue('es',n.comment) as comment_es").next();
      assertTrue(singleRecord.get("label").asList().contains("title@en") &&
          singleRecord.get("label").asList().contains("titulo@es"));
      assertTrue( singleRecord.get("comment").asList().contains("The title of a film@en") &&
          singleRecord.get("comment").asList().contains("El titulo de una pelicula@es"));
      assertEquals("title", singleRecord.get("label_en").asString());
      assertEquals("titulo", singleRecord.get("label_es").asString());
      assertEquals("The title of a film", singleRecord.get("comment_en").asString());
      assertEquals("El titulo de una pelicula", singleRecord.get("comment_es").asString());

    }

  }

  @Test
  public void testImportDatesAndTimes() throws Exception {
    try (Driver driver = GraphDatabase.driver(neo4j.boltURI(),
        Config.build().withEncryptionLevel(Config.EncryptionLevel.NONE).toConfig())) {

      Session session = driver.session();

      createIndices(neo4j.getGraphDatabaseService());

      StatementResult importResults1 = session.run("CALL semantics.importRDF('" +
          RDFImportTest.class.getClassLoader().getResource("datetime/datetime-simple.ttl").toURI()
          + "','Turtle')");
      assertEquals(2L, importResults1.single().get("triplesLoaded").asLong());
      Record result = session.run(
          "MATCH (n) RETURN n.ns0__reportedOn AS rep, n.`ns0__creation-date` AS cre")
          .next();
      assertEquals(LocalDateTime.parse("2012-12-31T23:57"),
          result.get("rep").asLocalDateTime());
      assertEquals(LocalDate.parse("1999-08-16"),
          result.get("cre").asLocalDate());
    }
  }

  @Test
  public void testImportDatesAndTimes2() throws Exception {
    try (Driver driver = GraphDatabase.driver(neo4j.boltURI(),
        Config.build().withEncryptionLevel(Config.EncryptionLevel.NONE).toConfig())) {

      Session session = driver.session();

      createIndices(neo4j.getGraphDatabaseService());

      StatementResult importResults1 = session.run("CALL semantics.importRDF('" +
          RDFImportTest.class.getClassLoader().getResource("datetime/datetime-complex.ttl").toURI()
          + "','Turtle')");
      assertEquals(23L, importResults1.single().get("triplesLoaded").asLong());
      Record result = session.run(
          "MATCH (n:ns0__Issue) RETURN n.ns0__reportedOn AS report, n.ns0__reproducedOn AS reprod")
          .next();
      assertEquals(LocalDateTime.parse("2012-12-31T23:57:00"),
          result.get("report").asLocalDateTime());
      assertEquals(LocalDateTime.parse("2012-11-30T23:57:00"),
          result.get("reprod").asLocalDateTime());
    }
  }

  @Test
  public void testImportDatesAndTimesMultivalued() throws Exception {
    try (Driver driver = GraphDatabase.driver(neo4j.boltURI(),
        Config.build().withEncryptionLevel(Config.EncryptionLevel.NONE).toConfig())) {

      Session session = driver.session();

      createIndices(neo4j.getGraphDatabaseService());

      StatementResult importResults1 = session.run("CALL semantics.importRDF('" +
          RDFImportTest.class.getClassLoader()
              .getResource("datetime/datetime-simple-multivalued.ttl").toURI()
          + "','Turtle', { handleMultival: 'ARRAY' })");
      assertEquals(5L, importResults1.single().get("triplesLoaded").asLong());

      Set<LocalDate> expectedDates = new HashSet<>();
      expectedDates.add(LocalDate.parse("1999-08-16"));
      expectedDates.add(LocalDate.parse("1999-08-17"));
      expectedDates.add(LocalDate.parse("1999-08-18"));

      Set<LocalDateTime> expectedDatetimes = new HashSet<>();
      expectedDatetimes.add(LocalDateTime.parse("2012-12-31T23:57:00"));
      expectedDatetimes.add(LocalDateTime.parse("2012-12-30T23:57:00"));

      Record result = session.run(
          "MATCH (n) RETURN n.ns0__someDateValue as dates, n.ns0__someDateTimeValues as dateTimes")
          .next();
      Set<LocalDate> actualDates = new HashSet<LocalDate>();
      result.get("dates").asList().forEach(x -> actualDates.add((LocalDate) x));

      Set<LocalDateTime> actualDateTimes = new HashSet<LocalDateTime>();
      result.get("dateTimes").asList().forEach(x -> actualDateTimes.add((LocalDateTime) x));

      assertTrue(actualDates.containsAll(expectedDates));
      assertTrue(expectedDates.containsAll(actualDates));

      assertTrue(actualDateTimes.containsAll(expectedDatetimes));
      assertTrue(expectedDatetimes.containsAll(actualDateTimes));

    }
  }


  @Test
  public void testImportQuadRDFTriG() throws Exception {
    try (Driver driver = GraphDatabase.driver(neo4j.boltURI(),
        Config.build().toConfig()); Session session = driver.session()) {

      createIndices(neo4j.getGraphDatabaseService());

      StatementResult importResults = session.run("CALL semantics.importQuadRDF('" +
          RDFImportTest.class.getClassLoader().getResource("RDFDatasets/RDFDataset.trig")
              .toURI()
          + "','TriG',{ handleVocabUris: 'KEEP', typesToLabels: true, commitSize: 500, keepCustomDataTypes: true, handleMultival: 'ARRAY'})");

      assertEquals(13L, importResults.next().get("triplesLoaded").asLong());
      StatementResult result = session
          .run("MATCH (n:Resource {uri: 'http://www.example.org/exampleDocument#Monica'})"
              + "RETURN count(n) AS count");
      assertEquals(3, result.next().get("count").asInt());
      result = session
          .run("MATCH (n:Resource {uri: 'http://www.example.org/exampleDocument#John'})"
              + "RETURN count(n) AS count");
      assertEquals(3, result.next().get("count").asInt());
      result = session
          .run("MATCH (n:Resource {uri: 'http://www.example.org/exampleDocument#Monica'})"
              + "RETURN n.graphUri AS graphUri ORDER BY graphUri");
      List<Record> list = result.list();
      assertEquals("http://www.example.org/exampleDocument#G1",
          list.get(0).get("graphUri").asString());
      assertEquals("http://www.example.org/exampleDocument#G2",
          list.get(1).get("graphUri").asString());
      result = session.run("MATCH (n:Resource {uri: 'http://www.example.org/exampleDocument#G1'})"
          + "RETURN n.`http://www.example.org/vocabulary#created` AS created");
      assertEquals(LocalDate.parse("2019-06-06"),
          result.next().get("created").asList().get(0));
      result = session.run("MATCH (n:Resource {uri: 'http://www.example.org/exampleDocument#G2'})"
          + "RETURN n.`http://www.example.org/vocabulary#created` AS created");
      assertEquals(LocalDateTime.parse("2019-06-07T10:15:30"),
          result.next().get("created").asList().get(0));
      result = session.run("MATCH (n {uri: 'http://www.example.org/exampleDocument#Monica'})"
          + "WHERE NOT EXISTS(n.graphUri)"
          + "RETURN labels(n) AS labels");
      Record record = result.next();
      assertEquals("Resource",
          record.get("labels").asList().get(0));
      assertEquals("http://www.example.org/vocabulary#Person",
          record.get("labels").asList().get(1));
      result = session.run(
          "MATCH (n {uri: 'http://www.example.org/exampleDocument#John', "
              + "graphUri: 'http://www.example.org/exampleDocument#G3'})"
              + "RETURN labels(n) AS labels");
      record = result.next();
      assertEquals("Resource",
          record.get("labels").asList().get(0));
      assertEquals("http://www.example.org/vocabulary#Person",
          record.get("labels").asList().get(1));
      result = session
          .run(
              "MATCH (n:Resource)"
                  + "-[:`http://www.example.org/vocabulary#friendOf`]->"
                  + "(m:Resource)"
                  + "RETURN NOT EXISTS(n.graphUri) AND NOT EXISTS(m.graphUri) AS result");
      assertTrue(result.next().get("result").asBoolean());
      result = session
          .run(
              "MATCH (n:Resource)"
                  + "-[:`http://www.example.org/vocabulary#knows`]->"
                  + "(m:Resource)"
                  + "RETURN NOT EXISTS(n.graphUri) AND NOT EXISTS(m.graphUri) AS result");
      assertFalse(result.next().get("result").asBoolean());
    }
  }

  @Test
  public void testImportQuadRDFNQuads() throws Exception {
    try (Driver driver = GraphDatabase.driver(neo4j.boltURI(),
        Config.build().toConfig()); Session session = driver.session()) {

      createIndices(neo4j.getGraphDatabaseService());

      StatementResult importResults = session.run("CALL semantics.importQuadRDF('" +
          RDFImportTest.class.getClassLoader().getResource("RDFDatasets/RDFDataset.nq")
              .toURI()
          + "','N-Quads',{ handleVocabUris: 'KEEP', typesToLabels: true, commitSize: 500, keepCustomDataTypes: true, handleMultival: 'ARRAY'})");

      assertEquals(13L, importResults.next().get("triplesLoaded").asLong());
      StatementResult result = session
          .run("MATCH (n:Resource {uri: 'http://www.example.org/exampleDocument#Monica'})"
              + "RETURN count(n) AS count");
      assertEquals(3, result.next().get("count").asInt());
      result = session
          .run("MATCH (n:Resource {uri: 'http://www.example.org/exampleDocument#John'})"
              + "RETURN count(n) AS count");
      assertEquals(3, result.next().get("count").asInt());
      result = session
          .run("MATCH (n:Resource {uri: 'http://www.example.org/exampleDocument#Monica'})"
              + "RETURN n.graphUri AS graphUri ORDER BY graphUri");
      List<Record> list = result.list();
      assertEquals("http://www.example.org/exampleDocument#G1",
          list.get(0).get("graphUri").asString());
      assertEquals("http://www.example.org/exampleDocument#G2",
          list.get(1).get("graphUri").asString());
      result = session.run("MATCH (n:Resource {uri: 'http://www.example.org/exampleDocument#G1'})"
          + "RETURN n.`http://www.example.org/vocabulary#created` AS created");
      assertEquals(LocalDate.parse("2019-06-06"),
          result.next().get("created").asList().get(0));
      result = session.run("MATCH (n:Resource {uri: 'http://www.example.org/exampleDocument#G2'})"
          + "RETURN n.`http://www.example.org/vocabulary#created` AS created");
      assertEquals(LocalDateTime.parse("2019-06-07T10:15:30"),
          result.next().get("created").asList().get(0));
      result = session.run("MATCH (n {uri: 'http://www.example.org/exampleDocument#Monica'})"
          + "WHERE NOT EXISTS(n.graphUri)"
          + "RETURN labels(n) AS labels");
      Record record = result.next();
      assertEquals("Resource",
          record.get("labels").asList().get(0));
      assertEquals("http://www.example.org/vocabulary#Person",
          record.get("labels").asList().get(1));
      result = session.run(
          "MATCH (n {uri: 'http://www.example.org/exampleDocument#John', "
              + "graphUri: 'http://www.example.org/exampleDocument#G3'})"
              + "RETURN labels(n) AS labels");
      record = result.next();
      assertEquals("Resource",
          record.get("labels").asList().get(0));
      assertEquals("http://www.example.org/vocabulary#Person",
          record.get("labels").asList().get(1));
      result = session
          .run(
              "MATCH (n:Resource)"
                  + "-[:`http://www.example.org/vocabulary#friendOf`]->"
                  + "(m:Resource)"
                  + "RETURN NOT EXISTS(n.graphUri) AND NOT EXISTS(m.graphUri) AS result");
      assertTrue(result.next().get("result").asBoolean());
      result = session
          .run(
              "MATCH (n:Resource)"
                  + "-[:`http://www.example.org/vocabulary#knows`]->"
                  + "(m:Resource)"
                  + "RETURN NOT EXISTS(n.graphUri) AND NOT EXISTS(m.graphUri) AS result");
      assertFalse(result.next().get("result").asBoolean());
    }
  }

  @Test
  public void testDeleteQuadRDFTriG() throws Exception {
    try (Driver driver = GraphDatabase.driver(neo4j.boltURI(),
        Config.build().toConfig()); Session session = driver.session()) {

      createIndices(neo4j.getGraphDatabaseService());

      StatementResult importResults = session.run("CALL semantics.importQuadRDF('" +
          RDFImportTest.class.getClassLoader().getResource("RDFDatasets/RDFDataset.trig")
              .toURI()
          + "','TriG',{ handleVocabUris: 'KEEP', typesToLabels: true, commitSize: 500, keepCustomDataTypes: true, handleMultival: 'ARRAY'})");

      assertEquals(13L, importResults.next().get("triplesLoaded").asLong());
      StatementResult result = session.run("MATCH (n:Resource)"
          + "RETURN n");
      assertEquals(12, result.list().size());

      StatementResult deleteResult = session.run("CALL semantics.deleteQuadRDF('" +
          RDFImportTest.class.getClassLoader().getResource("RDFDatasets/RDFDatasetDelete.trig")
              .toURI()
          + "', 'TriG', {handleVocabUris: 'KEEP', typesToLabels: true, commitSize: 500, keepCustomDataTypes: true, handleMultival: 'ARRAY'})");

      assertEquals(9L, deleteResult.next().get("triplesDeleted").asLong());

      result = session.run("MATCH (n:Resource)"
          + "RETURN n");
      assertEquals(5, result.list().size());

    }
  }

  @Test
  public void testDeleteQuadRDFNQuads() throws Exception {
    try (Driver driver = GraphDatabase.driver(neo4j.boltURI(),
        Config.build().toConfig()); Session session = driver.session()) {

      createIndices(neo4j.getGraphDatabaseService());

      StatementResult importResults = session.run("CALL semantics.importQuadRDF('" +
          RDFImportTest.class.getClassLoader().getResource("RDFDatasets/RDFDataset.nq")
              .toURI()
          + "','N-Quads',{ handleVocabUris: 'KEEP', typesToLabels: true, commitSize: 500, keepCustomDataTypes: true, handleMultival: 'ARRAY'})");

      assertEquals(13L, importResults.next().get("triplesLoaded").asLong());
      StatementResult result = session.run("MATCH (n:Resource)"
          + "RETURN n");
      assertEquals(12, result.list().size());

      StatementResult deleteResult = session.run("CALL semantics.deleteQuadRDF('" +
          RDFImportTest.class.getClassLoader().getResource("RDFDatasets/RDFDatasetDelete.nq")
              .toURI()
          + "', 'N-Quads', {handleVocabUris: 'KEEP', typesToLabels: true, commitSize: 500, keepCustomDataTypes: true, handleMultival: 'ARRAY'})");

      assertEquals(9L, deleteResult.next().get("triplesDeleted").asLong());

      result = session.run("MATCH (n:Resource)"
          + "RETURN n");
      assertEquals(5, result.list().size());

    }
  }

  @Test
  public void testRepetitiveDeletionQuadRDF() throws Exception {
    try (Driver driver = GraphDatabase.driver(neo4j.boltURI(),
        Config.build().toConfig()); Session session = driver.session()) {

      createIndices(neo4j.getGraphDatabaseService());

      StatementResult importResults = session.run("CALL semantics.importQuadRDF('" +
          RDFImportTest.class.getClassLoader().getResource("RDFDatasets/RDFDataset.trig")
              .toURI()
          + "','TriG',{ handleVocabUris: 'KEEP', typesToLabels: true, commitSize: 500, keepCustomDataTypes: true, handleMultival: 'ARRAY'})");

      assertEquals(13L, importResults.next().get("triplesLoaded").asLong());
      StatementResult result = session.run("MATCH (n:Resource)"
          + "RETURN n");
      assertEquals(12, result.list().size());

      StatementResult deleteResult = session.run("CALL semantics.deleteQuadRDF('" +
          RDFImportTest.class.getClassLoader().getResource("RDFDatasets/RDFDatasetDelete.trig")
              .toURI()
          + "', 'TriG', {handleVocabUris: 'KEEP', typesToLabels: true, commitSize: 500, keepCustomDataTypes: true, handleMultival: 'ARRAY'})");

      assertEquals(9L, deleteResult.next().get("triplesDeleted").asLong());

      result = session.run("MATCH (n:Resource)"
          + "RETURN n");
      assertEquals(5, result.list().size());

      deleteResult = session.run("CALL semantics.deleteQuadRDF('" +
          RDFImportTest.class.getClassLoader().getResource("RDFDatasets/RDFDatasetDelete.trig")
              .toURI()
          + "', 'TriG', {handleVocabUris: 'KEEP', typesToLabels: true, commitSize: 500, keepCustomDataTypes: true, handleMultival: 'ARRAY'})");

      assertEquals(0L, deleteResult.next().get("triplesDeleted").asLong());

    }
  }

  private void createIndices(GraphDatabaseService db) {
    db.execute("CREATE INDEX ON :Resource(uri)");
  }



  @Test
  public void testLoadJSONAsTreeEmptyJSON() throws Exception {
    try (Driver driver = GraphDatabase.driver(neo4j.boltURI(),
        Config.build().toConfig()); Session session = driver.session()) {

      createIndices(neo4j.getGraphDatabaseService());

      String jsonFragment = "";

      StatementResult importResults
          = session
          .run("CREATE (n:Node)  WITH n "
                  + " CALL semantics.importJSONAsTree(n, '" + jsonFragment + "')"
              + " YIELD node RETURN node ");
      assertFalse(importResults.hasNext());
    }
  }

  @Test
  public void testLoadJSONAsTreeListAtRoot() throws Exception {
    try (Driver driver = GraphDatabase.driver(neo4j.boltURI(),
        Config.build().toConfig()); Session session = driver.session()) {

      createIndices(neo4j.getGraphDatabaseService());

      //String jsonFragment = "[]";
      String jsonFragment = "[{\"menu\": {\n"
          + "  \"id\": \"file\",\n"
          + "  \"value\": \"File\",\n"
          + "  \"popup\": {\n"
          + "    \"menuitem\": [\n"
          + "      {\"value\": \"New\", \"onclick\": \"CreateNewDoc()\"},\n"
          + "      {\"value\": \"Open\", \"onclick\": \"OpenDoc()\"},\n"
          + "      {\"value\": \"Close\", \"onclick\": \"CloseDoc()\"}\n"
          + "    ]\n"
          + "  }\n"
          + "}}, { \"message\": \"hello!\"} ]";

      StatementResult importResults
          = session.run("CREATE (n:Node)  WITH n "
          + " CALL semantics.importJSONAsTree(n, '" + jsonFragment + "','MY_JSON')"
          + " YIELD node RETURN node ");
      assertTrue(importResults.hasNext());
      StatementResult queryresult = session
          .run("match (n:Node)-[:MY_JSON]->(r) return count(r) as ct ");
      assertEquals(2, queryresult.next().get("ct").asInt());
      queryresult = session
          .run("match (n:Node)-[:MY_JSON]->()-[:menu]->(thing)-[:popup]->() return thing ");
      assertEquals("File", queryresult.next().get("thing").asNode().asMap().get("value"));
      queryresult = session
          .run("match (n:Node)-[:MY_JSON]->(thing) where not (thing)-->() "
              + "return thing.message as msg ");
      assertEquals("hello!", queryresult.next().get("msg").asString());
    }
  }


  @Test
  public void testLoadJSONAsTree() throws Exception {
    try (Driver driver = GraphDatabase.driver(neo4j.boltURI(),
        Config.build().toConfig()); Session session = driver.session()) {

      createIndices(neo4j.getGraphDatabaseService());

      String jsonFragment = "{\"menu\": {\n"
          + "  \"id\": \"file\",\n"
          + "  \"value\": \"File\",\n"
          + "  \"popup\": {\n"
          + "    \"menuitem\": [\n"
          + "      {\"value\": \"New\", \"onclick\": \"CreateNewDoc()\"},\n"
          + "      {\"value\": \"Open\", \"onclick\": \"OpenDoc()\"},\n"
          + "      {\"value\": \"Close\", \"onclick\": \"CloseDoc()\"}\n"
          + "    ]\n"
          + "  }\n"
          + "}}";

      StatementResult importResults
          = session
          .run("CREATE (n:Node { id: 'record node'})  WITH n "
              + " CALL semantics.importJSONAsTree(n, '" + jsonFragment + "') YIELD node "
              + " RETURN node ");
      assertTrue(importResults.hasNext());
      assertEquals("record node", importResults.next().get("node").asNode()
          .get("id").asString());
      StatementResult queryresult = session
          .run("match (n:Node:Resource)-[:_jsonTree]->()-[:menu]->()-[:popup]->()"
              + "-[:menuitem]->(mi { value: 'Open', onclick: 'OpenDoc()'}) return mi ");
      assertEquals("Resource",
          queryresult.next().get("mi").asNode().labels().iterator().next());
    }
  }


  @Test
  public void testLoadJSONAsTreeWithUrisAndContext() throws Exception {
    try (Driver driver = GraphDatabase.driver(neo4j.boltURI(),
        Config.build().toConfig()); Session session = driver.session()) {

      createIndices(neo4j.getGraphDatabaseService());

      String jsonFragment = "{\n"
          + "  \"@context\": {\n"
          + "    \"name\": \"http://xmlns.com/foaf/0.1/name\",\n"
          + "    \"knows\": \"http://xmlns.com/foaf/0.1/knows\",\n"
          + "\t\"modified\": \"http://xmlns.com/foaf/0.1/modified\"\n"
          + "  },\n"
          + "  \"@id\": \"http://me.markus-lanthaler.com/\",\n"
          + "  \"name\": \"Markus Lanthaler\",\n"
          + "  \"knows\": [\n"
          + "    {\n"
          + "      \"@id\": \"http://manu.sporny.org/about#manu\",\n"
          + "      \"name\": \"Manu Sporny\"\n"
          + "    },\n"
          + "    {\n"
          + "      \"name\": \"Dave Longley\",\n"
          + "\t  \"modified\":\n"
          + "\t    {\n"
          + "\t      \"@value\": \"2010-05-29T14:17:39+02:00\",\n"
          + "\t      \"@type\": \"http://www.w3.org/2001/XMLSchema#dateTime\"\n"
          + "\t    }\n"
          + "    }\n"
          + "  ]\n"
          + "}";

      StatementResult importResults
          = session
          .run("CREATE (n:Node { id: 'I\\'m the hook node'})  WITH n "
              + " CALL semantics.importJSONAsTree(n, '" + jsonFragment + "') YIELD node "
              + " RETURN node ");
      assertTrue(importResults.hasNext());
      assertEquals("I'm the hook node",
          importResults.next().get("node").asNode().get("id").asString());
      StatementResult queryresult = session
          .run("match (n:Node:Resource)-[l:_jsonTree]->"
              + "(:Resource { uri: 'http://me.markus-lanthaler.com/'}) return l ");
      assertTrue(queryresult.hasNext());
      queryresult = session
          .run("match (n:Node:Resource)-[:_jsonTree]->"
              + "(:Resource { uri: 'http://me.markus-lanthaler.com/'})-[:knows]->"
              + "(friend) return collect(friend.name) as friends ");
      assertTrue(queryresult.hasNext());
      List<Object> friends = queryresult.next().get("friends").asList();
      assertTrue(friends.contains("Dave Longley"));
      assertTrue(friends.contains("Manu Sporny"));
      assertEquals(2,friends.size());
    }
  }

  @Test
  public void testLoadJSONAsTree2() throws Exception {
    try (Driver driver = GraphDatabase.driver(neo4j.boltURI(),
        Config.build().toConfig()); Session session = driver.session()) {

      createIndices(neo4j.getGraphDatabaseService());

      String jsonFragment = "{\"widget\": {\n"
          + "    \"debug\": \"on\",\n"
          + "    \"window\": {\n"
          + "        \"title\": \"Sample Konfabulator Widget\",\n"
          + "        \"name\": \"main_window\",\n"
          + "        \"width\": 333,\n"
          + "        \"height\": 500\n"
          + "    },\n"
          + "    \"image\": { \n"
          + "        \"src\": \"Images/Sun.png\",\n"
          + "        \"name\": \"sun1\",\n"
          + "        \"hOffset\": 250,\n"
          + "        \"vOffset\": 250,\n"
          + "        \"alignment\": \"center\"\n"
          + "    },\n"
          + "    \"text\": {\n"
          + "        \"data\": \"Click Here\",\n"
          + "        \"size\": 36,\n"
          + "        \"style\": \"bold\",\n"
          + "        \"name\": \"text1\",\n"
          + "        \"hOffset\": 250,\n"
          + "        \"vOffset\": 100,\n"
          + "        \"alignment\": \"center\",\n"
          + "        \"onMouseUp\": \"sun1.opacity = (sun1.opacity / 100) * 90;\"\n"
          + "    }\n"
          + "}}    ";

      StatementResult importResults
          = session
          .run("CREATE (n:Node)  WITH n "
              + " CALL semantics.importJSONAsTree(n, '" + jsonFragment + "') YIELD node "
              + " RETURN node ");
      assertTrue(importResults.hasNext());
      StatementResult queryresult = session
          .run("match (n:Node)-[:_jsonTree]->()-[:widget]->( { debug: 'on'})"
              + "-[:window]->(w) return w.title as title, w.width as width ");
      Record next = queryresult.next();
      assertEquals("Sample Konfabulator Widget", next.get("title").asString());
      assertEquals(333, next.get("width").asInt());

      queryresult = session
          .run("match (n:Node)-[:_jsonTree]->()-[:widget]->( { debug: 'on'})"
              + "-->(w) return count(w) as ct ");
      assertEquals(3, queryresult.next().get("ct").asInt());
    }
  }


  @Test
  public void testLoadJSONAsTree3() throws Exception {
    try (Driver driver = GraphDatabase.driver(neo4j.boltURI(),
        Config.build().toConfig()); Session session = driver.session()) {

      createIndices(neo4j.getGraphDatabaseService());

      String jsonFragment = "{\"menu\": {\n"
          + "    \"header\": \"SVG Viewer\",\n"
          + "    \"items\": [\n"
          + "        {\"id\": \"Open\"},\n"
          + "        {\"id\": \"OpenNew\", \"label\": \"Open New\"},\n"
          + "        null,\n"
          + "        {\"id\": \"ZoomIn\", \"label\": \"Zoom In\"},\n"
          + "        {\"id\": \"ZoomOut\", \"label\": \"Zoom Out\"},\n"
          + "        {\"id\": \"OriginalView\", \"label\": \"Original View\"},\n"
          + "        null,\n"
          + "        {\"id\": \"Quality\"},\n"
          + "        {\"id\": \"Pause\"},\n"
          + "        {\"id\": \"Mute\"},\n"
          + "        null,\n"
          + "        {\"id\": \"Find\", \"label\": \"Find...\"},\n"
          + "        {\"id\": \"FindAgain\", \"label\": \"Find Again\"},\n"
          + "        {\"id\": \"Copy\"},\n"
          + "        {\"id\": \"CopyAgain\", \"label\": \"Copy Again\"},\n"
          + "        {\"id\": \"CopySVG\", \"label\": \"Copy SVG\"},\n"
          + "        {\"id\": \"ViewSVG\", \"label\": \"View SVG\"},\n"
          + "        {\"id\": \"ViewSource\", \"label\": \"View Source\"},\n"
          + "        {\"id\": \"SaveAs\", \"label\": \"Save As\"},\n"
          + "        null,\n"
          + "        {\"id\": \"Help\"},\n"
          + "        {\"id\": \"About\", \"label\": \"About Adobe CVG Viewer...\"}\n"
          + "    ]\n"
          + "}}";

      StatementResult importResults
          = session
          .run("CREATE (n:Node)  WITH n "
              + " CALL semantics.importJSONAsTree(n, '" + jsonFragment + "') YIELD node "
              + " RETURN node ");
      assertTrue(importResults.hasNext());
      StatementResult queryresult = session
          .run("match (n:Node)-[:_jsonTree]->()-[:menu]->( { header: 'SVG Viewer'})"
              + "-[:items]->(item) return count(item) as itemcount, "
              + " count(distinct item.label) as labelcount ");
      Record next = queryresult.next();
      assertEquals(18, next.get("itemcount").asInt());
      assertEquals(12, next.get("labelcount").asInt());

      queryresult = session
          .run("match (n:Node)-[:_jsonTree]->()-[:menu]->( { header: 'SVG Viewer'})"
              + "-[:items]->(item { id: 'ViewSource'}) return item.label as label ");
      assertEquals("View Source", queryresult.next().get("label").asString());
    }
  }

}
