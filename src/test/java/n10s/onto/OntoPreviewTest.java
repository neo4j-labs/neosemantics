package n10s.onto;

import n10s.graphconfig.GraphConfigProcedures;
import n10s.onto.load.OntoLoadProcedures;
import n10s.onto.preview.OntoPreviewProcedures;
import n10s.rdf.RDFProcedures;
import org.junit.*;
import org.neo4j.driver.Record;
import org.neo4j.driver.*;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.driver.types.Node;
import org.neo4j.driver.types.Relationship;
import org.neo4j.harness.junit.rule.Neo4jRule;

import java.net.URI;
import java.net.URISyntaxException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import static n10s.CommonProcedures.UNIQUENESS_CONSTRAINT_STATEMENT;
import static org.junit.Assert.*;

/**
 * Tests for n10s.onto.preview.* procedures (inline and fetch variants).
 */
public class OntoPreviewTest {
  public static Driver driver;

  @ClassRule
  public static Neo4jRule neo4j = new Neo4jRule()
          .withProcedure(OntoLoadProcedures.class)
          .withProcedure(OntoPreviewProcedures.class)
          .withProcedure(GraphConfigProcedures.class)
          .withFunction(RDFProcedures.class);

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


  String restrictionsBasicTurtle = "" +
          "@prefix : <http://www.semanticweb.org/jb/ontologies/2021/5/untitled-ontology-2#> .\n" +
          "@prefix owl: <http://www.w3.org/2002/07/owl#> .\n" +
          "@prefix rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#> .\n" +
          "@prefix rdfs: <http://www.w3.org/2000/01/rdf-schema#> .\n" +
          "@base <http://www.semanticweb.org/jb/ontologies/2021/5/untitled-ontology-2> .\n" +
          "\n" +
          "\n" +
          ":Parent rdf:type owl:Class ;\n" +
          "        owl:equivalentClass [ rdf:type owl:Restriction ;\n" +
          "                              owl:onProperty :hasChild ;\n" +
          "                              owl:someValuesFrom :Person\n" +
          "                            ] ;\n" +
          "        rdfs:subClassOf [ rdf:type owl:Restriction ;\n" +
          "                          owl:onProperty :hasPet ;\n" +
          "                          owl:allValuesFrom :Animal\n" +
          "                        ] .";

  String restrictionsWithDomAndRange = "" +
          "@prefix : <http://www.semanticweb.org/jb/ontologies/2021/5/untitled-ontology-2#> .\n" +
          "@prefix owl: <http://www.w3.org/2002/07/owl#> .\n" +
          "@prefix rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#> .\n" +
          "@prefix xml: <http://www.w3.org/XML/1998/namespace> .\n" +
          "@prefix xsd: <http://www.w3.org/2001/XMLSchema#> .\n" +
          "@prefix rdfs: <http://www.w3.org/2000/01/rdf-schema#> .\n" +
          "@base <http://www.semanticweb.org/jb/ontologies/2021/5/untitled-ontology-2> .\n" +
          "\n" +
          "\n" +
          ":Parent rdf:type owl:Class ;\n" +
          "        rdfs:subClassOf [ rdf:type owl:Restriction ;\n" +
          "                          owl:onProperty :hasChild ;\n" +
          "                          owl:someValuesFrom :Person\n" +
          "                        ] .\n" +
          "\n" +
          ":PetOwner rdf:type owl:Class ;\n" +
          "        owl:equivalentClass [ rdf:type owl:Restriction ;\n" +
          "                              owl:onProperty :hasPet ;\n" +
          "                              owl:someValuesFrom :Animal\n" +
          "                            ] .\n" +
          "\n" +
          ":Animal rdfs:label \"Animal\" ; \n" +
          "        rdfs:comment \"an animal\" .\n" +
          "\n" +
          " :hasChild rdf:type owl:ObjectProperty ;\n" +
          "        rdfs:domain :Person ;\n" +
          "        rdfs:range :Person ;\n" +
          "        rdfs:label \"has child\" ;\n" +
          "        rdfs:comment \"be the parent of\" .\n" +
          "\n";

  String cardinalityRestriction = "<rdf:RDF\n" +
          "    xmlns:rdf=\"http://www.w3.org/1999/02/22-rdf-syntax-ns#\"\n" +
          "    xmlns:owl=\"http://www.w3.org/2002/07/owl#\"\n" +
          "    xmlns:rdfs=\"http://www.w3.org/2000/01/rdf-schema#\">\n" +
          "  <owl:Ontology rdf:about=\"http://test\"/>\n" +
          "  <owl:Class rdf:about=\"http://test#Characteristic\"/>\n" +
          "  <owl:Class rdf:about=\"http://test#Scale\"/>\n" +
          "  <owl:Class rdf:about=\"http://test#Numeric\">\n" +
          "    <rdfs:subClassOf>\n" +
          "      <owl:Restriction>\n" +
          "        <owl:onClass rdf:resource=\"http://test#Scale\"/>\n" +
          "        <owl:maxQualifiedCardinality rdf:datatype=\"http://www.w3.org/2001/XMLSchema#nonNegativeInteger\"\n" +
          "        >1</owl:maxQualifiedCardinality>\n" +
          "        <owl:onProperty>\n" +
          "          <owl:ObjectProperty rdf:about=\"http://test#hasUnit\"/>\n" +
          "        </owl:onProperty>\n" +
          "      </owl:Restriction>\n" +
          "    </rdfs:subClassOf>\n" +
          "    <rdfs:subClassOf rdf:resource=\"http://test#Characteristic\"/>\n" +
          "    <rdfs:label>Numeric</rdfs:label>\n" +
          "  </owl:Class>\n" +
          "</rdf:RDF>\n";

  private static URI file(String path) {
    try {
      return OntoPreviewTest.class.getClassLoader().getResource(path).toURI();
    } catch (URISyntaxException e) {
      String msg = String.format("Failed to load the resource with path '%s'", path);
      throw new RuntimeException(msg, e);
    }
  }

  @Test
  public void testOntoPreviewFromSnippet() throws Exception {
    Session session = driver.session();

      initialiseGraphDB(neo4j.defaultDatabaseService(),
          "{}");

      Map<String, Object> params = new HashMap<>();
      params.put("rdf", this.turtleMultilangOntology);

      Result importResults
          = session
          .run("CALL n10s.onto.preview.inline($rdf,'Turtle')", params);
      Map<String, Object> next = importResults
          .next().asMap();
      final List<Node> nodes = (List<Node>) next.get("nodes");
      assertEquals(14, nodes.size());
      final List<Relationship> rels = (List<Relationship>) next.get("relationships");
      assertEquals(17, rels.size());
  }

  @Test
  public void testOntoPreviewFromSnippetLimit() throws Exception {
    Session session = driver.session();

      initialiseGraphDB(neo4j.defaultDatabaseService(),
          "{}");

      Map<String, Object> params = new HashMap<>();
      params.put("rdf", this.turtleOntology);

      Result importResults
          = session
          .run("CALL n10s.onto.preview.inline($rdf,'Turtle')", params);
      Map<String, Object> next = importResults
          .next().asMap();
      List<Node> nodes = (List<Node>) next.get("nodes");
      assertEquals(14, nodes.size());
      List<Relationship> rels = (List<Relationship>) next.get("relationships");
      assertEquals(17, rels.size());

      //now  limiting it to 5 triples
      importResults
          = session
          .run("CALL n10s.onto.preview.inline($rdf,'Turtle',  { limit: 14 })", params);
      next = importResults
          .next().asMap();
      nodes = (List<Node>) next.get("nodes");
      assertEquals(5, nodes.size());
      rels = (List<Relationship>) next.get("relationships");
      assertEquals(1, rels.size());
  }

  @Test
  public void testOntoPreviewFromSnippetWithRestrictions() throws Exception {
    Session session = driver.session();

      initialiseGraphDB(neo4j.defaultDatabaseService(),
              "{}");

      Map<String, Object> params = new HashMap<>();
      params.put("rdf", this.restrictionsBasicTurtle);

      Result importResults
              = session
              .run("CALL n10s.onto.preview.inline($rdf,'Turtle')", params);
      Map<String, Object> next = importResults
              .next().asMap();
      final List<Node> nodes = (List<Node>) next.get("nodes");
      assertEquals(5, nodes.size());
      final List<Relationship> rels = (List<Relationship>) next.get("relationships");
      assertEquals(2, rels.size());
  }

  @Test
  public void testOntoPreviewFromSnippetWithRestrictionsAndOtherElements() throws Exception {
    Session session = driver.session();

      initialiseGraphDB(neo4j.defaultDatabaseService(),
              "{ handleVocabUris: 'IGNORE'}");

      Map<String, Object> params = new HashMap<>();
      params.put("rdf", this.restrictionsWithDomAndRange);

      Result importResults
              = session
              .run("CALL n10s.onto.preview.inline($rdf,'Turtle')", params);
      Map<String, Object> next = importResults
              .next().asMap();
      final List<Node> nodes = (List<Node>) next.get("nodes");
      assertEquals(6, nodes.size());
      final List<Relationship> rels = (List<Relationship>) next.get("relationships");
      assertEquals(4, rels.size());

      int domainCount = 0;
      int rangeCount = 0;
      int restrictionCount = 0;
      Iterator<Relationship> relsIterator = rels.iterator();
      while (relsIterator.hasNext()) {
        String relName = relsIterator.next().type();
        if (relName.equals("DOMAIN")) {
          domainCount++;
        } else if (relName.equals("RANGE")) {
          rangeCount++;
        } else if (relName.equals("SCO_RESTRICTION") || relName.equals("EQC_RESTRICTION")) {
          restrictionCount++;
        }
      }
      assertEquals(1,domainCount);
      assertEquals(1,rangeCount);
      assertEquals(2,restrictionCount);

      Iterator<Node> nodesIterator = nodes.iterator();
      while (nodesIterator.hasNext()) {
        Map<String, Object> nodeAsMap = nodesIterator.next().asMap();
        if(nodeAsMap.get("uri").equals("http://www.semanticweb.org/jb/ontologies/2021/5/untitled-ontology-2#Animal")){
          assertEquals("Animal", nodeAsMap.get("label"));
          assertEquals("an animal", nodeAsMap.get("comment"));
        } else if(nodeAsMap.get("uri").equals("http://www.semanticweb.org/jb/ontologies/2021/5/untitled-ontology-2#hasChild")){
          assertEquals("has child", nodeAsMap.get("label"));
          assertEquals("be the parent of", nodeAsMap.get("comment"));
        } else {
          assertFalse(nodeAsMap.containsKey("label") || nodeAsMap.containsKey("comment"));
        }

      }
  }

  @Test
  public void testOntoPreviewFromSnippetWithCardinalityRestrictions() throws Exception {
    Session session = driver.session();

      initialiseGraphDB(neo4j.defaultDatabaseService(),
              "{ handleVocabUris: 'IGNORE'}");

      Map<String, Object> params = new HashMap<>();
      params.put("rdf", this.cardinalityRestriction);

      Result importResults
              = session
              .run("CALL n10s.onto.preview.inline($rdf,'RDF/XML')", params);
      Map<String, Object> next = importResults
              .next().asMap();
      final List<Node> nodes = (List<Node>) next.get("nodes");
      assertEquals(4, nodes.size());
      final List<Relationship> rels = (List<Relationship>) next.get("relationships");
      assertEquals(2, rels.size());

      int restrictionCount = 0;
      Iterator<Relationship> relsIterator = rels.iterator();
      while (relsIterator.hasNext()) {
        Relationship rel = relsIterator.next();
        String relName = rel.type();
        if (relName.equals("SCO_RESTRICTION") || relName.equals("EQC_RESTRICTION")) {
          restrictionCount++;
          Map<String, Object> relprops = rel.asMap();
          assertEquals("http://test#hasUnit", relprops.get("onPropertyURI"));
          assertEquals("MAXQUALIFIEDCARDINALITY", relprops.get("restrictionType"));
          assertEquals(1L, relprops.get("cardinalityVal"));
          assertEquals("hasUnit", relprops.get("onPropertyName"));
        }
      }
      assertEquals(1,restrictionCount);

      Iterator<Node> nodesIterator = nodes.iterator();
      while (nodesIterator.hasNext()) {
        Map<String, Object> nodeAsMap = nodesIterator.next().asMap();
        if(nodeAsMap.get("uri").equals("http://test#hasUnit")){
          assertEquals("hasUnit", nodeAsMap.get("name"));
        }
      }
  }

  @Test
  public void testOntoPreviewFromFileLimit() throws Exception {
    Session session = driver.session();

      initialiseGraphDB(neo4j.defaultDatabaseService(),
          "{}");

      Result importResults
          = session
          .run("CALL n10s.onto.preview.fetch('" + OntoPreviewTest.class.getClassLoader()
              .getResource("moviesontology.owl")
              .toURI() + "','RDF/XML')");
      Map<String, Object> next = importResults
          .next().asMap();
      List<Node> nodes = (List<Node>) next.get("nodes");
      assertEquals(14, nodes.size());
      List<Relationship> rels = (List<Relationship>) next.get("relationships");
      assertEquals(17, rels.size());

      //now  limiting it to 5 triples
      importResults
          = session
          .run("CALL n10s.onto.preview.fetch(' " + OntoPreviewTest.class.getClassLoader()
              .getResource("moviesontology.owl")
              .toURI() + "','RDF/XML',  { limit: 6 })");
      next = importResults
          .next().asMap();
      nodes = (List<Node>) next.get("nodes");
      assertEquals(2, nodes.size());
      rels = (List<Relationship>) next.get("relationships");
      assertEquals(0, rels.size());
  }

  private void initialiseGraphDB(GraphDatabaseService db, String graphConfigParams) {
    db.executeTransactionally(UNIQUENESS_CONSTRAINT_STATEMENT);
    db.executeTransactionally("CALL n10s.graphconfig.init(" +
        (graphConfigParams != null ? graphConfigParams : "{}") + ")");
  }
}
