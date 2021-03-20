package n10s;

import static n10s.graphconfig.Params.PREFIX_SEPARATOR;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.neo4j.driver.Values.NULL;
import static org.neo4j.driver.Values.ofNode;

import java.net.URI;
import java.net.URISyntaxException;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import n10s.experimental.ExperimentalImports;
import n10s.graphconfig.GraphConfigProcedures;
import n10s.mapping.MappingUtils;
import n10s.nsprefixes.NsPrefixDefProcedures;
import n10s.onto.load.OntoLoadProcedures;
import n10s.onto.preview.OntoPreviewProcedures;
import n10s.quadrdf.delete.QuadRDFDeleteProcedures;
import n10s.quadrdf.load.QuadRDFLoadProcedures;
import n10s.rdf.RDFProcedures;
import n10s.rdf.delete.RDFDeleteProcedures;
import n10s.rdf.load.RDFLoadProcedures;
import n10s.rdf.preview.RDFPreviewProcedures;
import n10s.rdf.stream.RDFStreamProcedures;
import n10s.skos.load.SKOSLoadProcedures;
import org.eclipse.rdf4j.model.vocabulary.XMLSchema;
import org.junit.Rule;
import org.junit.Test;
import org.neo4j.driver.Config;
import org.neo4j.driver.Driver;
import org.neo4j.driver.GraphDatabase;
import org.neo4j.driver.Record;
import org.neo4j.driver.Result;
import org.neo4j.driver.Session;
import org.neo4j.driver.internal.InternalRelationship;
import org.neo4j.driver.internal.value.IntegerValue;
import org.neo4j.driver.internal.value.ListValue;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.harness.junit.rule.Neo4jRule;

/**
 * Created by jbarrasa on 21/03/2016.
 */
public class RDFProceduresTest {

  @Rule
  public Neo4jRule neo4j = new Neo4jRule()
      .withProcedure(RDFLoadProcedures.class)
      .withProcedure(RDFDeleteProcedures.class)
      .withProcedure(RDFPreviewProcedures.class)
      .withProcedure(RDFStreamProcedures.class)
      .withFunction(RDFProcedures.class)
      .withProcedure(OntoLoadProcedures.class)
      .withProcedure(OntoPreviewProcedures.class)
      .withProcedure(QuadRDFLoadProcedures.class)
      .withProcedure(QuadRDFDeleteProcedures.class)
      .withProcedure(MappingUtils.class)
      .withProcedure(GraphConfigProcedures.class).withProcedure(NsPrefixDefProcedures.class)
      .withProcedure(ExperimentalImports.class)
      .withProcedure(SKOSLoadProcedures.class);

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

  private String turtleFragmentTypes = "@prefix show: <http://example.org/vocab/show/> .\n" +
      " show:218 show:localName \"That Seventies Show\"@en . " +
      " show:218 rdf:type show:Show . ";

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

  private static final String SKOS_FRAGMENT_TURTLE =
      "@prefix skos: <http://www.w3.org/2004/02/skos/core#> .\n"
          + "@prefix thesaurus: <http://vocabularies.unesco.org/thesaurus/> .\n"
          + "@prefix isothes: <http://purl.org/iso25964/skos-thes#> .\n"
          + "@prefix dc: <http://purl.org/dc/terms/> .\n"
          + "@prefix xsd: <http://www.w3.org/2001/XMLSchema#> .\n"
          + "\n"
          + "<http://vocabularies.unesco.org/thesaurus>\n"
          + "  a skos:ConceptScheme ;\n"
          + "  skos:prefLabel \"UNESCO Thesaurus\"@en, \"Thésaurus de lUNESCO\"@fr, \"Тезаурус ЮНЕСКО\"@ru, \"Tesauro de la UNESCO\"@es .\n"
          + "\n"
          + "thesaurus:concept2094\n"
          + "  a skos:Concept ;\n"
          + "  skos:prefLabel \"Lengua altaica\"@es, \"Langue altaïque\"@fr, \"Алтайские языки\"@ru, \"Altaic languages\"@en ;\n"
          + "  skos:narrower thesaurus:concept2096 .\n"
          + "\n"
          + "thesaurus:mt3.35\n"
          + "  a isothes:ConceptGroup, <http://vocabularies.unesco.org/ontology#MicroThesaurus>, skos:Collection ;\n"
          + "  skos:prefLabel \"Languages\"@en, \"Lenguas\"@es, \"Langues\"@fr, \"Языки\"@ru ;\n"
          + "  skos:member thesaurus:concept2096 .\n"
          + "\n"
          + "thesaurus:concept2096\n"
          + "  dc:modified \"2006-05-23T00:00:00\"^^xsd:dateTime ;\n"
          + "  a skos:Concept ;\n"
          + "  skos:inScheme <http://vocabularies.unesco.org/thesaurus> ;\n"
          + "  skos:prefLabel \"Azerbaijani\"@en, \"Azéri\"@fr, \"Азербайджанский язык\"@ru, \"Azerbaiyano\"@es ;\n"
          + "  skos:hiddenLabel \"Azeri\"@fr, \"Азербаиджанскии язык\"@ru ;\n"
          + "  skos:broader thesaurus:concept2094 .\n"
          + "\n"
          + "thesaurus:domain3\n"
          + "  a isothes:ConceptGroup, <http://vocabularies.unesco.org/ontology#Domain>, skos:Collection ;\n"
          + "  skos:prefLabel \"Culture\"@en, \"Culture\"@fr, \"Культура\"@ru, \"Cultura\"@es ;\n"
          + "  skos:member thesaurus:mt3.35 .";

  String rdfStarFragment = "@prefix neoind: <neo4j://individuals#> .\n"
      + "@prefix neovoc: <neo4j://vocabulary#> .\n"
      + "@prefix rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#> .\n"
      + "\n"
      + "neoind:0 a neovoc:Movie;\n"
      + "  neovoc:released \"1999\"^^<http://www.w3.org/2001/XMLSchema#long>;\n"
      + "  neovoc:tagline \"Welcome to the Real World\";\n"
      + "  neovoc:title \"The Matrix\" .\n"
      + "\n"
      + "neoind:4 a neovoc:Person;\n"
      + "  neovoc:ACTED_IN neoind:0;\n"
      + "  neovoc:born \"1961\"^^<http://www.w3.org/2001/XMLSchema#long>;\n"
      + "  neovoc:name \"Laurence Fishburne\" .\n"
      + "\n"
      + "<<neoind:4 neovoc:ACTED_IN neoind:0>> neovoc:roles \"Morpheus\" .\n"
      + "\n"
      + "neoind:16 a neovoc:Person;\n"
      + "  neovoc:ACTED_IN neoind:0;\n"
      + "  neovoc:born \"1978\"^^<http://www.w3.org/2001/XMLSchema#long>;\n"
      + "  neovoc:name \"Emil Eifrem\" .\n"
      + "\n"
      + "<<neoind:16 neovoc:ACTED_IN neoind:0>> neovoc:roles \"Emil\" .";

  String rdfStarFragmentWithTriplesAsObjects = "@prefix neoind: <neo4j://individuals#> .\n"
          + "@prefix neovoc: <neo4j://vocabulary#> .\n"
          + "@prefix rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#> .\n"
          + "\n"
          + "neoind:0 a neovoc:Movie;\n"
          + "  neovoc:released \"1999\"^^<http://www.w3.org/2001/XMLSchema#long>;\n"
          + "  neovoc:tagline \"Welcome to the Real World\";\n"
          + "  neovoc:title \"The Matrix\" .\n"
          + "\n"
          + "<<neoind:16 neovoc:ACTED_IN neoind:0>> neovoc:roles \"Emil\" ."
          + "\n"
          + "neoind:4 a neovoc:Person;\n"
          + "  neovoc:ACTED_IN neoind:0;\n"
          + "  neovoc:born \"1961\"^^<http://www.w3.org/2001/XMLSchema#long>;\n"
          + "  neovoc:name \"Laurence Fishburne\" .\n"
          + "\n"
          + "neoind:4 neovoc:SAID <<neoind:16 neovoc:ACTED_IN neoind:0>> .\n"
          + "\n"
          + "neoind:16 a neovoc:Person;\n"
          + "  neovoc:ACTED_IN neoind:0;\n"
          + "  neovoc:born \"1978\"^^<http://www.w3.org/2001/XMLSchema#long>;\n"
          + "  neovoc:name \"Emil Eifrem\" . " ;


  String rdfTriGSnippet  = "@prefix ex: <http://www.example.org/vocabulary#> .\n"
      + "@prefix exDoc: <http://www.example.org/exampleDocument#> .\n"
      + "@prefix xsd: <http://www.w3.org/2001/XMLSchema#> .\n"
      + "\n"
      + "exDoc:G1 ex:created \"2019-06-06\"^^xsd:date .\n"
      + "exDoc:G2 ex:created \"2019-06-07T10:15:30\"^^xsd:dateTime .\n"
      + "\n"
      + "exDoc:Monica a ex:Person ;\n"
      + "             ex:friendOf exDoc:John .\n"
      + "\n"
      + "exDoc:G1 {\n"
      + "    exDoc:Monica\n"
      + "              ex:name \"Monica Murphy\" ;\n"
      + "              ex:homepage <http://www.monicamurphy.org> ;\n"
      + "              ex:email <mailto:monica@monicamurphy.org> ;\n"
      + "              ex:hasSkill ex:Management ,\n"
      + "                                  ex:Programming ;\n"
      + "              ex:knows exDoc:John . }\n"
      + "\n"
      + "exDoc:G2 {\n"
      + "    exDoc:Monica\n"
      + "              ex:city \"New York\" ;\n"
      + "              ex:country \"USA\" . }\n"
      + "\n"
      + "\n"
      + "exDoc:G3 {\n"
      + "    exDoc:John a ex:Person . }\n"
      + "\n";

  private static URI file(String path) {
    try {
      return RDFProceduresTest.class.getClassLoader().getResource(path).toURI();
    } catch (URISyntaxException e) {
      String msg = String.format("Failed to load the resource with path '%s'", path);
      throw new RuntimeException(msg, e);
    }
  }

  @Test
  public void testAbortIfNoIndices() throws Exception {
    try (Driver driver = GraphDatabase.driver(neo4j.boltURI(),
        Config.builder().withoutEncryption().build()); Session session = driver.session()) {

      Result importResults
          = session.run("CALL n10s.rdf.import.fetch('" +
          RDFProceduresTest.class.getClassLoader().getResource("mini-ld.json").toURI() +
          "','JSON-LD',{ handleVocabUris: 'SHORTEN', handleRDFTypes: 'LABELS', commitSize: 500})");

      Map<String, Object> singleResult = importResults
          .single().asMap();

      assertEquals(0L, singleResult.get("triplesLoaded"));
      assertEquals("KO", singleResult.get("terminationStatus"));
      assertEquals("The following constraint is required for importing RDF. Please "
              + "run 'CREATE CONSTRAINT n10s_unique_uri ON (r:Resource) ASSERT r.uri IS UNIQUE' and try again.",
          singleResult.get("extraInfo"));
    }
  }

  @Test
  public void testFullTextIndexesPresent() throws Exception {
    try (Driver driver = GraphDatabase.driver(neo4j.boltURI(),
        Config.builder().withoutEncryption().build()); Session session = driver.session()) {

      session.run("CALL db.index.fulltext.createNodeIndex(\"multiLabelIndex\","
          + "[\"Movie\", \"Book\"],[\"title\", \"description\"])");

      Result importResults
          = session.run("CALL n10s.rdf.import.fetch('file:///fileDoesnotExist.txt','JSON-LD',{})");

      Map<String, Object> singleResult = importResults
          .single().asMap();

      assertEquals(0L, singleResult.get("triplesLoaded"));
      assertEquals("KO", singleResult.get("terminationStatus"));
      assertEquals("The following constraint is required for importing RDF. "
              + "Please run 'CREATE CONSTRAINT n10s_unique_uri ON (r:Resource) ASSERT r.uri IS UNIQUE' and try again.",
          singleResult.get("extraInfo"));
    }
  }

  @Test
  public void testCompositeIndexesPresent() throws Exception {
    try (Driver driver = GraphDatabase.driver(neo4j.boltURI(),
        Config.builder().withoutEncryption().build()); Session session = driver.session()) {

      session.run("CREATE INDEX ON :Person(age, country)");

      Result importResults
          = session.run("CALL n10s.rdf.import.fetch('file:///fileDoesnotExist.txt','JSON-LD',{})");

      Map<String, Object> singleResult = importResults
          .single().asMap();

      assertEquals(0L, singleResult.get("triplesLoaded"));
      assertEquals("KO", singleResult.get("terminationStatus"));
      assertEquals("The following constraint is required for importing RDF. Please "
              + "run 'CREATE CONSTRAINT n10s_unique_uri ON (r:Resource) ASSERT r.uri IS UNIQUE' "
              + "and try again.",
          singleResult.get("extraInfo"));
    }
  }

  @Test
  public void testAbortIfNoIndicesImportSnippet() throws Exception {
    try (Driver driver = GraphDatabase.driver(neo4j.boltURI(),
        Config.builder().withoutEncryption().build())) {

      Session session = driver.session();

      Result importResults1 = session.run("CALL n10s.rdf.import.inline('" +
          turtleFragment +
          "','Turtle')");

      Map<String, Object> singleResult = importResults1.single().asMap();

      assertEquals(0L, singleResult.get("triplesLoaded"));
      assertEquals("KO", singleResult.get("terminationStatus"));
      assertEquals("The following constraint is required for importing RDF. Please run "
              + "'CREATE CONSTRAINT n10s_unique_uri ON (r:Resource) ASSERT r.uri IS UNIQUE' and try again.",
          singleResult.get("extraInfo"));
    }
  }

  @Test
  public void testImportJSONLD() throws Exception {
    try (Driver driver = GraphDatabase.driver(neo4j.boltURI(),
        Config.builder().withoutEncryption().build()); Session session = driver.session()) {

      initialiseGraphDB(neo4j.defaultDatabaseService(),
          "{ handleVocabUris: 'KEEP', handleRDFTypes: 'LABELS' }");

      Result importResults
          = session.run("CALL n10s.rdf.import.fetch('" +
          RDFProceduresTest.class.getClassLoader().getResource("mini-ld.json").toURI()
          + "','JSON-LD',"
          +
          "{ commitSize: 500, headerParams : { authorization: 'Basic bla bla bla', accept: 'rdf/xml' } })");

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
  public void testInvalidSerialisationFormat() throws Exception {
    try (Driver driver = GraphDatabase.driver(neo4j.boltURI(),
            Config.builder().withoutEncryption().build()); Session session = driver.session()) {

      initialiseGraphDB(neo4j.defaultDatabaseService(),
              "{ handleVocabUris: 'KEEP', handleRDFTypes: 'LABELS' }");

      try {
        Result importResults
              = session.run("CALL n10s.rdf.stream.fetch('" +
              RDFProceduresTest.class.getClassLoader().getResource("mini-ld.json").toURI()
              + "','Invalid-Format')");


        importResults.single();
        assertTrue(false);
      } catch (Exception e){
        //expected
        assertEquals("Failed to invoke procedure `n10s.rdf.stream.fetch`: Caused by: n10s.RDFImportException: Unrecognized serialization format: Invalid-Format",
                e.getMessage());
      }

      try {
        Result importResults
                = session.run("CALL n10s.rdf.preview.fetch('" +
                RDFProceduresTest.class.getClassLoader().getResource("mini-ld.json").toURI()
                + "','Invalid-Format')");


        importResults.single();
        assertTrue(false);
      } catch (Exception e){
        //expected
        assertEquals("Failed to invoke procedure `n10s.rdf.preview.fetch`: Caused by: n10s.RDFImportException: Unrecognized serialization format: Invalid-Format",
                e.getMessage());
      }


      try {
        Result importResults
                = session.run("CALL n10s.rdf.import.fetch('" +
                RDFProceduresTest.class.getClassLoader().getResource("mini-ld.json").toURI()
                + "','Invalid-Format')");


        Record single = importResults.single();
        assertEquals("KO",single.get("terminationStatus").asString());
        assertEquals(0L,single.get("triplesLoaded").asLong());
        assertEquals("Unrecognized serialization format: Invalid-Format",single.get("extraInfo").asString());
      } catch (Exception e){
        //no exceptions raised with import
        assertTrue(false);
      }

    }
  }

  @Test
  public void testImportZippedSingleFile() throws Exception {
    try (Driver driver = GraphDatabase.driver(neo4j.boltURI(),
            Config.builder().withoutEncryption().build()); Session session = driver.session()) {

      initialiseGraphDB(neo4j.defaultDatabaseService(),
              "{ handleVocabUris: 'KEEP', handleRDFTypes: 'LABELS' }");

      Result importResults
              = session.run("CALL n10s.rdf.import.fetch('" +
              RDFProceduresTest.class.getClassLoader().getResource("schema.rdf.gz").toURI()
              + "','RDF/XML',"
              +
              "{ commitSize: 500, headerParams : { authorization: 'Basic bla bla bla', accept: 'rdf/xml' } })");

      assertEquals(10774L, importResults
              .single().get("triplesLoaded").asLong());

      importResults
              = session.run("CALL n10s.rdf.import.fetch('" +
              RDFProceduresTest.class.getClassLoader().getResource("schema.rdf.tgz").toURI() + "!schema.rdf"
              + "','RDF/XML',"
              +
              "{ commitSize: 500, headerParams : { authorization: 'Basic bla bla bla', accept: 'rdf/xml' } })");

      assertEquals(10774L, importResults
              .single().get("triplesLoaded").asLong());


      importResults
              = session.run("CALL n10s.rdf.import.fetch('" +
              RDFProceduresTest.class.getClassLoader().getResource("schema.rdf.bz2").toURI()
              + "','RDF/XML',"
              +
              "{ commitSize: 500, headerParams : { authorization: 'Basic bla bla bla', accept: 'rdf/xml' } })");

      assertEquals(10774L, importResults
              .single().get("triplesLoaded").asLong());

      importResults
              = session.run("CALL n10s.rdf.import.fetch('" +
              RDFProceduresTest.class.getClassLoader().getResource("schema.rdf.zip").toURI() + "!schema.rdf"
              + "','RDF/XML',"
              +
              "{ commitSize: 500, headerParams : { authorization: 'Basic bla bla bla', accept: 'rdf/xml' } })");

      assertEquals(10774L, importResults
              .single().get("triplesLoaded").asLong());

      importResults
              = session.run("CALL n10s.rdf.import.fetch('" +
              RDFProceduresTest.class.getClassLoader().getResource("schema.rdf.zip").toURI()
              + "','RDF/XML',"
              +
              "{ commitSize: 500, headerParams : { authorization: 'Basic bla bla bla', accept: 'rdf/xml' } })");

      try{
        importResults.single();
        //should not get here
        assertTrue(false);
      } catch (Exception e){
        assertEquals("Failed to invoke procedure `n10s.rdf.import.fetch`: Caused by: java.lang.IllegalArgumentException: Filename is required for zip files (use '!' notation)", e.getMessage());
      }

      importResults
              = session.run("CALL n10s.rdf.import.fetch('" +
              RDFProceduresTest.class.getClassLoader().getResource("schema.rdf.bz2").toURI() + "!schema.rdf"
              + "','RDF/XML',"
              +
              "{ commitSize: 500, headerParams : { authorization: 'Basic bla bla bla', accept: 'rdf/xml' } })");

      try{
        importResults.single();
        //should not get here
        assertTrue(false);
      } catch (Exception e){
        assertEquals("Failed to invoke procedure `n10s.rdf.import.fetch`: Caused by: java.lang.IllegalArgumentException: '!' notation for filenames can only be used with zip or tgz files", e.getMessage());
      }
    }
  }

  @Test
  public void testImportZippedMultiFile() throws Exception {
    try (Driver driver = GraphDatabase.driver(neo4j.boltURI(),
            Config.builder().withoutEncryption().build()); Session session = driver.session()) {

      initialiseGraphDB(neo4j.defaultDatabaseService(),
              "{ handleVocabUris: 'KEEP', handleRDFTypes: 'LABELS' }");

      Result importResults
              = session.run("CALL n10s.rdf.import.fetch('" +
              RDFProceduresTest.class.getClassLoader().getResource("rdf.tar.gz").toURI() + "!rdf/moviesontology.owl"
              + "','RDF/XML',"
              +
              "{ commitSize: 500, headerParams : { authorization: 'Basic bla bla bla', accept: 'rdf/xml' } })");

      assertEquals(60L, importResults
              .single().get("triplesLoaded").asLong());


      importResults
              = session.run("CALL n10s.rdf.import.fetch('" +
              RDFProceduresTest.class.getClassLoader().getResource("rdf.zip").toURI() + "!rdf/moviesontology.owl"
              + "','RDF/XML',"
              +
              "{ commitSize: 500, headerParams : { authorization: 'Basic bla bla bla', accept: 'rdf/xml' } })");

      assertEquals(60L, importResults
              .single().get("triplesLoaded").asLong());


    }
  }


  @Test
  public void testImportSKOSInline() throws Exception {
    try (Driver driver = GraphDatabase.driver(neo4j.boltURI(),
        Config.builder().withoutEncryption().build()); Session session = driver.session()) {

      initialiseGraphDB(neo4j.defaultDatabaseService(),
          "{ handleVocabUris: 'IGNORE', handleMultival: 'ARRAY' }");

      Result importResults
          = session.run("CALL n10s.skos.import.inline('" +
          SKOS_FRAGMENT_TURTLE + "','Turtle')"); //,{   commitSize: 1000 }

      assertEquals(26L, importResults
          .single().get("triplesLoaded").asLong());
      Result queryResults = session.run(
          "MATCH (n:Resource  { uri: 'http://vocabularies.unesco.org/thesaurus/concept2096'}) RETURN [ x in labels(n) where x <> 'Resource' | x][0] AS label, n.prefLabel  as name limit 1");
      assertTrue(queryResults.hasNext());
      Record result = queryResults.next();
      assertEquals("Class", result.get("label").asString());
      assertEquals(Arrays.asList("Azerbaijani", "Azéri", "Азербайджанский язык", "Azerbaiyano"),
          result.get("name").asList());
      assertFalse(queryResults.hasNext());
    }
  }

  @Test
  public void testImportSKOSFetchWithParams() throws Exception {
    try (Driver driver = GraphDatabase.driver(neo4j.boltURI(),
            Config.builder().withoutEncryption().build()); Session session = driver.session()) {

      initialiseGraphDB(neo4j.defaultDatabaseService(),
              "{}");

      Result importResults
              = session.run("CALL n10s.skos.import.fetch('" +
              RDFProceduresTest.class.getClassLoader().getResource("unesco-thesaurus.ttl").toURI()
              + "','Turtle',{   commitSize: 100, languageFilter: 'fr' })"); //

      assertEquals(57185L, importResults
              .single().get("triplesLoaded").asLong());
      Result queryResults = session.run(
              "MATCH (n:n4sch__Class  { uri: 'http://vocabularies.unesco.org/thesaurus/concept10928'}) "
                      + "RETURN labels(n) AS labels, properties(n) as props limit 1");
      assertTrue(queryResults.hasNext());
      Record result = queryResults.next();
      List<Object> labels = result.get("labels").asList();
      assertTrue(labels.contains("Resource"));
      assertTrue(labels.contains("n4sch__Class"));
      assertEquals(2, labels.size());
      Map<String, Object> props = result.get("props").asMap();
      assertEquals("concept10928", props.get("n4sch__name"));
      assertEquals("Industrie alimentaire", props.get("skos__prefLabel"));
      assertFalse(queryResults.hasNext());
    }
  }

  @Test
  public void testImportRDFStar () throws Exception {
    try (Driver driver = GraphDatabase.driver(neo4j.boltURI(),
        Config.builder().withoutEncryption().build()); Session session = driver.session()) {

      initialiseGraphDB(neo4j.defaultDatabaseService(),
          "{ handleVocabUris: 'IGNORE' }");

      Result importResults
          = session.run("CALL n10s.rdf.import.fetch('" +
          RDFProceduresTest.class.getClassLoader().getResource("movies.ttls").toURI()
          + "','Turtle*')"); //,{   commitSize: 1000 }

      assertEquals(1372L, importResults
          .single().get("triplesLoaded").asLong());
      Result queryResults = session.run(
          "MATCH (ee:Person { name: 'Emil Eifrem'})-[ai:ACTED_IN]->(m) "
              + " RETURN ee.born as born, ai.roles as roles, m.title as title limit 1");
      assertTrue(queryResults.hasNext());
      Record result = queryResults.next();
      assertEquals(1978L, result.get("born").asLong());
      assertEquals("Emil", result.get("roles").asString());
      assertEquals("The Matrix", result.get("title").asString());
      assertFalse(queryResults.hasNext());
    }
  }

  @Test
  public void testImportRDFStarWithArrayMultiVal () throws Exception {
    try (Driver driver = GraphDatabase.driver(neo4j.boltURI(),
            Config.builder().withoutEncryption().build()); Session session = driver.session()) {

      initialiseGraphDB(neo4j.defaultDatabaseService(),
              "{ handleVocabUris: 'IGNORE' , " +
                      "handleMultival: 'ARRAY', multivalPropList: ['neo4j://vocabulary#roles']}");

      Result importResults
              = session.run("CALL n10s.rdf.import.fetch('" +
              RDFProceduresTest.class.getClassLoader().getResource("movies.ttls").toURI()
              + "','Turtle*')");

      assertEquals(1372L, importResults
              .single().get("triplesLoaded").asLong());
      Result queryResults = session.run(
              "MATCH (ee:Person { name: 'Bill Paxton'})-[ai:ACTED_IN]->(m) " +
                      " WHERE m.tagline = 'Houston, we have a problem.'"
                      + " RETURN ee.born as born, ai.roles as roles, m.title as title limit 1");
      assertTrue(queryResults.hasNext());
      Record result = queryResults.next();
      assertEquals(1955L, result.get("born").asLong());
      List<Object> theRoles = result.get("roles").asList();
      assertEquals(1,theRoles.size());
      assertEquals("Fred Haise", theRoles.get(0));
      assertEquals("Apollo 13", result.get("title").asString());
      assertFalse(queryResults.hasNext());
    }
  }

  @Test
  public void testImportSKOSFetchMultivalArray() throws Exception {
    try (Driver driver = GraphDatabase.driver(neo4j.boltURI(),
        Config.builder().withoutEncryption().build()); Session session = driver.session()) {

      initialiseGraphDB(neo4j.defaultDatabaseService(),
          "{ handleMultival: 'ARRAY', keepLangTag: true }");

      Result importResults
          = session.run("CALL n10s.skos.import.fetch('" +
          RDFProceduresTest.class.getClassLoader().getResource("unesco-thesaurus.ttl").toURI()
          + "','Turtle')"); //,{   commitSize: 1000 }

      assertEquals(57185, importResults
          .single().get("triplesLoaded").asLong());
      Result queryResults = session.run(
          "MATCH (n:Resource  { uri: 'http://vocabularies.unesco.org/thesaurus/concept10928'}) "
              + "RETURN [ x in labels(n) where x <> 'Resource' | x][0] AS label, n.n4sch__name  as name, " +
                  "n.skos__prefLabel  as labels limit 1");
      assertTrue(queryResults.hasNext());
      Record result = queryResults.next();
      assertEquals("n4sch__Class", result.get("label").asString());
      assertEquals("concept10928", result.get("name").asString());
      List<Object> labels = result.get("labels").asList();
      String values [] = new String[] {
              "Food industry@en", "Industrie alimentaire@fr", "Industria alimentaria@es", "Пищевая промышленность@ru" };
      for (String x:values) {
        assertTrue(labels.contains(x));
      }
      assertFalse(queryResults.hasNext());
    }
  }


  @Test
  public void testImportJSONLDImportSnippet() throws Exception {
    try (Driver driver = GraphDatabase.driver(neo4j.boltURI(),
        Config.builder().withoutEncryption().build())) {

      Session session = driver.session();

      initialiseGraphDB(neo4j.defaultDatabaseService(),
          " { handleVocabUris: 'KEEP', handleRDFTypes: 'LABELS'} ");

      Result importResults1 = session.run("CALL n10s.rdf.import.inline('" +
          jsonLdFragment + "','JSON-LD',"
          + "{ commitSize: 500, headerParams : { authorization: 'Basic bla bla bla', accept: 'rdf/xml' } })");
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
        Config.builder().withoutEncryption().build()); Session session = driver.session()) {

      initialiseGraphDB(neo4j.defaultDatabaseService(),
          "{ handleVocabUris: 'SHORTEN', handleRDFTypes: 'LABELS' }");

      Result importResults
          = session.run("CALL n10s.rdf.import.fetch('" +
          RDFProceduresTest.class.getClassLoader().getResource("mini-ld.json").toURI()
          + "','JSON-LD',"
          +
          "{ commitSize: 10 })");
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
          session.run("call n10s.nsprefixes.list() yield prefix, namespace "
              + "with prefix, namespace where namespace = 'http://xmlns.com/foaf/0.1/' "
              + "return prefix, namespace").next().get("prefix").asString());

      session.run("MATCH (n) DETACH DELETE n ;");
      //reset graph config
      session.run(
          "CALL n10s.graphconfig.init({ handleVocabUris: 'SHORTEN', handleRDFTypes: 'LABELS' });");

      importResults = session.run("CALL n10s.rdf.import.inline('" +
          jsonLdFragment + "','JSON-LD', { commitSize: 10 })");
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
              "call n10s.nsprefixes.list() yield prefix, namespace "
                  + " with prefix, namespace where namespace = 'http://xmlns.com/foaf/0.1/' "
                  + " return prefix ")
              .next().get("prefix").asString());
    }

  }

  @Test
  public void testImportJSONLDShorteningStrict() throws Exception {
    try (Driver driver = GraphDatabase.driver(neo4j.boltURI(),
        Config.builder().withoutEncryption().build()); Session session = driver.session()) {

      initialiseGraphDB(neo4j.defaultDatabaseService(),
          "{ handleVocabUris: 'SHORTEN_STRICT', handleRDFTypes: 'LABELS' }");
    }
    try (Driver driver = GraphDatabase.driver(neo4j.boltURI(),
        Config.builder().withoutEncryption().build()); Session session = driver.session()) {
      Result importResults
          = session.run("CALL n10s.rdf.import.fetch('" +
          RDFProceduresTest.class.getClassLoader().getResource("mini-ld.json").toURI()
          + "','JSON-LD',"
          +
          "{ commitSize: 10 })");
      Record next = importResults.next();

      assertTrue(false);

    } catch (Exception e) {
      assertEquals("Failed to invoke procedure `n10s.rdf.import.fetch`: Caused by: "
          + "n10s.utils.NamespaceWithUndefinedPrefix: No prefix has been defined for "
          + "namespace <http://xmlns.com/foaf/0.1/> and 'handleVocabUris' is set "
          + "to 'SHORTEN_STRICT'", e.getMessage());
    }
    try (Driver driver = GraphDatabase.driver(neo4j.boltURI(),
        Config.builder().withoutEncryption().build()); Session session = driver.session()) {
      assertEquals("one",
          session.run("call n10s.nsprefixes.add('one','http://xmlns.com/foaf/0.1/')").next()
              .get("prefix").asString());

      Result importResults
          = session.run("CALL n10s.rdf.import.fetch('" +
          RDFProceduresTest.class.getClassLoader().getResource("mini-ld.json").toURI()
          + "','JSON-LD',"
          +
          "{ commitSize: 10 })");
      assertEquals(6L, importResults.next().get("triplesLoaded").asLong());
      assertEquals("http://me.markus-lanthaler.com/",
          session.run(
              "MATCH (n{ one" + PREFIX_SEPARATOR
                  + "name : 'Markus Lanthaler'}) RETURN n.uri AS uri")
              .next().get("uri").asString());

      assertEquals(1L,
          session.run("MATCH (n) WHERE exists(n.one" + PREFIX_SEPARATOR
              + "modified) RETURN count(n) AS count")
              .next().get("count").asLong());
    }

  }

  @Test
  public void testImportRDFXML() throws Exception {
    try (Driver driver = GraphDatabase.driver(neo4j.boltURI(),
        Config.builder().withoutEncryption().build()); Session session = driver.session()) {

      initialiseGraphDB(neo4j.defaultDatabaseService(),
          "{ handleVocabUris: 'KEEP', handleRDFTypes: 'LABELS' }");

      Result importResults
          = session.run("CALL n10s.rdf.import.fetch('" +
          RDFProceduresTest.class.getClassLoader()
              .getResource("jeu-de-donnees-des-jeux-de-donnees-open-data-paris.rdf")
              .toURI()
          + "','RDF/XML', { commitSize: 500 } )");
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
        Config.builder().withoutEncryption().build()); Session session = driver.session()) {

      initialiseGraphDB(neo4j.defaultDatabaseService(), null);

      Result importResults
          = session.run("CALL n10s.rdf.import.fetch('" +
          RDFProceduresTest.class.getClassLoader()
              .getResource("jeu-de-donnees-des-jeux-de-donnees-open-data-paris.rdf")
              .toURI()
          + "','RDF/XML',{ commitSize: 500})");
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
          session.run("call n10s.nsprefixes.list() yield prefix, namespace\n"
              + "with prefix, namespace where namespace = 'http://www.w3.org/ns/dcat#'\n"
              + "return prefix, namespace").next().get("prefix").asString());

    }
  }

  @Test
  public void testImportRDFXMLShorteningWithPrefixPreDefinition() throws Exception {
    try (Driver driver = GraphDatabase.driver(neo4j.boltURI(),
        Config.builder().withoutEncryption().build()); Session session = driver.session()) {

      initialiseGraphDB(neo4j.defaultDatabaseService(), null);
      session.run("call n10s.nsprefixes.add('dct','http://purl.org/dc/terms/')");
      session.run("call n10s.nsprefixes.add('rdf','http://www.w3.org/1999/02/22-rdf-syntax-ns#')");
      session.run("call n10s.nsprefixes.add('owl','http://www.w3.org/2002/07/owl#')");
      session.run("call n10s.nsprefixes.add('dcat','http://www.w3.org/ns/dcat#')");
      session.run("call n10s.nsprefixes.add('rdfs','http://www.w3.org/2000/01/rdf-schema#')");
      session.run("call n10s.nsprefixes.add('foaf','http://xmlns.com/foaf/0.1/')");

      Result importResults
          = session.run("CALL n10s.rdf.import.fetch('" +
          RDFProceduresTest.class.getClassLoader()
              .getResource("jeu-de-donnees-des-jeux-de-donnees-open-data-paris.rdf")
              .toURI()
          + "','RDF/XML', { handleVocabUris: 'SHORTEN', handleRDFTypes: 'LABELS', commitSize: 500})");
      assertEquals(38L, importResults
          .next().get("triplesLoaded").asLong());
      assertEquals(7L,
          session
              .run("MATCH ()-[r:dct" + PREFIX_SEPARATOR + "relation]->(b) RETURN count(b) as count")
              .next().get("count").asLong());

      assertEquals(
          "http://opendata.paris.fr/opendata/jsp/site/Portal.jsp?document_id=109&portlet_id=106",
          session
              .run("MATCH (x) WHERE x.rdfs" + PREFIX_SEPARATOR + "label = 'harvest_dataset_url'" +
                  "\nRETURN x.rdf" + PREFIX_SEPARATOR + "value AS datasetUrl").next()
              .get("datasetUrl").asString());

      assertEquals("dcat",
          session.run("call n10s.nsprefixes.list() yield prefix, namespace\n"
              + " with prefix, namespace where namespace = 'http://www.w3.org/ns/dcat#' \n"
              + " return prefix, namespace")
              .next().get("prefix").asString());

    }
  }

  @Test
  public void testImportRDFXMLShorteningWithPrefixPreDefinitionOneTriple() throws Exception {
    try (Driver driver = GraphDatabase.driver(neo4j.boltURI(),
        Config.builder().withoutEncryption().build()); Session session = driver.session()) {

      initialiseGraphDB(neo4j.defaultDatabaseService(), null);
      session.run("call n10s.nsprefixes.add('voc','http://neo4j.com/voc/')");

      Result importResults = session.run("CALL n10s.rdf.import.fetch('" +
          RDFProceduresTest.class.getClassLoader().getResource("oneTriple.rdf")
              .toURI()
          + "','RDF/XML',{ handleVocabUris: 'SHORTEN', handleRDFTypes: 'LABELS', commitSize: 500})");
      assertEquals(1L, importResults.next().get("triplesLoaded").asLong());
      assertEquals("JB",
          session.run(
              "MATCH (jb {uri: 'http://neo4j.com/invividual/JB'}) RETURN jb.voc" + PREFIX_SEPARATOR
                  + "name AS name")
              .next().get("name").asString());

      assertEquals("voc",
          session.run("call n10s.nsprefixes.list() yield prefix, namespace "
              + " with prefix where namespace = 'http://neo4j.com/voc/' "
              + " return  prefix ")
              .next().get("prefix").asString());
    }
  }

  @Test
  public void testImportBadUrisTtl() throws Exception {
    try (Driver driver = GraphDatabase.driver(neo4j.boltURI(),
        Config.builder().withoutEncryption().build()); Session session = driver.session()) {

      initialiseGraphDB(neo4j.defaultDatabaseService(),
          "{ handleVocabUris: 'SHORTEN', handleRDFTypes: 'LABELS' }");

      session.run("call n10s.nsprefixes.add('pr','http://example.org/vocab/show/')");

      Result importResults
          = session.run("CALL n10s.rdf.import.fetch('" +
          RDFProceduresTest.class.getClassLoader().getResource("badUri.ttl")
              .toURI()
          + "','Turtle',{ commitSize: 500, verifyUriSyntax: false})");
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
        Config.builder().withoutEncryption().build()); Session session = driver.session()) {

      initialiseGraphDB(neo4j.defaultDatabaseService(), null);

      session.run("WITH {`http://example.org/vocab/show/`:'pr' } as nslist\n" +
          "MERGE (n:_NsPrefDef)\n" +
          "SET n+=nslist " +
          "RETURN n ");

      Result importResults
          = session.run("CALL n10s.rdf.import.fetch('" +
          RDFProceduresTest.class.getClassLoader().getResource("badUri.ttl")
              .toURI()
          + "','Turtle',{ handleVocabUris: 'SHORTEN', handleRDFTypes: 'LABELS', commitSize: 500})");
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
        Config.builder().withoutEncryption().build()); Session session = driver.session()) {

      initialiseGraphDB(neo4j.defaultDatabaseService(), null);
      session.run("call n10s.nsprefixes.add('voc','http://neo4j.com/voc/')");

      Result importResults
          = session.run("CALL n10s.rdf.import.fetch('" +
          RDFProceduresTest.class.getClassLoader().getResource("badUris.rdf")
              .toURI()
          + "','RDF/XML',{ handleVocabUris: 'SHORTEN', handleRDFTypes: 'LABELS', commitSize: 500})");
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
        Config.builder().withoutEncryption().build()); Session session = driver.session()) {

      initialiseGraphDB(neo4j.defaultDatabaseService(), null);

      session.run("call n10s.nsprefixes.add('voc','http://example.org/vocab/show/')");

      Result importResults
          = session.run("CALL n10s.rdf.import.fetch('" +
          RDFProceduresTest.class.getClassLoader().getResource("multilang.ttl")
              .toURI()
          + "','Turtle',{ languageFilter: 'en', commitSize: 500})");
      assertEquals(1L, importResults
          .next().get("triplesLoaded").asLong());
      assertEquals("That Seventies Show",
          session.run(
              "MATCH (t {uri: 'http://example.org/vocab/show/218'}) RETURN t.voc" + PREFIX_SEPARATOR
                  + "localName AS name")
              .next().get("name").asString());

      session.run("MATCH (t {uri: 'http://example.org/vocab/show/218'}) DETACH DELETE t ");

      importResults
          = session.run("CALL n10s.rdf.import.fetch('" +
          RDFProceduresTest.class.getClassLoader().getResource("multilang.ttl")
              .toURI()
          + "','Turtle',{ languageFilter: 'fr', commitSize: 500})");
      assertEquals(1L, importResults
          .next().get("triplesLoaded").asLong());
      assertEquals("Cette Série des Années Soixante-dix",
          session.run(
              "MATCH (t {uri: 'http://example.org/vocab/show/218'}) RETURN t.voc" + PREFIX_SEPARATOR
                  + "localName AS name")
              .next().get("name").asString());

      session.run("MATCH (t {uri: 'http://example.org/vocab/show/218'}) DETACH DELETE t ");

      importResults
          = session.run("CALL n10s.rdf.import.fetch('" +
          RDFProceduresTest.class.getClassLoader().getResource("multilang.ttl")
              .toURI()
          + "','Turtle',{ handleVocabUris: 'SHORTEN', handleRDFTypes: 'LABELS', languageFilter: 'fr-be', commitSize: 500})");
      assertEquals(1L, importResults
          .next().get("triplesLoaded").asLong());
      assertEquals("Cette Série des Années Septante",
          session.run(
              "MATCH (t {uri: 'http://example.org/vocab/show/218'}) RETURN t.voc" + PREFIX_SEPARATOR
                  + "localName AS name")
              .next().get("name").asString());

      session.run("MATCH (t {uri: 'http://example.org/vocab/show/218'}) DETACH DELETE t ");

      importResults
          = session.run("CALL n10s.rdf.import.fetch('" +
          RDFProceduresTest.class.getClassLoader().getResource("multilang.ttl")
              .toURI()
          + "','Turtle',{ handleVocabUris: 'SHORTEN', handleRDFTypes: 'LABELS', commitSize: 500})");
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
        Config.builder().withoutEncryption().build()); Session session = driver.session()) {

      initialiseGraphDB(neo4j.defaultDatabaseService(),
          "{ keepLangTag : true, handleMultival: 'ARRAY'}");
      String importCypher = "CALL n10s.rdf.import.fetch('" +
          RDFProceduresTest.class.getClassLoader().getResource("multilang.ttl")
              .toURI() + "','Turtle')";
      Result importResults
          = session.run(importCypher);
      Record next = importResults
          .next();
      assertEquals(3, next.get("triplesLoaded").asInt());

      importResults
          = session.run(
          "match (n:Resource) return n.ns0__localName as all, n10s.rdf.getLangValue('en',n.ns0__localName) as en_name, "
              +
              "n10s.rdf.getLangValue('fr',n.ns0__localName) as fr_name, n10s.rdf.getLangValue('fr-be',n.ns0__localName) as frbe_name");
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
        Config.builder().withoutEncryption().build()); Session session = driver.session()) {

      initialiseGraphDB(neo4j.defaultDatabaseService(),
          "{ handleMultival: 'ARRAY', " +
              "multivalPropList : ['http://example.org/vocab/show/availableInLang','http://example.org/vocab/show/localName'] }");
      String importCypher = "CALL n10s.rdf.import.fetch('" +
          RDFProceduresTest.class.getClassLoader().getResource("multival.ttl")
              .toURI()
          + "','Turtle')";
      Result importResults
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
      assertTrue(next.get("all").asList().containsAll(localNames));
      List<String> availableInLang = new ArrayList<>();
      availableInLang.add("EN");
      availableInLang.add("FR");
      availableInLang.add("ES");
      assertTrue(next.get("ail").asList().containsAll(availableInLang));
      assertEquals(218, next.get("sid").asLong());
      assertEquals("Joanna Smith", next.get("prod").asString());
    }
  }

  @Test
  public void testImportMultivalWithExclusionList() throws Exception {
    try (Driver driver = GraphDatabase.driver(neo4j.boltURI(),
        Config.builder().withoutEncryption().build()); Session session = driver.session()) {

      initialiseGraphDB(neo4j.defaultDatabaseService(),
          "{ handleMultival: 'ARRAY' }");
      String importCypher = "CALL n10s.rdf.import.fetch('" +
          RDFProceduresTest.class.getClassLoader().getResource("multival.ttl")
              .toURI()
          + "','Turtle',  { predicateExclusionList : ['http://example.org/vocab/show/availableInLang','http://example.org/vocab/show/localName'] })";
      Result importResults
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
        Config.builder().withoutEncryption().build()); Session session = driver.session()) {

      initialiseGraphDB(neo4j.defaultDatabaseService(),
          "{ handleVocabUris: 'KEEP', handleRDFTypes: 'LABELS' }");

      Result importResults
          = session.run("CALL n10s.rdf.import.fetch('" +
          RDFProceduresTest.class.getClassLoader().getResource("opentox-example.ttl")
              .toURI()
          + "','Turtle',{ commitSize: 500 })");
      assertEquals(157L, importResults
          .next().get("triplesLoaded").asLong());
      Result algoNames = session
          .run("MATCH (n:`http://www.opentox.org/api/1.1#Algorithm`) " +
              "\nRETURN n.`http://purl.org/dc/elements/1.1/title` AS algos ORDER By algos");

      assertEquals("J48", algoNames.next().get("algos").asString());
      assertEquals("XLogP", algoNames.next().get("algos").asString());

      Result compounds = session.run(
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
        Config.builder().withoutEncryption().build()); Session session = driver.session()) {

      initialiseGraphDB(neo4j.defaultDatabaseService(), null);
      session.run("call n10s.nsprefixes.add('ex','http://www.example.com/ontology/1.0.0#')");
      session.run("call n10s.nsprefixes.add('rdf','http://www.w3.org/1999/02/22-rdf-syntax-ns#')");

      Result importResults = session.run(String.format(
          "CALL n10s.rdf.import.fetch('%s','Turtle',{nodeCacheSize: 1})",
          file("myrdf/testImportTurtle02.ttl")));
      assertEquals(5, importResults.next().get("triplesLoaded").asInt());

      Result result = session.run(
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
        Config.builder().withoutEncryption().build()); Session session = driver.session()) {

      initialiseGraphDB(neo4j.defaultDatabaseService(),
          "{handleVocabUris: 'KEEP', handleRDFTypes: 'NODES' }");

      Result importResults
          = session
          .run("CALL n10s.rdf.preview.inline('" + wrongUriTtl
              + "','Turtle',{ verifyUriSyntax: false})");
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
        Config.builder().withoutEncryption().build()); Session session = driver.session()) {

      initialiseGraphDB(neo4j.defaultDatabaseService(),
          "{ handleVocabUris: 'KEEP', handleRDFTypes: 'NODES'}");

      try {
        Result importResults
            = session
            .run("CALL n10s.rdf.preview.inline('" + wrongUriTtl
                + "','Turtle')");

        importResults.next();
        //we should not get here
        assertTrue(false);
      } catch (Exception e) {
        //expected
        assertTrue(e.getMessage().contains("Illegal percent encoding U+25"));
      }
    }
  }

  @Test
  public void testPreviewFromSnippet() throws Exception {
    try (Driver driver = GraphDatabase.driver(neo4j.boltURI(),
        Config.builder().withoutEncryption().build()); Session session = driver.session()) {

      initialiseGraphDB(neo4j.defaultDatabaseService(),
          "{ handleVocabUris: 'KEEP', handleRDFTypes: 'NODES'}");
      Result importResults
          = session
          .run("CALL n10s.rdf.preview.inline('" + jsonLdFragment
              + "','JSON-LD')");
      Map<String, Object> next = importResults
          .next().asMap();
      final List<Node> nodes = (List<Node>) next.get("nodes");
      assertEquals(3, nodes.size());
      final List<Relationship> rels = (List<Relationship>) next.get("relationships");
      assertEquals(2, rels.size());
    }
  }

  @Test
  public void testPreviewFromSnippetLimit() throws Exception {
    try (Driver driver = GraphDatabase.driver(neo4j.boltURI(),
        Config.builder().withoutEncryption().build()); Session session = driver.session()) {

      initialiseGraphDB(neo4j.defaultDatabaseService(),
          "{ handleVocabUris: 'KEEP', handleRDFTypes: 'NODES'}");

      Map<String, Object> params = new HashMap<>();
      params.put("rdf", this.turtleOntology);

      Result importResults
          = session
          .run("CALL n10s.rdf.preview.inline($rdf,'Turtle')", params);
      Map<String, Object> next = importResults
          .next().asMap();
      List<Node> nodes = (List<Node>) next.get("nodes");
      assertEquals(18, nodes.size());
      List<Relationship> rels = (List<Relationship>) next.get("relationships");
      assertEquals(31, rels.size());

      //now  limiting it to 5 triples
      importResults
          = session
          .run("CALL n10s.rdf.preview.inline($rdf,'Turtle',  { limit: 5 })", params);
      next = importResults
          .next().asMap();
      nodes = (List<Node>) next.get("nodes");
      assertEquals(4, nodes.size());
      rels = (List<Relationship>) next.get("relationships");
      assertEquals(2, rels.size());
    }
  }

  @Test
  public void testPreviewFromFileLimit() throws Exception {
    try (Driver driver = GraphDatabase.driver(neo4j.boltURI(),
        Config.builder().withoutEncryption().build()); Session session = driver.session()) {

      initialiseGraphDB(neo4j.defaultDatabaseService(),
          "{ handleVocabUris: 'KEEP', handleRDFTypes: 'NODES'}");

      Map<String, Object> params = new HashMap<>();
      params.put("rdf", this.turtleOntology);

      Result importResults
          = session
          .run("CALL n10s.rdf.preview.fetch('" + RDFProceduresTest.class.getClassLoader()
              .getResource("moviesontology.owl")
              .toURI() + "','RDF/XML')", params);
      Map<String, Object> next = importResults
          .next().asMap();
      List<Node> nodes = (List<Node>) next.get("nodes");
      assertEquals(18, nodes.size());
      List<Relationship> rels = (List<Relationship>) next.get("relationships");
      assertEquals(31, rels.size());

      //now  limiting it to 5 triples
      importResults
          = session
          .run("CALL n10s.rdf.preview.fetch(' " + RDFProceduresTest.class.getClassLoader()
              .getResource("moviesontology.owl")
              .toURI() + "','RDF/XML',  { limit: 5 })", params);
      next = importResults
          .next().asMap();
      nodes = (List<Node>) next.get("nodes");
      assertEquals(4, nodes.size());
      rels = (List<Relationship>) next.get("relationships");
      assertEquals(2, rels.size());
    }
  }

  @Test
  public void testPreviewRDFStarFromSnippet() throws Exception {
    try (Driver driver = GraphDatabase.driver(neo4j.boltURI(),
        Config.builder().withoutEncryption().build()); Session session = driver.session()) {

      initialiseGraphDB(neo4j.defaultDatabaseService(),
          "{handleVocabUris: 'IGNORE'}");
      Result importResults
          = session
          .run("CALL n10s.rdf.preview.inline('" + rdfStarFragment
              + "','Turtle*')");
      Map<String, Object> next = importResults
          .next().asMap();
      final List<Node> nodes = (List<Node>) next.get("nodes");
      assertEquals(3, nodes.size());
      final List<InternalRelationship> rels = (List<InternalRelationship>) next.get("relationships");
      assertEquals(2, rels.size());
      rels.forEach(r ->  assertTrue(((InternalRelationship)r).hasType("ACTED_IN") &&
          (((InternalRelationship)r).asMap().get("roles").equals("Emil") ||
              ((InternalRelationship)r).asMap().get("roles").equals("Morpheus"))));
    }
  }

  @Test
  public void testRDFStarWithTriplesAsOobjectFromSnippet() throws Exception {
    try (Driver driver = GraphDatabase.driver(neo4j.boltURI(),
            Config.builder().withoutEncryption().build()); Session session = driver.session()) {

      initialiseGraphDB(neo4j.defaultDatabaseService(),
              "{handleVocabUris: 'IGNORE'}");
      Result importResults
              = session
              .run("CALL n10s.rdf.import.inline('" + rdfStarFragmentWithTriplesAsObjects
                      + "','Turtle*')");
      Map<String, Object> next = importResults
              .next().asMap();
      ;
      assertEquals(13L, next.get("triplesLoaded"));
      assertEquals(14L, next.get("triplesParsed"));
      assertFalse(session.run("MATCH (a)-[:SAID]->(b) RETURN a, b").hasNext());

    }
  }


  @Test
  public void testOntoPreviewFromSnippet() throws Exception {
    try (Driver driver = GraphDatabase.driver(neo4j.boltURI(),
        Config.builder().withoutEncryption().build()); Session session = driver.session()) {

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
  }

  @Test
  public void testOntoPreviewFromSnippetLimit() throws Exception {
    try (Driver driver = GraphDatabase.driver(neo4j.boltURI(),
        Config.builder().withoutEncryption().build()); Session session = driver.session()) {

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
  }

  @Test
  public void testOntoPreviewFromFileLimit() throws Exception {
    try (Driver driver = GraphDatabase.driver(neo4j.boltURI(),
        Config.builder().withoutEncryption().build()); Session session = driver.session()) {

      initialiseGraphDB(neo4j.defaultDatabaseService(),
          "{}");

      Result importResults
          = session
          .run("CALL n10s.onto.preview.fetch('" + RDFProceduresTest.class.getClassLoader()
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
          .run("CALL n10s.onto.preview.fetch(' " + RDFProceduresTest.class.getClassLoader()
              .getResource("moviesontology.owl")
              .toURI() + "','RDF/XML',  { limit: 6 })");
      next = importResults
          .next().asMap();
      nodes = (List<Node>) next.get("nodes");
      assertEquals(2, nodes.size());
      rels = (List<Relationship>) next.get("relationships");
      assertEquals(0, rels.size());
    }
  }


  @Test
  public void testLoadFromSnippetCreateNodes() throws Exception {
    try (Driver driver = GraphDatabase.driver(neo4j.boltURI(),
        Config.builder().withoutEncryption().build()); Session session = driver.session()) {

      initialiseGraphDB(neo4j.defaultDatabaseService(),
          "{ handleVocabUris: 'KEEP', handleRDFTypes: 'NODES'}");
      Result importResults
          = session
          .run("CALL n10s.rdf.import.inline('" + turtleFragmentTypes
              + "','Turtle')");
      Map<String, Object> next = importResults
          .next().asMap();
      assertEquals(2L, next.get("triplesLoaded"));
      Record results = session
          .run(
              "MATCH (x:Resource)-[:`http://www.w3.org/1999/02/22-rdf-syntax-ns#type`]->(t:Resource) RETURN x, t, "
                  + "[x in labels(x) where x<>'Resource' | x ][0] as xlabel").next();
      assertEquals("http://example.org/vocab/show/218",
          results.get("x").asNode().get("uri").asString());
      assertEquals("http://example.org/vocab/show/Show",
          results.get("t").asNode().get("uri").asString());
      assertTrue(results.get("xlabel").isNull());
    }
  }

  @Test
  public void testLoadFromSnippetCreateNodesAndLabels() throws Exception {
    try (Driver driver = GraphDatabase.driver(neo4j.boltURI(),
        Config.builder().withoutEncryption().build()); Session session = driver.session()) {

      initialiseGraphDB(neo4j.defaultDatabaseService(),
          "{ handleVocabUris: 'SHORTEN', handleRDFTypes: 'LABELS_AND_NODES'}");
      Result importResults
          = session
          .run("CALL n10s.rdf.import.inline('" + turtleFragmentTypes
              + "','Turtle')");
      Map<String, Object> next = importResults
          .next().asMap();
      assertEquals(2L, next.get("triplesLoaded"));
      Record results = session
          .run("MATCH (x:Resource)-[:rdf__type]->(t:Resource) RETURN x, t, "
              + "n10s.rdf.fullUriFromShortForm([x in labels(x) where x<>'Resource' | x ][0]) as xlabelAsUri")
          .next();
      assertEquals("http://example.org/vocab/show/218",
          results.get("x").asNode().get("uri").asString());
      assertEquals("http://example.org/vocab/show/Show",
          results.get("t").asNode().get("uri").asString());
      assertEquals("http://example.org/vocab/show/Show", results.get("xlabelAsUri").asString());
    }
  }

  @Test
  public void testPreviewFromSnippetLangFilter() throws Exception {
    try (Driver driver = GraphDatabase.driver(neo4j.boltURI(),
        Config.builder().withoutEncryption().build()); Session session = driver.session()) {

      initialiseGraphDB(neo4j.defaultDatabaseService(),
          "{ handleVocabUris: 'KEEP', handleRDFTypes: 'NODES' }");

      String turtleFragment = "@prefix show: <http://example.org/vocab/show/> .\n" +
          "\n" +
          "show:218 show:localName \"That Seventies Show\"@en .                 # literal with a language tag\n"
          +
          "show:218 show:localName \"Cette Série des Années Soixante-dix\"@fr . \n" +
          "show:218 show:localName \"Cette Série des Années Septante\"@fr-be .  # literal with a region subtag";
      Result importResults
          = session
          .run("CALL n10s.rdf.preview.inline('" + turtleFragment
              + "','Turtle',{ languageFilter: 'fr'})");
      Record next = importResults
          .next();
      assertEquals(1, next.get("nodes").size());
      assertEquals("Cette Série des Années Soixante-dix",
          next.get("nodes").asList(ofNode()).get(0).get("http://example.org/vocab/show/localName")
              .asString());
      assertEquals(0, next.get("relationships").size());

      importResults
          = session.run("CALL n10s.rdf.preview.inline('" + turtleFragment
          + "','Turtle',{ languageFilter: 'en'})");
      assertEquals("That Seventies Show", importResults
          .next().get("nodes").asList(ofNode()).get(0)
          .get("http://example.org/vocab/show/localName").asString());

    }
  }

  @Test
  public void testPreviewFromFile() throws Exception {
    try (Driver driver = GraphDatabase.driver(neo4j.boltURI(),
        Config.builder().withoutEncryption().build()); Session session = driver.session()) {

      initialiseGraphDB(neo4j.defaultDatabaseService(),
          "{ handleVocabUris: 'KEEP', handleRDFTypes: 'NODES' }");

      Result importResults
          = session.run("CALL n10s.rdf.preview.fetch('" +
          RDFProceduresTest.class.getClassLoader()
              .getResource("jeu-de-donnees-des-jeux-de-donnees-open-data-paris.rdf")
              .toURI() + "','RDF/XML')");
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
        Config.builder().withoutEncryption().build()); Session session = driver.session()) {

      initialiseGraphDB(neo4j.defaultDatabaseService(), null);

      Result importResults
          = session.run("CALL n10s.rdf.preview.fetch('" +
          RDFProceduresTest.class.getClassLoader()
              .getResource("badUri.ttl")
              .toURI()
          + "','Turtle',{ handleVocabUris: 'KEEP', handleRDFTypes: 'NODES', verifyUriSyntax: false})");
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
        Config.builder().withoutEncryption().build()); Session session = driver.session()) {

      initialiseGraphDB(neo4j.defaultDatabaseService(),
          "{ handleVocabUris: 'KEEP', handleRDFTypes: 'NODES'}");

      try {
        Result importResults
            = session.run("CALL n10s.rdf.preview.fetch('" +
            RDFProceduresTest.class.getClassLoader()
                .getResource("badUri.ttl")
                .toURI() + "','Turtle')");

        importResults.next();
        //we should not get here
        assertTrue(false);
      } catch (Exception e) {
        //expected
        assertTrue(e.getMessage().contains("Illegal percent encoding U+25"));
      }
    }
  }

  @Test
  public void testPreviewFromFileLangFilter() throws Exception {
    try (Driver driver = GraphDatabase.driver(neo4j.boltURI(),
        Config.builder().withoutEncryption().build()); Session session = driver.session()) {

      initialiseGraphDB(neo4j.defaultDatabaseService(),
          "{ handleVocabUris: 'KEEP', handleRDFTypes: 'NODES', keepLangTag : false }");

      Result importResults
          = session.run("CALL n10s.rdf.preview.fetch('" +
          RDFProceduresTest.class.getClassLoader().getResource("multilang.ttl")
              .toURI()
          + "','Turtle', { languageFilter: 'fr' })");
      Record next = importResults
          .next();

      assertEquals(1, next.get("nodes").size());
      assertEquals("Cette Série des Années Soixante-dix",
          next.get("nodes").asList(ofNode()).get(0).get("http://example.org/vocab/show/localName")
              .asString());
      assertEquals(0, (next.get("relationships")).size());

      importResults
          = session.run("CALL n10s.rdf.preview.fetch('" +
          RDFProceduresTest.class.getClassLoader().getResource("multilang.ttl").toURI()
          + "','Turtle', { languageFilter: 'en' })");
      assertEquals("That Seventies Show", importResults
          .next().get("nodes").asList(ofNode()).get(0)
          .get("http://example.org/vocab/show/localName").asString());
    }
  }

  @Test
  public void testImportFromFileWithMapping() throws Exception {
    try (Driver driver = GraphDatabase.driver(neo4j.boltURI(),
        Config.builder().withoutEncryption().build()); Session session = driver.session()) {
      session.run("call n10s.nsprefixes.add('voc','http://neo4j.com/voc/')");
      session.run("call n10s.nsprefixes.add('cats','http://neo4j.com/category/')");
    }

    try (Driver driver = GraphDatabase.driver(neo4j.boltURI(),
        Config.builder().withoutEncryption().build()); Session session = driver.session()) {

      initialiseGraphDB(neo4j.defaultDatabaseService(), "{ handleVocabUris: 'MAP'}");

      session.run(" call n10s.mapping.add('http://neo4j.com/voc/name','uniqueName') ");
      session.run(" call n10s.mapping.add('http://neo4j.com/category/Publication','Media') ");

      Result importResults
          = session.run("CALL n10s.rdf.import.fetch('" +
          RDFProceduresTest.class.getClassLoader().getResource("myrdf/three.rdf")
              .toURI() + "','RDF/XML')");
      assertEquals(6L, importResults
          .next().get("triplesLoaded").asLong());
      Result mediaNames = session.run("MATCH (m:Media) " +
          "\nRETURN m.uniqueName AS nm, m.uri AS uri");

      Record next = mediaNames.next();
      assertEquals("The Financial Times", next.get("nm").asString());
      assertEquals("http://neo4j.com/invividual/FT", next.get("uri").asString());

      Result personNames = session.run("MATCH (m { PersonName : 'JC'}) " +
          "\nRETURN m.LivesIn AS li, m.uri AS uri");

      next = personNames.next();
      assertEquals("Chesham", next.get("li").asString());
      assertEquals("http://neo4j.com/invividual/JC", next.get("uri").asString());
    }
  }

  @Test
  public void testImportFromFileIgnoreNs() throws Exception {
    try (Driver driver = GraphDatabase.driver(neo4j.boltURI(),
        Config.builder().withoutEncryption().build()); Session session = driver.session()) {

      initialiseGraphDB(neo4j.defaultDatabaseService(), "{ handleVocabUris: 'IGNORE'}");

      Result importResults
          = session.run("CALL n10s.rdf.import.fetch('" +
          RDFProceduresTest.class.getClassLoader().getResource("myrdf/three.rdf")
              .toURI() + "','RDF/XML')");
      assertEquals(6L, importResults
          .next().get("triplesLoaded").asLong());
      Result mediaNames = session.run("MATCH (m:Publication) " +
          "\nRETURN m.name AS nm, m.uri AS uri");

      Record next = mediaNames.next();
      assertEquals("The Financial Times", next.get("nm").asString());
      assertEquals("http://neo4j.com/invividual/FT", next.get("uri").asString());

      Result rels = session.run(
          "MATCH ({ PersonName: 'JC'})-[r:reads]-(:Publication { name: 'The Financial Times'}) " +
              "\nRETURN count(r) as ct");

      next = rels.next();
      assertEquals(1L, next.get("ct").asLong());

    }
  }

  @Test
  public void testImportFromFileIgnoreNsApplyNeoNaming() throws Exception {
    try (Driver driver = GraphDatabase.driver(neo4j.boltURI(),
        Config.builder().withoutEncryption().build()); Session session = driver.session()) {

      initialiseGraphDB(neo4j.defaultDatabaseService(),
          "{ handleVocabUris: 'IGNORE', applyNeo4jNaming: true }");

      Result importResults
          = session.run("CALL n10s.rdf.import.fetch('" +
          RDFProceduresTest.class.getClassLoader().getResource("myrdf/three.rdf")
              .toURI() + "','RDF/XML')");
      assertEquals(6L, importResults
          .next().get("triplesLoaded").asLong());
      Result mediaNames = session.run("MATCH (m:Publication) " +
          "\nRETURN m.name AS nm, m.uri AS uri");

      Record next = mediaNames.next();
      assertEquals("The Financial Times", next.get("nm").asString());
      assertEquals("http://neo4j.com/invividual/FT", next.get("uri").asString());

      Result rels = session.run(
          "MATCH ({ personName: 'JC'})-[r:READS]-(:Publication { name: 'The Financial Times'}) " +
              "\nRETURN count(r) as ct");

      next = rels.next();
      assertEquals(1L, next.get("ct").asLong());

    }
  }

  @Test
  public void testImportFromFileWithPredFilter() throws Exception {

    try (Driver driver = GraphDatabase.driver(neo4j.boltURI(),
        Config.builder().withoutEncryption().build()); Session session = driver.session()) {
      session.run("call n10s.nsprefixes.add('sch','http://schema.org/')");
      session.run("call n10s.nsprefixes.add('cats','http://neo4j.com/category/')");
    }

    try (Driver driver = GraphDatabase.driver(neo4j.boltURI(),
        Config.builder().withoutEncryption().build()); Session session = driver.session()) {

      initialiseGraphDB(neo4j.defaultDatabaseService(),"{ handleVocabUris: 'MAP'}");

      session.run(" call n10s.mapping.add(\"http://schema.org/location\",\"WHERE\") ");
      session.run(" call n10s.mapping.add(\"http://schema.org/description\",\"desc\") ");

      Result importResults
          = session.run("CALL n10s.rdf.import.fetch('" +
          RDFProceduresTest.class.getClassLoader().getResource("event.json")
              .toURI()
          + "','JSON-LD', {predicateExclusionList: ['http://schema.org/price','http://schema.org/priceCurrency'] })");
      assertEquals(26L, importResults
          .next().get("triplesLoaded").asLong());

      Result postalAddresses = session.run("MATCH (m:PostalAddress) " +
          "\nRETURN m.postalCode as zip");

      Record next = postalAddresses.next();
      assertEquals("95051", next.get("zip").asString());

      Result whereRels = session.run("MATCH (e:Event)-[:WHERE]->(p:Place) " +
          "\nRETURN p.name as placeName, e.desc as desc ");

      next = whereRels.next();
      assertEquals(
          "Join us for an afternoon of Jazz with Santa Clara resident and pianist Andy Lagunoff. " +
              "Complimentary food and beverages will be served.",
          next.get("desc").asString());
      assertEquals("Santa Clara City Library, Central Park Library",
          next.get("placeName").asString());

    }
  }

  @Test
  public void testStreamFromFile() throws Exception {
    try (Driver driver = GraphDatabase.driver(neo4j.boltURI(),
        Config.builder().withoutEncryption().build()); Session session = driver.session()) {

      Result importResults
          = session.run("CALL n10s.rdf.stream.fetch('" +
          RDFProceduresTest.class.getClassLoader().getResource("oneTriple.rdf")
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
  public void testStreamFromFileWithLimit() throws Exception {
    try (Driver driver = GraphDatabase.driver(neo4j.boltURI(),
        Config.builder().withoutEncryption().build()); Session session = driver.session()) {

      Result importResults
          = session.run("CALL n10s.rdf.stream.fetch('" +
          RDFProceduresTest.class.getClassLoader().getResource("event.json")
              .toURI() + "','JSON-LD',{ limit: 2})");
      assertTrue(importResults.hasNext());
      importResults.next();
      importResults.next();
      assertFalse(importResults.hasNext());
    }
  }

  @Test
  public void testStreamFromFileWithExclusionList() throws Exception {
    try (Driver driver = GraphDatabase.driver(neo4j.boltURI(),
            Config.builder().withoutEncryption().build()); Session session = driver.session()) {

      String filteredPred = "http://schema.org/image";

      Result importResults
              = session.run("CALL n10s.rdf.stream.fetch('" +
              RDFProceduresTest.class.getClassLoader().getResource("event.json")
                      .toURI() + "','JSON-LD')");

      int tripleCount = 0;
      int filteredPredicatesCount = 0;
      while(importResults.hasNext()){
        Record next = importResults.next();
        if(next.get("predicate").asString().equals(filteredPred)){
          filteredPredicatesCount++;
        }
        tripleCount++;
      }
      assertEquals(28,tripleCount);
      assertEquals(3,filteredPredicatesCount);


      importResults
              = session.run("CALL n10s.rdf.stream.fetch('" +
              RDFProceduresTest.class.getClassLoader().getResource("event.json")
                      .toURI() + "','JSON-LD',{ predicateExclusionList: ['" + filteredPred + "'] })");

      tripleCount = 0;
      filteredPredicatesCount = 0;
      while(importResults.hasNext()){
        Record next = importResults.next();
        if(next.get("predicate").asString().equals(filteredPred)){
          filteredPredicatesCount++;
        }
        tripleCount++;
      }
      assertEquals(25,tripleCount);
      assertEquals(0,filteredPredicatesCount);
    }
  }

  @Test
  public void testStreamFromString() throws Exception {
    try (Driver driver = GraphDatabase.driver(neo4j.boltURI(),
        Config.builder().withoutEncryption().build()); Session session = driver.session()) {

      String rdf = "<rdf:RDF xmlns:owl=\"http://www.w3.org/2002/07/owl#\"\n"
          + "         xmlns:rdfs=\"http://www.w3.org/2000/01/rdf-schema#\"\n"
          + "         xmlns:rdf=\"http://www.w3.org/1999/02/22-rdf-syntax-ns#\"\n"
          + "         xmlns:voc=\"http://neo4j.com/voc/\">\n"
          + "         <rdf:Description rdf:about=\"http://neo4j.com/invividual/JB\">\n"
          + "            <voc:name>JB</voc:name>\n"
          + "         </rdf:Description>\n"
          + "</rdf:RDF>";

      Result importResults
          = session.run("CALL n10s.rdf.stream.inline('" + rdf + "','RDF/XML',{})");
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
        Config.builder().withoutEncryption().build()); Session session = driver.session()) {

      Result importResults
          = session.run("CALL n10s.rdf.stream.fetch('" +
          RDFProceduresTest.class.getClassLoader().getResource("badUri.ttl")
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
        Config.builder().withoutEncryption().build()); Session session = driver.session()) {

      String rdf = "@prefix pr: <http://example.org/vocab/show/> .\n"
          + "pr:ent\n"
          + "      pr:P854 <https://suasprod.noc-science.at/XLCubedWeb/WebForm/ShowReport.aspx?rep=004+studierende%2f001+universit%u00e4> ;\n"
          + "      pr:name \"test name\" .";
      Result importResults
          = session
          .run("CALL n10s.rdf.stream.inline('" + rdf + "','Turtle',{verifyUriSyntax: false})");
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
        Config.builder().withoutEncryption().build()); Session session = driver.session()) {

      try {
        Result importResults
            = session.run("CALL n10s.rdf.stream.fetch('" +
            RDFProceduresTest.class.getClassLoader().getResource("badUri.ttl")
                .toURI() + "','Turtle')");
        importResults.hasNext();
        assertFalse(true);
      } catch (Exception e) {
        assertTrue(e.getMessage().contains("Illegal percent encoding"));
      }
    }
  }

  @Test
  public void testGetLangValUDF() throws Exception {
    try (Driver driver = GraphDatabase.driver(neo4j.boltURI(),
        Config.builder().withoutEncryption().build()); Session session = driver.session()) {

      initialiseGraphDB(neo4j.defaultDatabaseService(), null);

      Result importResults
          = session.run(
          "return n10s.rdf.getLangValue('fr',[\"The Hague@en\", \"Den Haag@nl\", \"La Haye@fr\"]) as val");
      Map<String, Object> next = importResults
          .next().asMap();
      assertEquals("La Haye", next.get("val"));

      importResults
          = session.run(
          "return n10s.rdf.getLangValue('es',[\"The Hague@en\", \"Den Haag@nl\", \"La Haye@fr\"]) as val");
      next = importResults
          .next().asMap();
      assertNull(next.get("val"));

      importResults
          = session.run("return n10s.rdf.getLangValue('fr','La Haye@fr') as val");
      next = importResults
          .next().asMap();
      assertEquals("La Haye", next.get("val"));

      importResults
          = session.run("return n10s.rdf.getLangValue('es','La Haye@fr') as val");
      next = importResults
          .next().asMap();
      assertNull(next.get("val"));

      importResults
          = session.run("return n10s.rdf.getLangValue('es',[2, 45, 3]) as val");
      next = importResults
          .next().asMap();
      assertNull(next.get("val"));

      session.run(
          "create (n:Thing { prop: [\"That Seventies Show@en\", \"Cette Série des Années Soixante-dix@fr\", \"Cette Série des Années Septante@fr-be\"] })");
      importResults
          = session.run(
          "match (n:Thing) return n10s.rdf.getLangValue('en',n.prop) as en_name, n10s.rdf.getLangValue('fr',n.prop) as fr_name, n10s.rdf.getLangValue('fr-be',n.prop) as frbe_name");
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
          "match (n:Thing) return n10s.rdf.getLangValue('en-US',n.prop) as enus_name, n10s.rdf.getLangValue('fr-custom-tag',n.prop) as frcust_name, n10s.rdf.getLangValue('zh-Hans-CN',n.prop) as cn_name");
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
        Config.builder().withoutEncryption().build()); Session session = driver.session()) {

      initialiseGraphDB(neo4j.defaultDatabaseService(), null);

      Result importResults
          = session.run("return n10s.rdf.getLangTag('The Hague@en') as val_en,"
          + "n10s.rdf.getLangTag('Den Haag@nl') as val_nl, "
          + "n10s.rdf.getLangTag('La Haye@fr') as val_fr,"
          + "n10s.rdf.getLangTag('That Seventies Show@en-US') as val_us,"
          + "n10s.rdf.getLangTag([2, 45, 3]) as val_array,"
          + "n10s.rdf.getLangTag('hello') as val_no_tag");
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
          "match (n:Thing) return n10s.rdf.getLangTag(n.prop[0]) as enus_tag, "
              + "n10s.rdf.getLangTag(n.prop[1]) as frcust_tag, n10s.rdf.getLangTag(n.prop[2]) as cn_tag");
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
        Config.builder().withoutEncryption().build()); Session session = driver.session()) {

      initialiseGraphDB(neo4j.defaultDatabaseService(), null);

      Result importResults
          = session.run("return n10s.rdf.hasLangTag('en','The Hague@en') as val_en,"
          + "n10s.rdf.hasLangTag('nl','Den Haag@nl') as val_nl, "
          + "n10s.rdf.hasLangTag('en','La Haye@fr') as val_fr_no,"
          + "n10s.rdf.hasLangTag('en-US','That Seventies Show@en-US') as val_us,"
          + "n10s.rdf.hasLangTag('it',[2, 45, 3]) as val_array,"
          + "n10s.rdf.hasLangTag('ru','hello') as val_no_tag");
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
          "match (n:Thing) return n10s.rdf.hasLangTag('en-US',n.prop[0]) as enus_tag, "
              + "n10s.rdf.hasLangTag('fr-custom-tag',n.prop[1]) as frcust_tag, "
              + "n10s.rdf.hasLangTag('es',n.prop[1]) as frcust_tag_no, "
              + "n10s.rdf.hasLangTag('zh-Hans-CN',n.prop[2]) as cn_tag");
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
        Config.builder().withoutEncryption().build()); Session session = driver.session()) {

      initialiseGraphDB(neo4j.defaultDatabaseService(),
          "{ handleVocabUris: 'SHORTEN', handleRDFTypes: 'LABELS' }");

      Result importResults
          = session.run("CALL n10s.rdf.import.fetch('" +
          RDFProceduresTest.class.getClassLoader().getResource("mini-ld.json").toURI()
          + "','JSON-LD',"
          +
          "{ commitSize: 500 })");
      assertEquals(6L, importResults
          .next().get("triplesLoaded").asLong());
      assertEquals("http://xmlns.com/foaf/0.1/knows",
          session.run("MATCH (n{ns0" + PREFIX_SEPARATOR + "name : 'Markus Lanthaler'})-[r]-() " +
              " RETURN n10s.rdf.fullUriFromShortForm(type(r)) AS uri")
              .next().get("uri").asString());

      assertEquals("ns0" + PREFIX_SEPARATOR + "knows",
          session
              .run("RETURN n10s.rdf.shortFormFromFullUri('http://xmlns.com/foaf/0.1/knows') AS uri")
              .next().get("uri").asString());
    }
  }

  @Test
  public void testGetDataType() throws Exception {
    try (Driver driver = GraphDatabase.driver(neo4j.boltURI(),
        Config.builder().withoutEncryption().build()); Session session = driver.session()) {

      initialiseGraphDB(neo4j.defaultDatabaseService(), null);

      Result importResults = session
          .run("return n10s.rdf.getDataType('2008-04-17^^ns1__date') AS val");
      Map<String, Object> next = importResults.next().asMap();
      assertEquals("ns1__date", next.get("val"));

      importResults = session
          .run("return n10s.rdf.getDataType('10000^^http://example.org/USD') AS val");
      next = importResults.next().asMap();
      assertEquals("http://example.org/USD", next.get("val"));

      importResults = session.run("return n10s.rdf.getDataType('10000') AS val");
      next = importResults.next().asMap();
      assertEquals(XMLSchema.STRING.stringValue(), next.get("val"));

      importResults = session.run("return n10s.rdf.getDataType(10000) AS val");
      next = importResults.next().asMap();
      assertEquals(XMLSchema.LONG.stringValue(), next.get("val"));

      importResults = session.run("return n10s.rdf.getDataType(10000.0) AS val");
      next = importResults.next().asMap();
      assertEquals(XMLSchema.DOUBLE.stringValue(), next.get("val"));

      importResults = session.run("return n10s.rdf.getDataType(true) AS val");
      next = importResults.next().asMap();
      assertEquals(XMLSchema.BOOLEAN.stringValue(), next.get("val"));

      importResults = session.run("return n10s.rdf.getDataType(date('1986-07-19')) AS val");
      next = importResults.next().asMap();
      assertEquals(XMLSchema.DATE.stringValue(), next.get("val"));

      importResults = session
          .run("return n10s.rdf.getDataType(localdatetime('1986-07-09T18:06:36')) AS val");
      next = importResults.next().asMap();
      assertEquals(XMLSchema.DATETIME.stringValue(), next.get("val"));

    }
  }

  @Test
  public void testGetValue() throws Exception {
    try (Driver driver = GraphDatabase.driver(neo4j.boltURI(),
        Config.builder().withoutEncryption().build()); Session session = driver.session()) {

      initialiseGraphDB(neo4j.defaultDatabaseService(), null);

      Result importResults = session
          .run("return n10s.rdf.getValue('2008-04-17^^ns1__date') AS val");
      Map<String, Object> next = importResults.next().asMap();
      assertEquals("2008-04-17", next.get("val"));

      importResults = session.run(
          "return n10s.rdf.getValue('10000^^http://example.org/USD') AS val");
      next = importResults.next().asMap();
      assertEquals("10000", next.get("val"));

      importResults = session.run("return n10s.rdf.getValue('10000') AS val");
      next = importResults.next().asMap();
      assertEquals("10000", next.get("val"));

      importResults = session.run("return n10s.rdf.getValue('This is a test@en') AS val");
      next = importResults.next().asMap();
      assertEquals("This is a test", next.get("val"));
    }
  }

  @Test
  public void testCustomDataTypesKeepURIs() throws Exception {
    try (Driver driver = GraphDatabase.driver(neo4j.boltURI(),
        Config.builder().withoutEncryption().build()); Session session = driver.session()) {

      initialiseGraphDB(neo4j.defaultDatabaseService(),
          "{ keepLangTag: true, handleMultival: 'ARRAY',  " +
              "multivalPropList: ['http://example.com/price', 'http://example.com/power', 'http://example.com/class'], "
              +
              "keepCustomDataTypes: true,  handleVocabUris: 'KEEP', handleRDFTypes: 'LABELS', " +
              "customDataTypePropList: ['http://example.com/price', 'http://example.com/color', 'http://example.com/power'] }");

      Result importResults
          = session.run("CALL n10s.rdf.import.fetch('" +
          RDFProceduresTest.class.getClassLoader().getResource("customDataTypes.ttl")
              .toURI() + "','Turtle',{ commitSize: 500 })");
      assertEquals(10L, importResults
          .next().get("triplesLoaded").asLong());
      Result cars = session.run("MATCH (n:`http://example.com/Car`) " +
          "\nRETURN n.`http://example.com/price` AS price," +
          "n.`http://example.com/power` AS power, " +
          "n.`http://example.com/color` AS color, " +
          "n.`http://example.com/class` AS class, n.`http://example.com/released` AS released, " +
          "n.`http://example.com/type` AS type ORDER BY price");

      Record car = cars.next();
      List price = car.get("price").asList();
      assertEquals(2, price.size());
      assertTrue(price.containsAll(Arrays.asList("10000^^http://example.com/EUR","11000^^http://example.com/USD")));
      assertTrue(car.get("power").asList().containsAll(Arrays.asList("300^^http://example.com/HP","223,71^^http://example.com/kW")));
      assertEquals("red^^http://example.com/Color", car.get("color").asString());
      assertTrue(car.get("class").asList().containsAll(Arrays.asList("A-Klasse@de","A-Class@en")));
      assertEquals(2019, car.get("released").asLong());
      assertEquals("Cabrio", car.get("type").asString());
    }
  }

  @Test
  public void testCustomDataTypesShortenURIs() throws Exception {
    try (Driver driver = GraphDatabase.driver(neo4j.boltURI(),
        Config.builder().withoutEncryption().build()); Session session = driver.session()) {

      initialiseGraphDB(neo4j.defaultDatabaseService(),
          " { keepLangTag: true, handleMultival: 'ARRAY',  " +
              "multivalPropList: ['http://example.com/price', 'http://example.com/power', 'http://example.com/class'], "
              +
              "keepCustomDataTypes: true,  " +
              "customDataTypePropList: ['http://example.com/price', 'http://example.com/color', 'http://example.com/power'], "
              +
              "handleVocabUris: 'SHORTEN', handleRDFTypes: 'LABELS' }");

      Result importResults
          = session.run("CALL n10s.rdf.import.fetch('" +
          RDFProceduresTest.class.getClassLoader().getResource("customDataTypes.ttl")
              .toURI() + "','Turtle',{ commitSize: 500 })");
      assertEquals(10L, importResults
          .next().get("triplesLoaded").asLong());
      Result cars = session.run("MATCH (n:ns0__Car) " +
          "\nRETURN n.ns0__price AS price," +
          "n.ns0__power AS power, " +
          "n.ns0__color AS color, " +
          "n.ns0__class AS class, n.ns0__released AS released, " +
          "n.ns0__type AS type ORDER BY price");

      Record car = cars.next();
      List price = car.get("price").asList();
      assertEquals(2, price.size());
      assertTrue(price.contains("10000^^ns0__EUR"));
      assertTrue(price.contains("11000^^ns0__USD"));
      assertTrue(car.get("power").asList().contains("300^^ns0__HP"));
      assertTrue(car.get("power").asList().contains("223,71^^ns0__kW"));
      assertEquals("red^^ns0__Color", car.get("color").asString());
      assertTrue(car.get("class").asList().contains("A-Klasse@de"));
      assertTrue(car.get("class").asList().contains("A-Class@en"));
      assertEquals(2019, car.get("released").asLong());
      assertEquals("Cabrio", car.get("type").asString());
    }
  }

  @Test
  public void testImportMultiValAfterImportSingelVal() throws Exception {
    try (Driver driver = GraphDatabase.driver(neo4j.boltURI(),
        Config.builder().withoutEncryption().build()); Session session = driver.session()) {

      initialiseGraphDB(neo4j.defaultDatabaseService(),
          "{ handleMultival: 'OVERWRITE', handleVocabUris: 'KEEP'  }");
      String importCypher = "CALL n10s.rdf.import.fetch('" +
          RDFProceduresTest.class.getClassLoader()
              .getResource("testImportMultiValAfterImportSingelVal.ttl")
              .toURI() + "','Turtle')";
      Result importResults = session.run(importCypher);
      Record next = importResults.next();
      assertEquals(3, next.get("triplesLoaded").asInt());
      Result queryResults = session
          .run("MATCH (n:Resource) RETURN n.`http://example.com/price` AS price");
      Object imports = queryResults.next().get("price");
      assertEquals(IntegerValue.class, imports.getClass());

      session.run("MATCH (n) DETACH DELETE n ;");
      //set graph config
      session
          .run("CALL n10s.graphconfig.init({ handleMultival: 'ARRAY', handleVocabUris: 'KEEP' });");

      importCypher = "CALL n10s.rdf.import.fetch('" +
          RDFProceduresTest.class.getClassLoader()
              .getResource("testImportMultiValAfterImportSingelVal.ttl")
              .toURI() + "','Turtle')";

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
        Config.builder().withoutEncryption().build()); Session session = driver.session()) {

      initialiseGraphDB(neo4j.defaultDatabaseService(),
          "{ handleVocabUris: 'KEEP', handleRDFTypes: 'LABELS' }");

      Result importResults
          = session.run("CALL n10s.rdf.import.fetch('" +
          RDFProceduresTest.class.getClassLoader().getResource("reification.ttl")
              .toURI()
          + "','Turtle')");
      assertEquals(25L, importResults
          .next().get("triplesLoaded").asLong());
      Result dates = session
          .run("MATCH (n:`http://www.w3.org/1999/02/22-rdf-syntax-ns#Statement`) " +
              "\nRETURN n.`http://example.com/from` AS fromDates ORDER BY fromDates DESC");

      assertEquals(LocalDate.parse("2019-09-01"), dates.next().get("fromDates").asLocalDate());
      assertEquals(LocalDate.parse("2016-09-01"), dates.next().get("fromDates").asLocalDate());

      Result statements = session.run("MATCH (statement)\n" +
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
        Config.builder().withoutEncryption().build()); Session session = driver.session()) {

      initialiseGraphDB(neo4j.defaultDatabaseService(), "{ handleMultival: 'ARRAY' }");

      Result importResults
          = session.run("CALL n10s.rdf.import.fetch('" +
          RDFProceduresTest.class.getClassLoader().getResource("incremental/step1.ttl")
              .toURI() + "','Turtle')");
      assertEquals(2L, importResults
          .next().get("triplesLoaded").asLong());
      importResults
          = session.run("CALL n10s.rdf.import.fetch('" +
          RDFProceduresTest.class.getClassLoader().getResource("incremental/step2.ttl")
              .toURI() + "','Turtle')");
      assertEquals(2L, importResults
          .next().get("triplesLoaded").asLong());

      Result result = session.run("MATCH (n:ns0__Thing) " +
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
        Config.builder().withoutEncryption().build()); Session session = driver.session()) {

      initialiseGraphDB(neo4j.defaultDatabaseService(), null);

      Result importResults
          = session.run("CALL n10s.rdf.import.fetch('" +
          RDFProceduresTest.class.getClassLoader().getResource("event.json")
              .toURI() + "','JSON-LD')");
      assertEquals(28L, importResults
          .next().get("triplesLoaded").asLong());
      Result nsDefResult = session.run("MATCH (n:_NsPrefDef) "
          + "RETURN properties(n) as defs");
      assertTrue(nsDefResult.hasNext());
      Map<String, Object> defsPre = nsDefResult.next().get("defs").asMap();
      assertFalse(nsDefResult.hasNext());
      importResults
          = session.run("CALL n10s.rdf.import.fetch('" +
          RDFProceduresTest.class.getClassLoader().getResource("fibo-fragment.rdf")
              .toURI() + "','RDF/XML')");
      assertEquals(171L, importResults
          .next().get("triplesLoaded").asLong());
      nsDefResult = session.run("MATCH (n:_NsPrefDef) "
          + "RETURN properties(n) as defs");
      assertTrue(nsDefResult.hasNext());
      Map<String, Object> defsPost = nsDefResult.next().get("defs").asMap();
      assertFalse(nsDefResult.hasNext());
      assertTrue(getPrePostDelta(defsPre, defsPost).isEmpty());
      importResults
          = session.run("CALL n10s.rdf.import.fetch('" +
          RDFProceduresTest.class.getClassLoader().getResource("oneTriple.rdf")
              .toURI() + "','RDF/XML')");
      assertEquals(1L, importResults
          .next().get("triplesLoaded").asLong());
      nsDefResult = session.run("MATCH (n:_NsPrefDef) "
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
        Config.builder().withoutEncryption().build()); Session session = driver.session()) {

      initialiseGraphDB(neo4j.defaultDatabaseService(), null);

      session.run("CREATE (:_NsPrefDef {\n"
          + "  `http://www.w3.org/2000/01/rdf-schema#`: 'myschema',\n"
          + "  `http://www.w3.org/1999/02/22-rdf-syntax-ns#`: 'myrdf'})");
      Result nsDefResult = session.run("MATCH (n:_NsPrefDef) "
          + "RETURN properties(n) as defs");
      assertTrue(nsDefResult.hasNext());
      Map<String, Object> defsPre = nsDefResult.next().get("defs").asMap();
      assertFalse(nsDefResult.hasNext());
      Result importResults = session.run("CALL n10s.rdf.import.fetch('" +
          RDFProceduresTest.class.getClassLoader().getResource("fibo-fragment.rdf")
              .toURI() + "','RDF/XML')");
      assertEquals(171L, importResults.next().get("triplesLoaded").asLong());
      nsDefResult = session.run("MATCH (n:_NsPrefDef) "
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
        Config.builder().withoutEncryption().build()); Session session = driver.session()) {

      initialiseGraphDB(neo4j.defaultDatabaseService(), null);

      Result importResults
          = session.run("CALL n10s.rdf.import.fetch('" +
          RDFProceduresTest.class.getClassLoader().getResource("incremental/step1.ttl")
              .toURI() + "','Turtle')");
      assertEquals(2L, importResults
          .next().get("triplesLoaded").asLong());

      try {
        Result modifyGraphConfigResult = session
            .run("CALL n10s.graphconfig.init({ handleMultival: 'ARRAY' });");
        modifyGraphConfigResult.hasNext();
        assertFalse(true);
      } catch (Exception e) {
        //expected
        assertTrue(e.getMessage().contains("The graph is non-empty. Config cannot be changed."));
      }

      session.run("MATCH (n) DETACH DELETE n ;");
      //set graph config
      session.run("CALL n10s.graphconfig.init({ handleMultival: 'ARRAY' });");

      //{ handleMultival: 'ARRAY' }
      importResults
          = session.run("CALL n10s.rdf.import.fetch('" +
          RDFProceduresTest.class.getClassLoader().getResource("incremental/step2.ttl")
              .toURI() + "','Turtle')");
      assertEquals(2L, importResults
          .next().get("triplesLoaded").asLong());

      Result result = session.run("MATCH (n:ns0__Thing) " +
          "\nRETURN n.ns0__prop as multival ");

      List<String> vals = new ArrayList<String>();
      vals.add("two");
      assertEquals(vals, result.next().get("multival").asList());


    }
  }

  @Test
  public void testIncrementalLoadAtomicValueOnPreviouslyArray() throws Exception {
    try (Driver driver = GraphDatabase.driver(neo4j.boltURI(),
        Config.builder().withoutEncryption().build()); Session session = driver.session()) {

      initialiseGraphDB(neo4j.defaultDatabaseService(), null);

      Result importResults
          = session.run("CALL n10s.rdf.import.fetch('" +
          RDFProceduresTest.class.getClassLoader().getResource("incremental/step1.ttl")
              .toURI() + "','Turtle',{ handleMultival: 'ARRAY' })");
      assertEquals(2L, importResults
          .next().get("triplesLoaded").asLong());
      importResults
          = session.run("CALL n10s.rdf.import.fetch('" +
          RDFProceduresTest.class.getClassLoader().getResource("incremental/step3.ttl")
              .toURI() + "','Turtle')");
      assertEquals(2L, importResults
          .next().get("triplesLoaded").asLong());

      Result result = session.run("MATCH (n:ns0__Thing) " +
          "\nRETURN n.ns0__prop as singleVal ");

      assertEquals(230L, result.next().get("singleVal").asLong());


    }
  }

  @Test
  public void testLargerFileManyTransactions() throws Exception {
    try (Driver driver = GraphDatabase.driver(neo4j.boltURI(),
        Config.builder().withoutEncryption().build()); Session session = driver.session()) {

      initialiseGraphDB(neo4j.defaultDatabaseService(), null);

      Result importResults
          = session.run("CALL n10s.rdf.import.fetch('" +
          RDFProceduresTest.class.getClassLoader().getResource("100k.nt").toURI() + "','N-Triples',"
          + "{ commitSize: 5 , predicateExclusionList: ['http://www.w3.org/2004/02/skos/core#prefLabel']})");
      assertEquals(92712L, importResults
          .next().get("triplesLoaded").asLong());
    }

  }

  @Test
  public void dbpediaFragmentTest() throws Exception {
    try (Driver driver = GraphDatabase.driver(neo4j.boltURI(),
            Config.builder().withoutEncryption().build()); Session session = driver.session()) {

      initialiseGraphDB(neo4j.defaultDatabaseService(), "{handleMultival:'ARRAY', handleRDFTypes: 'NODES'}");

      Result importResults
              = session.run("CALL n10s.rdf.import.fetch('" +
              RDFProceduresTest.class.getClassLoader().getResource("dbpedia-fragment.ttl").toURI()
              + "','Turtle', { commitSize: 200 })");

      Record importResult = importResults.next();
      assertEquals(24869L, importResult.get("triplesLoaded").asLong());
      assertEquals(25000L, importResult.get("triplesParsed").asLong());

      Result result = session.run("MATCH (n:Resource) RETURN count(n) as nodeCount ");
      Record next = result.next();
      assertEquals(4497L, next.get("nodeCount").asLong());

      assertTrue(session.run("MATCH (r:Resource) DETACH DELETE r RETURN count(r) as ct").next().get("ct").asLong()>0);

      importResults
              = session.run("CALL n10s.rdf.import.fetch('" +
              RDFProceduresTest.class.getClassLoader().getResource("dbpedia-fragment.ttl").toURI()
              + "','Turtle', { commitSize: 200 , strictDataTypeCheck: false})");

      importResult = importResults.next();
      assertEquals(25000L, importResult.get("triplesLoaded").asLong());
      assertEquals(25000L, importResult.get("triplesParsed").asLong());

      result = session.run("MATCH (n:Resource) RETURN count(n) as nodeCount ");
      next = result.next();
      assertEquals(4497L, next.get("nodeCount").asLong());

    }

  }

  @Test
  public void multivalMultitypeSamePartialTx() throws Exception {
    try (Driver driver = GraphDatabase.driver(neo4j.boltURI(),
            Config.builder().withoutEncryption().build()); Session session = driver.session()) {

      initialiseGraphDB(neo4j.defaultDatabaseService(), "{handleMultival:'ARRAY', handleRDFTypes: 'NODES'}");

      Result importResults
              = session.run("CALL n10s.rdf.import.fetch('" +
              RDFProceduresTest.class.getClassLoader().getResource("multival-multitype.ttl").toURI()
              + "','Turtle', { commitSize: 200 })");

      Record importResult = importResults.next();
      assertEquals(26L, importResult.get("triplesLoaded").asLong());
      assertEquals(27L, importResult.get("triplesParsed").asLong());
      assertEquals("Some triples were discarded because of heterogeneous data typing of values for the same property. " +
                      "Check logs  for details.", importResult.get("extraInfo").asString());

      assertEquals(6, session.run("MATCH (n:Resource) RETURN count(n) as nodeCount ").next().get("nodeCount").asInt());

      assertEquals(0L,session.run("MATCH (n:Resource) WHERE '45.75^^xsd__double' in n.ns0__totalLength RETURN count(n) as ct ").next().get("ct").asLong());
      assertEquals(1L,session.run("MATCH (n:Resource) WHERE 45.75 in n.ns0__totalLength  RETURN count(n) as ct ").next().get("ct").asLong());

      assertTrue(session.run("MATCH (r:Resource) DETACH DELETE r RETURN count(r) as ct").next().get("ct").asLong()>0);

      importResults
              = session.run("CALL n10s.rdf.import.fetch('" +
              RDFProceduresTest.class.getClassLoader().getResource("multival-multitype.ttl").toURI()
              + "','Turtle', { commitSize: 200 , strictDataTypeCheck: false })");

      importResult = importResults.next();
      assertEquals(27L, importResult.get("triplesLoaded").asLong());
      assertEquals(27L, importResult.get("triplesParsed").asLong());
      assertEquals("Some heterogeneous data typing of values for the same property was found. Values were imported as typed strings. " +
              "Check logs  for details.", importResult.get("extraInfo").asString());

      assertEquals(6, session.run("MATCH (n:Resource) RETURN count(n) as nodeCount ").next().get("nodeCount").asInt());

      Result result = session.run("MATCH (n:Resource) RETURN n.ns0__totalLength as tl ");
      Record next = result.next();
      assertTrue(next.get("tl").asList().containsAll(Arrays.asList("45.75^^xsd__double", "2271.0"))); //"2271.0^^ns1__second" if custom datatypes were being kept

      assertEquals(1L,session.run("MATCH (n:Resource) WHERE '45.75^^xsd__double' in n.ns0__totalLength RETURN count(n) as ct ").next().get("ct").asLong());

      assertTrue(session.run("MATCH (r:Resource) DETACH DELETE r RETURN count(r) as ct").next().get("ct").asLong()>0);

      Result importResults2NdTry
              = session.run("CALL n10s.rdf.import.fetch('" +
              RDFProceduresTest.class.getClassLoader().getResource("multival-multitype.ttl").toURI()
              + "','Turtle', { commitSize: 5 })");

      importResult = importResults2NdTry.next();
      assertEquals(26L, importResult.get("triplesLoaded").asLong());
      assertEquals(27L, importResult.get("triplesParsed").asLong());

      assertEquals(6, session.run("MATCH (n:Resource) RETURN count(n) as nodeCount ").next().get("nodeCount").asInt());

      assertTrue(session.run("MATCH (r:Resource) DETACH DELETE r RETURN count(r) as ct").next().get("ct").asLong()>0);

      importResults2NdTry
              = session.run("CALL n10s.rdf.import.fetch('" +
              RDFProceduresTest.class.getClassLoader().getResource("multival-multitype.ttl").toURI()
              + "','Turtle', { commitSize: 5, strictDataTypeCheck: false })");

      importResult = importResults2NdTry.next();
      assertEquals(27L, importResult.get("triplesLoaded").asLong());
      assertEquals(27L, importResult.get("triplesParsed").asLong());

      assertEquals(6, session.run("MATCH (n:Resource) RETURN count(n) as nodeCount ").next().get("nodeCount").asInt());

      assertEquals(1L,session.run("MATCH (n:Resource) WHERE '45.75^^xsd__double' in n.ns0__totalLength RETURN count(n) as ct ").next().get("ct").asLong());

    }

  }

  @Test
  public void homogeneousMultivalAcrossPartialCommits() throws Exception {
    try (Driver driver = GraphDatabase.driver(neo4j.boltURI(),
            Config.builder().withoutEncryption().build()); Session session = driver.session()) {

      initialiseGraphDB(neo4j.defaultDatabaseService(), "{handleMultival:'ARRAY', handleRDFTypes: 'NODES'}");

      Result importResults
              = session.run("CALL n10s.rdf.import.fetch('" +
              RDFProceduresTest.class.getClassLoader().getResource("multival-multi-tx.ttl").toURI()
              + "','Turtle', { commitSize: 6 })");

      Record importResult = importResults.next();
      assertEquals(23L, importResult.get("triplesLoaded").asLong());
      assertEquals(23L, importResult.get("triplesParsed").asLong());

      Result result = session.run("MATCH (n:Resource) RETURN count(n) as nodeCount ");
      assertEquals(1, result.next().get("nodeCount").asInt());

      result = session.run("MATCH (n:Resource) RETURN n.ns0__totalLengthDouble as dou, n.ns0__totalLengthInt as int," +
              "n.ns0__dateValue as dat, n.ns0__dateTimeProp as dtim, n.ns0__titleBool as boo, n.ns0__title as str1, " +
              "n.ns0__rev as str2 ");
      Record singleResult = result.next();
      assertTrue(singleResult.get("dou").asList().containsAll(Arrays.asList(45.75D, 50.75D, 510D)));
      assertTrue(singleResult.get("int").asList().containsAll(Arrays.asList(4L, 43L, 4187L)));
      assertTrue(singleResult.get("dat").asList().containsAll(Arrays.asList(
              LocalDate.of(2002, 9, 24), LocalDate.of(1973, 8, 28),
              LocalDate.of(2002, 9, 25))));
      assertTrue(singleResult.get("dtim").asList().containsAll(Arrays.asList(
              LocalDateTime.of(2002, 8, 28, 9, 8, 0),
              LocalDateTime.of(2002, 5, 30, 9, 0, 0),
              LocalDateTime.of(2012, 5, 30, 9, 9, 0))));
      assertTrue(singleResult.get("boo").asList().containsAll(Arrays.asList(true, false)));
      assertTrue(singleResult.get("str1").asList().containsAll(Arrays.asList("I Know No One",
              "The Intimacy of the World with the World", "No Flashlight", "The Air in the Morning",
              "No Inside, No Out", "Stop Singing", "In the Bat's Mouth")));
    }

  }


  @Test
  public void heterogeneousMultivalAcrossPartialCommits() throws Exception {
    try (Driver driver = GraphDatabase.driver(neo4j.boltURI(),
            Config.builder().withoutEncryption().build()); Session session = driver.session()) {

      initialiseGraphDB(neo4j.defaultDatabaseService(),
              "{handleMultival:'ARRAY', handleRDFTypes: 'NODES', keepCustomDataTypes: true}");

      Result importResults
              = session.run("CALL n10s.rdf.import.fetch('" +
              RDFProceduresTest.class.getClassLoader().getResource("multival-multi-tx-heterogeneous-types.ttl").toURI()
              + "','Turtle', { commitSize: 6 })");

      Record importResult = importResults.next();
      assertEquals(19L, importResult.get("triplesLoaded").asLong());
      assertEquals(24L, importResult.get("triplesParsed").asLong());

      Result result = session.run("MATCH (n:Resource) RETURN count(n) as nodeCount ");
      assertEquals(1, result.next().get("nodeCount").asInt());

      result = session.run("MATCH (n:Resource) RETURN n.ns0__totalLengthDouble as dou, n.ns0__totalLengthInt as int," +
              "n.ns0__dateValue as dat, n.ns0__dateTimeProp as dtim, n.ns0__titleBool as boo, n.ns0__title as str1, " +
              "n.ns0__rev as str2 ");
      Record singleResult = result.next();
      assertTrue(singleResult.get("dou").asList().containsAll(Arrays.asList(45.75D, 510D)));
      assertTrue(singleResult.get("int").asList().containsAll(Arrays.asList(4L, 4187L)));
      assertTrue(singleResult.get("dat").asList().containsAll(Arrays.asList(
              LocalDate.of(2002, 9, 24), LocalDate.of(1973, 8, 28))));
      assertTrue(singleResult.get("dtim").asList().containsAll(Arrays.asList(
              LocalDateTime.of(2002, 8, 28, 9, 8, 0),
              LocalDateTime.of(2002, 5, 30, 9, 0, 0))));
      assertTrue(singleResult.get("boo").asList().containsAll(Arrays.asList(true, false)));
      assertTrue(singleResult.get("str1").asList().containsAll(Arrays.asList(
              "The Intimacy of the World with the World", "No Flashlight", "The Air in the Morning",
              "No Inside, No Out", "Stop Singing", "In the Bat's Mouth")));

      assertTrue(session.run("MATCH (r:Resource) DETACH DELETE r RETURN count(r) as ct").next().get("ct").asLong() > 0);

      importResults
              = session.run("CALL n10s.rdf.import.fetch('" +
              RDFProceduresTest.class.getClassLoader().getResource("multival-multi-tx-heterogeneous-types.ttl").toURI()
              + "','Turtle', { commitSize: 12 })");

      importResult = importResults.next();
      assertEquals(19L, importResult.get("triplesLoaded").asLong());
      assertEquals(24L, importResult.get("triplesParsed").asLong());

      result = session.run("MATCH (n:Resource) RETURN count(n) as nodeCount ");
      assertEquals(1, result.next().get("nodeCount").asInt());

      result = session.run("MATCH (n:Resource) RETURN n.ns0__totalLengthDouble as dou, n.ns0__totalLengthInt as int," +
              "n.ns0__dateValue as dat, n.ns0__dateTimeProp as dtim, n.ns0__titleBool as boo, n.ns0__title as str1, " +
              "n.ns0__rev as str2 ");
      singleResult = result.next();
      assertTrue(singleResult.get("dou").asList().containsAll(Arrays.asList(45.75D, 510D)));
      assertTrue(singleResult.get("int").asList().containsAll(Arrays.asList(4L, 4187L)));
      assertTrue(singleResult.get("dat").asList().containsAll(Arrays.asList(
              LocalDate.of(2002, 9, 24), LocalDate.of(1973, 8, 28))));
      assertTrue(singleResult.get("dtim").asList().containsAll(Arrays.asList(
              LocalDateTime.of(2002, 8, 28, 9, 8, 0),
              LocalDateTime.of(2002, 5, 30, 9, 0, 0))));
      assertTrue(singleResult.get("boo").asList().containsAll(Arrays.asList(true, false)));
      assertTrue(singleResult.get("str1").asList().containsAll(Arrays.asList(
              "The Intimacy of the World with the World", "No Flashlight", "The Air in the Morning",
              "No Inside, No Out", "Stop Singing", "In the Bat's Mouth")));

      assertTrue(session.run("MATCH (r:Resource) DETACH DELETE r RETURN count(r) as ct").next().get("ct").asLong() > 0);

      importResults
              = session.run("CALL n10s.rdf.import.fetch('" +
              RDFProceduresTest.class.getClassLoader().getResource("multival-multi-tx-heterogeneous-types.ttl").toURI()
              + "','Turtle', { commitSize: 100 })");

      importResult = importResults.next();
      assertEquals(19L, importResult.get("triplesLoaded").asLong());
      assertEquals(24L, importResult.get("triplesParsed").asLong());

      result = session.run("MATCH (n:Resource) RETURN count(n) as nodeCount ");
      assertEquals(1, result.next().get("nodeCount").asInt());

      result = session.run("MATCH (n:Resource) RETURN n.ns0__totalLengthDouble as dou, n.ns0__totalLengthInt as int," +
              "n.ns0__dateValue as dat, n.ns0__dateTimeProp as dtim, n.ns0__titleBool as boo, n.ns0__title as str1, " +
              "n.ns0__rev as str2 ");

      singleResult = result.next();

      assertTrue(singleResult.get("dou").asList().containsAll(Arrays.asList(45.75D, 510D)));
      assertTrue(singleResult.get("int").asList().containsAll(Arrays.asList(4L, 4187L)));
      assertTrue(singleResult.get("dat").asList().containsAll(Arrays.asList(
              LocalDate.of(2002, 9, 24), LocalDate.of(1973, 8, 28))));
      assertTrue(singleResult.get("dtim").asList().containsAll(Arrays.asList(
              LocalDateTime.of(2002, 8, 28, 9, 8, 0),
              LocalDateTime.of(2002, 5, 30, 9, 0, 0))));
      assertTrue(singleResult.get("boo").asList().containsAll(Arrays.asList(true, false)));
      assertTrue(singleResult.get("str1").asList().containsAll(Arrays.asList(
              "The Intimacy of the World with the World", "No Flashlight", "The Air in the Morning",
              "No Inside, No Out", "Stop Singing", "In the Bat's Mouth")));

      assertTrue(session.run("MATCH (r:Resource) DETACH DELETE r RETURN count(r) as ct").next().get("ct").asLong() > 0);

      importResults
              = session.run("CALL n10s.rdf.import.fetch('" +
              RDFProceduresTest.class.getClassLoader().getResource("multival-multi-tx-heterogeneous-types.ttl").toURI()
              + "','Turtle', { commitSize: 12, strictDataTypeCheck: false})");

      importResult = importResults.next();
      assertEquals(24L, importResult.get("triplesLoaded").asLong());
      assertEquals(24L, importResult.get("triplesParsed").asLong());

      result = session.run("MATCH (n:Resource) RETURN count(n) as nodeCount ");
      assertEquals(1, result.next().get("nodeCount").asInt());

      result = session.run("MATCH (n:Resource) RETURN n.ns0__totalLengthDouble as dou, n.ns0__totalLengthInt as int," +
              "n.ns0__dateValue as dat, n.ns0__dateTimeProp as dtim, n.ns0__titleBool as boo, n.ns0__title as str1, " +
              "n.ns0__rev as str2 ");
      singleResult = result.next();
      assertTrue(singleResult.get("dou").asList().containsAll(Arrays.asList("45.75^^xsd__double", "50.75^^ns1__Double", "510.0^^xsd__double")));
      assertTrue(singleResult.get("int").asList().containsAll(Arrays.asList("4^^xsd__long", "43^^ns1__Integer", "4187^^xsd__long")));
      assertTrue(singleResult.get("dat").asList().containsAll(Arrays.asList("2002-09-24^^xsd__date","2002-09-25^^ns1__Date","1973-08-28^^xsd__date")));
      assertTrue(singleResult.get("dtim").asList().containsAll(Arrays.asList("2002-05-30T09:00^^xsd__dateTime","2012-05-30T09:09:00^^ns1__DateTime","2002-08-28T09:08^^xsd__dateTime")));
      assertTrue(singleResult.get("boo").asList().containsAll(Arrays.asList("true^^xsd__boolean", "false^^xsd__boolean","true^^ns1__Bool")));
      assertTrue(singleResult.get("str1").asList().containsAll(Arrays.asList("I Know No One^^ns1__String",
              "The Intimacy of the World with the World", "No Flashlight", "The Air in the Morning",
              "No Inside, No Out", "Stop Singing", "In the Bat's Mouth")));
    }
  }


  @Test
  public void heterogeneousMultivalAcrossPartialCommitsWithKeepURIs() throws Exception {
    try (Driver driver = GraphDatabase.driver(neo4j.boltURI(),
            Config.builder().withoutEncryption().build()); Session session = driver.session()) {

      initialiseGraphDB(neo4j.defaultDatabaseService(),
              "{handleMultival:'ARRAY', handleVocabUris: 'KEEP', keepCustomDataTypes: true}");

      Result importResults
              = session.run("CALL n10s.rdf.import.fetch('" +
              RDFProceduresTest.class.getClassLoader().getResource("multival-multi-tx-heterogeneous-types.ttl").toURI()
              + "','Turtle', { commitSize: 12, strictDataTypeCheck: false})");

      Record importResult = importResults.next();
      assertEquals(24L, importResult.get("triplesLoaded").asLong());
      assertEquals(24L, importResult.get("triplesParsed").asLong());

      Result result = session.run("MATCH (n:Resource) RETURN count(n) as nodeCount ");
      assertEquals(1, result.next().get("nodeCount").asInt());

      result = session.run("MATCH (n:Resource) RETURN n.`http://dbpedia.org/property/totalLengthDouble` as dou, n.`http://dbpedia.org/property/totalLengthInt` as int," +
              "n.`http://dbpedia.org/property/dateValue` as dat, n.`http://dbpedia.org/property/dateTimeProp` as dtim, n.`http://dbpedia.org/property/titleBool` as boo, n.`http://dbpedia.org/property/title` as str1, " +
              "n.`http://dbpedia.org/property/rev` as str2 ");
      Record singleResult = result.next();
      assertTrue(singleResult.get("dou").asList().containsAll(Arrays.asList("45.75^^http://www.w3.org/2001/XMLSchema#double", "50.75^^http://neo4j.com/customTypes/Double", "510.0^^http://www.w3.org/2001/XMLSchema#double")));
      assertTrue(singleResult.get("int").asList().containsAll(Arrays.asList("4^^http://www.w3.org/2001/XMLSchema#long", "43^^http://neo4j.com/customTypes/Integer", "4187^^http://www.w3.org/2001/XMLSchema#long")));
      assertTrue(singleResult.get("dat").asList().containsAll(Arrays.asList("2002-09-24^^http://www.w3.org/2001/XMLSchema#date","2002-09-25^^http://neo4j.com/customTypes/Date","1973-08-28^^http://www.w3.org/2001/XMLSchema#date")));
      assertTrue(singleResult.get("dtim").asList().containsAll(Arrays.asList("2002-05-30T09:00^^http://www.w3.org/2001/XMLSchema#dateTime","2012-05-30T09:09:00^^http://neo4j.com/customTypes/DateTime","2002-08-28T09:08^^http://www.w3.org/2001/XMLSchema#dateTime")));
      assertTrue(singleResult.get("boo").asList().containsAll(Arrays.asList("true^^http://www.w3.org/2001/XMLSchema#boolean", "false^^http://www.w3.org/2001/XMLSchema#boolean","true^^http://neo4j.com/customTypes/Bool")));
      assertTrue(singleResult.get("str1").asList().containsAll(Arrays.asList("I Know No One^^http://neo4j.com/customTypes/String",
              "The Intimacy of the World with the World", "No Flashlight", "The Air in the Morning",
              "No Inside, No Out", "Stop Singing", "In the Bat's Mouth")));
    }
  }


  @Test
  public void testTypesOnlySeparateTx() throws Exception {
    try (Driver driver = GraphDatabase.driver(neo4j.boltURI(),
            Config.builder().withoutEncryption().build()); Session session = driver.session()) {

      initialiseGraphDB(neo4j.defaultDatabaseService(),
              "{handleMultival:'ARRAY', keepCustomDataTypes: true}");

      Result importResults
              = session.run("CALL n10s.rdf.import.fetch('" +
              RDFProceduresTest.class.getClassLoader().getResource("two-types-two-tx-no-props.ttl").toURI()
              + "','Turtle', { commitSize: 1, strictDataTypeCheck: false})");

      Record importResult = importResults.next();
      assertEquals(2L, importResult.get("triplesLoaded").asLong());
      assertEquals(2L, importResult.get("triplesParsed").asLong());

      assertEquals(1, session.run("MATCH (n:Resource) RETURN count(n) as nodeCount ").next().get("nodeCount").asInt());
      assertEquals(3, session.run("MATCH (n:Resource) RETURN size(labels(n)) as labelCount ").next().get("labelCount").asInt());



    }
  }



  @Test
  public void testDeleteRelationshipKeepURIs() throws Exception {
    try (Driver driver = GraphDatabase.driver(neo4j.boltURI(),
        Config.builder().withoutEncryption().build()); Session session = driver.session()) {

      initialiseGraphDB(neo4j.defaultDatabaseService(),
          "{ handleVocabUris: 'KEEP', handleRDFTypes: 'LABELS', keepCustomDataTypes: true, handleMultival: 'ARRAY'}");

      Result importResults = session.run("CALL n10s.rdf.import.fetch('" +
          RDFProceduresTest.class.getClassLoader().getResource("deleteRDF/dataset1.ttl")
              .toURI()
          + "','Turtle', { commitSize: 500 })");

      assertEquals(15L, importResults.next().get("triplesLoaded").asLong());
      Result result = session.run("MATCH (n {uri: 'http://example.org/Resource1'}),"
          + "(m {uri: 'http://example.org/Resource2'})"
          + "OPTIONAL MATCH (n)-[r]->(m) "
          + "RETURN n.uri AS nUri, type(r) AS type, m.uri AS mUri");
      Record record = result.next();
      assertEquals("http://example.org/Predicate3", record.get("type").asString());
      assertEquals("http://example.org/Resource1", record.get("nUri").asString());
      assertEquals("http://example.org/Resource2", record.get("mUri").asString());

      Result deleteResults = session.run("CALL n10s.rdf.delete.fetch('" +
          RDFProceduresTest.class.getClassLoader().getResource("deleteRDF/dataset1Delete1.ttl")
              .toURI()
          + "', 'Turtle', { commitSize: 500 })");

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
        Config.builder().withoutEncryption().build()); Session session = driver.session()) {

      initialiseGraphDB(neo4j.defaultDatabaseService(), null);

      Result importResults = session.run("CALL n10s.rdf.import.fetch('" +
          RDFProceduresTest.class.getClassLoader().getResource("deleteRDF/dataset1.ttl")
              .toURI()
          + "','Turtle',{ handleVocabUris: 'SHORTEN', handleRDFTypes: 'LABELS', commitSize: 500, keepCustomDataTypes: true, handleMultival: 'ARRAY'})");

      assertEquals(15L, importResults.next().get("triplesLoaded").asLong());
      Result result = session.run("MATCH (n {uri: 'http://example.org/Resource1'}),"
          + "(m {uri: 'http://example.org/Resource2'})"
          + "OPTIONAL MATCH (n)-[r]->(m) "
          + "RETURN n.uri AS nUri, type(r) AS type, m.uri AS mUri");
      Record record = result.next();
      assertEquals("ns0__Predicate3", record.get("type").asString());
      assertEquals("http://example.org/Resource1", record.get("nUri").asString());
      assertEquals("http://example.org/Resource2", record.get("mUri").asString());

      Result deleteResults = session.run("CALL n10s.rdf.delete.fetch('" +
          RDFProceduresTest.class.getClassLoader().getResource("deleteRDF/dataset1Delete1.ttl")
              .toURI()
          + "', 'Turtle', {handleVocabUris: 'SHORTEN', handleRDFTypes: 'LABELS', commitSize: 500, keepCustomDataTypes: true})");

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
        Config.builder().withoutEncryption().build()); Session session = driver.session()) {

      initialiseGraphDB(neo4j.defaultDatabaseService(), null);

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

      Result importResults = session.run("CALL n10s.rdf.import.inline('" +
          rdf
          + "','Turtle',{ handleVocabUris: 'SHORTEN', handleRDFTypes: 'LABELS', commitSize: 500, keepCustomDataTypes: true, handleMultival: 'ARRAY'})");

      assertEquals(15L, importResults.next().get("triplesLoaded").asLong());
      Result result = session.run("MATCH (n {uri: 'http://example.org/Resource1'}),"
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
      Result deleteResults = session.run("CALL n10s.rdf.delete.inline('" +
          deleteRdf1
          + "', 'Turtle', {handleVocabUris: 'SHORTEN', handleRDFTypes: 'LABELS', commitSize: 500, keepCustomDataTypes: true})");

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
        Config.builder().withoutEncryption().build()); Session session = driver.session()) {

      initialiseGraphDB(neo4j.defaultDatabaseService(),
          "{ handleVocabUris: 'KEEP', handleRDFTypes: 'LABELS', " +
              "keepCustomDataTypes: true, handleMultival: 'ARRAY'}");

      Result importResults = session.run("CALL n10s.rdf.import.fetch('" +
          RDFProceduresTest.class.getClassLoader().getResource("deleteRDF/dataset1.ttl")
              .toURI()
          + "','Turtle',{ commitSize: 500 })");

      assertEquals(15L, importResults.next().get("triplesLoaded").asLong());
      Result result = session.run("MATCH (n {uri: 'http://example.org/Resource1'})"
          + "RETURN n.`http://example.org/Predicate2` AS nP2");

      Record record = result.next();
      assertEquals(1, record.get("nP2").asList().size());
      assertTrue(record.get("nP2").asList().contains("test"));

      Result deleteResults = session.run("CALL n10s.rdf.delete.fetch('" +
          RDFProceduresTest.class.getClassLoader().getResource("deleteRDF/dataset1Delete2.ttl")
              .toURI()
          + "', 'Turtle', {  commitSize: 500 })");

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
        Config.builder().withoutEncryption().build()); Session session = driver.session()) {

      initialiseGraphDB(neo4j.defaultDatabaseService(),
          " { handleVocabUris: 'SHORTEN', handleRDFTypes: 'LABELS', " +
              "keepCustomDataTypes: true, handleMultival: 'ARRAY'} ");

      Result importResults = session.run("CALL n10s.rdf.import.fetch('" +
          RDFProceduresTest.class.getClassLoader().getResource("deleteRDF/dataset1.ttl")
              .toURI()
          + "','Turtle',{ commitSize: 500 })");

      assertEquals(15L, importResults.next().get("triplesLoaded").asLong());
      Result result = session.run("MATCH (n {uri: 'http://example.org/Resource1'})"
          + "RETURN n.ns0__Predicate2 AS nP2");

      Record record = result.next();
      assertEquals(1, record.get("nP2").asList().size());
      assertTrue(record.get("nP2").asList().contains("test"));

      Result deleteResults = session.run("CALL n10s.rdf.delete.fetch('" +
          RDFProceduresTest.class.getClassLoader().getResource("deleteRDF/dataset1Delete2.ttl")
              .toURI()
          + "', 'Turtle', { commitSize: 500 })");

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
        Config.builder().withoutEncryption().build()); Session session = driver.session()) {

      initialiseGraphDB(neo4j.defaultDatabaseService(),
          "{ handleVocabUris: 'SHORTEN', handleRDFTypes: 'LABELS', keepCustomDataTypes: true, handleMultival: 'ARRAY'}");

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

      Result importResults = session.run("CALL n10s.rdf.import.inline('" +
          rdf
          + "','Turtle',{ commitSize: 500 })");

      assertEquals(15L, importResults.next().get("triplesLoaded").asLong());
      Result result = session.run("MATCH (n {uri: 'http://example.org/Resource1'})"
          + "RETURN n.ns0__Predicate2 AS nP2");

      Record record = result.next();
      assertEquals(1, record.get("nP2").asList().size());
      assertTrue(record.get("nP2").asList().contains("test"));

      String deleteRdf = "@prefix ex: <http://example.org/> .\n"
          + "\n"
          + "ex:Resource1\n"
          + "  ex:Predicate2 \"test\" .\n";

      Result deleteResults = session.run("CALL n10s.rdf.delete.inline('" +
          deleteRdf
          + "', 'Turtle', { commitSize: 500 })");

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
        Config.builder().withoutEncryption().build()); Session session = driver.session()) {

      initialiseGraphDB(neo4j.defaultDatabaseService(), null);

      Result importResults = session.run("CALL n10s.rdf.import.fetch('" +
          RDFProceduresTest.class.getClassLoader().getResource("deleteRDF/dataset1.ttl")
              .toURI()
          + "','Turtle',{ handleVocabUris: 'KEEP', handleRDFTypes: 'LABELS', commitSize: 500, keepCustomDataTypes: true, handleMultival: 'ARRAY'})");

      assertEquals(15L, importResults.next().get("triplesLoaded").asLong());
      Result result = session.run("MATCH (n:Resource)"
          + "RETURN n");
      assertEquals(3, result.list().size());
      result = session.run("MATCH (n {uri: 'http://example.org/Resource2'})"
          + "RETURN labels(n) AS labels");
      Record record = result.next();
      assertEquals(2, record.get("labels").asList().size());

      Result deleteResult = session.run("CALL n10s.rdf.delete.fetch('" +
          RDFProceduresTest.class.getClassLoader().getResource("deleteRDF/dataset1Delete3.ttl")
              .toURI()
          + "', 'Turtle', {handleVocabUris: 'KEEP', handleRDFTypes: 'LABELS', commitSize: 500, keepCustomDataTypes: true, handleMultival: 'ARRAY'})");

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
        Config.builder().withoutEncryption().build()); Session session = driver.session()) {

      initialiseGraphDB(neo4j.defaultDatabaseService(), null);

      Result importResults = session.run("CALL n10s.rdf.import.fetch('" +
          RDFProceduresTest.class.getClassLoader().getResource("deleteRDF/dataset1.ttl")
              .toURI()
          + "','Turtle',{ handleVocabUris: 'KEEP', handleRDFTypes: 'LABELS', commitSize: 500, keepCustomDataTypes: true, handleMultival: 'ARRAY'})");

      assertEquals(15L, importResults.next().get("triplesLoaded").asLong());
      Result result = session.run("MATCH (n:Resource)"
          + "RETURN n");
      assertEquals(3, result.list().size());

      Result deleteResult = session.run("CALL n10s.rdf.delete.fetch('" +
          RDFProceduresTest.class.getClassLoader().getResource("deleteRDF/dataset1Delete4.ttl")
              .toURI()
          + "', 'Turtle', {handleVocabUris: 'KEEP', handleRDFTypes: 'LABELS', commitSize: 500, keepCustomDataTypes: true, handleMultival: 'ARRAY'})");

      assertEquals(8L, deleteResult.next().get("triplesDeleted").asLong());

      result = session.run("MATCH (n:Resource)"
          + "RETURN n");
      assertEquals(1, result.list().size());

    }
  }

  @Test
  public void testDeleteMultiLiteral() throws Exception {
    try (Driver driver = GraphDatabase.driver(neo4j.boltURI(),
        Config.builder().withoutEncryption().build()); Session session = driver.session()) {

      initialiseGraphDB(neo4j.defaultDatabaseService(),
          "{handleVocabUris: 'KEEP', handleRDFTypes: 'LABELS', keepCustomDataTypes: true, " +
              "handleMultival: 'ARRAY'}");

      Result importResults = session.run("CALL n10s.rdf.import.fetch('" +
          RDFProceduresTest.class.getClassLoader().getResource("deleteRDF/dataset1.ttl")
              .toURI()
          + "','Turtle',{ commitSize: 500 })");

      assertEquals(15L, importResults.next().get("triplesLoaded").asLong());
      Result result = session.run("MATCH (n {uri: 'http://example.org/Resource1'}), "
          + "(m {uri: 'http://example.org/Resource2'})"
          + "RETURN n.`http://example.org/Predicate4` AS nP4, "
          + "m.`http://example.org/Predicate3` AS mP3, "
          + "m.`http://example.org/Predicate4` AS mP4");

      Record record = result.next();
      assertTrue(record.get("nP4").asList().containsAll(Arrays.asList("val1", "val2", "val3", "val4")));
      assertTrue(record.get("mP3").asList().contains(100L));
      assertTrue(record.get("mP3").asList().contains(200L));
      assertTrue(record.get("mP4").asList().contains(300.0));
      assertTrue(record.get("mP4").asList().contains(400.0));

      Result deleteResult = session.run("CALL n10s.rdf.delete.fetch('" +
          RDFProceduresTest.class.getClassLoader().getResource("deleteRDF/dataset1Delete5.ttl")
              .toURI()
          + "', 'Turtle', { commitSize: 500 })");

      assertEquals(3L, deleteResult.next().get("triplesDeleted").asLong());

      result = session.run("MATCH (n {uri: 'http://example.org/Resource1'})"
          + "RETURN n.`http://example.org/Predicate4` AS nP4");
      record = result.next();
      assertArrayEquals(new String[]{"val2"}, record.get("nP4").asList().toArray());

      deleteResult = session.run("CALL n10s.rdf.delete.fetch('" +
          RDFProceduresTest.class.getClassLoader().getResource("deleteRDF/dataset1Delete6.ttl")
              .toURI()
          + "', 'Turtle', { commitSize: 500 })");

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
        Config.builder().withoutEncryption().build()); Session session = driver.session()) {

      initialiseGraphDB(neo4j.defaultDatabaseService(), null);

      Result importResults = session.run("CALL n10s.rdf.import.fetch('" +
          RDFProceduresTest.class.getClassLoader().getResource("deleteRDF/dataset1.ttl")
              .toURI()
          + "','Turtle',{ handleVocabUris: 'KEEP', handleRDFTypes: 'LABELS', commitSize: 500, keepCustomDataTypes: true, handleMultival: 'ARRAY'})");

      assertEquals(15L, importResults.next().get("triplesLoaded").asLong());
      Result result = session.run("MATCH (n:Resource)"
          + "RETURN n");
      assertEquals(3, result.list().size());
      result = session.run("MATCH (n {uri: 'http://example.org/Resource3'})"
          + "RETURN n.uri");
      Record record = result.next();
      assertEquals("http://example.org/Resource3", record.get("n.uri").asString());

      Result deleteResult = session.run("CALL n10s.rdf.delete.fetch('" +
          RDFProceduresTest.class.getClassLoader().getResource("deleteRDF/dataset1Delete7.ttl")
              .toURI()
          + "', 'Turtle', {handleVocabUris: 'KEEP', handleRDFTypes: 'LABELS', commitSize: 500, keepCustomDataTypes: true, handleMultival: 'ARRAY'})");

      assertEquals(1L, deleteResult.next().get("triplesDeleted").asLong());

      result = session.run("MATCH (n {uri: 'http://example.org/Resource3'})"
          + "RETURN n.uri");
      assertFalse(result.hasNext());

    }
  }

  @Test
  public void testRepetitiveDeletion() throws Exception {
    try (Driver driver = GraphDatabase.driver(neo4j.boltURI(),
        Config.builder().withoutEncryption().build()); Session session = driver.session()) {

      initialiseGraphDB(neo4j.defaultDatabaseService(), "" +
          "{ handleVocabUris: 'KEEP', handleRDFTypes: 'LABELS', keepCustomDataTypes: true, handleMultival: 'ARRAY'}");

      Result importResults = session.run("CALL n10s.rdf.import.fetch('" +
          RDFProceduresTest.class.getClassLoader().getResource("deleteRDF/dataset1.ttl")
              .toURI()
          + "','Turtle',{ commitSize: 500 })");

      assertEquals(15L, importResults.next().get("triplesLoaded").asLong());
      Result result = session.run("MATCH (n:Resource)"
          + "RETURN n");
      assertEquals(3, result.list().size());
      result = session.run("MATCH (n {uri: 'http://example.org/Resource3'})"
          + "RETURN n.uri");
      Record record = result.next();
      assertEquals("http://example.org/Resource3", record.get("n.uri").asString());

      Result deleteResult = session.run("CALL n10s.rdf.delete.fetch('" +
          RDFProceduresTest.class.getClassLoader().getResource("deleteRDF/dataset1Delete4.ttl")
              .toURI()
          + "', 'Turtle', { commitSize: 500 })");

      assertEquals(8L, deleteResult.next().get("triplesDeleted").asLong());

      deleteResult = session.run("CALL n10s.rdf.delete.fetch('" +
          RDFProceduresTest.class.getClassLoader().getResource("deleteRDF/dataset1Delete4.ttl")
              .toURI()
          + "', 'Turtle', { commitSize: 500 })");

      assertEquals(0L, deleteResult.next().get("triplesDeleted").asLong());

    }
  }

  @Test
  public void ontoImportTest() throws Exception {
    try (Driver driver = GraphDatabase.driver(neo4j.boltURI(),
        Config.builder().withoutEncryption().build())) {

      initialiseGraphDB(neo4j.defaultDatabaseService(), null);
      Session session = driver.session();

      Result importResults = session.run("CALL n10s.onto.import.fetch('" +
          RDFProceduresTest.class.getClassLoader().getResource("moviesontology.owl").toURI()
          + "','RDF/XML', {  domainRelName: 'DMN'})"); //this setting should be ignored

      assertEquals(57L, importResults.next().get("triplesLoaded").asLong());

      assertEquals(2L,
          session.run("MATCH (n:n4sch__Class) RETURN count(n) AS count").next().get("count").asLong());

      assertEquals(5L,
          session.run("MATCH (n:n4sch__Property)-[:n4sch__DOMAIN]->(:n4sch__Class)  RETURN count(n) AS count").next()
              .get("count").asLong());

      assertEquals(6L,
          session.run("MATCH (n:n4sch__Relationship) RETURN count(n) AS count").next().get("count")
              .asLong());
    }

  }

  @Test
  public void ontoSnippetImportTest() throws Exception {
    try (Driver driver = GraphDatabase.driver(neo4j.boltURI(),
        Config.builder().withoutEncryption().build())) {

      initialiseGraphDB(neo4j.defaultDatabaseService(), null);
      Session session = driver.session();

      Map<String, Object> params = new HashMap<>();
      params.put("rdf", this.turtleOntology);

      Result importResults = session
          .run("CALL n10s.onto.import.inline($rdf,'Turtle')",
              params);

      assertEquals(57L, importResults.next().get("triplesLoaded").asLong());

      assertEquals(2L,
          session.run("MATCH (n:n4sch__Class) RETURN count(n) AS count").next().get("count").asLong());

      assertEquals(5L,
          session.run("MATCH (n:n4sch__Property)-[:n4sch__DOMAIN]->(:n4sch__Class)  RETURN count(n) AS count").next()
              .get("count").asLong());

      assertEquals(6L,
          session.run("MATCH (n:n4sch__Relationship) RETURN count(n) AS count").next().get("count")
              .asLong());
    }

  }

  @Test
  public void ontoImportWithCustomNames() throws Exception {
    try (Driver driver = GraphDatabase.driver(neo4j.boltURI(),
        Config.builder().withoutEncryption().build())) {

      initialiseGraphDB(neo4j.defaultDatabaseService(),
          " { classLabel : 'Category', objectPropertyLabel: 'Rel', dataTypePropertyLabel: 'Prop'}");
      Session session = driver.session();

      Result importResults = session.run("CALL n10s.onto.import.fetch('" +
          RDFProceduresTest.class.getClassLoader().getResource("moviesontology.owl").toURI()
          + "','RDF/XML')");

      assertEquals(57L, importResults.next().get("triplesLoaded").asLong());

      assertEquals(0L,
          session.run("MATCH (n:n4sch__Class) RETURN count(n) AS count").next().get("count").asLong());

      assertEquals(2L,
          session.run("MATCH (n:n4sch__Category) RETURN count(n) AS count").next().get("count").asLong());

      assertEquals(0L,
          session.run("MATCH (n:n4sch__Property) RETURN count(n) AS count").next().get("count").asLong());

      assertEquals(5L,
          session.run("MATCH (n:n4sch__Prop)-[:n4sch__DOMAIN]->(:n4sch__Category)  RETURN count(n) AS count").next()
              .get("count").asLong());

      assertEquals(0L,
          session.run("MATCH (n:n4sch__Relationship) RETURN count(n) AS count").next().get("count")
              .asLong());

      assertEquals(6L,
          session.run("MATCH (n:n4sch__Rel) RETURN count(n) AS count").next().get("count").asLong());

      assertEquals(13L,
          session.run("MATCH (n:Resource) RETURN count(distinct n.n4sch__label) AS count")
              .next().get("count").asLong());

      assertEquals(13L,
          session.run("MATCH (n:Resource) RETURN count(distinct n.n4sch__comment) AS count")
              .next().get("count").asLong());

    }

  }

  @Test
  public void ontoImportWithCustomNamesIgnoreMode() throws Exception {
    try (Driver driver = GraphDatabase.driver(neo4j.boltURI(),
            Config.builder().withoutEncryption().build())) {

      initialiseGraphDB(neo4j.defaultDatabaseService(),
              " { classLabel : 'Category', objectPropertyLabel: 'Rel', dataTypePropertyLabel: 'Prop', handleVocabUris: 'IGNORE'}");
      Session session = driver.session();

      Result importResults = session.run("CALL n10s.onto.import.fetch('" +
              RDFProceduresTest.class.getClassLoader().getResource("moviesontology.owl").toURI()
              + "','RDF/XML')");

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
        Config.builder().withoutEncryption().build())) {

      initialiseGraphDB(neo4j.defaultDatabaseService(),
          "{ classLabel : 'Category', objectPropertyLabel: 'Rel', dataTypePropertyLabel: 'Prop', handleVocabUris: 'IGNORE'}");
      Session session = driver.session();

      Map<String, Object> params = new HashMap<>();
      params.put("rdf", this.turtleOntology);

      Result importResults = session.run("CALL n10s.onto.import.inline("
          + "$rdf, 'Turtle')", params);

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
        Config.builder().withoutEncryption().build())) {

      initialiseGraphDB(neo4j.defaultDatabaseService(), "{ classLabel : 'Category', "
          + "objectPropertyLabel: 'Rel', dataTypePropertyLabel: 'Prop' , handleVocabUris: 'IGNORE' }");
      Session session = driver.session();

      Result importResults = session.run("CALL n10s.onto.import.fetch('" +
          RDFProceduresTest.class.getClassLoader().getResource("moviesontology.owl").toURI()
          + "','RDF/XML', { predicateExclusionList: ['http://www.w3.org/2000/01/rdf-schema#label',"
          + "'http://www.w3.org/2000/01/rdf-schema#comment'] })");

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
        Config.builder().withoutEncryption().build())) {

      initialiseGraphDB(neo4j.defaultDatabaseService(), "{ classLabel : 'Category', "
          + " objectPropertyLabel: 'Rel', dataTypePropertyLabel: 'Prop' , handleVocabUris: 'IGNORE' }");
      Session session = driver.session();

      Map<String, Object> params = new HashMap<>();
      params.put("rdf", this.turtleOntology);

      Result importResults = session.run("CALL n10s.onto.import.inline($rdf,"
          + "'Turtle', { predicateExclusionList: ['http://www.w3.org/2000/01/rdf-schema#label',"
          + "'http://www.w3.org/2000/01/rdf-schema#comment'] })", params);

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
        Config.builder().withoutEncryption().build())) {

      initialiseGraphDB(neo4j.defaultDatabaseService(), null);
      Session session = driver.session();

      session.run("CALL n10s.onto.import.fetch('" +
          RDFProceduresTest.class.getClassLoader().getResource("schema.rdf").toURI() +
          "','RDF/XML')");

      assertEquals(592L,
          session.run("MATCH (n:n4sch__Class) RETURN count(n) AS count").next().get("count").asLong());

      assertEquals(343L,
          session.run("MATCH (n:n4sch__Property)-[:n4sch__DOMAIN]->(:n4sch__Class)  RETURN count(n) AS count").next()
              .get("count").asLong());

      assertEquals(292L,
          session.run("MATCH (n:n4sch__Relationship)-[:n4sch__DOMAIN]->(:n4sch__Class)  RETURN count(n) AS count").next()
              .get("count").asLong());

      assertEquals(0L,
          session.run("MATCH (n:n4sch__Property)-[:n4sch__DOMAIN]->(:n4sch__Relationship) RETURN count(n) AS count")
              .next().get("count").asLong());

      assertEquals(416L,
          session.run("MATCH (n:n4sch__Relationship) RETURN count(n) AS count").next().get("count")
              .asLong());
      session.close();
    }

  }

  @Test
  public void ontoImportClassHierarchy() throws Exception {
    try (Driver driver = GraphDatabase.driver(neo4j.boltURI(),
        Config.builder().withoutEncryption().build())) {

      initialiseGraphDB(neo4j.defaultDatabaseService(), null);
      Session session = driver.session();

      Result importResults = session.run("CALL n10s.onto.import.fetch('" +
          RDFProceduresTest.class.getClassLoader().getResource("class-hierarchy-test.rdf")
              .toURI() +
          "','RDF/XML')");
      Record importSummary = importResults.next();
      assertEquals(5L, importSummary.get("triplesLoaded").asLong());

      assertEquals(1L,
          session.run("MATCH p=(:n4sch__Class{ n4sch__name:'Code'})-[:n4sch__SCO]->(:n4sch__Class{ n4sch__name:'Intangible'})" +
              " RETURN count(p) AS count").next().get("count").asLong());
      session.close();
    }
  }

  @Test
  public void ontoImportPropHierarchy() throws Exception {
    try (Driver driver = GraphDatabase.driver(neo4j.boltURI(),
        Config.builder().withoutEncryption().build())) {

      initialiseGraphDB(neo4j.defaultDatabaseService(), null);
      Session session = driver.session();

      Result importResults = session.run("CALL n10s.onto.import.fetch('" +
          RDFProceduresTest.class.getClassLoader().getResource("SPOTest.owl").toURI() +
          "','RDF/XML')");

      assertEquals(1L,
          session.run("MATCH p=(:n4sch__Property { n4sch__name:'prop1'})-[:n4sch__SPO]->(:n4sch__Property{ n4sch__name:'superprop'})" +
              " RETURN count(p) AS count").next().get("count").asLong());
      session.close();
    }
  }

  @Test
  public void ontoImportMultilabel() throws Exception {
    try (Driver driver = GraphDatabase.driver(neo4j.boltURI(),
        Config.builder().withoutEncryption().build())) {

      initialiseGraphDB(neo4j.defaultDatabaseService(),
          "{ keepLangTag: true, handleMultival: 'ARRAY' , handleVocabUris: 'MAP' } "); //, handleVocabUris: 'IGNORE'
      Session session = driver.session();

      Result importResults = session.run("CALL n10s.onto.import.fetch('" +
          RDFProceduresTest.class.getClassLoader()
              .getResource("moviesontologyMultilabel.owl").toURI()
          + "','RDF/XML')");

      assertEquals(81L, importResults.next().get("triplesLoaded").asLong());

      assertEquals(2L,
          session.run("MATCH (n:Class) RETURN count(n) AS count").next().get("count").asLong());

      assertEquals(5L,
          session.run("MATCH (n:Property)-[:DOMAIN]->(:Class)  RETURN count(n) AS count").next()
              .get("count").asLong());

      assertEquals(6L,
          session.run("MATCH (n:Relationship) RETURN count(n) AS count").next().get("count")
              .asLong());

      Record singleRecord = session
          .run("MATCH (n:Class { uri: 'http://neo4j.com/voc/movies#Movie'}) "
              + " RETURN n.label as label, n.comment as comment, "
              + " n10s.rdf.getLangValue('en',n.label) as label_en, "
              + " n10s.rdf.getLangValue('es',n.label) as label_es, "
              + " n10s.rdf.getLangValue('en',n.comment) as comment_en, "
              + " n10s.rdf.getLangValue('es',n.comment) as comment_es").next();
      assertTrue(singleRecord.get("label").asList().contains("Movie@en") &&
          singleRecord.get("label").asList().contains("Pelicula@es"));
      assertTrue(singleRecord.get("comment").asList().contains("A film@en") &&
          singleRecord.get("comment").asList().contains("Una pelicula@es"));
      assertEquals("Movie", singleRecord.get("label_en").asString());
      assertEquals("Pelicula", singleRecord.get("label_es").asString());
      assertEquals("A film", singleRecord.get("comment_en").asString());
      assertEquals("Una pelicula", singleRecord.get("comment_es").asString());

      singleRecord = session
          .run("MATCH (n:Relationship { uri: 'http://neo4j.com/voc/movies#PRODUCED'}) "
              + " RETURN n.label as label, n.comment as comment, "
              + " n10s.rdf.getLangValue('en',n.label) as label_en, "
              + " n10s.rdf.getLangValue('es',n.label) as label_es, "
              + " n10s.rdf.getLangValue('en',n.comment) as comment_en, "
              + " n10s.rdf.getLangValue('es',n.comment) as comment_es").next();
      assertTrue(singleRecord.get("label").asList().contains("PRODUCED@en") &&
          singleRecord.get("label").asList().contains("PRODUCE@es"));
      assertTrue(singleRecord.get("comment").asList().contains("Producer produced film@en") &&
          singleRecord.get("comment").asList().contains("productor de una pelicula@es"));
      assertEquals("PRODUCED", singleRecord.get("label_en").asString());
      assertEquals("PRODUCE", singleRecord.get("label_es").asString());
      assertEquals("Producer produced film", singleRecord.get("comment_en").asString());
      assertEquals("productor de una pelicula", singleRecord.get("comment_es").asString());

      singleRecord = session.run("MATCH (n:Property { uri: 'http://neo4j.com/voc/movies#title'}) "
          + " RETURN n.label as label, n.comment as comment, "
          + " n10s.rdf.getLangValue('en',n.label) as label_en, "
          + " n10s.rdf.getLangValue('es',n.label) as label_es, "
          + " n10s.rdf.getLangValue('en',n.comment) as comment_en, "
          + " n10s.rdf.getLangValue('es',n.comment) as comment_es").next();
      assertTrue(singleRecord.get("label").asList().contains("title@en") &&
          singleRecord.get("label").asList().contains("titulo@es"));
      assertTrue(singleRecord.get("comment").asList().contains("The title of a film@en") &&
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
        Config.builder().withoutEncryption().build())) {

      initialiseGraphDB(neo4j.defaultDatabaseService(),
          "{ keepLangTag: true, handleMultival: 'ARRAY', handleVocabUris: 'IGNORE' }");
      Session session = driver.session();

      Map<String, Object> params = new HashMap<>();
      params.put("rdf", this.turtleMultilangOntology);

      Result importResults = session
          .run("CALL n10s.onto.import.inline($rdf,'Turtle')", params);

      assertEquals(81L, importResults.next().get("triplesLoaded").asLong());

      assertEquals(2L,
          session.run("MATCH (n:Class) RETURN count(n) AS count").next().get("count").asLong());

      assertEquals(5L,
          session.run("MATCH (n:Property)-[:DOMAIN]->(:Class)  RETURN count(n) AS count").next()
              .get("count").asLong());

      assertEquals(6L,
          session.run("MATCH (n:Relationship) RETURN count(n) AS count").next().get("count")
              .asLong());

      Record singleRecord = session
          .run("MATCH (n:Class { uri: 'http://neo4j.com/voc/movies#Movie'}) "
              + " RETURN n.label as label, n.comment as comment, "
              + " n10s.rdf.getLangValue('en',n.label) as label_en, "
              + " n10s.rdf.getLangValue('es',n.label) as label_es, "
              + " n10s.rdf.getLangValue('en',n.comment) as comment_en, "
              + " n10s.rdf.getLangValue('es',n.comment) as comment_es").next();
      assertTrue(singleRecord.get("label").asList().contains("Movie@en") &&
          singleRecord.get("label").asList().contains("Pelicula@es"));
      assertTrue(singleRecord.get("comment").asList().contains("A film@en") &&
          singleRecord.get("comment").asList().contains("Una pelicula@es"));
      assertEquals("Movie", singleRecord.get("label_en").asString());
      assertEquals("Pelicula", singleRecord.get("label_es").asString());
      assertEquals("A film", singleRecord.get("comment_en").asString());
      assertEquals("Una pelicula", singleRecord.get("comment_es").asString());

      singleRecord = session
          .run("MATCH (n:Relationship { uri: 'http://neo4j.com/voc/movies#PRODUCED'}) "
              + " RETURN n.label as label, n.comment as comment, "
              + " n10s.rdf.getLangValue('en',n.label) as label_en, "
              + " n10s.rdf.getLangValue('es',n.label) as label_es, "
              + " n10s.rdf.getLangValue('en',n.comment) as comment_en, "
              + " n10s.rdf.getLangValue('es',n.comment) as comment_es").next();
      assertTrue(singleRecord.get("label").asList().contains("PRODUCED@en") &&
          singleRecord.get("label").asList().contains("PRODUCE@es"));
      assertTrue(singleRecord.get("comment").asList().contains("Producer produced film@en") &&
          singleRecord.get("comment").asList().contains("productor de una pelicula@es"));
      assertEquals("PRODUCED", singleRecord.get("label_en").asString());
      assertEquals("PRODUCE", singleRecord.get("label_es").asString());
      assertEquals("Producer produced film", singleRecord.get("comment_en").asString());
      assertEquals("productor de una pelicula", singleRecord.get("comment_es").asString());

      singleRecord = session.run("MATCH (n:Property { uri: 'http://neo4j.com/voc/movies#title'}) "
          + " RETURN n.label as label, n.comment as comment, "
          + " n10s.rdf.getLangValue('en',n.label) as label_en, "
          + " n10s.rdf.getLangValue('es',n.label) as label_es, "
          + " n10s.rdf.getLangValue('en',n.comment) as comment_en, "
          + " n10s.rdf.getLangValue('es',n.comment) as comment_es").next();
      assertTrue(singleRecord.get("label").asList().contains("title@en") &&
          singleRecord.get("label").asList().contains("titulo@es"));
      assertTrue(singleRecord.get("comment").asList().contains("The title of a film@en") &&
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
        Config.builder().withoutEncryption().build())) {

      Session session = driver.session();

      initialiseGraphDB(neo4j.defaultDatabaseService(), null);

      Result importResults1 = session.run("CALL n10s.rdf.import.fetch('" +
          RDFProceduresTest.class.getClassLoader().getResource("datetime/datetime-simple.ttl")
              .toURI()
          + "','Turtle')");
      assertEquals(2L, importResults1.single().get("triplesLoaded").asLong());
      Record result = session.run(
          "MATCH (n:Resource) RETURN n.ns0__reportedOn AS rep, n.`ns0__creation-date` AS cre")
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
        Config.builder().withoutEncryption().build())) {

      Session session = driver.session();

      initialiseGraphDB(neo4j.defaultDatabaseService(), null);

      Result importResults1 = session.run("CALL n10s.rdf.import.fetch('" +
          RDFProceduresTest.class.getClassLoader().getResource("datetime/datetime-complex.ttl")
              .toURI()
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
        Config.builder().withoutEncryption().build())) {

      Session session = driver.session();

      initialiseGraphDB(neo4j.defaultDatabaseService(), "{ handleMultival: 'ARRAY' }");

      Result importResults1 = session.run("CALL n10s.rdf.import.fetch('" +
          RDFProceduresTest.class.getClassLoader()
              .getResource("datetime/datetime-simple-multivalued.ttl").toURI()
          + "','Turtle')");
      assertEquals(5L, importResults1.single().get("triplesLoaded").asLong());

      Set<LocalDate> expectedDates = new HashSet<>();
      expectedDates.add(LocalDate.parse("1999-08-16"));
      expectedDates.add(LocalDate.parse("1999-08-17"));
      expectedDates.add(LocalDate.parse("1999-08-18"));

      Set<LocalDateTime> expectedDatetimes = new HashSet<>();
      expectedDatetimes.add(LocalDateTime.parse("2012-12-31T23:57:00"));
      expectedDatetimes.add(LocalDateTime.parse("2012-12-30T23:57:00"));

      Record result = session.run(
          "MATCH (n:Resource) RETURN n.ns0__someDateValue as dates, n.ns0__someDateTimeValues as dateTimes")
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
        Config.builder().withoutEncryption().build()); Session session = driver.session()) {

      initialiseGraphDBForQuads(neo4j.defaultDatabaseService(),
          "{ handleVocabUris: 'KEEP', handleRDFTypes: 'LABELS', keepCustomDataTypes: true, handleMultival: 'ARRAY' }");

      Result importResults = session.run("CALL n10s.experimental.quadrdf.import.fetch('" +
          RDFProceduresTest.class.getClassLoader().getResource("RDFDatasets/RDFDataset.trig")
              .toURI()
          + "','TriG',{ commitSize: 500 })");

      assertEquals(13L, importResults.next().get("triplesLoaded").asLong());
      Result result = session
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
  public void testImportInlineQuadRDFTriG() throws Exception {
    try (Driver driver = GraphDatabase.driver(neo4j.boltURI(),
        Config.builder().withoutEncryption().build()); Session session = driver.session()) {
      session.run("call n10s.nsprefixes.add('ns0','http://www.example.org/vocabulary#')");
    }

    try (Driver driver = GraphDatabase.driver(neo4j.boltURI(),
        Config.builder().withoutEncryption().build()); Session session = driver.session()) {

      initialiseGraphDBForQuads(neo4j.defaultDatabaseService(),
          "{ keepCustomDataTypes: true, handleMultival: 'ARRAY' }");

      Result importResults = session.run("CALL n10s.experimental.quadrdf.import.inline('" +
          rdfTriGSnippet
          + "','TriG')");

      assertEquals(13L, importResults.next().get("triplesLoaded").asLong());
      Result result = session
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
          + "RETURN n.`ns0__created` AS created");
      assertEquals(LocalDate.parse("2019-06-06"),
          result.next().get("created").asList().get(0));
      result = session.run("MATCH (n:Resource {uri: 'http://www.example.org/exampleDocument#G2'})"
          + "RETURN n.`ns0__created` AS created");
      assertEquals(LocalDateTime.parse("2019-06-07T10:15:30"),
          result.next().get("created").asList().get(0));
      result = session.run("MATCH (n {uri: 'http://www.example.org/exampleDocument#Monica'})"
          + "WHERE NOT EXISTS(n.graphUri)"
          + "RETURN labels(n) AS labels");
      Record record = result.next();
      assertEquals("Resource",
          record.get("labels").asList().get(0));
      assertEquals("ns0__Person",
          record.get("labels").asList().get(1));
      result = session.run(
          "MATCH (n {uri: 'http://www.example.org/exampleDocument#John', "
              + "graphUri: 'http://www.example.org/exampleDocument#G3'})"
              + "RETURN labels(n) AS labels");
      record = result.next();
      assertEquals("Resource",
          record.get("labels").asList().get(0));
      assertEquals("ns0__Person",
          record.get("labels").asList().get(1));
      result = session
          .run(
              "MATCH (n:Resource)"
                  + "-[:`ns0__friendOf`]->"
                  + "(m:Resource)"
                  + "RETURN NOT EXISTS(n.graphUri) AND NOT EXISTS(m.graphUri) AS result");
      assertTrue(result.next().get("result").asBoolean());
      result = session
          .run(
              "MATCH (n:Resource)"
                  + "-[:`ns0__knows`]->"
                  + "(m:Resource)"
                  + "RETURN NOT EXISTS(n.graphUri) AND NOT EXISTS(m.graphUri) AS result");
      assertFalse(result.next().get("result").asBoolean());
    }
  }

  @Test
  public void testImportQuadRDFNQuads() throws Exception {
    try (Driver driver = GraphDatabase.driver(neo4j.boltURI(),
        Config.builder().withoutEncryption().build()); Session session = driver.session()) {

      initialiseGraphDBForQuads(neo4j.defaultDatabaseService(),
          "{ handleVocabUris: 'KEEP', handleRDFTypes: 'LABELS', keepCustomDataTypes: true, handleMultival: 'ARRAY' }");

      Result importResults = session.run("CALL n10s.experimental.quadrdf.import.fetch('" +
          RDFProceduresTest.class.getClassLoader().getResource("RDFDatasets/RDFDataset.nq")
              .toURI()
          + "','N-Quads',{ commitSize: 500 })");

      assertEquals(13L, importResults.next().get("triplesLoaded").asLong());
      Result result = session
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
        Config.builder().withoutEncryption().build()); Session session = driver.session()) {

      initialiseGraphDBForQuads(neo4j.defaultDatabaseService(),
          "{ handleVocabUris: 'KEEP', handleRDFTypes: 'LABELS', keepCustomDataTypes: true, handleMultival: 'ARRAY' }");

      Result importResults = session.run("CALL n10s.experimental.quadrdf.import.fetch('" +
          RDFProceduresTest.class.getClassLoader().getResource("RDFDatasets/RDFDataset.trig")
              .toURI()
          + "','TriG',{ commitSize: 500 })");

      assertEquals(13L, importResults.next().get("triplesLoaded").asLong());
      Result result = session.run("MATCH (n:Resource)"
          + "RETURN n");
      assertEquals(12, result.list().size());

      Result deleteResult = session.run("CALL n10s.experimental.quadrdf.delete.fetch('" +
          RDFProceduresTest.class.getClassLoader().getResource("RDFDatasets/RDFDatasetDelete.trig")
              .toURI()
          + "', 'TriG', { commitSize: 500 })");

      assertEquals(9L, deleteResult.next().get("triplesDeleted").asLong());

      result = session.run("MATCH (n:Resource)"
          + "RETURN n");
      assertEquals(5, result.list().size());

    }
  }

  @Test
  public void testDeleteQuadRDFNQuads() throws Exception {
    try (Driver driver = GraphDatabase.driver(neo4j.boltURI(),
        Config.builder().withoutEncryption().build()); Session session = driver.session()) {

      initialiseGraphDBForQuads(neo4j.defaultDatabaseService(),
          " { handleVocabUris: 'KEEP', handleRDFTypes: 'LABELS', keepCustomDataTypes: true, handleMultival: 'ARRAY' } ");

      Result importResults = session.run("CALL n10s.experimental.quadrdf.import.fetch('" +
          RDFProceduresTest.class.getClassLoader().getResource("RDFDatasets/RDFDataset.nq")
              .toURI()
          + "','N-Quads',{ commitSize: 500 })");

      assertEquals(13L, importResults.next().get("triplesLoaded").asLong());
      Result result = session.run("MATCH (n:Resource)"
          + "RETURN n");
      assertEquals(12, result.list().size());

      Result deleteResult = session.run("CALL n10s.experimental.quadrdf.delete.fetch('" +
          RDFProceduresTest.class.getClassLoader().getResource("RDFDatasets/RDFDatasetDelete.nq")
              .toURI()
          + "', 'N-Quads', { commitSize: 500 })");

      assertEquals(9L, deleteResult.next().get("triplesDeleted").asLong());

      result = session.run("MATCH (n:Resource)"
          + "RETURN n");
      assertEquals(5, result.list().size());

    }
  }

  @Test
  public void testRepetitiveDeletionQuadRDF() throws Exception {
    try (Driver driver = GraphDatabase.driver(neo4j.boltURI(),
        Config.builder().withoutEncryption().build()); Session session = driver.session()) {

      initialiseGraphDBForQuads(neo4j.defaultDatabaseService(),
          "{ handleVocabUris: 'KEEP', handleRDFTypes: 'LABELS', keepCustomDataTypes: true, handleMultival: 'ARRAY'}");

      Result importResults = session.run("CALL n10s.experimental.quadrdf.import.fetch('" +
          RDFProceduresTest.class.getClassLoader().getResource("RDFDatasets/RDFDataset.trig")
              .toURI()
          + "','TriG', { commitSize: 500 })");

      assertEquals(13L, importResults.next().get("triplesLoaded").asLong());
      Result result = session.run("MATCH (n:Resource)"
          + "RETURN n");
      assertEquals(12, result.list().size());

      Result deleteResult = session.run("CALL n10s.experimental.quadrdf.delete.fetch('" +
          RDFProceduresTest.class.getClassLoader().getResource("RDFDatasets/RDFDatasetDelete.trig")
              .toURI()
          + "', 'TriG', { commitSize: 500 })");

      assertEquals(9L, deleteResult.next().get("triplesDeleted").asLong());

      result = session.run("MATCH (n:Resource)"
          + "RETURN n");
      assertEquals(5, result.list().size());

      deleteResult = session.run("CALL n10s.experimental.quadrdf.delete.fetch('" +
          RDFProceduresTest.class.getClassLoader().getResource("RDFDatasets/RDFDatasetDelete.trig")
              .toURI()
          + "', 'TriG', { commitSize: 500 })");

      assertEquals(0L, deleteResult.next().get("triplesDeleted").asLong());

    }
  }

  private void initialiseGraphDB(GraphDatabaseService db, String graphConfigParams) {
    db.executeTransactionally("CREATE CONSTRAINT n10s_unique_uri "
        + "ON (r:Resource) ASSERT r.uri IS UNIQUE");
    db.executeTransactionally("CALL n10s.graphconfig.init(" +
        (graphConfigParams != null ? graphConfigParams : "{}") + ")");
  }

  private void initialiseGraphDBForQuads(GraphDatabaseService db, String graphConfigParams) {
    db.executeTransactionally("CREATE INDEX ON :Resource(uri)");
    db.executeTransactionally("CALL n10s.graphconfig.init(" +
        (graphConfigParams != null ? graphConfigParams : "{}") + ")");
  }


  @Test
  public void testLoadJSONAsTreeEmptyJSON() throws Exception {
    try (Driver driver = GraphDatabase.driver(neo4j.boltURI(),
        Config.builder().withoutEncryption().build()); Session session = driver.session()) {

      initialiseGraphDB(neo4j.defaultDatabaseService(), null);

      Result importResults
          = session
          .run("CREATE (n:Node)  WITH n "
              + " CALL n10s.experimental.importJSONAsTree(n, '')"
              + " YIELD node RETURN node ");
      assertFalse(importResults.hasNext());
    }
  }

  @Test
  public void testLoadJSONAsTreeListAtRoot() throws Exception {
    try (Driver driver = GraphDatabase.driver(neo4j.boltURI(),
        Config.builder().withoutEncryption().build()); Session session = driver.session()) {

      initialiseGraphDB(neo4j.defaultDatabaseService(), "{ handleVocabUris: 'IGNORE' }");

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

      Result importResults
          = session.run("CREATE (n:Node)  WITH n "
          + " CALL n10s.experimental.importJSONAsTree(n, '" + jsonFragment + "','MY_JSON')"
          + " YIELD node RETURN node ");
      assertTrue(importResults.hasNext());
      Result queryresult = session
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
        Config.builder().withoutEncryption().build()); Session session = driver.session()) {

      initialiseGraphDB(neo4j.defaultDatabaseService(), "{ handleVocabUris: 'IGNORE' }");

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

      Result importResults
          = session
          .run("CREATE (n:Node { id: 'record node'})  WITH n "
              + " CALL n10s.experimental.importJSONAsTree(n, '" + jsonFragment + "') YIELD node "
              + " RETURN node ");
      assertTrue(importResults.hasNext());
      assertEquals("record node", importResults.next().get("node").asNode()
          .get("id").asString());
      Result queryresult = session
          .run("match (n:Node:Resource)-[:_jsonTree]->()-[:menu]->()-[:popup]->()"
              + "-[:menuitem]->(mi { value: 'Open', onclick: 'OpenDoc()'}) return mi ");
      assertEquals("Resource",
          queryresult.next().get("mi").asNode().labels().iterator().next());
    }
  }


  @Test
  public void testLoadJSONAsTreeWithUrisAndContext() throws Exception {
    try (Driver driver = GraphDatabase.driver(neo4j.boltURI(),
        Config.builder().withoutEncryption().build()); Session session = driver.session()) {

      initialiseGraphDB(neo4j.defaultDatabaseService(), "{ handleVocabUris: 'IGNORE' }");

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

      Result importResults
          = session
          .run("CREATE (n:Node { id: 'I\\'m the hook node'})  WITH n "
              + " CALL n10s.experimental.importJSONAsTree(n, '" + jsonFragment + "') YIELD node "
              + " RETURN node ");
      assertTrue(importResults.hasNext());
      assertEquals("I'm the hook node",
          importResults.next().get("node").asNode().get("id").asString());
      Result queryresult = session
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
      assertEquals(2, friends.size());
    }
  }

  @Test
  public void testLoadJSONAsTree2() throws Exception {
    try (Driver driver = GraphDatabase.driver(neo4j.boltURI(),
        Config.builder().withoutEncryption().build()); Session session = driver.session()) {

      initialiseGraphDB(neo4j.defaultDatabaseService(), "{ handleVocabUris: 'IGNORE' }");

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

      Result importResults
          = session
          .run("CREATE (n:Node)  WITH n "
              + " CALL n10s.experimental.importJSONAsTree(n, '" + jsonFragment + "') YIELD node "
              + " RETURN node ");
      assertTrue(importResults.hasNext());
      Result queryresult = session
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
        Config.builder().withoutEncryption().build()); Session session = driver.session()) {

      initialiseGraphDB(neo4j.defaultDatabaseService(), "{ handleVocabUris: 'IGNORE' }");

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

      Result importResults
          = session
          .run("CREATE (n:Node)  WITH n "
              + " CALL n10s.experimental.importJSONAsTree(n, '" + jsonFragment + "') YIELD node "
              + " RETURN node ");
      assertTrue(importResults.hasNext());
      Result queryresult = session
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
