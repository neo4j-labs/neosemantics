package semantics;

import static semantics.Params.BASE_INDIV_NS;
import static semantics.Params.BASE_VOCAB_NS;

import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Stream;
import org.eclipse.rdf4j.model.IRI;
import org.eclipse.rdf4j.model.Literal;
import org.eclipse.rdf4j.model.Statement;
import org.eclipse.rdf4j.model.Value;
import org.eclipse.rdf4j.model.ValueFactory;
import org.eclipse.rdf4j.model.impl.SimpleValueFactory;
import org.eclipse.rdf4j.model.vocabulary.RDF;
import org.eclipse.rdf4j.model.vocabulary.RDFS;
import org.eclipse.rdf4j.model.vocabulary.XMLSchema;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Path;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.Result;


public class LPGToRDFProcesssor {

  protected GraphDatabaseService graphdb;


  public LPGToRDFProcesssor(GraphDatabaseService graphdb) {
    this.graphdb = graphdb;
  }

  public Stream<Statement> streamLocalOntology(Map<String, Object> params){
    final ValueFactory vf = SimpleValueFactory.getInstance();

    return this.graphdb.execute(buildOntoQuery(params)).stream().map(
        triple ->
          vf.createStatement(vf.createIRI((String) triple.get("subject")),
              vf.createIRI((String) triple.get("predicate")),
              vf.createIRI((String) triple.get("object")))
    );
  }

  private String buildOntoQuery(Map<String, Object> params) {
    // this query has to be modified to use customized terms
    return " MATCH (rel)-[:DOMAIN]->(domain) RETURN '" + BASE_VOCAB_NS +"' + rel.name AS subject, '"
        + RDFS.DOMAIN + "' AS predicate, '" + BASE_VOCAB_NS +"' + domain.name AS object "
        + " UNION "
        + " MATCH (rel)-[:RANGE]->(range) RETURN '" + BASE_VOCAB_NS +"' + rel.name AS subject, '"
        + RDFS.RANGE + "' AS predicate, '" + BASE_VOCAB_NS +"' + range.name AS object "
        + " UNION "
        + " MATCH (child)-[:SCO]->(parent) RETURN '" + BASE_VOCAB_NS +"' + child.name AS subject, '"
        + RDFS.SUBCLASSOF + "' AS predicate, '" + BASE_VOCAB_NS +"' + parent.name AS object "
        + "  UNION "
        + " MATCH (child)-[:SPO]->(parent) RETURN '" + BASE_VOCAB_NS +"' + child.name AS subject, '"
        + RDFS.SUBPROPERTYOF + "' AS predicate, '" + BASE_VOCAB_NS +"' + parent.name AS object " ;
  }

  public Stream<Statement> streamAllDBAsTriples(String cypher, Map<String, Object> params){
    return null;
  }

  public Stream<Statement> streamTriplesFromCypher(String cypher, Map<String, Object> params){

    final Result result = this.graphdb.execute(cypher, params);
    final ValueFactory valueFactory = SimpleValueFactory.getInstance();
    // first version ignores mappings
    Map<String, String> mappings = new HashMap<>();
    Map<Long, IRI> ontologyEntitiesUris = new HashMap<>();

    Set<Long> serializedNodeIds = new HashSet<>();
    return result.stream().flatMap(row -> {
      Set<Statement> rowResult = new HashSet<>();
      Set<Entry<String, Object>> entries = row.entrySet();

      List<Node> nodes = new ArrayList<>();
      List<Relationship> rels = new ArrayList<>();
      List<Path> paths = new ArrayList<>();

      for (Entry<String, Object> entry:entries) {
        Object o = entry.getValue();
        if (o instanceof Node) {
          nodes.add((Node) o);
        } else if (o instanceof Relationship) {
          rels.add((Relationship) o);
        } else if (o instanceof Path) {
          paths.add((Path) o);
        }
        //if it's not a node, a  rel or a path then it cannot be converted to triples so we ignore it
      }

      for(Node node:nodes){
          if (!serializedNodeIds.contains(node.getId())) {
            serializedNodeIds.add(node.getId());
            rowResult.addAll(processNodeInLPG(mappings, node, ontologyEntitiesUris, false,valueFactory));
          }
      }

      for(Relationship rel:rels){
          rowResult.add(processRelOnLPG(mappings, rel, ontologyEntitiesUris,false,valueFactory));
        }

      for(Path p:paths){
          p.iterator().forEachRemaining(propertyContainer -> {
                if (propertyContainer instanceof Node) {
                  Node node = (Node) propertyContainer;
                  if (!serializedNodeIds.contains(node.getId())) {
                    serializedNodeIds.add(node.getId());
                    rowResult.addAll(processNodeInLPG(mappings, node, ontologyEntitiesUris,
                        false, valueFactory));
                  }
                } else if (propertyContainer instanceof Relationship) {
                  rowResult.add(
                      processRelOnLPG(mappings, (Relationship) propertyContainer, ontologyEntitiesUris,
                          false, valueFactory));
                }
              }
            );
        }

        return rowResult.stream();

    });
  }

  private Statement processRelOnLPG(Map<String, String> mappings, Relationship rel,
      Map<Long, IRI> ontologyEntitiesUris, boolean onlyMappedInfo, ValueFactory valueFactory) {

    Statement statement =  null;

    if (rel.getType().name().equals("SCO") || rel.getType().name().equals("SPO") ||
        rel.getType().name().equals("DOMAIN") || rel.getType().name().equals("RANGE")){
      //if it's  an ontlogy rel, it must apply to an ontology entity
      //TODO: Deal with cases where standards not followed (label not set, name not present, etc.)
      statement = valueFactory.createStatement(ontologyEntitiesUris.get(rel.getStartNodeId()),
          getUriforRelName(rel.getType().name()), ontologyEntitiesUris.get(rel.getEndNodeId()));
    } else {
      if (!onlyMappedInfo || mappings.containsKey(rel.getType().name())) {
        statement = valueFactory.createStatement(
            valueFactory.createIRI(BASE_INDIV_NS, String.valueOf(rel.getStartNode().getId())),

            mappings.containsKey(rel.getType().name()) ? valueFactory
                .createIRI(mappings.get(rel.getType().name()))
                :
                    valueFactory.createIRI(BASE_VOCAB_NS, rel.getType().name()),
            valueFactory.createIRI(BASE_INDIV_NS, String.valueOf(rel.getEndNode().getId())));
      }
    }
    return statement;

  }

  private IRI getUriforRelName(String name) {
    if (name.equals("SCO")){
      return RDFS.SUBCLASSOF;
    } else if  (name.equals("SPO")){
      return RDFS.SUBPROPERTYOF;
    } else if  (name.equals("DOMAIN")){
      return RDFS.DOMAIN;
    } else if  (name.equals("RANGE")){
      return RDFS.RANGE;
    } else
      //This should not happen :)
      return RDFS.SUBCLASSOF;
  }

  private Set<Statement> processNodeInLPG(Map<String, String> mappings, Node node,
      Map<Long, IRI>  ontologyEntitiesUris, boolean onlyMappedInfo,
      ValueFactory valueFactory) {
    Set<Statement> statements =  new HashSet<>();
    List<Label> nodeLabels = new ArrayList<>();
    node.getLabels().forEach( l -> nodeLabels.add(l));
    IRI subject;

    if (nodeLabels.contains(Label.label("Class"))||nodeLabels.contains(Label.label("Relationship"))||
        nodeLabels.contains(Label.label("Property"))){
      // it's an ontology element (for now we're not dealing with ( name: "a")-[:DOMAIN]->( name: "b")
      subject = valueFactory.createIRI(BASE_VOCAB_NS, (String)node.getProperty("name","unnamedEntity"));
      ontologyEntitiesUris.put(node.getId(),subject);
    } else{
      subject = valueFactory.createIRI(BASE_INDIV_NS, String.valueOf(node.getId()));
    }


    for (Label label : nodeLabels) {
      if (!onlyMappedInfo || mappings.containsKey(label.name())) {
        if(label.equals(Label.label("Class"))) {
          statements.add(valueFactory.createStatement(subject, RDF.TYPE, RDFS.CLASS));
        } else if(label.equals(Label.label("Property"))) {
          statements.add(valueFactory.createStatement(subject, RDF.TYPE, RDF.PROPERTY));
        } else if(label.equals(Label.label("Relationship"))) {
          statements.add(valueFactory.createStatement(subject, RDF.TYPE, RDF.PROPERTY));
        } else {
          statements.add(valueFactory.createStatement(subject,
              RDF.TYPE, mappings.containsKey(label.name()) ? valueFactory
                  .createIRI(mappings.get(label.name()))
                  : valueFactory.createIRI(BASE_VOCAB_NS, label.name())));
        }
      }
    }


    Map<String, Object> allProperties = node.getAllProperties();
    if (nodeLabels.contains(Label.label("Class"))||nodeLabels.contains(Label.label("Relationship"))||
        nodeLabels.contains(Label.label("Property"))){
      //TODO: this assumes property 'name' exists. This is true for imported ontos but
      // maybe we should define default in case it's not present?
      statements.add(valueFactory.createStatement(subject,
          valueFactory.createIRI("neo4j://neo4j.org/rdfs/1#", "name"),
          valueFactory.createLiteral((String)allProperties.get("name"))));
      allProperties.remove("name");
    }

    for (String key : allProperties.keySet()) {
      if (!onlyMappedInfo || mappings.containsKey(key)) {
        IRI predicate = (mappings.containsKey(key)? valueFactory.createIRI(mappings.get(key)):
                    valueFactory.createIRI(BASE_VOCAB_NS,key));
        Object propertyValueObject = allProperties.get(key);
        if (propertyValueObject instanceof Object[]) {
          for (Object o : (Object[]) propertyValueObject) {
            statements.add(valueFactory.createStatement(subject, predicate,
                createTypedLiteral(valueFactory, o)));
          }
        } else {
          statements.add(valueFactory.createStatement(subject, predicate,
              createTypedLiteral(valueFactory, propertyValueObject)));
        }
      }

    }
    return statements;
  }


  private Value createTypedLiteral(ValueFactory valueFactory, Object value) {
    Literal result;
    if (value instanceof String) {
      result = getLiteralWithTagOrDTIfPresent((String) value, valueFactory);
    } else if (value instanceof Integer) {
      result = valueFactory.createLiteral((Integer) value);
    } else if (value instanceof Long) {
      result = valueFactory.createLiteral((Long) value);
    } else if (value instanceof Float) {
      result = valueFactory.createLiteral((Float) value);
    } else if (value instanceof Double) {
      result = valueFactory.createLiteral((Double) value);
    } else if (value instanceof Boolean) {
      result = valueFactory.createLiteral((Boolean) value);
    } else if (value instanceof LocalDateTime) {
      result = valueFactory
          .createLiteral(((LocalDateTime) value).format(DateTimeFormatter.ISO_LOCAL_DATE_TIME),
              XMLSchema.DATETIME);
    } else if (value instanceof LocalDate) {
      result = valueFactory
          .createLiteral(((LocalDate) value).format(DateTimeFormatter.ISO_LOCAL_DATE),
              XMLSchema.DATE);
    } else {
      // default to string
      result = getLiteralWithTagOrDTIfPresent((String) value, valueFactory);
    }

    return result;
  }

  private Literal getLiteralWithTagOrDTIfPresent(String value, ValueFactory vf) {
    Pattern langTagPattern = Pattern.compile("^(.*)@([a-z,\\-]+)$");
    final Pattern customDataTypePattern = Pattern
        .compile("^(.*)" + Pattern.quote(Params.CUSTOM_DATA_TYPE_SEPERATOR) + "(.*)$");

    Matcher langTag = langTagPattern.matcher(value);
    Matcher customDT = customDataTypePattern.matcher(value);
    if (langTag.matches()) {
      return vf.createLiteral(langTag.group(1), langTag.group(2));
    } else if (customDT.matches()) {
      return vf.createLiteral(customDT.group(1), vf.createIRI(customDT.group(2)));
    } else {
      return vf.createLiteral(value);
    }
  }

}
