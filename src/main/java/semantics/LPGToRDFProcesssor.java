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
import org.eclipse.rdf4j.model.vocabulary.OWL;
import org.eclipse.rdf4j.model.vocabulary.RDF;
import org.eclipse.rdf4j.model.vocabulary.RDFS;
import org.eclipse.rdf4j.model.vocabulary.XMLSchema;
import org.neo4j.graphdb.*;


public class LPGToRDFProcesssor {

  private final Map<String, String> exportMappings;
  private final boolean exportOnlyMappedElems;
  private GraphDatabaseService graphdb;
  private Transaction tx;
  private final ValueFactory vf = SimpleValueFactory.getInstance();


  public LPGToRDFProcesssor(GraphDatabaseService graphdb, Transaction tx) {

    this.graphdb = graphdb;
    this.tx = tx;
    this.exportMappings = new HashMap<>();
    this.exportOnlyMappedElems = false;
  }

  public LPGToRDFProcesssor(GraphDatabaseService gds, Transaction tx, Map<String, String> exportMappings,
      boolean mappedElemsOnly) {
    this.graphdb = gds;
    this.tx = tx;
    this.exportMappings = exportMappings;
    this.exportOnlyMappedElems = mappedElemsOnly;

  }

  public Stream<Statement> streamLocalImplicitOntology() {
    Result res = tx.execute("CALL db.schema.visualization() ");
    Set<Statement> statements = new HashSet<>();
    Map<String, Object> next = res.next();
    List<Node> nodeList = (List<Node>) next.get("nodes");
    nodeList.forEach(node -> {
      String catName = node.getAllProperties().get("name").toString();
      // Resource and NamespacePrefix should be named _Resource... to avoid conflicts
      if (!catName.equals("Resource") && !catName.equals("NamespacePrefixDefinition")) {
        IRI subject = vf.createIRI(BASE_VOCAB_NS, catName);
        statements.add(vf.createStatement(subject, RDF.TYPE, OWL.CLASS));
        statements.add(vf
            .createStatement(subject, RDFS.LABEL, vf.createLiteral(catName)));
      }
    });

    List<Relationship> relationshipList = (List<Relationship>) next.get("relationships");
    for (Relationship r : relationshipList) {
      IRI relUri = vf.createIRI(BASE_VOCAB_NS, r.getType().name());
      statements.add(vf.createStatement(relUri, RDF.TYPE, OWL.OBJECTPROPERTY));
      statements.add(vf.createStatement(relUri, RDFS.LABEL,
          vf.createLiteral(r.getType().name())));
      String domainLabel =
          r.getStartNode().getLabels().iterator().next().name();
      // Resource should be named _Resource... to avoid conflicts
      if (!domainLabel.equals("Resource")) {
        statements.add(vf.createStatement(relUri, RDFS.DOMAIN,
            vf
                .createIRI(BASE_VOCAB_NS, domainLabel)));
      }
      String rangeLabel = r.getEndNode().getLabels().iterator().next().name();
      // Resource should be named _Resource... to avoid conflicts
      if (!rangeLabel.equals("Resource")) {
        statements.add(vf.createStatement(relUri, RDFS.RANGE,
            vf.createIRI(BASE_VOCAB_NS, rangeLabel)));
      }
    }
    return statements.stream();
  }

  public Stream<Statement> streamLocalExplicitOntology(Map<String, Object> params) {

    return this.tx.execute(buildOntoQuery(params)).stream().map(
        triple ->
            vf.createStatement(vf.createIRI((String) triple.get("subject")),
                vf.createIRI((String) triple.get("predicate")),
                vf.createIRI((String) triple.get("object")))
    );
  }

  private String buildOntoQuery(Map<String, Object> params) {
    // TODO: this query has to be modified to use customized terms
    return " MATCH (rel)-[:DOMAIN]->(domain) RETURN '" + BASE_VOCAB_NS
        + "' + rel.name AS subject, '"
        + RDFS.DOMAIN + "' AS predicate, '" + BASE_VOCAB_NS + "' + domain.name AS object "
        + " UNION "
        + " MATCH (rel)-[:RANGE]->(range) RETURN '" + BASE_VOCAB_NS + "' + rel.name AS subject, '"
        + RDFS.RANGE + "' AS predicate, '" + BASE_VOCAB_NS + "' + range.name AS object "
        + " UNION "
        + " MATCH (child)-[:SCO]->(parent) RETURN '" + BASE_VOCAB_NS
        + "' + child.name AS subject, '"
        + RDFS.SUBCLASSOF + "' AS predicate, '" + BASE_VOCAB_NS + "' + parent.name AS object "
        + "  UNION "
        + " MATCH (child)-[:SPO]->(parent) RETURN '" + BASE_VOCAB_NS
        + "' + child.name AS subject, '"
        + RDFS.SUBPROPERTYOF + "' AS predicate, '" + BASE_VOCAB_NS + "' + parent.name AS object ";
  }

  public Stream<Statement> streamNodeById(Long nodeId, boolean streamContext) {
    Map<Long, IRI> ontologyEntitiesUris = new HashMap<>();
    Node node = this.tx.getNodeById(nodeId);
    Set<Statement> result = processNodeInLPG(node, ontologyEntitiesUris);
    if (streamContext) {
      Iterable<Relationship> relationships = node.getRelationships();
      for (Relationship rel : relationships) {
        result.add(processRelOnLPG(rel, ontologyEntitiesUris));
      }
    }
    return result.stream();
  }

  public Stream<Statement> streamNodesBySearch(String label, String property, String propVal,
      String valType, boolean includeContext) {
    Set<Statement> result = new HashSet<>();
    Map<Long, IRI> ontologyEntitiesUris = new HashMap<>();
    ResourceIterator<Node> nodes = tx.findNodes(Label.label(label), property,
        (valType == null ? propVal : castValue(valType, propVal)));
    while (nodes.hasNext()) {
      Node node = nodes.next();
      result.addAll(processNodeInLPG(node, ontologyEntitiesUris));
      if (includeContext) {
        Iterable<Relationship> relationships = node.getRelationships();
        for (Relationship rel : relationships) {
          result.add(processRelOnLPG(rel, ontologyEntitiesUris));
        }
      }
    }
    return result.stream();
  }

  private Object castValue(String valType, String propVal) {
    switch (valType) {
      case "INTEGER":
        return Integer.valueOf(propVal);
      case "FLOAT":
        return Float.valueOf(propVal);
      case "BOOLEAN":
        return Boolean.valueOf(propVal);
      default:
        return propVal;
    }
  }

  public Stream<Statement> streamTriplesFromCypher(String cypher, Map<String, Object> params) {

    final Result result = this.tx.execute(cypher, params);
    Map<Long, IRI> ontologyEntitiesUris = new HashMap<>();

    Set<Long> serializedNodeIds = new HashSet<>();
    return result.stream().flatMap(row -> {
      Set<Statement> rowResult = new HashSet<>();
      Set<Entry<String, Object>> entries = row.entrySet();

      List<Node> nodes = new ArrayList<>();
      List<Relationship> rels = new ArrayList<>();
      List<Path> paths = new ArrayList<>();

      for (Entry<String, Object> entry : entries) {
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

      for (Node node : nodes) {
        if (!serializedNodeIds.contains(node.getId())) {
          serializedNodeIds.add(node.getId());
          rowResult.addAll(processNodeInLPG(node, ontologyEntitiesUris));
        }
      }

      for (Relationship rel : rels) {
        rowResult.add(processRelOnLPG(rel, ontologyEntitiesUris));
      }

      for (Path p : paths) {
        p.iterator().forEachRemaining(propertyContainer -> {
              if (propertyContainer instanceof Node) {
                Node node = (Node) propertyContainer;
                if (!serializedNodeIds.contains(node.getId())) {
                  serializedNodeIds.add(node.getId());
                  rowResult.addAll(processNodeInLPG(node, ontologyEntitiesUris));
                }
              } else if (propertyContainer instanceof Relationship) {
                rowResult.add(
                    processRelOnLPG((Relationship) propertyContainer, ontologyEntitiesUris));
              }
            }
        );
      }

      return rowResult.stream();

    });
  }

  private Statement processRelOnLPG(Relationship rel, Map<Long, IRI> ontologyEntitiesUris) {

    Statement statement = null;

    if (rel.getType().name().equals("SCO") || rel.getType().name().equals("SPO") ||
        rel.getType().name().equals("DOMAIN") || rel.getType().name().equals("RANGE")) {
      //if it's  an ontlogy rel, it must apply to an ontology entity
      //TODO: Deal with cases where standards not followed (label not set, name not present, etc.)
      statement = vf.createStatement(ontologyEntitiesUris.get(rel.getStartNodeId()),
          getUriforRelName(rel.getType().name()), ontologyEntitiesUris.get(rel.getEndNodeId()));
    } else {
      if (!exportOnlyMappedElems || exportMappings.containsKey(rel.getType().name())) {
        statement = vf.createStatement(
            vf.createIRI(BASE_INDIV_NS, String.valueOf(rel.getStartNode().getId())),

            exportMappings.containsKey(rel.getType().name()) ? vf
                .createIRI(exportMappings.get(rel.getType().name()))
                :
                    vf.createIRI(BASE_VOCAB_NS, rel.getType().name()),
            vf.createIRI(BASE_INDIV_NS, String.valueOf(rel.getEndNode().getId())));
      }
    }
    return statement;

  }

  private IRI getUriforRelName(String name) {
    switch (name) {
      case "SCO":
        return RDFS.SUBCLASSOF;
      case "SPO":
        return RDFS.SUBPROPERTYOF;
      case "DOMAIN":
        return RDFS.DOMAIN;
      case "RANGE":
        return RDFS.RANGE;
      default:
        //This should not happen :)
        return RDFS.SUBCLASSOF;
    }
  }

  private Set<Statement> processNodeInLPG(Node node, Map<Long, IRI> ontologyEntitiesUris) {
    Set<Statement> statements = new HashSet<>();
    List<Label> nodeLabels = new ArrayList<>();
    node.getLabels().forEach(l -> nodeLabels.add(l));
    IRI subject;

    if (nodeLabels.contains(Label.label("Class")) || nodeLabels
        .contains(Label.label("Relationship")) ||
        nodeLabels.contains(Label.label("Property"))) {
      // it's an ontology element (for now we're not dealing with ( name: "a")-[:DOMAIN]->( name: "b")
      subject = vf.createIRI(BASE_VOCAB_NS, (String) node.getProperty("name", "unnamedEntity"));
      ontologyEntitiesUris.put(node.getId(), subject);
    } else {
      subject = vf.createIRI(BASE_INDIV_NS, String.valueOf(node.getId()));
    }

    for (Label label : nodeLabels) {
      if (!exportOnlyMappedElems || exportMappings.containsKey(label.name())) {
        if (label.equals(Label.label("Class"))) {
          statements.add(vf.createStatement(subject, RDF.TYPE, RDFS.CLASS));
        } else if (label.equals(Label.label("Property"))) {
          statements.add(vf.createStatement(subject, RDF.TYPE, RDF.PROPERTY));
        } else if (label.equals(Label.label("Relationship"))) {
          statements.add(vf.createStatement(subject, RDF.TYPE, RDF.PROPERTY));
        } else {
          statements.add(vf.createStatement(subject,
              RDF.TYPE, exportMappings.containsKey(label.name()) ? vf
                  .createIRI(exportMappings.get(label.name()))
                  : vf.createIRI(BASE_VOCAB_NS, label.name())));
        }
      }
    }

    Map<String, Object> allProperties = node.getAllProperties();
    if (nodeLabels.contains(Label.label("Class")) || nodeLabels
        .contains(Label.label("Relationship")) ||
        nodeLabels.contains(Label.label("Property"))) {
      //TODO: this assumes property 'name' exists. This is true for imported ontos but
      // maybe we should define default in case it's not present?
      statements.add(vf.createStatement(subject,
          vf.createIRI("neo4j://neo4j.org/rdfs/1#", "name"),
          vf.createLiteral((String) allProperties.get("name"))));
      allProperties.remove("name");
    }

    for (String key : allProperties.keySet()) {
      if (!exportOnlyMappedElems || exportMappings.containsKey(key)) {
        IRI predicate = (exportMappings.containsKey(key) ? vf.createIRI(exportMappings.get(key)) :
            vf.createIRI(BASE_VOCAB_NS, key));
        Object propertyValueObject = allProperties.get(key);
        if (propertyValueObject instanceof Object[]) {
          for (Object o : (Object[]) propertyValueObject) {
            statements.add(vf.createStatement(subject, predicate,
                createTypedLiteral(vf, o)));
          }
        } else {
          statements.add(vf.createStatement(subject, predicate,
              createTypedLiteral(vf, propertyValueObject)));
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
