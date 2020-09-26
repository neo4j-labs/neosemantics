package n10s.rdf.export;

import static n10s.graphconfig.GraphConfig.GRAPHCONF_MULTIVAL_PROP_ARRAY;
import static n10s.graphconfig.GraphConfig.GRAPHCONF_RDFTYPES_AS_LABELS;
import static n10s.graphconfig.Params.BASE_INDIV_NS;
import static n10s.graphconfig.Params.BASE_VOCAB_NS;
import static n10s.utils.UriUtils.translateUri;

import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import n10s.graphconfig.GraphConfig;
import n10s.graphconfig.Params;
import n10s.utils.InvalidNamespacePrefixDefinitionInDB;
import n10s.utils.UriUtils;
import org.eclipse.rdf4j.model.IRI;
import org.eclipse.rdf4j.model.Literal;
import org.eclipse.rdf4j.model.Statement;
import org.eclipse.rdf4j.model.Value;
import org.eclipse.rdf4j.model.ValueFactory;
import org.eclipse.rdf4j.model.vocabulary.OWL;
import org.eclipse.rdf4j.model.vocabulary.RDF;
import org.eclipse.rdf4j.model.vocabulary.RDFS;
import org.eclipse.rdf4j.model.vocabulary.XMLSchema;
import org.neo4j.graphdb.*;


public class LPGToRDFProcesssor extends ExportProcessor {

  private final Map<String, String> exportMappings;
  private final boolean exportOnlyMappedElems;

  public LPGToRDFProcesssor(GraphDatabaseService gds, Transaction tx,
      GraphConfig gc, Map<String, String> exportMappings,
      boolean mappedElemsOnly, boolean isRDFStarSerialisation) {
    super(tx,gds, gc);
    this.exportMappings = exportMappings;
    this.exportPropertiesInRels = isRDFStarSerialisation;
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
      if (!catName.equals("Resource") && !catName.equals("_NsPrefDef")
          && !catName.equals("_n10sValidatorConfig") && !catName.equals("_MapNs")
          && !catName.equals("_MapDef") && !catName.equals("_GraphConfig")) {
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
    Set<Statement> result = processNode(node, ontologyEntitiesUris, null);
    if (streamContext) {
      Iterable<Relationship> relationships = node.getRelationships();
      for (Relationship rel : relationships) {
        Statement baseStatement = processRelationship(rel, ontologyEntitiesUris);
        result.add(baseStatement);
        if(this.exportPropertiesInRels) {
          rel.getAllProperties().forEach((k, v) -> processPropOnRel(result, baseStatement, k, v));
        }
      }
    }
    return result.stream();
  }

  @Override
  protected boolean filterRelationship(Relationship rel, Map<Long, IRI> ontologyEntitiesUris) {
    return filterNode(rel.getStartNode(), ontologyEntitiesUris) || filterNode(rel.getEndNode(), ontologyEntitiesUris);
  }

  @Override
  protected boolean filterNode(Node node, Map<Long, IRI> ontologyEntitiesUris) {
    return node.hasLabel(Label.label("_MapDef")) || node.hasLabel(Label.label("_MapNs"))||
        node.hasLabel(Label.label("_NsPrefDef")) || node.hasLabel(Label.label("_n10sValidatorConfig"))
        || node.hasLabel(Label.label("_GraphConfig"));
  }

  @Override
  protected void processPropOnRel(Set<Statement> statementSet,
      Statement baseStatement, String key, Object propertyValueObject) {


      if (!exportOnlyMappedElems || exportMappings.containsKey(key)) {
        IRI predicate = (exportMappings.containsKey(key) ? vf.createIRI(exportMappings.get(key)) :
            vf.createIRI(BASE_VOCAB_NS, key));
        if (propertyValueObject instanceof Object[]) {
          for (Object o : (Object[]) propertyValueObject) {
            statementSet.add(vf.createStatement(vf.createTriple(
                baseStatement.getSubject(), baseStatement.getPredicate(), baseStatement.getObject()),
                predicate, createTypedLiteral(o)));
          }
        } else {
          statementSet.add(vf.createStatement(vf.createTriple(
              baseStatement.getSubject(), baseStatement.getPredicate(), baseStatement.getObject()),
              predicate, createTypedLiteral(propertyValueObject)));
        }
      }

  }

  public Stream<Statement> streamNodesBySearch(String label, String property, String propVal,
      String valType, boolean includeContext) {
    Set<Statement> result = new HashSet<>();
    Map<Long, IRI> ontologyEntitiesUris = new HashMap<>();
    ResourceIterator<Node> nodes = tx.findNodes(Label.label(label), property,
        (valType == null ? propVal : castValue(valType, propVal)));
    while (nodes.hasNext()) {
      Node node = nodes.next();
      result.addAll(processNode(node, ontologyEntitiesUris, null));
      if (includeContext) {
        Iterable<Relationship> relationships = node.getRelationships();
        for (Relationship rel : relationships) {
          Statement baseStatement = processRelationship(rel, ontologyEntitiesUris);
          result.add(baseStatement);
          if(this.exportPropertiesInRels) {
            rel.getAllProperties().forEach((k, v) -> processPropOnRel(result, baseStatement, k, v));
          }
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


  @Override
  protected Statement processRelationship(Relationship rel, Map<Long, IRI> ontologyEntitiesUris) {

    Statement statement = null;

    //TODO: FIX THIS USING THE GraphConfig
    if (rel.getType().name().equals("SCO") || rel.getType().name().equals("SPO") ||
        rel.getType().name().equals("DOMAIN") || rel.getType().name().equals("RANGE")) {
      //if it's  an ontlogy rel, it must apply to an ontology entity
      if (!ontologyEntitiesUris.containsKey(rel.getStartNode().getId())) {
        ontologyEntitiesUris.put(rel.getStartNode().getId(),
            vf.createIRI(BASE_VOCAB_NS,
                (String) rel.getStartNode().getProperty("name", "unnamedEntity")));
      }
      if (!ontologyEntitiesUris.containsKey(rel.getEndNode().getId())) {
        ontologyEntitiesUris.put(rel.getEndNode().getId(),
            vf.createIRI(BASE_VOCAB_NS,
                (String) rel.getEndNode().getProperty("name", "unnamedEntity")));
      }
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


  @Override
  protected  Set<Statement> processNode(Node node, Map<Long, IRI> ontologyEntitiesUris, String propNameFilter) {
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
                createTypedLiteral(o)));
          }
        } else {
          statements.add(vf.createStatement(subject, predicate,
              createTypedLiteral(propertyValueObject)));
        }
      }

    }
    return statements;
  }



  @Override
  public Stream<Statement> streamTriplesFromTriplePattern(TriplePattern tp)
          throws InvalidNamespacePrefixDefinitionInDB {
    //unimplemented
    return null;
  }

}
