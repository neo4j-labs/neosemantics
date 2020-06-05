package n10s.rdf.export;

import static n10s.graphconfig.GraphConfig.GRAPHCONF_MULTIVAL_PROP_ARRAY;
import static n10s.graphconfig.GraphConfig.GRAPHCONF_RDFTYPES_AS_LABELS;
import static n10s.graphconfig.Params.BASE_VOCAB_NS;
import static n10s.graphconfig.Params.CUSTOM_DATA_TYPE_SEPERATOR;
import static n10s.graphconfig.Params.PREFIX_SEPARATOR;
import static n10s.utils.UriUtils.translateUri;

import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
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
import n10s.utils.NsPrefixMap;
import n10s.utils.UriUtils.UriNamespaceHasNoAssociatedPrefix;
import org.eclipse.rdf4j.model.IRI;
import org.eclipse.rdf4j.model.Literal;
import org.eclipse.rdf4j.model.Resource;
import org.eclipse.rdf4j.model.Statement;
import org.eclipse.rdf4j.model.Value;
import org.eclipse.rdf4j.model.ValueFactory;
import org.eclipse.rdf4j.model.impl.SimpleValueFactory;
import org.eclipse.rdf4j.model.vocabulary.OWL;
import org.eclipse.rdf4j.model.vocabulary.RDF;
import org.eclipse.rdf4j.model.vocabulary.RDFS;
import org.eclipse.rdf4j.model.vocabulary.XMLSchema;
import org.eclipse.rdf4j.rio.RDFHandlerException;
import org.neo4j.graphdb.Direction;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.graphdb.Result;
import org.neo4j.graphdb.Transaction;


public class LPGRDFToRDFProcesssor extends ExportProcessor {

  private final Pattern customDataTypedLiteralShortenedURIPattern = Pattern.compile(
      "(.+)" + Pattern.quote(CUSTOM_DATA_TYPE_SEPERATOR) + "(\\w+)" + Pattern
          .quote(PREFIX_SEPARATOR) + "(.+)$");

  private final NsPrefixMap namespaces;


  public LPGRDFToRDFProcesssor(GraphDatabaseService graphdb, Transaction tx, GraphConfig gc, boolean isRDFStarSerialisation)
      throws InvalidNamespacePrefixDefinitionInDB {
    super(tx,graphdb, gc);
    this.exportPropertiesInRels = isRDFStarSerialisation;
    this.namespaces = new NsPrefixMap(tx, false);

  }


  public Stream<Statement> streamLocalImplicitOntology() {
    Set<Statement> statements = new HashSet<>();
    Result res = tx.execute("CALL db.schema.visualization() ");

    Map<String, Object> next = res.next();
    List<Node> nodeList = (List<Node>) next.get("nodes");
    nodeList.forEach(node -> {
      String catName = node.getAllProperties().get("name").toString();
      if (!catName.equals("Resource") && !catName.equals("_NsPrefDef")
          && !catName.equals("_GraphConfig") && !catName.equals("_n10sValidatorConfig")
          && !catName.equals("_MapNs") && !catName.equals("_MapDef") && !catName.equals("_GraphConfig")) {
        IRI subject = vf.createIRI(buildURI(BASE_VOCAB_NS, catName));
        statements.add(vf.createStatement(subject, RDF.TYPE, OWL.CLASS));
        statements.add(vf.createStatement(subject, RDFS.LABEL,
            vf.createLiteral(subject.getLocalName())));
      }
    });

    List<Relationship> relationshipList = (List<Relationship>) next.get("relationships");
    for (Relationship r : relationshipList) {
      IRI relUri = vf
          .createIRI(buildURI(BASE_VOCAB_NS, r.getType().name()));
      statements.add(vf.createStatement(relUri, RDF.TYPE, OWL.OBJECTPROPERTY));
      statements.add(vf.createStatement(relUri, RDFS.LABEL,
          vf.createLiteral(relUri.getLocalName())));
      String domainClassStr = r.getStartNode().getLabels().iterator().next().name();
      if (!domainClassStr.equals("Resource")) {
        IRI domainUri = vf
            .createIRI(buildURI(BASE_VOCAB_NS, domainClassStr));
        statements.add(vf.createStatement(relUri, RDFS.DOMAIN, domainUri));
      }
      String rangeClassStr = r.getEndNode().getLabels().iterator().next().name();
      if (!rangeClassStr.equals("Resource")) {
        IRI rangeUri = vf
            .createIRI(buildURI(BASE_VOCAB_NS, rangeClassStr));
        statements.add(vf.createStatement(relUri, RDFS.RANGE, rangeUri));
      }
    }

    return statements.stream();
  }

  private String buildURI(String baseVocabNS, String name) {
    //TODO: we know what kind of graph we have from the config (fix this)
    Pattern regex = Pattern.compile("^(\\w+)" + PREFIX_SEPARATOR + "(.*)$");
    Matcher matcher = regex.matcher(name);
    if (matcher.matches()) {
      String prefix = matcher.group(1);
      String uriNsPart = namespaces.getNsForPrefix(prefix);
      if (uriNsPart == null) {
        throw new MissingNamespacePrefixDefinition("Prefix ".concat(prefix)
            .concat(" in use but not in the namespace prefix definition"));
      }
      String localName = matcher.group(2);
      return uriNsPart + localName;
    } else if (name.startsWith("http")) {
      //TODO make this test better?
      return name;
    } else {
      return baseVocabNS + name;
    }

  }

  private class MissingNamespacePrefixDefinition extends RDFHandlerException {

    MissingNamespacePrefixDefinition(String msg) {
      super("RDF Serialization ERROR: ".concat(msg));
    }
  }

  public Stream<Statement> streamNodeByUri(String uri, String graphId, boolean excludeContext) {

    //TODO: Until import RDF* is implemnted, there is no way this can return reified statements
    String queryWithContext;
    String queryNoContext;
    Map<String, Object> params = new HashMap<>();
    params.put("uri", uri);
    if (graphId == null || graphId.equals("")) {
      queryWithContext = "MATCH (x:Resource {uri:$uri}) " +
          "WHERE NOT EXISTS(x.graphUri)\n" +
          "OPTIONAL MATCH (x)-[r]-(val:Resource) " +
          "WHERE exists(val.uri)\n" +
          "AND NOT EXISTS(val.graphUri)\n" +
          "RETURN x, r, val.uri AS value";

      queryNoContext = "MATCH (x:Resource {uri:$uri}) " +
          "WHERE NOT EXISTS(x.graphUri)\n" +
          "RETURN x, null AS r, null AS value";
    } else {
      queryWithContext = "MATCH (x:Resource {uri:$uri, graphUri:$graphUri}) " +
          "OPTIONAL MATCH (x)-[r]-(val:Resource {graphUri:$graphUri}) " +
          "WHERE exists(val.uri)\n" +
          "RETURN x, r, val.uri AS value";

      queryNoContext = "MATCH (x:Resource {uri:$uri, graphUri:$graphUri}) " +
          "RETURN x, null AS r, null AS value";
      params.put("graphUri", graphId);
    }

    Set<Statement> statementResults = new HashSet<>();

    Result result = tx
        .execute((excludeContext ? queryNoContext : queryWithContext), params);

    boolean doneOnce = false;
    while (result.hasNext()) {
      Map<String, Object> row = result.next();
      Node node = (Node) row.get("x");
      if (!doneOnce) {
        //Output only once the props of the selected node as literal properties
        statementResults.addAll(processNode(node, null, null));
        doneOnce = true;
      }
      Relationship rel = (Relationship) row.get("r");
      if (rel != null) {
        // no need to  check rels connect to other resources as we're sure they will since they come
        //  in/out of a Resource
        statementResults.add(processRelationship(rel, null));
      }
    }
    return statementResults.stream();
  }

  @Override
  protected boolean filterRelationship(Relationship rel, Map<Long, IRI> ontologyEntitiesUris) {
    //TODO: this type check is going to slow down the query. think how to improve it
    return !rel.getStartNode().hasLabel(Label.label("Resource")) ||
        !rel.getEndNode().hasLabel(Label.label("Resource"));
  }

  @Override
  protected boolean filterNode(Node node, Map<Long, IRI> ontologyEntitiesUris) {
    return !node.hasLabel(Label.label("Resource"));
  }

  @Override
  protected void processPropOnRel(Set<Statement> rowResult, Statement baseStatement, String key,
      Object val) {
    //TODO implement
  }

  @Override
  protected Statement processRelationship(Relationship rel, Map<Long, IRI> ontologyEntitiesUris) {
    Resource subject = buildSubjectOrContext(rel.getStartNode().getProperty("uri").toString());
    IRI predicate = vf.createIRI(buildURI(BASE_VOCAB_NS, rel.getType().name()));
    Resource object = buildSubjectOrContext(rel.getEndNode().getProperty("uri").toString());
    Resource context = null;
    if (rel.getStartNode().hasProperty("graphUri") && rel.getEndNode().hasProperty("graphUri")) {
      if (rel.getStartNode().getProperty("graphUri").toString()
          .equals(rel.getEndNode().getProperty("graphUri").toString())) {
        context = buildSubjectOrContext(rel.getStartNode().getProperty("graphUri").toString());
      } else {
        throw new IllegalStateException(
            "Graph uri of a statement has to be the same for both start and end node of the relationship!");
      }
    } else if (rel.getStartNode().hasProperty("graphUri") != rel.getEndNode()
        .hasProperty("graphUri")) {
      throw new IllegalStateException(
          "Graph uri of a statement has to be the same for both start and end node of the relationship!");
    }
    return vf.createStatement(subject, predicate, object, context);
  }

  @Override
  protected Set<Statement> processNode(Node node, Map<Long, IRI> ontologyEntitiesUris, String propNameFilter) {
    //TODO:  Ontology entities not used here. Rethink???
    Set<Statement> result = new HashSet<>();
    if(propNameFilter==null || propNameFilter.equals(RDF.TYPE.stringValue())
            || propNameFilter.equals("rdf__type")) {
      //labels  not to be exported if there's a filter on the property
      Iterable<Label> nodeLabels = node.getLabels();
      for (Label label : nodeLabels) {
        if (!label.name().equals("Resource")) {
          result.add(vf.createStatement(
              buildSubjectOrContext(node.getProperty("uri").toString()),
              RDF.TYPE,
              vf.createIRI(buildURI(BASE_VOCAB_NS, label.name())),
              node.hasProperty("graphUri") ? vf
                  .createIRI(node.getProperty("graphUri").toString()) : null));

        }
      }
    }

    Map<String, Object> allProperties = node.getAllProperties();
    for (String key : allProperties.keySet()) {
      if (!key.equals("uri") && !key.equals("graphUri") && (propNameFilter==null || key.equals(propNameFilter))) {
        Resource subject = buildSubjectOrContext(node.getProperty("uri").toString());
        IRI predicate = vf.createIRI(buildURI(BASE_VOCAB_NS, key));
        Object propertyValueObject = allProperties.get(key);
        Resource context = null;
        if (node.hasProperty("graphUri")) {
          context = buildSubjectOrContext(node.getProperty("graphUri").toString());
        }
        if (propertyValueObject instanceof long[]) {
          for (int i = 0; i < ((long[]) propertyValueObject).length; i++) {
            Literal object = createTypedLiteral(((long[]) propertyValueObject)[i]);
            result.add(
                vf.createStatement(subject, predicate, object, context));
          }
        } else if (propertyValueObject instanceof double[]) {
          for (int i = 0; i < ((double[]) propertyValueObject).length; i++) {
            Literal object = createTypedLiteral(((double[]) propertyValueObject)[i]);
            result.add(
                vf.createStatement(subject, predicate, object, context));
          }
        } else if (propertyValueObject instanceof boolean[]) {
          for (int i = 0; i < ((boolean[]) propertyValueObject).length; i++) {
            Literal object = createTypedLiteral(((boolean[]) propertyValueObject)[i]);
            result.add(
                vf.createStatement(subject, predicate, object, context));
          }
        } else if (propertyValueObject instanceof LocalDateTime[]) {
          for (int i = 0; i < ((LocalDateTime[]) propertyValueObject).length; i++) {
            Literal object = createTypedLiteral(((LocalDateTime[]) propertyValueObject)[i]);
            result.add(
                vf.createStatement(subject, predicate, object, context));
          }
        } else if (propertyValueObject instanceof LocalDate[]) {
          for (int i = 0; i < ((LocalDate[]) propertyValueObject).length; i++) {
            Literal object = createTypedLiteral(((LocalDate[]) propertyValueObject)[i]);
            result.add(
                vf.createStatement(subject, predicate, object, context));
          }
        } else if (propertyValueObject instanceof Object[]) {
          for (int i = 0; i < ((Object[]) propertyValueObject).length; i++) {
            Literal object = createTypedLiteral(
                (buildCustomDTFromShortURI((String) ((Object[]) propertyValueObject)[i])));
            result.add(
                vf.createStatement(subject, predicate, object, context));
          }
        } else {
          Literal object;
          if (propertyValueObject instanceof String) {
            object = createTypedLiteral(
                (buildCustomDTFromShortURI((String) propertyValueObject)));
          } else {
            object = createTypedLiteral(propertyValueObject);
          }
          result.add(
              vf.createStatement(subject, predicate, object, context));
        }
      }
    }
    return result;
  }

  @Override
  public Stream<Statement> streamTriplesFromTriplePattern(TriplePattern tp)
      throws InvalidNamespacePrefixDefinitionInDB {

    if (tp.getSubject() != null){
      Set<Statement> allStatements = new HashSet<>();
      Node resource = tx.findNode(Label.label("Resource"), "uri", tp.getSubject());
      if (resource != null) {
        String predicate = null;
        try {
          predicate = tp.getPredicate() != null ? translateUri(tp.getPredicate(), tx, graphConfig) : null;
        } catch (UriNamespaceHasNoAssociatedPrefix e) {
          //graph is in shorten mode but the uri in the filter is not in use in the graph
          predicate = tp.getPredicate();
          //ugly way of making the filter not return anything.
          //TODO: Check this has no unexpected result in rare corner cases
        }
        if (tp.getObject() == null) {
          //labels and properties
          allStatements.addAll(processNode(resource, null, predicate));
          //relationships
          Iterable<Relationship> relationships =
                  tp.getPredicate() == null ? resource.getRelationships(Direction.OUTGOING) : resource.getRelationships(
                          Direction.OUTGOING, RelationshipType.withName(predicate));
          for (Relationship r : relationships) {
            allStatements.add(processRelationship(r, null));
          }
        } else {
          //filter on value (object)
          Value object = getValueFromTriplePatternObject(tp);
          allStatements.addAll(processNode(resource, null, predicate).stream()
                  .filter(st -> st.getObject().equals(object)).collect(Collectors.toSet()));

          //if filter on object  is of type literal then we  can skip the rels, it will be a prop
          if (!tp.getLiteral()) {
            Iterable<Relationship> relationships =
                    tp.getPredicate() == null ? resource.getRelationships(Direction.OUTGOING)
                            : resource.getRelationships(
                            Direction.OUTGOING, RelationshipType.withName(predicate));
            for (Relationship r : relationships) {
              if (r.getOtherNode(resource).getProperty("uri").equals(object.stringValue())) {
                allStatements.add(processRelationship(r, null));
              }
            }
          }
        }
      }
      return allStatements.stream();
    }
    else {
      String predicate = null;
      try {
        predicate = tp.getPredicate() != null?translateUri(tp.getPredicate(),tx, graphConfig):null;
      }  catch (UriNamespaceHasNoAssociatedPrefix e) {
        //graph is in shorten mode but the uri in the filter is not in use in the graph
        predicate = tp.getPredicate();
        //ugly way of making the filter not return anything.
      }
      if(tp.getObject()==null) {
        //null,x,null
        Result result;
        if(predicate!=null) {
          //null, pred, null
          if(tp.getPredicate().equals(RDF.TYPE.stringValue()) &&
              graphConfig.getHandleRDFTypes()== GRAPHCONF_RDFTYPES_AS_LABELS){
            result = tx.execute("MATCH (r:Resource) WHERE size(labels(r))>1 RETURN r");
          }  else {
            result = tx.execute(String
                .format("MATCH (r:Resource) WHERE exists(r.`%s`) RETURN r\n"
                        + "UNION \n"
                        + "MATCH (:Resource)-[r:`%s`]->() RETURN r",
                    predicate, predicate));
          }
        } else {
          //no subject, pred, no object: null, null, null -> return all triples
          result = tx.execute("MATCH (r:Resource) RETURN r\n"
                      + "UNION \n"
                      + "MATCH (:Resource)-[r]->() RETURN r");
        }
        String finalPredicate = predicate;
        return result.stream().flatMap(row -> {
          Set<Statement> rowResult = new HashSet<>();
          Object r = row.get("r");
          if (r instanceof Node) {
            rowResult.addAll(processNode((Node) r, null, finalPredicate));
          } else if (r instanceof Relationship) {
            rowResult.add(processRelationship((Relationship) r, null));
          }
          return rowResult.stream();
        });
      } else {
        //filter on value (object)
        Value object = getValueFromTriplePatternObject(tp);
        Result result;
        Map<String, Object> params = new HashMap<>();
        if(predicate!=null) {
          // null, pred, obj
          if(tp.getPredicate().equals(RDF.TYPE.stringValue()) &&
              graphConfig.getHandleRDFTypes()== GRAPHCONF_RDFTYPES_AS_LABELS){
            String objectAsShortenedUri = null;
            if (object instanceof IRI) {
              try {
                objectAsShortenedUri = translateUri(object.stringValue(), tx, graphConfig);
              } catch (UriNamespaceHasNoAssociatedPrefix e) {
                //TODO: is this ok
                e.printStackTrace();
              }
            } else {
              //TODO: maye this should just folllow the previous for the  case of the exception
              // otherwise objectAsShortenedUri stays null. -->> UnitTest
              objectAsShortenedUri = "____";
            }
            result = tx.execute(String.format("MATCH (r:`%s`) RETURN r",objectAsShortenedUri));

          }  else {
            if (object instanceof IRI) {
              params.put("uri", object.stringValue());
              //query for relationships
              result = tx.execute(String
                  .format("MATCH (:Resource)-[r:`%s`]->(o:Resource { uri:  $uri }) RETURN r",
                      predicate), params);
            } else {
              //it's a Literal
              params.put("propVal",
                  object.stringValue());//translateLiteral((Literal)object, graphConfig));
              if(graphConfig.getHandleMultival() == GRAPHCONF_MULTIVAL_PROP_ARRAY &&
                      ( graphConfig.getMultivalPropList() == null ||
                              graphConfig.getMultivalPropList().contains(tp.getPredicate()))){
                result = tx.execute(String
                        .format("MATCH (r:Resource) WHERE $propVal in r.`%s` RETURN r",
                                predicate), params);
              } else {
                result = tx.execute(String
                        .format("MATCH (r:Resource) WHERE r.`%s` = $propVal RETURN r",
                                predicate), params);
              }
            }
          }
        } else {
          //null, null, obj
          if (object instanceof IRI) {
            //query for relationships
            params.put("uri", object.stringValue());
            result = tx.execute("MATCH ()-[r]->(o:Resource { uri:  $uri }) RETURN r", params);
          } else {
            //it's a Literal
            params.put("propVal",
                object.stringValue());//translateLiteral((Literal)object, graphConfig));
            result = tx.execute("MATCH (r:Resource) UNWIND keys(r) as propName \n"
                        + "WITH r, propName\n"
                        + "WHERE $propVal in [] + r[propName] \n"
                        + "RETURN r, propName", params);
          }

        }
        //refactor with previous section
        String finalPredicate1 = predicate;
        return result.stream().flatMap(row -> {
          Set<Statement> rowResult = new HashSet<>();
          Object r = row.get("r");
          if(r instanceof Node){
            rowResult.addAll(processNode((Node)r, null,
                (finalPredicate1!=null?finalPredicate1:(String)row.get("propName"))));
          } else if(r instanceof Relationship){
            rowResult.add(processRelationship((Relationship)r,null));
          }
          return rowResult.stream();
        }).filter(st -> st.getObject().equals(object));
        //post filtering on the generated statements.
      }
    }

  }




  private Value getValueFromTriplePatternObject(TriplePattern tp) {
    Value object;
    if (tp.getLiteral()) {
      if (tp.getLiteralLang() != null) {
        object = vf.createLiteral(tp.getObject(), tp.getLiteralLang());
      } else {
        object = vf.createLiteral(tp.getObject(), vf.createIRI(tp.getLiteralType()));
      }
    } else {
      object = vf.createIRI(tp.getObject());
    }
    return object;
  }


  private String buildCustomDTFromShortURI(String literal) {
    Matcher matcher = customDataTypedLiteralShortenedURIPattern.matcher(literal);
    if (matcher.matches()) {
      String value = matcher.group(1);
      String prefix = matcher.group(2);
      String uriNsPart = namespaces.getNsForPrefix(prefix);
      if (uriNsPart == null) {
        throw new MissingNamespacePrefixDefinition("Prefix ".concat(prefix)
            .concat(" in use but not in the namespace prefix definition"));
      }
      String localName = matcher.group(3);
      return value + CUSTOM_DATA_TYPE_SEPERATOR + uriNsPart + localName;
    } else {
      return literal;
    }
  }

  private Resource buildSubjectOrContext(String id) {
    Resource result;
    try {
      result = vf.createIRI(id);
    } catch (IllegalArgumentException e) {
      result = vf.createBNode(id);
    }

    return result;
  }

  public Stream<Statement> streamLocalExplicitOntology(Map<String, Object> params) {
    final ValueFactory vf = SimpleValueFactory.getInstance();

    return null;
  }

  private Literal createTypedLiteral(Object value) {
    Literal result;
    if (value instanceof String) {
      result = getLiteralWithTagOrDTIfPresent((String) value, vf);
    } else if (value instanceof Integer) {
      result = vf.createLiteral((Integer) value);
    } else if (value instanceof Long) {
      result = vf.createLiteral((Long) value);
    } else if (value instanceof Float) {
      result = vf.createLiteral((Float) value);
    } else if (value instanceof Double) {
      result = vf.createLiteral((Double) value);
    } else if (value instanceof Boolean) {
      result = vf.createLiteral((Boolean) value);
    } else if (value instanceof LocalDateTime) {
      result = vf
          .createLiteral(((LocalDateTime) value).format(DateTimeFormatter.ISO_LOCAL_DATE_TIME),
              XMLSchema.DATETIME);
    } else if (value instanceof LocalDate) {
      result = vf
          .createLiteral(((LocalDate) value).format(DateTimeFormatter.ISO_LOCAL_DATE),
              XMLSchema.DATE);
    } else {
      // default to string
      result = getLiteralWithTagOrDTIfPresent((String) value, vf);
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
