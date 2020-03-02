package semantics;

import static semantics.config.Params.BASE_VOCAB_NS;
import static semantics.config.Params.CUSTOM_DATA_TYPE_SEPERATOR;
import static semantics.config.Params.PREFIX_SEPARATOR;

import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;
import org.eclipse.rdf4j.model.IRI;
import org.eclipse.rdf4j.model.Literal;
import org.eclipse.rdf4j.model.Resource;
import org.eclipse.rdf4j.model.Statement;
import org.eclipse.rdf4j.model.ValueFactory;
import org.eclipse.rdf4j.model.impl.SimpleValueFactory;
import org.eclipse.rdf4j.model.vocabulary.OWL;
import org.eclipse.rdf4j.model.vocabulary.RDF;
import org.eclipse.rdf4j.model.vocabulary.RDFS;
import org.eclipse.rdf4j.model.vocabulary.XMLSchema;
import org.eclipse.rdf4j.rio.RDFHandlerException;
import org.neo4j.graphdb.*;
import semantics.config.Params;


public class LPGRDFToRDFProcesssor {

  private final Pattern customDataTypedLiteralShortenedURIPattern = Pattern.compile(
      "(.+)" + Pattern.quote(CUSTOM_DATA_TYPE_SEPERATOR) + "(\\w+)" + Pattern
          .quote(PREFIX_SEPARATOR) + "(.+)$");

  private final Map<String, String> namespaces;
  private Transaction tx;
  private GraphDatabaseService graphdb;
  private final ValueFactory vf = SimpleValueFactory.getInstance();


  public LPGRDFToRDFProcesssor(GraphDatabaseService graphdb, Transaction tx) {
    this.graphdb = graphdb;
    this.tx = tx;
    this.namespaces = getNamespacesFromDB(graphdb);

  }


  public Stream<Statement> streamLocalImplicitOntology() {
    Set<Statement> statements = new HashSet<>();
    Result res = tx.execute("CALL db.schema.visualization() ");

    Map<String, Object> next = res.next();
    List<Node> nodeList = (List<Node>) next.get("nodes");
    nodeList.forEach(node -> {
      String catName = node.getAllProperties().get("name").toString();
      if (!catName.equals("Resource") && !catName.equals("NamespacePrefixDefinition")) {
        IRI subject = vf.createIRI(buildURI(BASE_VOCAB_NS, catName, namespaces));
        statements.add(vf.createStatement(subject, RDF.TYPE, OWL.CLASS));
        statements.add(vf.createStatement(subject, RDFS.LABEL,
            vf.createLiteral(subject.getLocalName())));
      }
    });

    List<Relationship> relationshipList = (List<Relationship>) next.get("relationships");
    for (Relationship r : relationshipList) {
      IRI relUri = vf
          .createIRI(buildURI(BASE_VOCAB_NS, r.getType().name(), namespaces));
      statements.add(vf.createStatement(relUri, RDF.TYPE, OWL.OBJECTPROPERTY));
      statements.add(vf.createStatement(relUri, RDFS.LABEL,
          vf.createLiteral(relUri.getLocalName())));
      String domainClassStr = r.getStartNode().getLabels().iterator().next().name();
      if (!domainClassStr.equals("Resource")) {
        IRI domainUri = vf
            .createIRI(buildURI(BASE_VOCAB_NS, domainClassStr, namespaces));
        statements.add(vf.createStatement(relUri, RDFS.DOMAIN, domainUri));
      }
      String rangeClassStr = r.getEndNode().getLabels().iterator().next().name();
      if (!rangeClassStr.equals("Resource")) {
        IRI rangeUri = vf
            .createIRI(buildURI(BASE_VOCAB_NS, rangeClassStr, namespaces));
        statements.add(vf.createStatement(relUri, RDFS.RANGE, rangeUri));
      }
    }

    return statements.stream();
  }


  private Map<String, String> getNamespacesFromDB(GraphDatabaseService graphdb) {

    Result nslist = tx.execute("MATCH (n:NamespacePrefixDefinition) \n" +
        "UNWIND keys(n) AS namespace\n" +
        "RETURN namespace, n[namespace] AS prefix");

    Map<String, String> result = new HashMap<>();
    while (nslist.hasNext()) {
      Map<String, Object> ns = nslist.next();
      result.put((String) ns.get("namespace"), (String) ns.get("prefix"));
    }
    return result;
  }

  private String buildURI(String baseVocabNS, String name, Map<String, String> namespaces) {
    Pattern regex = Pattern.compile("^(\\w+)" + PREFIX_SEPARATOR + "(.*)$");
    Matcher matcher = regex.matcher(name);
    if (matcher.matches()) {
      String prefix = matcher.group(1);
      String uriPrefix = getKeyFromValue(prefix);
      String localName = matcher.group(2);
      return uriPrefix + localName;
    } else if (name.startsWith("http")) {
      //TODO make this test better?
      return name;
    } else {
      return baseVocabNS + name;
    }

  }

  private String getKeyFromValue(String prefix) {
    for (String key : namespaces.keySet()) {
      if (namespaces.get(key).equals(prefix)) {
        return key;
      }
    }
    throw new MissingNamespacePrefixDefinition("Prefix ".concat(prefix)
        .concat(" in use but not defined in the 'NamespacePrefixDefinition' node"));
  }

  private class MissingNamespacePrefixDefinition extends RDFHandlerException {

    MissingNamespacePrefixDefinition(String msg) {
      super("RDF Serialization ERROR: ".concat(msg));
    }
  }


  public Stream<Statement> streamTriplesFromCypher(String cypher, Map<String, Object> params) {
    Set<Statement> statementResults = new HashSet<>();
    final Result result = this.tx.execute(cypher, params);
    Map<Long, IRI> ontologyEntitiesUris = new HashMap<>();

    Set<ContextResource> serializedNodes = new HashSet<>();

    while (result.hasNext()) {
      Map<String, Object> row = result.next();
      Set<Entry<String, Object>> entries = row.entrySet();
      for (Entry<String, Object> entry : entries) {
        Object o = entry.getValue();
        if (o instanceof org.neo4j.graphdb.Path) {
          org.neo4j.graphdb.Path path = (org.neo4j.graphdb.Path) o;
          path.nodes().forEach(n -> {
            ContextResource currentContextResource = new ContextResource(
                n.hasProperty("uri") ?
                    n.getProperty("uri").toString() : null,
                n.hasProperty("graphUri") ?
                    n.getProperty("graphUri").toString() : null);
            if (!serializedNodes.contains(currentContextResource)) {
              statementResults.addAll(processNode(n));
              serializedNodes.add(currentContextResource);
            }
          });
          path.relationships().forEach(
              r -> statementResults.addAll(processRelationship(r)));
        } else if (o instanceof Node) {
          Node node = (Node) o;
          ContextResource currentContextResource = new ContextResource(
              node.hasProperty("uri") ?
                  node.getProperty("uri").toString() : null,
              node.hasProperty("graphUri") ?
                  node.getProperty("graphUri").toString() : null);
          if (StreamSupport.stream(node.getLabels().spliterator(), false)
              .anyMatch(name -> Label.label("Resource").equals(name)) &&
              !serializedNodes.contains(currentContextResource)) {
            statementResults.addAll(processNode(node));
            serializedNodes.add(currentContextResource);
          }
        } else if (o instanceof Relationship) {
          statementResults.addAll(processRelationship((Relationship) o));
        }
      }
    }
    return statementResults.stream();
  }

  public Stream<Statement> streamNodeByUri(String uri, String graphId, boolean excludeContext) {

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
        statementResults.addAll(processNode(node));
        doneOnce = true;
      }
      Relationship rel = (Relationship) row.get("r");
      if (rel != null) {
        statementResults.addAll(processRelationship(rel));
      }
    }
    return statementResults.stream();
  }

  private Set<Statement> processRelationship(Relationship rel) {
    Set<Statement> result = new HashSet<>();
    Resource subject = buildSubjectOrContext(rel.getStartNode().getProperty("uri").toString());
    IRI predicate = vf.createIRI(buildURI(BASE_VOCAB_NS, rel.getType().name(), namespaces));
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
    result.add(vf.createStatement(subject, predicate, object, context));

    return result;
  }

  private Set<Statement> processNode(Node node) {
    Set<Statement> result = new HashSet<>();
    Iterable<Label> nodeLabels = node.getLabels();
    for (Label label : nodeLabels) {
      //Exclude the URI, Resource and Bnode categories created by the importer to emulate RDF
      if (!(label.name().equals("Resource") || label.name().equals("URI") ||
          label.name().equals("BNode"))) {
        result.add(vf.createStatement(
            buildSubjectOrContext(node.getProperty("uri").toString()),
            RDF.TYPE,
            vf.createIRI(buildURI(BASE_VOCAB_NS, label.name(), namespaces)),
            node.hasProperty("graphUri") ? vf
                .createIRI(node.getProperty("graphUri").toString()) : null));

      }
    }

    Map<String, Object> allProperties = node.getAllProperties();
    for (String key : allProperties.keySet()) {
      if (!key.equals("uri") && !key.equals("graphUri")) {
        Resource subject = buildSubjectOrContext(node.getProperty("uri").toString());
        IRI predicate = vf.createIRI(buildURI(BASE_VOCAB_NS, key, namespaces));
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


  private String buildCustomDTFromShortURI(String literal) {
    Matcher matcher = customDataTypedLiteralShortenedURIPattern.matcher(literal);
    if (matcher.matches()) {
      String value = matcher.group(1);
      String prefix = matcher.group(2);
      String uriPrefix = getKeyFromValue(prefix);
      String localName = matcher.group(3);
      return value + CUSTOM_DATA_TYPE_SEPERATOR + uriPrefix + localName;
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
