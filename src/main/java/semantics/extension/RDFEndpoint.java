package semantics.extension;

import static semantics.RDFImport.CUSTOM_DATA_TYPE_SEPERATOR;
import static semantics.RDFImport.PREFIX_SEPARATOR;
import static semantics.mapping.MappingUtils.getExportMappingsFromDB;

import java.io.IOException;
import java.io.OutputStream;
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
import java.util.stream.StreamSupport;
import javax.ws.rs.GET;
import javax.ws.rs.HeaderParam;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.WebApplicationException;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.StreamingOutput;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.type.TypeReference;
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
import org.eclipse.rdf4j.rio.RDFFormat;
import org.eclipse.rdf4j.rio.RDFHandlerException;
import org.eclipse.rdf4j.rio.RDFWriter;
import org.eclipse.rdf4j.rio.Rio;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.ResourceIterator;
import org.neo4j.graphdb.Result;
import org.neo4j.graphdb.Transaction;
import org.neo4j.logging.Log;
import semantics.ContextResource;

/**
 * Created by jbarrasa on 08/09/2016.
 */
@Path("/")
public class RDFEndpoint {

  private static final String BASE_VOCAB_NS = "neo4j://vocabulary#";
  private static final String BASE_INDIV_NS = "neo4j://individuals#";
  private static final ObjectMapper objectMapper = new ObjectMapper();
  private static RDFFormat[] availableParsers = new RDFFormat[]{RDFFormat.RDFXML, RDFFormat.JSONLD,
      RDFFormat.TURTLE, RDFFormat.NTRIPLES, RDFFormat.TRIG, RDFFormat.NQUADS};
  private final Pattern langTagPattern = Pattern.compile("^(.*)@([a-z,\\-]+)$");
  private final Pattern customDataTypePattern = Pattern
      .compile("^(.*)" + Pattern.quote(CUSTOM_DATA_TYPE_SEPERATOR) + "(.*)$");
  private final Pattern customDataTypedLiteralShortenedURIPattern = Pattern.compile(
      "(.+)" + Pattern.quote(CUSTOM_DATA_TYPE_SEPERATOR) + "(\\w+)" + Pattern
          .quote(PREFIX_SEPARATOR) + "(.+)$");

  @Context
  public Log log;

  @POST
  @Path("/cypher")
  @Produces({"application/rdf+xml", "text/plain", "text/turtle", "text/n3",
      "application/trig", "application/ld+json", "application/n-quads"})
  public Response cypherOnPlainLPG(@Context GraphDatabaseService gds,
      @HeaderParam("accept") String acceptHeaderParam, String body) {
    return Response.ok().entity(new StreamingOutput() {
      @Override
      public void write(OutputStream outputStream) throws IOException, WebApplicationException {
        Map<String, Object> jsonMap = objectMapper
            .readValue(body,
                new TypeReference<Map<String, Object>>() {
                });
        try (Transaction tx = gds.beginTx()) {
          final boolean onlyMapped = jsonMap.containsKey("mappedElemsOnly");
          final Map<String, Object> queryParams = (Map<String, Object>) jsonMap
              .getOrDefault("cypherParams", new HashMap<String, Object>());
          Result result = gds.execute((String) jsonMap.get("cypher"), queryParams);
          Set<Long> serializedNodes = new HashSet<Long>();
          RDFWriter writer = Rio
              .createWriter(getFormat(acceptHeaderParam, (String) jsonMap.get("format")),
                  outputStream);
          SimpleValueFactory valueFactory = SimpleValueFactory.getInstance();
          handleNamespaces(writer, gds);
          writer.handleNamespace("rdf", RDF.NAMESPACE);
          writer.handleNamespace("neovoc", BASE_VOCAB_NS);
          writer.handleNamespace("neoind", BASE_INDIV_NS);
          writer.startRDF();
          Map<String, String> mappings = getExportMappingsFromDB(gds);
          while (result.hasNext()) {
            Map<String, Object> row = result.next();
            Set<Map.Entry<String, Object>> entries = row.entrySet();
            for (Map.Entry<String, Object> entry : entries) {
              Object o = entry.getValue();
              if (o instanceof org.neo4j.graphdb.Path) {
                org.neo4j.graphdb.Path path = (org.neo4j.graphdb.Path) o;
                path.nodes().forEach(n -> {
                  if (!serializedNodes.contains(n.getId())) {
                    processNodeInLPG(writer, valueFactory, mappings, n, onlyMapped);
                    serializedNodes.add(n.getId());
                  }
                });
                path.relationships()
                    .forEach(
                        r -> processRelOnLPG(writer, valueFactory, mappings, r, onlyMapped));
              } else if (o instanceof Node) {
                Node node = (Node) o;
                if (!serializedNodes.contains(node.getId())) {
                  processNodeInLPG(writer, valueFactory, mappings, node, onlyMapped);
                  serializedNodes.add(node.getId());
                }
              } else if (o instanceof Relationship) {
                processRelOnLPG(writer, valueFactory, mappings, (Relationship) o, onlyMapped);
              }
            }
          }
          writer.endRDF();
          result.close();
        } catch (Exception e) {
          handleSerialisationError(outputStream, e, acceptHeaderParam,
              (String) jsonMap.get("format"));
        }
      }
    }).build();
  }

  @POST
  @Path("/cypheronrdf")
  @Produces({"application/rdf+xml", "text/plain", "text/turtle", "text/n3",
      "application/trig", "application/ld+json", "application/n-quads"})
  public Response cypherOnImportedRDF(@Context GraphDatabaseService gds,
      @HeaderParam("accept") String acceptHeaderParam, String body) {
    return Response.ok().entity(new StreamingOutput() {
      @Override
      public void write(OutputStream outputStream) throws IOException, WebApplicationException {

        Map<String, String> namespaces = getNamespacesFromDB(gds);
        Map<String, Object> jsonMap = objectMapper
            .readValue(body,
                new TypeReference<Map<String, Object>>() {
                });
        try (Transaction tx = gds.beginTx(); Result result = gds.execute((String) jsonMap.get("cypher"), (Map<String, Object>) jsonMap.getOrDefault("cypherParams", new HashMap<String, Object>()))) {
          final boolean onlyMapped = jsonMap.containsKey("mappedElemsOnly");

          Set<ContextResource> serializedNodes = new HashSet<ContextResource>();
          RDFWriter writer = Rio
              .createWriter(getFormat(acceptHeaderParam, (String) jsonMap.get("format")),
                  outputStream);
          SimpleValueFactory valueFactory = SimpleValueFactory.getInstance();
          writer.handleNamespace("owl", OWL.NAMESPACE);
          writer.handleNamespace("rdfs", RDFS.NAMESPACE);
          writer.handleNamespace("rdf", RDF.NAMESPACE);
          writer.handleNamespace("neovoc", BASE_VOCAB_NS);
          writer.handleNamespace("neoind", BASE_INDIV_NS);
          writer.startRDF();
          boolean doneOnce = false;
          while (result.hasNext()) {
            Map<String, Object> row = result.next();
            Set<Map.Entry<String, Object>> entries = row.entrySet();
            for (Map.Entry<String, Object> entry : entries) {
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
                    processNode(namespaces, writer, valueFactory, BASE_VOCAB_NS, n);
                    serializedNodes.add(currentContextResource);
                  }
                });
                path.relationships().forEach(
                    r -> processRelationship(namespaces, writer, valueFactory, BASE_VOCAB_NS, r));
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
                  processNode(namespaces, writer, valueFactory, BASE_VOCAB_NS, node);
                  serializedNodes.add(currentContextResource);
                }
              } else if (o instanceof Relationship) {
                processRelationship(namespaces, writer, valueFactory, BASE_VOCAB_NS,
                    (Relationship) o);
              }
            }
          }
          writer.endRDF();
        } catch (Exception e) {
          handleSerialisationError(outputStream, e, acceptHeaderParam,
              (String) jsonMap.get("format"));
        }

      }
    }).build();
  }

  private void processRelationship(Map<String, String> namespaces, RDFWriter writer,
      SimpleValueFactory valueFactory, String baseVocabNS, Relationship rel) {
    Resource subject = buildSubjectOrContext(rel.getStartNode().getProperty("uri").toString(),
        valueFactory);
    IRI predicate = valueFactory.createIRI(buildURI(baseVocabNS, rel.getType().name(), namespaces));
    Resource object = buildSubjectOrContext(rel.getEndNode().getProperty("uri").toString(),
        valueFactory);
    Resource context = null;
    if (rel.getStartNode().hasProperty("graphUri") && rel.getEndNode().hasProperty("graphUri")) {
      if (rel.getStartNode().getProperty("graphUri").toString()
          .equals(rel.getEndNode().getProperty("graphUri").toString())) {
        context = buildSubjectOrContext(rel.getStartNode().getProperty("graphUri").toString(),
            valueFactory);
      } else {
        throw new IllegalStateException(
            "Graph uri of a statement has to be the same for both start and end node of the relationship!");
      }
    } else if (rel.getStartNode().hasProperty("graphUri") != rel.getEndNode()
        .hasProperty("graphUri")) {
      throw new IllegalStateException(
          "Graph uri of a statement has to be the same for both start and end node of the relationship!");
    }
    writer.handleStatement(valueFactory.createStatement(subject, predicate, object, context));
  }

  private void processNode(Map<String, String> namespaces, RDFWriter writer,
      SimpleValueFactory valueFactory,
      String baseVocabNS, Node node) {
    Iterable<Label> nodeLabels = node.getLabels();
    for (Label label : nodeLabels) {
      //Exclude the URI, Resource and Bnode categories created by the importer to emulate RDF
      if (!(label.name().equals("Resource") || label.name().equals("URI") ||
          label.name().equals("BNode"))) {
        writer.handleStatement(
            valueFactory
                .createStatement(
                    buildSubjectOrContext(node.getProperty("uri").toString(), valueFactory),
                    RDF.TYPE,
                    valueFactory.createIRI(buildURI(baseVocabNS, label.name(), namespaces)),
                    node.hasProperty("graphUri") ? valueFactory
                        .createIRI(node.getProperty("graphUri").toString()) : null));

      }
    }
    Map<String, Object> allProperties = node.getAllProperties();
    for (String key : allProperties.keySet()) {
      if (!key.equals("uri") && !key.equals("graphUri")) {
        Resource subject = buildSubjectOrContext(node.getProperty("uri").toString(), valueFactory);
        IRI predicate = valueFactory.createIRI(buildURI(baseVocabNS, key, namespaces));
        Object propertyValueObject = allProperties.get(key);
        Resource context = null;
        if (node.hasProperty("graphUri")) {
          context = buildSubjectOrContext(node.getProperty("graphUri").toString(), valueFactory);
        }
        if (propertyValueObject instanceof long[]) {
          for (int i = 0; i < ((long[]) propertyValueObject).length; i++) {
            Literal object = createTypedLiteral(valueFactory,
                ((long[]) propertyValueObject)[i]);
            writer.handleStatement(
                valueFactory.createStatement(subject, predicate, object, context));
          }
        } else if (propertyValueObject instanceof double[]) {
          for (int i = 0; i < ((double[]) propertyValueObject).length; i++) {
            Literal object = createTypedLiteral(valueFactory,
                ((double[]) propertyValueObject)[i]);
            writer.handleStatement(
                valueFactory.createStatement(subject, predicate, object, context));
          }
        } else if (propertyValueObject instanceof boolean[]) {
          for (int i = 0; i < ((boolean[]) propertyValueObject).length; i++) {
            Literal object = createTypedLiteral(valueFactory,
                ((boolean[]) propertyValueObject)[i]);
            writer.handleStatement(
                valueFactory.createStatement(subject, predicate, object, context));
          }
        } else if (propertyValueObject instanceof LocalDateTime[]) {
          for (int i = 0; i < ((LocalDateTime[]) propertyValueObject).length; i++) {
            Literal object = createTypedLiteral(valueFactory,
                ((LocalDateTime[]) propertyValueObject)[i]);
            writer.handleStatement(
                valueFactory.createStatement(subject, predicate, object, context));
          }
        } else if (propertyValueObject instanceof LocalDate[]) {
          for (int i = 0; i < ((LocalDate[]) propertyValueObject).length; i++) {
            Literal object = createTypedLiteral(valueFactory,
                ((LocalDate[]) propertyValueObject)[i]);
            writer.handleStatement(
                valueFactory.createStatement(subject, predicate, object, context));
          }
        } else if (propertyValueObject instanceof Object[]) {
          for (int i = 0; i < ((Object[]) propertyValueObject).length; i++) {
            Literal object = createTypedLiteral(valueFactory,
                (buildCustomDTFromShortURI((String) ((Object[]) propertyValueObject)[i],
                    namespaces)));
            writer.handleStatement(
                valueFactory.createStatement(subject, predicate, object, context));
          }
        } else {
          Literal object;
          if (propertyValueObject instanceof String) {
            object = createTypedLiteral(valueFactory,
                (buildCustomDTFromShortURI((String) propertyValueObject, namespaces)));
          } else {
            object = createTypedLiteral(valueFactory, propertyValueObject);
          }
          writer.handleStatement(
              valueFactory.createStatement(subject, predicate, object, context));
        }
      }
    }
  }

  private Resource buildSubjectOrContext(String id, ValueFactory vf) {
    Resource result;
    try {
      result = vf.createIRI(id);
    } catch (IllegalArgumentException e) {
      result = vf.createBNode(id);
    }

    return result;
  }


  @GET
  @Path("/describe/uri/{nodeuri}")
  @Produces({"application/rdf+xml", "text/plain", "text/turtle", "text/n3",
      "application/trig", "application/ld+json", "application/n-quads"})
  public Response nodebyuri(@Context GraphDatabaseService gds,
      @PathParam("nodeuri") String uriParam,
      @QueryParam("graphuri") String graphUriParam,
      @QueryParam("excludeContext") String excludeContextParam,
      @QueryParam("format") String format,
      @HeaderParam("accept") String acceptHeaderParam) {
    return Response.ok().entity(new StreamingOutput() {
      @Override
      public void write(OutputStream outputStream) throws IOException, WebApplicationException {

        Map<String, String> namespaces = getNamespacesFromDB(gds);
        String queryWithContext;
        String queryNoContext;
        Map<String, Object> params = new HashMap<>();
        params.put("uri", uriParam);
        if (graphUriParam == null || graphUriParam.equals("")) {
          queryWithContext = "MATCH (x:Resource {uri:{uri}}) " +
              "WHERE NOT EXISTS(x.graphUri)\n" +
              "OPTIONAL MATCH (x)-[r]-(val:Resource) " +
              "WHERE exists(val.uri)\n" +
              "AND NOT EXISTS(val.graphUri)\n" +
              "RETURN x, r, val.uri AS value";

          queryNoContext = "MATCH (x:Resource {uri:{uri}}) " +
              "WHERE NOT EXISTS(x.graphUri)\n" +
              "RETURN x, null AS r, null AS value";
        } else {
          queryWithContext = "MATCH (x:Resource {uri:{uri}, graphUri:{graphUri}}) " +
              "OPTIONAL MATCH (x)-[r]-(val:Resource {graphUri:{graphUri}}) " +
              "WHERE exists(val.uri)\n" +
              "RETURN x, r, val.uri AS value";

          queryNoContext = "MATCH (x:Resource {uri:{uri}, graphUri:{graphUri}}) " +
              "RETURN x, null AS r, null AS value";
          params.put("graphUri", graphUriParam);
        }
        try (Transaction tx = gds.beginTx()) {
          Result result = gds
              .execute((excludeContextParam != null ? queryNoContext : queryWithContext),
                  params);

          RDFWriter writer = Rio.createWriter(getFormat(acceptHeaderParam, format), outputStream);
          SimpleValueFactory valueFactory = SimpleValueFactory.getInstance();
          writer.handleNamespace("rdf", RDF.NAMESPACE);
          writer.handleNamespace("neovoc", BASE_VOCAB_NS);
          writer.handleNamespace("neoind", BASE_INDIV_NS);
          writer.startRDF();
          boolean doneOnce = false;
          while (result.hasNext()) {
            Map<String, Object> row = result.next();
            Node node = (Node) row.get("x");
            if (!doneOnce) {
              //Output only once the props of the selected node as literal properties
              processNode(namespaces, writer, valueFactory, BASE_VOCAB_NS, node);
              doneOnce = true;
            }
            Relationship rel = (Relationship) row.get("r");
            if (rel != null) {
              processRelationship(namespaces, writer, valueFactory, BASE_VOCAB_NS, rel);
            }
          }
          writer.endRDF();
          result.close();
        } catch (Exception e) {
          handleSerialisationError(outputStream, e, acceptHeaderParam, format);
        }

      }
    }).build();
  }

  private void handleSerialisationError(OutputStream outputStream, Exception e,
      @HeaderParam("accept") String acceptHeaderParam, @QueryParam("format") String format) {
    //output the error message using the right serialisation
    //TODO: maybe seiralise all that can be serialised and just comment the offending triples?
    RDFWriter writer = Rio.createWriter(getFormat(acceptHeaderParam, format), outputStream);
    writer.startRDF();
    writer.handleComment(e.getMessage());
    writer.endRDF();
  }

  private Resource getResource(String s, ValueFactory vf) {
    // taken from org.eclipse.rdf4j.model.impl.SimpleIRI
    // explicit storage of blank nodes in the graph to be considered
    if (s.indexOf(58) >= 0) {
      return vf.createIRI(s);
    } else {
      return vf.createBNode(s);
    }
  }

  private Map<String, String> getNamespacesFromDB(GraphDatabaseService graphdb) {

    Result nslist = graphdb.execute("MATCH (n:NamespacePrefixDefinition) \n" +
        "UNWIND keys(n) AS namespace\n" +
        "RETURN namespace, n[namespace] AS prefix");

    Map<String, String> result = new HashMap<String, String>();
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
      String uriPrefix = getKeyFromValue(prefix, namespaces);
      String localName = matcher.group(2);
      return uriPrefix + localName;
    } else if (name.startsWith("http")) {
      //make this test better
      return name;
    } else {
      return baseVocabNS + name;
    }

  }

  private String buildCustomDTFromShortURI(String literal, Map<String, String> namespaces) {
    Matcher matcher = customDataTypedLiteralShortenedURIPattern.matcher(literal);
    if (matcher.matches()) {
      String value = matcher.group(1);
      String prefix = matcher.group(2);
      String uriPrefix = getKeyFromValue(prefix, namespaces);
      String localName = matcher.group(3);
      return value + CUSTOM_DATA_TYPE_SEPERATOR + uriPrefix + localName;
    } else {
      return literal;
    }
  }

  private String getKeyFromValue(String prefix, Map<String, String> namespaces) {
    for (String key : namespaces.keySet()) {
      if (namespaces.get(key).equals(prefix)) {
        return key;
      }
    }
    throw new MissingNamespacePrefixDefinition("Prefix ".concat(prefix)
        .concat(" in use but not defined in the 'NamespacePrefixDefinition' node"));
  }

  private String getPrefix(String namespace, Map<String, String> namespaces) {
    if (namespaces.containsKey(namespace)) {
      return namespaces.get(namespace);
    } else {
      return namespace;
    }
  }

  @GET
  @Path("/describe/id/{nodeid}")
  @Produces({"application/rdf+xml", "text/plain", "text/turtle", "text/n3",
      "application/trig", "application/ld+json", "application/n-quads"})
  public Response nodebyid(@Context GraphDatabaseService gds, @PathParam("nodeid") Long idParam,
      @QueryParam("excludeContext") String excludeContextParam,
      @QueryParam("mappedElemsOnly") String onlyMappedInfo,
      @QueryParam("format") String format,
      @HeaderParam("accept") String acceptHeaderParam) {
    return Response.ok().entity(new StreamingOutput() {
      @Override
      public void write(OutputStream outputStream) throws IOException, WebApplicationException {

        RDFWriter writer = Rio.createWriter(getFormat(acceptHeaderParam, format), outputStream);
        SimpleValueFactory valueFactory = SimpleValueFactory.getInstance();
        handleNamespaces(writer, gds);
        writer.handleNamespace("rdf", RDF.NAMESPACE);
        writer.handleNamespace("neovoc", BASE_VOCAB_NS);
        writer.handleNamespace("neoind", BASE_INDIV_NS);
        writer.startRDF();
        try (Transaction tx = gds.beginTx()) {
          Map<String, String> mappings = getExportMappingsFromDB(gds);
          Node node = (Node) gds.getNodeById(idParam);
          processNodeInLPG(writer, valueFactory, mappings, node, onlyMappedInfo != null);
          if (excludeContextParam == null) {
            processRelsOnLPG(writer, valueFactory, mappings, node, onlyMappedInfo != null);
          }
          writer.endRDF();
        } catch (Exception e) {
          handleSerialisationError(outputStream, e, acceptHeaderParam, format);
        }
      }


    }).build();
  }


  @GET
  @Path("/describe/find/{label}/{property}/{propertyValue}")
  @Produces({"application/rdf+xml", "text/plain", "text/turtle", "text/n3",
      "application/trig", "application/ld+json", "application/n-quads"})
  public Response nodefind(@Context GraphDatabaseService gds, @PathParam("label") String label,
      @PathParam("property") String property, @PathParam("propertyValue") String propVal,
      @QueryParam("valType") String valType,
      @QueryParam("excludeContext") String excludeContextParam,
      @QueryParam("mappedElemsOnly") String onlyMappedInfo,
      @QueryParam("format") String format,
      @HeaderParam("accept") String acceptHeaderParam) {
    return Response.ok().entity(new StreamingOutput() {
      @Override
      public void write(OutputStream outputStream) throws IOException, WebApplicationException {

        RDFWriter writer = Rio.createWriter(getFormat(acceptHeaderParam, format), outputStream);
        SimpleValueFactory valueFactory = SimpleValueFactory.getInstance();
        handleNamespaces(writer, gds);
        writer.handleNamespace("rdf", RDF.NAMESPACE);
        writer.handleNamespace("neovoc", BASE_VOCAB_NS);
        writer.handleNamespace("neoind", BASE_INDIV_NS);
        writer.startRDF();
        try (Transaction tx = gds.beginTx()) {
          Map<String, String> mappings = getExportMappingsFromDB(gds);
          ResourceIterator<Node> nodes = gds.findNodes(Label.label(label), property,
              (valType == null ? propVal : castValue(valType, propVal)));
          while (nodes.hasNext()) {
            Node node = nodes.next();
            processNodeInLPG(writer, valueFactory, mappings, node, onlyMappedInfo != null);
            if (excludeContextParam == null) {
              processRelsOnLPG(writer, valueFactory, mappings, node, onlyMappedInfo != null);
            }
          }
          writer.endRDF();
        } catch (Exception e) {
          handleSerialisationError(outputStream, e, acceptHeaderParam, format);
        }
      }


    }).build();
  }

  private Object castValue(String valType, String propVal) {
    if (valType.equals("INTEGER")) {
      return Integer.valueOf(propVal);
    } else if (valType.equals("FLOAT")) {
      return Float.valueOf(propVal);
    } else if (valType.equals("BOOLEAN")) {
      return Boolean.valueOf(propVal);
    } else {
      return propVal;
    }
  }

  private void processRelsOnLPG(RDFWriter writer, SimpleValueFactory valueFactory,
      Map<String, String> mappings,
      Node node, boolean onlyMappedInfo) {
    Iterable<Relationship> relationships = node.getRelationships();
    relationships
        .forEach(rel -> processRelOnLPG(writer, valueFactory, mappings, rel, onlyMappedInfo));
  }

  private void processRelOnLPG(RDFWriter writer, SimpleValueFactory valueFactory,
      Map<String, String> mappings,
      Relationship rel, boolean onlyMappedInfo) {
    if (!onlyMappedInfo || mappings.containsKey(rel.getType().name())) {
      writer.handleStatement(valueFactory.createStatement(
          valueFactory.createIRI(BASE_INDIV_NS, String.valueOf(rel.getStartNode().getId())),
          valueFactory.createIRI(
              mappings.get(rel.getType().name()) != null ? mappings.get(rel.getType().name())
                  :
                      BASE_VOCAB_NS + rel.getType().name()),
          valueFactory.createIRI(BASE_INDIV_NS, String.valueOf(rel.getEndNode().getId()))));
    }
  }

  private void processNodeInLPG(RDFWriter writer, SimpleValueFactory valueFactory,
      Map<String, String> mappings,
      Node node, boolean onlyMappedInfo) {
    Iterable<Label> nodeLabels = node.getLabels();
    IRI subject = valueFactory.createIRI(BASE_INDIV_NS, String.valueOf(node.getId()));
    for (Label label : nodeLabels) {
      if (!onlyMappedInfo || mappings.containsKey(label.name())) {
        writer.handleStatement(
            valueFactory.createStatement(subject,
                RDF.TYPE,
                valueFactory
                    .createIRI(
                        mappings.get(label.name()) != null ? mappings.get(label.name())
                            :
                                BASE_VOCAB_NS + label.name())));
      }
    }
    Map<String, Object> allProperties = node.getAllProperties();
    for (String key : allProperties.keySet()) {
      if (!onlyMappedInfo || mappings.containsKey(key)) {
        IRI predicate = valueFactory
            .createIRI(
                mappings.get(key) != null ? mappings.get(key) : BASE_VOCAB_NS + key);
        Object propertyValueObject = allProperties.get(key);
        if (propertyValueObject instanceof Object[]) {
          for (Object o : (Object[]) propertyValueObject) {
            writer.handleStatement(valueFactory.createStatement(subject, predicate,
                createTypedLiteral(valueFactory, o)));
          }
        } else {
          writer.handleStatement(valueFactory.createStatement(subject, predicate,
              createTypedLiteral(valueFactory, propertyValueObject)));
        }
      }

    }
  }

  private void handleNamespaces(RDFWriter writer, GraphDatabaseService gds) {
    writer.handleNamespace("neovoc", BASE_VOCAB_NS);
    writer.handleNamespace("neoind", BASE_INDIV_NS);
    gds.execute(
        "MATCH (mns:_MapNs) WHERE exists(mns._prefix) RETURN mns._ns AS ns, mns._prefix AS prefix").
        forEachRemaining(
            result -> writer
                .handleNamespace((String) result.get("prefix"), (String) result.get("ns")));
  }

  @GET
  @Path("/ping")
  public Response ping() throws IOException {
    Map<String, String> results = new HashMap<String, String>() {{
      put("ping", "here!");
    }};
    return Response.ok().entity(objectMapper.writeValueAsString(results)).build();
  }

  @GET
  @Path("/onto")
  @Produces({"application/rdf+xml", "text/plain", "text/turtle", "text/n3",
      "application/trig", "application/ld+json", "application/n-quads"})
  public Response exportOnto(@Context GraphDatabaseService gds, @QueryParam("format") String format,
      @HeaderParam("accept") String acceptHeaderParam) {
    return Response.ok().entity(new StreamingOutput() {
      @Override
      public void write(OutputStream outputStream) throws IOException, WebApplicationException {
        RDFWriter writer = Rio.createWriter(getFormat(acceptHeaderParam, format), outputStream);
        SimpleValueFactory valueFactory = SimpleValueFactory.getInstance();
        writer.handleNamespace("owl", OWL.NAMESPACE);
        writer.handleNamespace("rdfs", RDFS.NAMESPACE);
        writer.handleNamespace("rdf", RDF.NAMESPACE);
        writer.handleNamespace("neovoc", BASE_VOCAB_NS);
        writer.handleNamespace("neoind", BASE_INDIV_NS);
        writer.startRDF();
        try (Transaction tx = gds.beginTx()) {
          Result res = gds.execute("CALL db.schema() ");
          Set<Statement> publishedStatements = new HashSet<>();
          Map<String, Object> next = res.next();
          List<Node> nodeList = (List<Node>) next.get("nodes");
          nodeList.forEach(node -> {
            String catName = node.getAllProperties().get("name").toString();
            // Resource and NamespacePrefix should be named _Resource... to avoid conflicts
            if (!catName.equals("Resource") && !catName.equals("NamespacePrefixDefinition")) {
              IRI subject = valueFactory.createIRI(BASE_VOCAB_NS, catName);
              publishStatement(publishedStatements, writer,
                  valueFactory.createStatement(subject, RDF.TYPE, OWL.CLASS));
              publishStatement(publishedStatements, writer,
                  valueFactory
                      .createStatement(subject, RDFS.LABEL, valueFactory.createLiteral(catName)));
            }
          });

          List<Relationship> relationshipList = (List<Relationship>) next.get("relationships");
          for (Relationship r : relationshipList) {
            IRI relUri = valueFactory.createIRI(BASE_VOCAB_NS, r.getType().name());
            publishStatement(publishedStatements, writer, (
                valueFactory.createStatement(relUri, RDF.TYPE, OWL.OBJECTPROPERTY)));
            publishStatement(publishedStatements, writer,
                valueFactory.createStatement(relUri, RDFS.LABEL,
                    valueFactory.createLiteral(r.getType().name())));
            String domainLabel = r.getStartNode().getLabels().iterator().next().name();
            // Resource should be named _Resource... to avoid conflicts
            if (!domainLabel.equals("Resource")) {
              publishStatement(publishedStatements, writer,
                  valueFactory.createStatement(relUri, RDFS.DOMAIN,
                      valueFactory.createIRI(BASE_VOCAB_NS, domainLabel)));
            }
            String rangeLabel = r.getEndNode().getLabels().iterator().next().name();
            // Resource should be named _Resource... to avoid conflicts
            if (!rangeLabel.equals("Resource")) {
              publishStatement(publishedStatements, writer,
                  valueFactory.createStatement(relUri, RDFS.RANGE,
                      valueFactory.createIRI(BASE_VOCAB_NS, rangeLabel)));
            }
          }

          writer.endRDF();

        } catch (Exception e) {
          handleSerialisationError(outputStream, e, acceptHeaderParam, format);
        }
      }
    }).build();
  }

  @GET
  @Path("/ontonrdf")
  @Produces({"application/rdf+xml", "text/plain", "text/turtle", "text/n3",
      "application/trig", "application/ld+json", "application/n-quads"})
  public Response exportRdfOnto(@Context GraphDatabaseService gds,
      @QueryParam("format") String format,
      @HeaderParam("accept") String acceptHeaderParam) {
    return Response.ok().entity(new StreamingOutput() {
      @Override
      public void write(OutputStream outputStream) throws IOException, WebApplicationException {

        Map<String, String> namespaces = getNamespacesFromDB(gds);
        String baseVocabNS = "neo4j://vocabulary#";

        RDFWriter writer = Rio.createWriter(getFormat(acceptHeaderParam, format), outputStream);
        SimpleValueFactory valueFactory = SimpleValueFactory.getInstance();
        writer.handleNamespace("owl", OWL.NAMESPACE);
        writer.handleNamespace("rdfs", RDFS.NAMESPACE);
        writer.handleNamespace("rdf", RDF.NAMESPACE);
        writer.handleNamespace("neovoc", BASE_VOCAB_NS);
        writer.handleNamespace("neoind", BASE_INDIV_NS);
        writer.startRDF();

        try (Transaction tx = gds.beginTx()) {
          Set<Statement> publishedStatements = new HashSet<>();
          Result res = gds.execute("CALL db.schema() ");

          Map<String, Object> next = res.next();
          List<Node> nodeList = (List<Node>) next.get("nodes");
          nodeList.forEach(node -> {
            String catName = node.getAllProperties().get("name").toString();
            if (!catName.equals("Resource") && !catName.equals("NamespacePrefixDefinition")) {
              IRI subject = valueFactory.createIRI(buildURI(BASE_VOCAB_NS, catName, namespaces));
              publishStatement(publishedStatements, writer,
                  valueFactory.createStatement(subject, RDF.TYPE, OWL.CLASS));
              publishStatement(publishedStatements, writer,
                  valueFactory.createStatement(subject, RDFS.LABEL,
                      valueFactory.createLiteral(subject.getLocalName())));
            }
          });

          List<Relationship> relationshipList = (List<Relationship>) next.get("relationships");
          for (Relationship r : relationshipList) {
            IRI relUri = valueFactory
                .createIRI(buildURI(BASE_VOCAB_NS, r.getType().name(), namespaces));
            publishStatement(publishedStatements, writer,
                valueFactory.createStatement(relUri, RDF.TYPE, OWL.OBJECTPROPERTY));
            publishStatement(publishedStatements, writer,
                valueFactory.createStatement(relUri, RDFS.LABEL,
                    valueFactory.createLiteral(relUri.getLocalName())));
            String domainClassStr = r.getStartNode().getLabels().iterator().next().name();
            if (!domainClassStr.equals("Resource")) {
              IRI domainUri = valueFactory
                  .createIRI(buildURI(BASE_VOCAB_NS, domainClassStr, namespaces));
              publishStatement(publishedStatements, writer,
                  valueFactory.createStatement(relUri, RDFS.DOMAIN, domainUri));
            }
            String rangeClassStr = r.getEndNode().getLabels().iterator().next().name();
            if (!rangeClassStr.equals("Resource")) {
              IRI rangeUri = valueFactory
                  .createIRI(buildURI(BASE_VOCAB_NS, rangeClassStr, namespaces));
              publishStatement(publishedStatements, writer,
                  valueFactory.createStatement(relUri, RDFS.RANGE, rangeUri));
            }
          }

          writer.endRDF();
        } catch (Exception e) {
          handleSerialisationError(outputStream, e, acceptHeaderParam, format);
        }
      }
    }).build();
  }

  private void publishStatement(Set<Statement> publishedStatements, RDFWriter writer,
      Statement statement) {
    // the call to db.schema generates all combinations of source-target for rels so we
    // filter duplicate statements
    if (!publishedStatements.contains(statement)) {
      publishedStatements.add(statement);
      writer.handleStatement(statement);
    }
  }


  private Literal createTypedLiteral(SimpleValueFactory valueFactory, Object value) {
    Literal result;
    if (value instanceof Integer) {
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

  private RDFFormat getFormat(String mimetype, String formatParam) {
    // format request param overrides the one defined in the accept header param
    if (formatParam != null) {
      log.info("serialization from param in request: " + formatParam);
      for (RDFFormat parser : availableParsers) {
        if (parser.getName().contains(formatParam)) {
          log.info("parser to be used: " + parser.getDefaultMIMEType());
          return parser;
        }
      }
    } else {
      if (mimetype != null) {
        log.info("serialization from media type in request: " + mimetype);
        for (RDFFormat parser : availableParsers) {
          if (parser.getMIMETypes().contains(mimetype)) {
            log.info("parser to be used: " + parser.getDefaultMIMEType());
            return parser;
          }
        }
      }
    }

    log.info("Unrecognized or undefined serialization. Defaulting to Turtle serialization");

    return RDFFormat.TURTLE;

  }

  private class MissingNamespacePrefixDefinition extends RDFHandlerException {

    public MissingNamespacePrefixDefinition(
        String msg) {
      super("RDF Serialization ERROR: ".concat(msg));
    }
  }
}
