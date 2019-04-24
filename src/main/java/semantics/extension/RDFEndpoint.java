package semantics.extension;

import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.type.TypeReference;
import org.eclipse.rdf4j.model.vocabulary.OWL;
import org.eclipse.rdf4j.model.vocabulary.RDFS;
import org.neo4j.graphdb.*;
import org.neo4j.graphdb.NotFoundException;
import org.neo4j.logging.Log;
import org.eclipse.rdf4j.model.IRI;
import org.eclipse.rdf4j.model.Literal;
import org.eclipse.rdf4j.model.Resource;
import org.eclipse.rdf4j.model.ValueFactory;
import org.eclipse.rdf4j.model.impl.SimpleValueFactory;
import org.eclipse.rdf4j.model.vocabulary.RDF;
import org.eclipse.rdf4j.rio.RDFFormat;
import org.eclipse.rdf4j.rio.RDFWriter;
import org.eclipse.rdf4j.rio.Rio;

import javax.ws.rs.*;
import javax.ws.rs.Path;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.StreamingOutput;
import java.io.IOException;
import java.io.OutputStream;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static semantics.RDFImport.PREFIX_SEPARATOR;
import static semantics.mapping.MappingUtils.getExportMappingsFromDB;

/**
 * Created by jbarrasa on 08/09/2016.
 */
@Path("/")
public class RDFEndpoint {

    public static final String BASE_VOCAB_NS = "neo4j://com.neo4j/voc#";
    public static final String BASE_INDIV_NS = "neo4j://com.neo4j/indiv#";

    @Context
    public Log log;

    private static final ObjectMapper objectMapper = new ObjectMapper();

    public static RDFFormat[] availableParsers = new RDFFormat[]{RDFFormat.RDFXML, RDFFormat.JSONLD, RDFFormat.TURTLE,
            RDFFormat.NTRIPLES, RDFFormat.TRIG};

    private final Pattern langTagPattern = Pattern.compile("^(.*)@([a-z,\\-]+)$");

    @POST
    @Path("/cypher")
    @Produces({"application/rdf+xml", "text/plain", "text/turtle", "text/n3", "application/trix", "application/x-trig",
            "application/ld+json"})
    public Response cypherOnPlainLPG(@Context GraphDatabaseService gds,
                                     @HeaderParam("accept") String acceptHeaderParam, String body) {
        return Response.ok().entity(new StreamingOutput() {
            @Override
            public void write(OutputStream outputStream) throws IOException, WebApplicationException {

                try (Transaction tx = gds.beginTx()) {
                    Map<String, String> jsonMap = objectMapper.readValue(body, new TypeReference<Map<String,String>>(){});
                    final boolean onlyMapped = jsonMap.containsKey("showOnlyMapped");
                    Result result = gds.execute(jsonMap.get("cypher"));
                    Set<Long> serializedNodes = new HashSet<Long>();
                    RDFWriter writer = Rio.createWriter(getFormat(acceptHeaderParam, jsonMap.get("format")), outputStream);
                    SimpleValueFactory valueFactory = SimpleValueFactory.getInstance();
                    handleNamespaces(writer,gds);
                    writer.startRDF();
                    Map<String, String> mappings = getExportMappingsFromDB(gds);
                    while (result.hasNext()) {
                        Map<String, Object> row = result.next();
                        Set<Map.Entry<String, Object>> entries = row.entrySet();
                        for (Map.Entry<String, Object> entry : entries) {
                            Object o = entry.getValue();
                            if (o instanceof org.neo4j.graphdb.Path) {
                                org.neo4j.graphdb.Path path = (org.neo4j.graphdb.Path)o;
                                path.nodes().forEach(n -> { if (!serializedNodes.contains(n.getId())) {
                                    processNodeInLPG(writer, valueFactory, mappings, n, onlyMapped);
                                    serializedNodes.add(n.getId());
                                }});
                                path.relationships().forEach(r -> processRelOnLPG(writer, valueFactory, mappings, r, onlyMapped));
                            } else if (o instanceof Node) {
                                Node node = (Node) o;
                                if (!serializedNodes.contains(node.getId())) {
                                    processNodeInLPG(writer, valueFactory, mappings, node, onlyMapped);
                                    serializedNodes.add(node.getId());
                                }
                            } else if (o instanceof Relationship) {
                                processRelOnLPG(writer, valueFactory, mappings,(Relationship) o, onlyMapped);
                            }
                        }
                    }
                    writer.endRDF();
                    result.close();
                }
            }
        }).build();
    }

    @POST
    @Path("/cypheronrdf")
    @Produces({"application/rdf+xml", "text/plain", "text/turtle", "text/n3", "application/trix", "application/x-trig",
            "application/ld+json"})
    public Response cypherOnImportedRDF(@Context GraphDatabaseService gds,
                                        @HeaderParam("accept") String acceptHeaderParam, String body) {
        return Response.ok().entity(new StreamingOutput() {
            @Override
            public void write(OutputStream outputStream) throws IOException, WebApplicationException {

                Map<String, String> namespaces = getNamespacesFromDB(gds);

                try (Transaction tx = gds.beginTx()) {
                    Map<String, String> jsonMap = objectMapper.readValue(body, new TypeReference<Map<String,String>>(){});
                    final boolean onlyMapped = jsonMap.containsKey("showOnlyMapped");
                    Result result = gds.execute(jsonMap.get("cypher"));
                    Set<String> serializedNodes = new HashSet<String>();
                    RDFWriter writer = Rio.createWriter(getFormat(acceptHeaderParam, jsonMap.get("format")), outputStream);
                    SimpleValueFactory valueFactory = SimpleValueFactory.getInstance();
                    String baseVocabNS = "neo4j://vocabulary#";
                    writer.handleNamespace("neovoc", baseVocabNS);
                    writer.startRDF();
                    boolean doneOnce = false;
                    while (result.hasNext()) {
                        Map<String, Object> row = result.next();
                        Set<Map.Entry<String, Object>> entries = row.entrySet();
                        for (Map.Entry<String, Object> entry : entries) {
                            Object o = entry.getValue();
                            if (o instanceof org.neo4j.graphdb.Path) {
                                org.neo4j.graphdb.Path path = (org.neo4j.graphdb.Path)o;
                                path.nodes().forEach(n -> { if (!serializedNodes.contains(n.getProperty("uri").toString())) {
                                    processNode(namespaces,writer, valueFactory, baseVocabNS, n);
                                    serializedNodes.add(n.getProperty("uri").toString());
                                }});
                                path.relationships().forEach(r -> processRelationship(namespaces, writer, valueFactory, baseVocabNS, r));
                            } else if (o instanceof Node) {
                                Node node = (Node) o;
                                if (!serializedNodes.contains(node.getProperty("uri").toString())) {
                                    processNode(namespaces, writer, valueFactory, baseVocabNS, node);
                                    serializedNodes.add(node.getProperty("uri").toString());
                                }
                            } else if (o instanceof Relationship) {
                                processRelationship(namespaces, writer, valueFactory, baseVocabNS, (Relationship) o);
                            }
                        }
                    }
                    writer.endRDF();
                    result.close();
                }

            }
        }).build();
    }

    private void processRelationship(Map<String, String> namespaces, RDFWriter writer, SimpleValueFactory valueFactory, String baseVocabNS, Relationship rel) {
        Resource subject = buildSubject(rel.getStartNode().getProperty("uri").toString(), valueFactory);
        IRI predicate = valueFactory.createIRI(buildURI(baseVocabNS, rel.getType().name(), namespaces));
        Resource object = buildSubject(rel.getEndNode().getProperty("uri").toString(), valueFactory);
        writer.handleStatement(valueFactory.createStatement(subject, predicate, object));
    }

    private void processNode(Map<String, String> namespaces, RDFWriter writer, SimpleValueFactory valueFactory, String baseVocabNS, Node node) {
        Iterable<Label> nodeLabels = node.getLabels();
        for (Label label : nodeLabels) {
            //Exclude the URI, Resource and Bnode categories created by the importer to emulate RDF
            if (!(label.name().equals("Resource") || label.name().equals("URI") ||
                    label.name().equals("BNode"))) {
                writer.handleStatement(
                        valueFactory.createStatement(buildSubject(node.getProperty("uri").toString(), valueFactory),
                                RDF.TYPE,
                                valueFactory.createIRI(buildURI(baseVocabNS, label.name(), namespaces))));

            }
        }
        Map<String, Object> allProperties = node.getAllProperties();
        for (String key : allProperties.keySet()) {
            if (!key.equals("uri")) {
                Resource subject = buildSubject(node.getProperty("uri").toString(), valueFactory);
                IRI predicate = valueFactory.createIRI(buildURI(baseVocabNS, key, namespaces));
                Object propertyValueObject = allProperties.get(key);
                if (propertyValueObject instanceof Object[]) {
                    for (int i = 0; i < ((Object[]) propertyValueObject).length; i++) {
                        Literal object = createTypedLiteral(valueFactory, ((Object[]) propertyValueObject)[i]);
                        writer.handleStatement(valueFactory.createStatement(subject, predicate, object));
                    }
                } else {
                    Literal object = createTypedLiteral(valueFactory, propertyValueObject);
                    writer.handleStatement(valueFactory.createStatement(subject, predicate, object));
                }
            }

        }
    }

    private Resource buildSubject(String id, ValueFactory vf) {
        Resource result;
        try {
            result = vf.createIRI(id);
        } catch (IllegalArgumentException e) {
            result = vf.createBNode(id);
        }

        return result;
    }


    @GET
    @Path("/describe/uri")
    @Produces({"application/rdf+xml", "text/plain", "text/turtle", "text/n3", "application/trix", "application/x-trig",
            "application/ld+json"})
    public Response nodebyuri(@Context GraphDatabaseService gds, @QueryParam("nodeuri") String idParam,
                              @QueryParam("excludeContext") String excludeContextParam,
                              @QueryParam("format") String format,
                              @HeaderParam("accept") String acceptHeaderParam) {
        return Response.ok().entity(new StreamingOutput() {
            @Override
            public void write(OutputStream outputStream) throws IOException, WebApplicationException {

                Map<String, String> namespaces = getNamespacesFromDB(gds);

                String queryWithContext = "MATCH (x:Resource {uri:{theuri}}) " +
                        "OPTIONAL MATCH (x)-[r]-(val:Resource) WHERE exists(val.uri)\n" +
                        "RETURN x, r, val.uri AS value";

                String queryNoContext = "MATCH (x:Resource {uri:{theuri}}) " +
                        "RETURN x, null AS r, null AS value";

                Map<String, Object> params = new HashMap<>();
                params.put("theuri", idParam);
                try (Transaction tx = gds.beginTx()) {
                    Result result = gds.execute((excludeContextParam != null ? queryNoContext : queryWithContext), params);


                    RDFWriter writer = Rio.createWriter(getFormat(acceptHeaderParam, format), outputStream);
                    SimpleValueFactory valueFactory = SimpleValueFactory.getInstance();
                    String baseVocabNS = "neo4j://vocabulary#";
                    writer.handleNamespace("neovoc", baseVocabNS);
                    writer.startRDF();
                    boolean doneOnce = false;
                    while (result.hasNext()) {
                        Map<String, Object> row = result.next();
                        if (!doneOnce) {
                            //Output only once the props of the selected node as literal properties
                            Node node = (Node) row.get("x");
                            Iterable<Label> nodeLabels = node.getLabels();
                            for (Label label : nodeLabels) {
                                //Exclude the URI, Resource and Bnode categories created by the importer to emulate RDF
                                if (!label.name().equals("Resource")) {

                                    writer.handleStatement(
                                            valueFactory.createStatement(getResource(idParam.toString(),valueFactory),
                                                    RDF.TYPE,
                                                    valueFactory.createIRI(buildURI(baseVocabNS, label.name(), namespaces))));
                                }
                            }
                            Map<String, Object> allProperties = node.getAllProperties();
                            for (String key : allProperties.keySet()) {
                                if (!key.equals("uri")) {
                                    Resource subject = getResource(idParam.toString(),valueFactory);
                                    IRI predicate = valueFactory.createIRI(buildURI(baseVocabNS, key, namespaces));
                                    Object propertyValueObject = allProperties.get(key);
                                    if (propertyValueObject instanceof Object[]) {
                                        for (int i = 0; i < ((Object[]) propertyValueObject).length; i++) {
                                            Literal object = createTypedLiteral(valueFactory, ((Object[]) propertyValueObject)[i]);
                                            writer.handleStatement(valueFactory.createStatement(subject, predicate, object));
                                        }
                                    } else {
                                        Literal object = createTypedLiteral(valueFactory, propertyValueObject);
                                        writer.handleStatement(valueFactory.createStatement(subject, predicate, object));
                                    }
                                }

                            }
                            doneOnce = true;
                        }
                        Relationship rel = (Relationship) row.get("r");
                        if (rel != null) {

                            Resource subject = getResource(rel.getStartNode().getProperty("uri").toString(),valueFactory);
                            IRI predicate = valueFactory.createIRI(buildURI(baseVocabNS, rel.getType().name(), namespaces));
                            IRI object = valueFactory.createIRI(rel.getEndNode().getProperty("uri").toString());
                            writer.handleStatement(valueFactory.createStatement(subject, predicate, object));
                        }
                    }
                    writer.endRDF();
                    result.close();
                }catch (Exception e){
                    System.out.println(e.getMessage());
                }

            }
        }).build();
    }

    private Resource getResource(String s, ValueFactory vf) {
        // taken from org.eclipse.rdf4j.model.impl.SimpleIRI
        // explicit storage of blank nodes in the graph to be considered
        if(s.indexOf(58) >= 0){
            return vf.createIRI(s);
        } else{
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
        //TODO
        // if uri then return as is
        Pattern regex = Pattern.compile("^(\\w+)" + PREFIX_SEPARATOR + "(.*)$");
        Matcher matcher = regex.matcher(name);
        if (matcher.matches()) {
            String prefix = matcher.group(1);
            String uriPrefix = getKeyFromValue(prefix, namespaces);
            // if namespace but does not exist, then ??? Default to default.
            // this can also be when a property name folows the name structure of pseudonamespace.

            String localName = matcher.group(2);
            return uriPrefix + localName;
        } else if (name.startsWith("http")) {
            //make this test better
            return name;
        } else {
            return baseVocabNS + name;
        }

    }

    private String getKeyFromValue(String prefix, Map<String, String> namespaces) {
        for (String key : namespaces.keySet()) {
            if (namespaces.get(key).equals(prefix)) {
                return key;
            }
        }
        return null;
    }

    private String getPrefix(String namespace, Map<String, String> namespaces) {
        if (namespaces.containsKey(namespace)) {
            return namespaces.get(namespace);
        } else {
            return namespace;
        }
    }

    @GET
    @Path("/describe/id")
    @Produces({"application/rdf+xml", "text/plain", "text/turtle", "text/n3", "application/trix", "application/x-trig",
            "application/ld+json"})
    public Response nodebyid(@Context GraphDatabaseService gds, @QueryParam("nodeid") Long idParam,
                             @QueryParam("excludeContext") String excludeContextParam,
                             @QueryParam("showOnlyMappedInfo") String onlyMappedInfo,
                             @QueryParam("format") String format,
                             @HeaderParam("accept") String acceptHeaderParam) {
        return Response.ok().entity(new StreamingOutput() {
            @Override
            public void write(OutputStream outputStream) throws IOException, WebApplicationException {

                RDFWriter writer = Rio.createWriter(getFormat(acceptHeaderParam, format), outputStream);
                SimpleValueFactory valueFactory = SimpleValueFactory.getInstance();
                handleNamespaces(writer, gds);
                writer.startRDF();
                try (Transaction tx = gds.beginTx()) {
                    Map<String, String> mappings = getExportMappingsFromDB(gds);
                    Node node = (Node) gds.getNodeById(idParam);
                    processNodeInLPG(writer, valueFactory, mappings, node, onlyMappedInfo!=null);
                    if (excludeContextParam == null) {
                        processRelsOnLPG(writer, valueFactory, mappings, node, onlyMappedInfo!=null);
                    }
                    writer.endRDF();
                }catch(NotFoundException e){
                    writer.endRDF();
                }
            }


        }).build();
    }

    private void processRelsOnLPG(RDFWriter writer, SimpleValueFactory valueFactory, Map<String, String> mappings, Node node, boolean onlyMappedInfo) {
        Iterable<Relationship> relationships = node.getRelationships();
        relationships.forEach(rel -> {
            if(!onlyMappedInfo || mappings.containsKey(rel.getType().name())) {
                writer.handleStatement(valueFactory.createStatement(
                        valueFactory.createIRI(BASE_INDIV_NS, String.valueOf(rel.getStartNode().getId())),
                        valueFactory.createIRI(mappings.get(rel.getType().name()) != null ? mappings.get(rel.getType().name()) : BASE_VOCAB_NS + rel.getType().name()),
                        valueFactory.createIRI(BASE_INDIV_NS, String.valueOf(rel.getEndNode().getId()))));
            }
        });
    }

    private void processRelOnLPG(RDFWriter writer, SimpleValueFactory valueFactory, Map<String, String> mappings, Relationship rel, boolean onlyMappedInfo) {
        if(!onlyMappedInfo || mappings.containsKey(rel.getType().name())) {
            writer.handleStatement(valueFactory.createStatement(
                    valueFactory.createIRI(BASE_INDIV_NS, String.valueOf(rel.getStartNode().getId())),
                    valueFactory.createIRI(mappings.get(rel.getType().name()) != null ? mappings.get(rel.getType().name()) : BASE_VOCAB_NS + rel.getType().name()),
                    valueFactory.createIRI(BASE_INDIV_NS, String.valueOf(rel.getEndNode().getId()))));
        }
    }

    private void processNodeInLPG(RDFWriter writer, SimpleValueFactory valueFactory, Map<String, String> mappings, Node node, boolean onlyMappedInfo) {
        Iterable<Label> nodeLabels = node.getLabels();
        IRI subject = valueFactory.createIRI(BASE_INDIV_NS, String.valueOf(node.getId()));
        for (Label label : nodeLabels) {
            if(!onlyMappedInfo || mappings.containsKey(label.name())) {
                writer.handleStatement(
                        valueFactory.createStatement(subject,
                                RDF.TYPE,
                                valueFactory.createIRI(mappings.get(label.name()) != null ? mappings.get(label.name()) : BASE_VOCAB_NS + label.name())));
            }
        }
        Map<String, Object> allProperties = node.getAllProperties();
        for (String key : allProperties.keySet()) {
            if(!onlyMappedInfo || mappings.containsKey(key)) {
                IRI predicate = valueFactory.createIRI(mappings.get(key) != null ? mappings.get(key) : BASE_VOCAB_NS + key);
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
        gds.execute("MATCH (mns:_MapNs) WHERE exists(mns._prefix) RETURN mns._ns as ns, mns._prefix as prefix").
                forEachRemaining(result -> writer.handleNamespace((String)result.get("prefix"), (String)result.get("ns")));
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
    @Produces({"application/rdf+xml", "text/plain", "text/turtle", "text/n3", "application/trix", "application/x-trig",
            "application/ld+json"})
    public Response exportOnto(@Context GraphDatabaseService gds, @QueryParam("format") String format,
                               @HeaderParam("accept") String acceptHeaderParam) {
        return Response.ok().entity(new StreamingOutput() {
            @Override
            public void write(OutputStream outputStream) throws IOException, WebApplicationException {
                RDFWriter writer = Rio.createWriter(getFormat(acceptHeaderParam, format), outputStream);
                SimpleValueFactory valueFactory = SimpleValueFactory.getInstance();
                writer.startRDF();

                Result res = gds.execute("CALL db.schema() ");

                Map<String, Object> next = res.next();
                List<Node> nodeList = (List<Node>) next.get("nodes");
                nodeList.forEach(node -> {
                    String catName = node.getAllProperties().get("name").toString();
                    IRI subject = valueFactory.createIRI(BASE_VOCAB_NS, catName);
                    writer.handleStatement(valueFactory.createStatement(subject, RDF.TYPE, OWL.CLASS));
                    writer.handleStatement(valueFactory.createStatement(subject, RDFS.LABEL, valueFactory.createLiteral(catName)));
                });

                List<Relationship> relationshipList = (List<Relationship>) next.get("relationships");
                for (Relationship r : relationshipList) {
                    IRI relUri = valueFactory.createIRI(BASE_VOCAB_NS, r.getType().name());
                    writer.handleStatement(valueFactory.createStatement(relUri, RDF.TYPE, OWL.OBJECTPROPERTY));
                    IRI domainUri = valueFactory.createIRI(BASE_VOCAB_NS, r.getStartNode().getLabels().iterator().next().name());
                    writer.handleStatement(valueFactory.createStatement(relUri, RDFS.DOMAIN, domainUri));
                    IRI rangeUri = valueFactory.createIRI(BASE_VOCAB_NS, r.getEndNode().getLabels().iterator().next().name());
                    writer.handleStatement(valueFactory.createStatement(relUri, RDFS.RANGE, rangeUri));
                }

                writer.endRDF();

            }
        }).build();
    }


    private Literal createTypedLiteral(SimpleValueFactory valueFactory, Object value) {
        Literal result = null;
        if (value instanceof String) {
            result = getLiteralWithTagIfPresent((String)value,valueFactory);
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
        } else {
            // default to string
            result = valueFactory.createLiteral("" + value);
        }

        return result;
    }

    private Literal getLiteralWithTagIfPresent(String value, ValueFactory vf) {
        Matcher m = langTagPattern.matcher(value);
        if(m.matches()){
            return vf.createLiteral(m.group(1), m.group(2));
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
        }
        else {
            if (mimetype != null) {
                log.info("serialization from mimetipe in request: " + mimetype);
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
}
