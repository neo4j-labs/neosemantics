package semantics;

import apoc.util.Util;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import org.neo4j.graphdb.*;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.logging.Log;
import org.openrdf.model.*;
import org.openrdf.model.Resource;
import org.openrdf.model.vocabulary.RDF;
import org.openrdf.model.vocabulary.XMLSchema;
import org.openrdf.rio.RDFHandler;
import org.openrdf.rio.RDFHandlerException;

import java.util.*;
import java.util.concurrent.Callable;

/**
 * Created by jbarrasa on 09/11/2016.
 */

class DirectStatementLoader implements RDFHandler, Callable<Integer> {

    public static final Label RESOURCE = Label.label("Resource");
    private final boolean shortenUris;
    private final String langFilter;
    private int ingestedTriples = 0;
    private int triplesParsed = 0;
    private GraphDatabaseService graphdb;
    private long commitFreq;
    private Map<String,Map<String,Object>> resourceProps = new HashMap<>();
    private Map<String,Set<String>> resourceLabels = new HashMap<>();
    private Set<Statement> statements = new HashSet<>();
    private Map<String,String> namespaces =  new HashMap<>();
    private final boolean labellise;
    Cache<String, Node> nodeCache;
    Log log;

    public DirectStatementLoader(GraphDatabaseService db, long batchSize, long nodeCacheSize,
                                 boolean shortenUrls, boolean typesToLabels, String languageFilter, Log l) {
        graphdb = db;
        commitFreq = batchSize;
        shortenUris = shortenUrls;
        labellise =  typesToLabels;
        nodeCache = CacheBuilder.newBuilder()
                .maximumSize(nodeCacheSize)
                .build();
        langFilter = languageFilter;
        log = l;
    }

    @Override
    public void startRDF() throws RDFHandlerException {
        getExistingNamespaces();
        log.info("Found " + namespaces.size() + " namespaces in the DB: " + namespaces);
    }

    private void getExistingNamespaces() {
        Result nslist = graphdb.execute("MATCH (n:NamespacePrefixDefinition) \n" +
                "UNWIND keys(n) AS namespace\n" +
                "RETURN namespace, n[namespace] as prefix");
        while (nslist.hasNext()){
            Map<String, Object> ns = nslist.next();
            namespaces.put((String)ns.get("namespace"),(String)ns.get("prefix"));
        }
    }

    @Override
    public void endRDF() throws RDFHandlerException {
        Util.inTx((GraphDatabaseAPI) graphdb, this);
        ingestedTriples += triplesParsed;
        addNamespaceNode();

        log.info("Successfully committed " + triplesParsed + " triples. " +
                "Total number of triples imported is " + ingestedTriples);
    }

    @Override
    public void handleNamespace(String prefix, String uri) throws RDFHandlerException {

    }

    private void addNamespaceNode() {
        Map<String, Object> params = new HashMap<>();
        params.put("props", namespaces);
        graphdb.execute("MERGE (n:NamespacePrefixDefinition) SET n+={props}", params);
    }

    private void addStatement(Statement st) {
        statements.add(st);
    }

    private void initialise(String subjectUri) {
        initialiseProps(subjectUri);
        initialiseLabels(subjectUri);
    }

    private Set<String> initialiseLabels(String subjectUri) {
        Set<String> labels =  new HashSet<>();
        resourceLabels.put(subjectUri, labels);
        return labels;
    }

    private HashMap<String, Object> initialiseProps(String subjectUri) {
        HashMap<String, Object> props = new HashMap<>();
        resourceProps.put(subjectUri,props);
        return props;
    }

    private void setProp(String subjectUri, String propName, Object propValue){
        Map<String, Object> props;

        if(!resourceProps.containsKey(subjectUri)){
            props = initialiseProps(subjectUri);
            initialiseLabels(subjectUri);
        } else {
            props = resourceProps.get(subjectUri);
        }
        // we are overwriting multivalued properties.
        // An array should be created. Check that all data types are compatible.
        props.put(propName, propValue);
    }

    private void setLabel(String subjectUri, String label){
        Set<String> labels;

        if(!resourceLabels.containsKey(subjectUri)){
            initialiseProps(subjectUri);
            labels = initialiseLabels(subjectUri);
        } else {
            labels = resourceLabels.get(subjectUri);
        }

        labels.add(label);
    }

    private void addResource(String subjectUri){

        if(!resourceLabels.containsKey(subjectUri)){
            initialise(subjectUri);
        }
    }

    @Override
    public void handleStatement(Statement st) {
        IRI predicate = st.getPredicate();
        Resource subject = st.getSubject(); //includes blank nodes
        Value object = st.getObject();
        if (object instanceof Literal) {
            final Object literalValue = getObjectValue((Literal) object);
            if (literalValue != null) {
                setProp(subject.stringValue(), shorten(predicate), literalValue);
                triplesParsed++;
            }
        } else if (labellise && predicate.equals(RDF.TYPE) && !(object instanceof BNode)) {
            setLabel(subject.stringValue(),shorten((IRI)object));
            triplesParsed++;
        } else {
            addResource(subject.stringValue());
            addResource(object.stringValue());
            addStatement(st);
            triplesParsed++;
        }
        if (triplesParsed % commitFreq == 0) {
            Util.inTx(graphdb, this);
            ingestedTriples += triplesParsed;
            log.info("Successful periodic commit of " + commitFreq + " triples. " +
                    ingestedTriples + " triples ingested so far...");
            triplesParsed = 0;
        }
    }

    @Override
    public void handleComment(String comment) throws RDFHandlerException {

    }

    private String shorten(IRI iri) {
        if (shortenUris) {
            String localName = iri.getLocalName();
            String prefix = getPrefix(iri.getNamespace());
            return prefix + "_" + localName;
        } else
        {
            return iri.stringValue();
        }
    }

    private String getPrefix(String namespace) {
        if (namespaces.containsKey(namespace)){
            return namespaces.get(namespace);
        } else{
            namespaces.put(namespace, createPrefix());
            return namespaces.get(namespace);
        }
    }

    private String createPrefix() {
        return "ns" + namespaces.size();
    }

    private Object getObjectValue(Literal object) {
        IRI datatype = object.getDatatype();
        if (datatype.equals(XMLSchema.INT) ||
                datatype.equals(XMLSchema.INTEGER) || datatype.equals(XMLSchema.LONG)){
            return object.longValue();
        } else if (datatype.equals(XMLSchema.DECIMAL) || datatype.equals(XMLSchema.DOUBLE) ||
                datatype.equals(XMLSchema.FLOAT)) {
            return object.doubleValue();
        } else if (datatype.equals(XMLSchema.BOOLEAN)) {
            return object.booleanValue();
        } else {
            // it's a string, and it can be tagged with language info.
            // if a language filter has been defined we apply it here
            final Optional<String> language = object.getLanguage();
            if(langFilter == null || !language.isPresent() || (language.isPresent() && langFilter.equals(language.get()))){
                return object.stringValue();
            }
            return null; //string is filtered
        }
    }

    public int getIngestedTriples() {
        return ingestedTriples;
    }
    public Map<String,String> getNamespaces() {

        return namespaces;
    }


    @Override
    public Integer call() throws Exception {
        int count = 0;

        for(Map.Entry<String, Set<String>> entry:resourceLabels.entrySet()){

            final Node node = nodeCache.get(entry.getKey(), new Callable<Node>() {
                @Override
                public Node call() {
                    Node node =  graphdb.findNode(RESOURCE, "uri", entry.getKey());
                    if (node==null){
                        node = graphdb.createNode(RESOURCE);
                        node.setProperty("uri",entry.getKey());
                    }
                    return node;
                }
            });

            entry.getValue().forEach( l -> node.addLabel(Label.label(l)));
            resourceProps.get(entry.getKey()).forEach(node::setProperty);
        }

        for(Statement st:statements){

            final Node fromNode = nodeCache.get(st.getSubject().stringValue(), new Callable<Node>() {
                @Override
                public Node call() {  //throws AnyException
                    return graphdb.findNode(RESOURCE, "uri", st.getSubject().stringValue());
                }
            });

            final Node toNode = nodeCache.get(st.getObject().stringValue(), new Callable<Node>() {
                @Override
                public Node call() {  //throws AnyException
                    return graphdb.findNode(RESOURCE, "uri", st.getSubject().stringValue());
                }
            });

            // check if the rel is already present. If so, don't recreate.
            // explore the node with the lowest degree
            boolean found = false;
            if(fromNode.getDegree(RelationshipType.withName(shorten(st.getPredicate())), Direction.OUTGOING) <
                    toNode.getDegree(RelationshipType.withName(shorten(st.getPredicate())), Direction.INCOMING)) {
                for (Relationship rel : fromNode.getRelationships(RelationshipType.withName(shorten(st.getPredicate())), Direction.OUTGOING)) {
                    if (rel.getEndNode().equals(toNode)) {
                        found = true;
                        break;
                    }
                }
            }else {
                for (Relationship rel : toNode.getRelationships(RelationshipType.withName(shorten(st.getPredicate())), Direction.INCOMING)) {
                    if (rel.getStartNode().equals(fromNode)) {
                        found = true;
                        break;
                    }
                }
            }

            if (!found) {
                fromNode.createRelationshipTo(
                        toNode,
                        RelationshipType.withName(shorten(st.getPredicate())));
            }
        }

        statements.clear();
        resourceLabels.clear();
        resourceProps.clear();

        //TODO what to return here? number of nodes and rels?
        return 0;
    }


}