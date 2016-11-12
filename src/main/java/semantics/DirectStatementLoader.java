package semantics;

import apoc.util.Util;
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

    private final boolean shortenUris;
    private int ingestedTriples = 0;
    private int triplesParsed = 0;
    private GraphDatabaseService graphdb;
    private long commitFreq;
    private Map<String,Map<String,Object>> resourceProps = new HashMap<>();
    private Map<String,Set<String>> resourceLabels = new HashMap<>();
    private List<Statement> statements = new ArrayList<>();
    private Map<String,String> namespaces =  new HashMap<>();
    private final boolean labellise;
    Log log;

    public DirectStatementLoader(GraphDatabaseService db, long batchSize, boolean shortenUrls, boolean typesToLabels, Log l) {
        graphdb = db;
        commitFreq = batchSize;
        shortenUris = shortenUrls;
        labellise =  typesToLabels;
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
        labels.add("Resource");
        resourceLabels.put(subjectUri, labels);
        return labels;
    }

    private HashMap<String, Object> initialiseProps(String subjectUri) {
        HashMap<String, Object> props = new HashMap<>();
        props.put("uri", subjectUri);
        resourceProps.put(subjectUri,props);
        return props;
    }

    private void setProp(String subjectUri, String propName, String propValue){
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
        String cypher;
        if (object instanceof Literal) {
            setProp(subject.stringValue().replace("'", "\'"), shorten(predicate), getObjectValue((Literal)object));
        } else if (labellise && predicate.equals(RDF.TYPE) && !(object instanceof BNode)) {
            setLabel(subject.stringValue().replace("'", "\'"),shorten((IRI)object));

        } else {
            addResource(subject.stringValue().replace("'", "\'"));
            addResource(object.stringValue().replace("'", "\'"));
            addStatement(st);
        }
        triplesParsed++;
        if (triplesParsed % commitFreq == 0) {
            Util.inTx((GraphDatabaseAPI) graphdb, this);
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
            namespaces.put(namespace, createPrefix(namespace));
            return namespaces.get(namespace);
        }
    }

    private String createPrefix(String namespace) {
        return "ns" + namespaces.size();
    }

    private String getObjectValue(Literal object) {
        IRI datatype = object.getDatatype();
        if (datatype.equals(XMLSchema.DECIMAL) || datatype.equals(XMLSchema.DOUBLE) ||
                datatype.equals(XMLSchema.FLOAT) || datatype.equals(XMLSchema.INT) ||
                datatype.equals(XMLSchema.INTEGER) || datatype.equals(XMLSchema.LONG)) {
            return object.stringValue();
        } else if (datatype.equals(XMLSchema.BOOLEAN)) {
            return object.stringValue();
        } else {
            return object.stringValue().replace("\\", "\\\\").replace("'", "\\'");
            //not sure this is the best way to 'clean' the property value
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
        Map<String,Node> nodes = new HashMap<>();
        for(String uri:resourceLabels.keySet()){
            final Node node = (graphdb.findNode(Label.label("Resource"), "uri", uri)==null?
                    graphdb.createNode(Label.label("Resource")):
                    graphdb.findNode(Label.label("Resource"), "uri", uri));
            resourceLabels.get(uri).forEach( l -> node.addLabel(Label.label(l)));
            resourceProps.get(uri).forEach(node::setProperty);
            nodes.put(uri, node);
        }

        for(Statement st:statements){
            nodes.get(st.getSubject().stringValue().replace("'", "\'")).createRelationshipTo(
                    nodes.get(st.getObject().stringValue().replace("'", "\'")),
                    RelationshipType.withName(shorten(st.getPredicate())));
        }

        //TODO do we clear all? keep cache?
        statements.clear();
        nodes.clear();
        resourceLabels.clear();
        resourceProps.clear();

        //TODO what to return here? number of nodes and rels?
        return 0;
    }


}