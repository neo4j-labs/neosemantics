package semantics;

import apoc.result.VirtualNode;
import apoc.result.VirtualRelationship;
import apoc.util.Util;
import org.neo4j.graphdb.*;
import org.neo4j.logging.Log;
import org.openrdf.model.*;
import org.openrdf.model.vocabulary.RDF;
import org.openrdf.model.vocabulary.XMLSchema;
import org.openrdf.rio.RDFHandler;
import org.openrdf.rio.RDFHandlerException;

import java.util.*;

/**
 * Created by jbarrasa on 09/11/2016.
 */
class StatementPreviewer implements RDFHandler {

    private final boolean shortenUris;
    private GraphDatabaseService graphdb;
    private Map<String,Map<String,Object>> resourceProps = new HashMap<>();
    private Map<String,Set<String>> resourceLabels = new HashMap<>();
    private List<Statement> statements = new ArrayList<>();

    private Map<String,String> namespaces =  new HashMap<>();
    private final boolean labellise;
    private Map<String, Node> vNodes;
    private List<Relationship> vRels;
    Log log;

    public StatementPreviewer(GraphDatabaseService db, boolean shortenUrls, boolean typesToLabels,
                              Map<String, Node> virtualNodes, List<Relationship> virtualRels, Log l) {
        graphdb = db;
        shortenUris = shortenUrls;
        labellise =  typesToLabels;
        vNodes = virtualNodes;
        vRels = virtualRels;
        log = l;
    }

    public void startRDF() throws RDFHandlerException {
        getExistingNamespaces(); //should it get existing namespaces?? probably yes.
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


    public void endRDF() throws RDFHandlerException {
        for(String uri:resourceLabels.keySet()){
            vNodes.put(uri,new VirtualNode(Util.labels(new ArrayList<>(resourceLabels.get(uri))),
                    resourceProps.get(uri), graphdb));
        }

        statements.forEach(st -> vRels.add(
                new VirtualRelationship(vNodes.get(st.getSubject().stringValue().replace("'", "\'")),
                        vNodes.get(st.getObject().stringValue().replace("'", "\'")),
                        RelationshipType.withName(shorten(st.getPredicate())))));
    }

    @Override
    public void handleNamespace(String prefix, String uri) throws RDFHandlerException {

    }


    public void handleStatement(Statement st) {
        IRI predicate = st.getPredicate();
        org.openrdf.model.Resource subject = st.getSubject(); //includes blank nodes
        Value object = st.getObject();
        if (object instanceof Literal) {
            setProp(subject.stringValue().replace("'", "\'"), shorten(predicate), getObjectValue((Literal)object));
        } else if (labellise && predicate.equals(RDF.TYPE) && !(object instanceof BNode)) {
            setLabel(subject.stringValue().replace("'", "\'"),shorten((IRI)object));

        } else {
            addResource(subject.stringValue().replace("'", "\'"));
            addResource(object.stringValue().replace("'", "\'"));
            addStatement(st);
        }

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
            namespaces.put(namespace, nextPrefix());
            return namespaces.get(namespace);
        }
    }

    private String nextPrefix() {
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

}
