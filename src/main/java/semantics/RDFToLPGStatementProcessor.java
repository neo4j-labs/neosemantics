package semantics;

import org.eclipse.rdf4j.model.*;
import org.eclipse.rdf4j.model.vocabulary.RDF;
import org.eclipse.rdf4j.model.vocabulary.XMLSchema;
import org.eclipse.rdf4j.rio.RDFHandler;
import org.eclipse.rdf4j.rio.RDFHandlerException;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Result;
import org.neo4j.logging.Log;

import java.util.*;

import static semantics.RDFImport.*;


/**
 * Created by jbarrasa on 15/04/2019.
 */

abstract class RDFToLPGStatementProcessor implements RDFHandler {

    protected GraphDatabaseService graphdb;
    protected Log log;
    protected Map<String,String> namespaces =  new HashMap<>();
    protected final String langFilter;
    protected final int handleUris;
    protected final boolean labellise;
    protected Set<Statement> statements = new HashSet<>();
    protected Map<String,Map<String,Object>> resourceProps = new HashMap<>();
    protected Map<String,Set<String>> resourceLabels = new HashMap<>();
    protected int totalTriplesParsed = 0;
    protected int totalTriplesMapped = 0;
    protected int mappedTripleCounter = 0;
    protected final long commitFreq;

    protected RDFToLPGStatementProcessor(GraphDatabaseService db, String langFilter, int handleUrls, boolean labellise, long commitFreq) {
        this.graphdb = db;
        this.langFilter = langFilter;
        this.handleUris = handleUrls;
        this.labellise = labellise;
        this.commitFreq = commitFreq;
    }


    protected void getExistingNamespaces() {
        Result nslist = graphdb.execute("MATCH (n:NamespacePrefixDefinition) \n" +
                "UNWIND keys(n) AS namespace\n" +
                "RETURN namespace, n[namespace] AS prefix");
        if (!nslist.hasNext()){
            namespaces.putAll(getPopularNamespaces());
        }
        while (nslist.hasNext()){
            Map<String, Object> ns = nslist.next();
            namespaces.put((String)ns.get("namespace"),(String)ns.get("prefix"));
        }
    }

    protected abstract Map<String,String> getPopularNamespaces();

    protected Map<String,String> namespaceList() {
        Map<String,String> ns =  new HashMap<>();
        ns.put("http://schema.org/","sch");
        ns.put("http://purl.org/dc/elements/1.1/","dc");
        ns.put("http://purl.org/dc/terms/","dct");
        ns.put("http://www.w3.org/2004/02/skos/core#","skos");
        ns.put("http://www.w3.org/2000/01/rdf-schema#","rdfs");
        ns.put("http://www.w3.org/2002/07/owl#","owl");
        ns.put("http://www.w3.org/1999/02/22-rdf-syntax-ns#","rdf");
        return ns;
    }


    String getPrefix(String namespace) {
        if (namespaces.containsKey(namespace)){
            return namespaces.get(namespace);
        } else{
            namespaces.put(namespace, nextPrefix());
            return namespaces.get(namespace);
        }
    }

    String nextPrefix() {
        return "ns" + namespaces.values().stream().filter(x -> x.startsWith("ns")).count();
    }

    protected Object getObjectValue(Literal object) {
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
            final Optional<String> language = object.getLanguage();
            if(langFilter == null || !language.isPresent() || (language.isPresent() && langFilter.equals(language.get()))){
                return object.stringValue();
            }
            return null;
        }
    }

    @Override
    public void handleComment(String comment) throws RDFHandlerException {

    }


    protected String handleIRI(IRI iri, int element) {
        //TODO: cache this? It's kind of cached in getPrefix()
        if (handleUris  ==  URL_SHORTEN) {
            String localName = iri.getLocalName();
            String prefix = getPrefix(iri.getNamespace());
            return prefix + PREFIX_SEPARATOR + localName;
        } else if (handleUris  ==  URL_IGNORE) {
            return neo4jCapitalisation(iri.getLocalName(), element);
        } else { //if (handleUris  ==  URL_KEEP){
            return iri.stringValue();
        }
    }

    private String neo4jCapitalisation(String name, int element) {
        if( element == RELATIONSHIP){
            return name.toUpperCase();
        } else if( element == LABEL){
            return name.substring(0, 1).toUpperCase() + name.substring(1);
        } else {
            return name;
        }
    }

    @Override
    public void startRDF() throws RDFHandlerException {
        if (handleUris!=URL_IGNORE){
            //differentiate between map/shorten and keep_long urls?
            getExistingNamespaces();
            log.info("Found " + namespaces.size() + " namespaces in the DB: " + namespaces);
        } else {
            log.info("URIs will be ignored. Only local names will be kept.");
        }

    }

    @Override
    public void handleNamespace(String prefix, String uri) throws RDFHandlerException {

    }

    protected void addStatement(Statement st) {
        statements.add(st);
    }


    protected void initialise(String subjectUri) {
        initialiseProps(subjectUri);
        initialiseLabels(subjectUri);
    }

    protected Set<String> initialiseLabels(String subjectUri) {
        Set<String> labels =  new HashSet<>();
        //        labels.add("Resource");  this was in the preview version (praaopt)
        resourceLabels.put(subjectUri, labels);
        return labels;
    }

    protected HashMap<String, Object> initialiseProps(String subjectUri) {
        HashMap<String, Object> props = new HashMap<>();
        //props.put("uri", subjectUri); this was in the preview version probably removed as an optimisation
        resourceProps.put(subjectUri,props);
        return props;
    }

    protected void setProp(String subjectUri, String propName, Object propValue){
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

    protected void setLabel(String subjectUri, String label){
        Set<String> labels;

        if(!resourceLabels.containsKey(subjectUri)){
            initialiseProps(subjectUri);
            labels = initialiseLabels(subjectUri);
        } else {
            labels = resourceLabels.get(subjectUri);
        }

        labels.add(label);
    }

    protected void addResource(String subjectUri){

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
                setProp(subject.stringValue(), handleIRI(predicate,PROPERTY), literalValue);
                mappedTripleCounter++;
            }
        } else if (labellise && predicate.equals(RDF.TYPE) && !(object instanceof BNode)) {
            setLabel(subject.stringValue(),handleIRI((IRI)object,LABEL));
            mappedTripleCounter++;
        } else {
            addResource(subject.stringValue());
            addResource(object.stringValue());
            addStatement(st);
            mappedTripleCounter++;
        }
        totalTriplesParsed++;

        if (commitFreq != Integer.MAX_VALUE && mappedTripleCounter % commitFreq == 0) {
            periodicOperation();
        }
    }

    protected abstract void periodicOperation();

}
