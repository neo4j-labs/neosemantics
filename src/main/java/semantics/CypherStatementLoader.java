package semantics;

import apoc.util.Util;
import apoc.util.Utils;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Result;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.logging.Log;
import org.openrdf.model.*;
import org.openrdf.model.vocabulary.RDF;
import org.openrdf.model.vocabulary.XMLSchema;
import org.openrdf.rio.RDFHandler;
import org.openrdf.rio.RDFHandlerException;
import org.openrdf.rio.helpers.RDFHandlerBase;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;

/**
 * Created by jbarrasa on 09/11/2016.
 */
class CypherStatementLoader implements RDFHandler, Callable<Integer> {

    private final boolean shortenUris;
    private int ingestedTriples = 0;
    private GraphDatabaseService graphdb;
    private long commitFreq;
    private List<QueryWithParams> cypherStatements = new ArrayList<QueryWithParams>();
    private Map<String,String> namespaces =  new HashMap<>();
    Log log;

    public CypherStatementLoader(GraphDatabaseService db, long batchSize, boolean shortenUrls, Log l) {
        graphdb = db;
        commitFreq = batchSize;
        shortenUris = shortenUrls;
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
        int finalCount = Util.inTx((GraphDatabaseAPI) graphdb, this);
        ingestedTriples += finalCount;
        addNamespaceNode();

        log.info("Successfully committed " + finalCount + " cypher statements. " +
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

    @Override
    public void handleStatement(Statement st) {
        IRI predicate = st.getPredicate();
        Resource subject = st.getSubject(); //includes blank nodes
        Value object = st.getObject();
        String cypher;
        if (object instanceof Literal) {
            // known issue: does not deal with multivalued properties. Probably the obvious approach would be
            // to create arrays when multiple values
            // predicates are always URIs
            Map<String, Object> params = new HashMap<>();
            params.put("subject", subject.stringValue().replace("'", "\'"));
            params.put("object", getObjectValue((Literal) object));
            cypherStatements.add(new QueryWithParams(String.format("MERGE (x:Resource:%s {uri:{subject}}) " +
                    "SET x.`%s` = {object}", (subject instanceof BNode ? "BNode" : "URI"), shorten(predicate)),
                    params));
        } else if (predicate.equals(RDF.TYPE) && !(object instanceof BNode)) {
            // Optimization -> rdf:type is transformed into a label. Reduces node density and uses indexes.
            // How often do we find classes identified by blank nodes? (x rdf:type _:abc), (_:abc rdf:type rdfs:Class)
            Map<String, Object> params = new HashMap<>();
            params.put("subject", subject.stringValue().replace("'", "\'"));
            params.put("object", shorten((IRI)object));
            cypherStatements.add(new QueryWithParams(String.format("MERGE (x:Resource:`%s` {uri:{subject}}) " +
                            "SET x:`%s` MERGE (y:URI {uri:{object}}) SET y:Class",
                    (subject instanceof BNode ? "BNode" : "URI"),
                    shorten((IRI)object)), params));
        } else {
            Map<String, Object> params = new HashMap<>();
            params.put("subject", subject.stringValue().replace("'", "\'"));
            params.put("object", object.stringValue());
            cypherStatements.add(new QueryWithParams(String.format("MERGE (x:Resource:%s {uri:{subject}}) " +
                            "MERGE (y:Resource:%s {uri:{object}}) MERGE (x)-[:`%s`]->(y)",
                    (subject instanceof BNode ? "BNode" : "URI"),
                    (object instanceof BNode ? "BNode" : "URI"),
                    shorten(predicate)), params));
        }

        if (cypherStatements.size() % commitFreq == 0) {
            ingestedTriples += Util.inTx((GraphDatabaseAPI) graphdb, this);
            log.info("Successful periodic commit of " + commitFreq + " cypher statements. " +
                    ingestedTriples + " triples ingested so far...");
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
        for (QueryWithParams qwp : cypherStatements) {
            graphdb.execute(qwp.getCypherString(), qwp.getParams());
            count++;
        }
        cypherStatements.clear();
        return count;
    }

    private class QueryWithParams {

        private final String cypherString;
        private final Map<String, Object> params;

        public QueryWithParams(String cypherStr, Map<String, Object> par) {
            cypherString = cypherStr;
            params = par;
        }


        public String getCypherString() {
            return cypherString;
        }

        public Map<String, Object> getParams() {
            return params;
        }
    }
}