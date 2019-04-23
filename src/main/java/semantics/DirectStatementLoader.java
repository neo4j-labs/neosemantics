package semantics;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import org.eclipse.rdf4j.model.Statement;
import org.eclipse.rdf4j.rio.RDFHandlerException;
import org.neo4j.graphdb.*;
import org.neo4j.logging.Log;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Callable;

import static semantics.RDFImport.RELATIONSHIP;

/**
 * Created by jbarrasa on 09/11/2016.
 */

class DirectStatementLoader extends RDFToLPGStatementProcessor implements Callable<Integer> {

    public static final Label RESOURCE = Label.label("Resource");
    Cache<String, Node> nodeCache;

    public DirectStatementLoader(GraphDatabaseService db, long batchSize, long nodeCacheSize,
                                 int handleUrls, int handleMultivals, Set<String> multivalPropUriList,
                                 Set<String> predicateExclusionList, boolean typesToLabels, boolean klt,
                                 String languageFilter, Log l) {

        super(db, languageFilter, handleUrls, handleMultivals, multivalPropUriList, predicateExclusionList, klt,
                typesToLabels, batchSize);
        nodeCache = CacheBuilder.newBuilder()
                .maximumSize(nodeCacheSize)
                .build();
        log = l;
    }

    @Override
    public void endRDF() throws RDFHandlerException {
        Util.inTx(graphdb, this);
        totalTriplesMapped += mappedTripleCounter;
        if (handleUris==0) {
            addNamespaceNode();
        }

        log.info("Successful (last) partial commit of " + mappedTripleCounter + " triples. " +
                "Total number of triples imported is " + totalTriplesMapped + " out of " +  totalTriplesParsed + " parsed.");
    }

    private void addNamespaceNode() {
        Map<String, Object> params = new HashMap<>();
        params.put("props", namespaces);
        graphdb.execute("MERGE (n:NamespacePrefixDefinition) SET n+={props}", params);
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
                    return graphdb.findNode(RESOURCE, "uri", st.getObject().stringValue());
                }
            });

            // check if the rel is already present. If so, don't recreate.
            // explore the node with the lowest degree
            boolean found = false;
            if(fromNode.getDegree(RelationshipType.withName(handleIRI(st.getPredicate(),RELATIONSHIP)), Direction.OUTGOING) <
                    toNode.getDegree(RelationshipType.withName(handleIRI(st.getPredicate(),RELATIONSHIP)), Direction.INCOMING)) {
                for (Relationship rel : fromNode.getRelationships(RelationshipType.withName(handleIRI(st.getPredicate(),RELATIONSHIP)), Direction.OUTGOING)) {
                    if (rel.getEndNode().equals(toNode)) {
                        found = true;
                        break;
                    }
                }
            }else {
                for (Relationship rel : toNode.getRelationships(RelationshipType.withName(handleIRI(st.getPredicate(),RELATIONSHIP)), Direction.INCOMING)) {
                    if (rel.getStartNode().equals(fromNode)) {
                        found = true;
                        break;
                    }
                }
            }

            if (!found) {
                fromNode.createRelationshipTo(
                        toNode,
                        RelationshipType.withName(handleIRI(st.getPredicate(),RELATIONSHIP)));
            }
        }

        statements.clear();
        resourceLabels.clear();
        resourceProps.clear();

        //TODO what to return here? number of nodes and rels?
        return 0;
    }


    @Override
    protected Map<String, String> getPopularNamespaces() {
        //get namespaces and persist them in the db
        Map<String, String> nsList = namespaceList();
        Map<String, Object> params = new HashMap();
        params.put("namespaces",nsList);
        graphdb.execute(" CREATE (ns:NamespacePrefixDefinition) SET ns = $namespaces ", params);
        return nsList;

    }

    @Override
    protected void periodicOperation() {
        Util.inTx(graphdb, this);
        totalTriplesMapped += mappedTripleCounter;
        log.info("Successful partial commit of " + mappedTripleCounter + " triples. " +
                totalTriplesMapped + " triples ingested so far...");
        mappedTripleCounter = 0;
    }
}
