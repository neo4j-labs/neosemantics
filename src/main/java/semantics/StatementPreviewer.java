package semantics;

import org.eclipse.rdf4j.rio.RDFHandlerException;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.logging.Log;
import semantics.result.VirtualNode;
import semantics.result.VirtualRelationship;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static semantics.RDFImport.RELATIONSHIP;

/**
 * Created by jbarrasa on 09/11/2016.
 */
class StatementPreviewer extends RDFToLPGStatementProcessor {

    private Map<String, Node> vNodes;
    private List<Relationship> vRels;

    public StatementPreviewer(GraphDatabaseService db, int handleUrls, boolean typesToLabels,
                              Map<String, Node> virtualNodes, List<Relationship> virtualRels, String languageFilter, Log l) {
        super(db, languageFilter, handleUrls, typesToLabels, Integer.MAX_VALUE);
        vNodes = virtualNodes;
        vRels = virtualRels;
        log = l;
    }

    public void endRDF() throws RDFHandlerException {
        for(String uri:resourceLabels.keySet()){
            vNodes.put(uri,new VirtualNode(Util.labels(new ArrayList<>(resourceLabels.get(uri))),
                    resourceProps.get(uri), graphdb));
        }

        statements.forEach(st -> vRels.add(
                new VirtualRelationship(vNodes.get(st.getSubject().stringValue().replace("'", "\'")),
                        vNodes.get(st.getObject().stringValue().replace("'", "\'")),
                        RelationshipType.withName(handleIRI(st.getPredicate(), RELATIONSHIP)))));
    }

    @Override
    protected Map<String, String> getPopularNamespaces() {
        return namespaceList();
    }

    @Override
    protected void periodicOperation() {
        //not needed for preview
    }
}
