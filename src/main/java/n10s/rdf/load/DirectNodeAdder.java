package n10s.rdf.load;

import n10s.graphconfig.RDFParserConfig;
import n10s.result.NodeResult;
import org.eclipse.rdf4j.model.Statement;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Transaction;
import org.neo4j.logging.Log;

import java.util.HashMap;

import static n10s.graphconfig.GraphConfig.GRAPHCONF_VOC_URI_SHORTEN;

public class DirectNodeAdder extends DirectStatementLoader {
    private String nodeUri;

    public DirectNodeAdder(GraphDatabaseService db, Transaction tx, RDFParserConfig conf, Log l) {
        super(db, tx, conf, l, false);
    }

    public NodeResult returnNode() {
        HashMap<String, Object> params = new HashMap<>();
        params.put("uri",nodeUri);
        return new NodeResult((Node)tx.execute("MATCH (r:Resource { uri: $uri }) RETURN r ", params).next().get("r"));
    }

    @Override
    public void handleStatement(Statement st) {
        nodeUri = st.getSubject().stringValue();
        super.handleStatement(st);
    }

    @Override
    protected void periodicOperation() {

        if (parserConfig.getGraphConf().getHandleVocabUris() == GRAPHCONF_VOC_URI_SHORTEN) {
            namespaces.partialRefresh(tx);
        }
        runPartialTx(tx);
    }


}
