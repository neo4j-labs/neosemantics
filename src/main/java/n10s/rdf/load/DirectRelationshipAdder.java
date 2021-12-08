package n10s.rdf.load;

import n10s.graphconfig.RDFParserConfig;
import n10s.result.RelationshipResult;
import org.eclipse.rdf4j.model.Statement;
import org.eclipse.rdf4j.model.Triple;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.Transaction;
import org.neo4j.logging.Log;

import java.util.HashMap;

import static n10s.graphconfig.GraphConfig.GRAPHCONF_VOC_URI_SHORTEN;

public class DirectRelationshipAdder extends DirectStatementLoader {
    private Statement relStatement;

    public DirectRelationshipAdder(GraphDatabaseService db, Transaction tx, RDFParserConfig conf, Log l) {
        super(db, tx, conf, l);
    }

    public RelationshipResult returnRel() {
        //TODO: Maybe find a better way to do this?
        HashMap<String, Object> params = new HashMap<>();
        params.put("fromuri",relStatement.getSubject().stringValue());
        params.put("touri",relStatement.getObject().stringValue());
        params.put("relname", relStatement.getPredicate().getLocalName());
        return new RelationshipResult(
                (Relationship) tx.execute("MATCH (:Resource { uri: $fromuri })-[r]-(:Resource { uri: $touri }) " +
                        "WHERE type(r) contains $relname RETURN r LIMIT 1", params).next().get("r"));
    }

    @Override
    public void handleStatement(Statement st) {
        if (!(st.getSubject() instanceof Triple)){
            relStatement = st;
        }
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
