package n10s.rdf.preview;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import n10s.RDFToLPGStatementProcessor;
import n10s.Util;
import n10s.graphconfig.RDFParserConfig;
import n10s.result.VirtualNode;
import n10s.result.VirtualRelationship;
import n10s.utils.NamespacePrefixConflictException;
import org.eclipse.rdf4j.model.Statement;
import org.eclipse.rdf4j.rio.RDFHandlerException;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.graphdb.Transaction;
import org.neo4j.logging.Log;

/**
 * Created by jbarrasa on 09/11/2016.
 */
public class StatementPreviewer extends RDFToLPGStatementProcessor {

  private Map<String, Node> vNodes;
  private List<Relationship> vRels;

  public StatementPreviewer(GraphDatabaseService db, Transaction tx, RDFParserConfig conf,
      Map<String, Node> virtualNodes,
      List<Relationship> virtualRels, Log l) {
    super(db, tx, conf, l, false);
    vNodes = virtualNodes;
    vRels = virtualRels;
  }

  @Override
  public void handleStatement(Statement st) {
    if (mappedTripleCounter < parserConfig.getStreamTripleLimit()) {
      super.handleStatement(st);
    } else {
      conclude();
      throw new TripleLimitReached(
          parserConfig.getStreamTripleLimit() + " triples added to preview");
    }
  }

  public void endRDF() throws RDFHandlerException {
    conclude();
  }

  private void conclude() {
    for (String uri : resourceLabels.keySet()) {
      vNodes.put(uri, new VirtualNode(Util.labels(new ArrayList<>(resourceLabels.get(uri))),
          getPropsPlusUri(uri)));
    }

    statements.forEach(st -> {
      try {
        VirtualRelationship vrel = new VirtualRelationship(
            vNodes.get(st.getSubject().stringValue().replace("'", "\'")),
            vNodes.get(st.getObject().stringValue().replace("'", "\'")),
            RelationshipType.withName(handleIRI(st.getPredicate(), RELATIONSHIP)));

        Map<String, Object> relProps = this.relProps.get(st);

        if (relProps!=null){
          for (Entry<String,Object> entry:relProps.entrySet()) {
            vrel.setProperty(entry.getKey(),entry.getValue());
          }
        }

        vRels.add(vrel);
      } catch (NamespacePrefixConflictException e) {
        e.printStackTrace();
      }
    });
  }

  private Map<String, Object> getPropsPlusUri(String uri) {
    Map<String, Object> props = resourceProps.get(uri);
    props.put("uri", uri);
    return props;
  }

  @Override
  protected void periodicOperation() {
    //not needed for preview
  }
}
