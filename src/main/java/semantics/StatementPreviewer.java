package semantics;

import static semantics.RDFImport.RELATIONSHIP;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.eclipse.rdf4j.rio.RDFHandlerException;
import org.neo4j.graphdb.*;
import org.neo4j.logging.Log;
import semantics.graphconfig.RDFParserConfig;
import semantics.result.VirtualNode;
import semantics.result.VirtualRelationship;
import semantics.utils.NamespacePrefixConflictException;

/**
 * Created by jbarrasa on 09/11/2016.
 */
class StatementPreviewer extends RDFToLPGStatementProcessor {

  private Map<String, Node> vNodes;
  private List<Relationship> vRels;

  StatementPreviewer(GraphDatabaseService db, Transaction tx, RDFParserConfig conf,
                     Map<String, Node> virtualNodes,
                     List<Relationship> virtualRels, Log l) {
    super(db, tx, conf, l);
    vNodes = virtualNodes;
    vRels = virtualRels;
  }

  public void endRDF() throws RDFHandlerException {
    for (String uri : resourceLabels.keySet()) {
      vNodes.put(uri, new VirtualNode(Util.labels(new ArrayList<>(resourceLabels.get(uri))),
          getPropsPlusUri(uri)));
    }

    statements.forEach(st -> {
      try {
        vRels.add(
            new VirtualRelationship(vNodes.get(st.getSubject().stringValue().replace("'", "\'")),
                vNodes.get(st.getObject().stringValue().replace("'", "\'")),
                RelationshipType.withName(handleIRI(st.getPredicate(), RELATIONSHIP))));
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
