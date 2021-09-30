package n10s.onto;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;

import n10s.Util;
import n10s.graphconfig.RDFParserConfig;
import n10s.result.VirtualNode;
import n10s.result.VirtualRelationship;
import n10s.utils.NamespacePrefixConflictException;
import org.eclipse.rdf4j.model.IRI;
import org.eclipse.rdf4j.model.Statement;
import org.eclipse.rdf4j.rio.RDFHandlerException;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.graphdb.Transaction;
import org.neo4j.logging.Log;

import static n10s.graphconfig.Params.DEFAULT_BASE_SCH_NS;

public class OntologyPreviewer extends OntologyImporter {


  private Map<String, Node> vNodes;
  private List<Relationship> vRels;

  public OntologyPreviewer(GraphDatabaseService db, Transaction tx, RDFParserConfig conf,
      Map<String, Node> virtualNodes, List<Relationship> virtualRels, Log log) {
    super(db, tx, conf, log);
    vNodes = virtualNodes;
    vRels = virtualRels;
  }


  public void endRDF() throws RDFHandlerException {
    conclude();
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

  //  TODO:Refactor with the one in preview RDF
  private void conclude() {
    for (String uri : resourceLabels.keySet()) {
      vNodes.put(uri, new VirtualNode(Util.labels(new ArrayList<>(resourceLabels.get(uri))),
          getPropsPlusUri(uri)));
    }

    openSubClassRestrictions.keySet().stream().forEach(c -> {
      openSubClassRestrictions.get(c).forEach( rest -> {
        processRestriction(c, rest, SUB_CLASS_REST);
      });
    });

    openEquivRestrictions.keySet().stream().forEach(c -> {
      openEquivRestrictions.get(c).forEach( rest -> {
        processRestriction(c, rest, EQUIV_CLASS_REST);
      });
    });

    statements.forEach(st -> {
      try {
        vRels.add(
            new VirtualRelationship(vNodes.get(st.getSubject().stringValue().replace("'", "\'")),
                vNodes.get(st.getObject().stringValue().replace("'", "\'")),
                RelationshipType.withName(handleIRI(translateRelName(st.getPredicate()),RELATIONSHIP))));
      } catch (NamespacePrefixConflictException e) {
        e.printStackTrace();
      }
    });
  }

  private void processRestriction(IRI c, OWLRestriction rest, int type) {
    try {
    if (rest.isComplete()) {
      Map<String,Object> relProps = new HashMap<>();
      relProps.put("onPropertyURI", rest.getRelName().stringValue());
      relProps.put("onPropertyName", rest.getRelName().getLocalName());
      relProps.put("restrictionType", getTypeAsString(rest));
      if(rest.isCardinalityConstraint()){
        relProps.put("cardinalityVal", rest.getCardinalityVal());
      }
      vRels.add(
              new VirtualRelationship(vNodes.get(c.stringValue().replace("'", "\'")),
                      vNodes.get(rest.getTargetClass().stringValue().replace("'", "\'")),
                      RelationshipType.withName(handleIRI(vf.createIRI(DEFAULT_BASE_SCH_NS + getRestrictionRelName(type)), RELATIONSHIP)), relProps));
    };
    } catch (NamespacePrefixConflictException e) {
      e.printStackTrace();
    }
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
