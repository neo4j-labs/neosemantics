package semantics;

import static semantics.RDFImport.RELATIONSHIP;
import static semantics.config.GraphConfig.GRAPHCONF_VOC_URI_SHORTEN;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.stream.Collectors;
import org.eclipse.rdf4j.model.Statement;
import org.eclipse.rdf4j.rio.RDFHandlerException;
import org.neo4j.graphdb.*;
import org.neo4j.logging.Log;
import semantics.config.GraphConfig;
import semantics.config.RDFParserConfig;

/**
 * Created by jbarrasa on 09/11/2016.
 */

class DirectStatementLoader extends RDFToLPGStatementProcessor implements Callable<Integer> {

  private static final Label RESOURCE = Label.label("Resource");
  private Cache<String, Node> nodeCache;

  DirectStatementLoader(GraphDatabaseService db, Transaction tx, RDFParserConfig conf, Log l) {

    super(db, tx, conf, l);
    nodeCache = CacheBuilder.newBuilder()
        .maximumSize(conf.getNodeCacheSize())
        .build();
  }

  @Override
  public void endRDF() throws RDFHandlerException {
    Util.inTx(graphdb, this);
    totalTriplesMapped += mappedTripleCounter;
    if (parserConfig.getGraphConf().getHandleVocabUris() == GRAPHCONF_VOC_URI_SHORTEN) { //URL_SHORTEN
      // Namespaces are only persisted at the end of each periodic commit.
      // This makes importRDF not thread safe when using url shortening. TODO: fix this.
      persistNamespaceNode();
    }

    log.debug("Import complete: " + totalTriplesMapped + "  triples ingested out of "
        + totalTriplesParsed + " parsed");
  }

  private void persistNamespaceNode() {
    Map<String, Object> params = new HashMap<>();
    params.put("props", namespaces);
    tx.execute("MERGE (n:NamespacePrefixDefinition) SET n+=$props", params);
  }

  @Override
  public Integer call() throws Exception {
    int count = 0;

   //get the ns from the db

    for (Map.Entry<String, Set<String>> entry : resourceLabels.entrySet()) {

      final Node node = nodeCache.get(entry.getKey(), () -> {
        Node node1 = tx.findNode(RESOURCE, "uri", entry.getKey());
        if (node1 == null) {
          node1 = tx.createNode(RESOURCE);
          node1.setProperty("uri", entry.getKey());
        }
        return node1;
      });

      entry.getValue().forEach(l -> node.addLabel(Label.label(l)));
      resourceProps.get(entry.getKey()).forEach((k, v) -> {
        if (v instanceof List) {
          Object currentValue = node.getProperty(k, null);
          if (currentValue == null) {
            node.setProperty(k, toPropertyValue(v));
          } else {
            if (currentValue.getClass().isArray()) {
              Object[] properties = (Object[]) currentValue;
              for (Object property : properties) {
                ((List) v).add(property);
                //here an exception can be raised if types are conflicting
              }
            } else {
              //TODO: this logic goes because it should not be possible to change
              // from atomic to multival without emptying the DB
              ((List) v).add(node.getProperty(k));
            }
            //we make it a set to remove duplicates. Semantics of multivalued props in RDF.
            node.setProperty(k, toPropertyValue(((List) v).stream().collect(Collectors.toSet())));
          }
        } else {
          node.setProperty(k, v);
        }
      });
    }

    for (Statement st : statements) {

      final Node fromNode = nodeCache
          .get(st.getSubject().stringValue(), () -> {  //throws AnyException
            return tx.findNode(RESOURCE, "uri", st.getSubject().stringValue());
          });

      final Node toNode = nodeCache.get(st.getObject().stringValue(), () -> {  //throws AnyException
        return tx.findNode(RESOURCE, "uri", st.getObject().stringValue());
      });

      // check if the rel is already present. If so, don't recreate.
      // explore the node with the lowest degree
      boolean found = false;
      if (fromNode.getDegree(RelationshipType.withName(handleIRI(st.getPredicate(), RELATIONSHIP)),
          Direction.OUTGOING) <
          toNode.getDegree(RelationshipType.withName(handleIRI(st.getPredicate(), RELATIONSHIP)),
              Direction.INCOMING)) {
        for (Relationship rel : fromNode
            .getRelationships(Direction.OUTGOING,
                    RelationshipType.withName(handleIRI(st.getPredicate(), RELATIONSHIP)))) {
          if (rel.getEndNode().equals(toNode)) {
            found = true;
            break;
          }
        }
      } else {
        for (Relationship rel : toNode
            .getRelationships(Direction.INCOMING,
                    RelationshipType.withName(handleIRI(st.getPredicate(), RELATIONSHIP)))) {
          if (rel.getStartNode().equals(fromNode)) {
            found = true;
            break;
          }
        }
      }

      if (!found) {
        fromNode.createRelationshipTo(
            toNode,
            RelationshipType.withName(handleIRI(st.getPredicate(), RELATIONSHIP)));
      }
    }

    statements.clear();
    resourceLabels.clear();
    resourceProps.clear();

    //TODO: get namespaces from db
    //if conflict, rollback use 0/1 to return ok or ko
    //throw namespaceprefixconflict
    //TODO what to return here? number of nodes and rels?
    return 0;
  }


  @Override
  protected void periodicOperation() {
    Util.inTx(graphdb, this);
    totalTriplesMapped += mappedTripleCounter;
    mappedTripleCounter = 0;
    persistNamespaceNode();
  }

}
