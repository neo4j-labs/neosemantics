package n10s.rdf.load;

import static n10s.graphconfig.GraphConfig.GRAPHCONF_VOC_URI_SHORTEN;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;
import n10s.RDFToLPGStatementProcessor;
import n10s.graphconfig.RDFParserConfig;
import org.eclipse.rdf4j.model.Statement;
import org.eclipse.rdf4j.rio.RDFHandlerException;
import org.neo4j.graphdb.Direction;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.graphdb.Transaction;
import org.neo4j.logging.Log;

/**
 * Created by jbarrasa on 09/11/2016.
 */

public class DirectStatementLoader extends RDFToLPGStatementProcessor {

  private static final Label RESOURCE = Label.label("Resource");
  private Cache<String, Node> nodeCache;

  public DirectStatementLoader(GraphDatabaseService db, Transaction tx, RDFParserConfig conf,
      Log l) {

    super(db, tx, conf, l);
    nodeCache = CacheBuilder.newBuilder()
        .maximumSize(conf.getNodeCacheSize())
        .build();
  }

  @Override
  public void endRDF() throws RDFHandlerException {
    periodicOperation();
    //carried out in main context tx
    //namespaces.partialRefresh(tx);
    //this.runPartialTx(tx);
    log.debug("Import complete: " + totalTriplesMapped + "  triples ingested out of "
        + totalTriplesParsed + " parsed");
  }

  public Integer runPartialTx(Transaction inThreadTransaction) {

    for (Map.Entry<String, Set<String>> entry : resourceLabels.entrySet()) {
      try {
        final Node node;
        node = nodeCache.get(entry.getKey(), () -> {
          Node node1 = inThreadTransaction.findNode(RESOURCE, "uri", entry.getKey());
          if (node1 == null) {
            node1 = inThreadTransaction.createNode(RESOURCE);
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
      } catch (ExecutionException e) {
        e.printStackTrace();
      }
    }

    for (Statement st : statements) {
      try {

        final Node fromNode = nodeCache
            .get(st.getSubject().stringValue(), () -> {  //throws AnyException
              return inThreadTransaction.findNode(RESOURCE, "uri", st.getSubject().stringValue());
            });

        final Node toNode = nodeCache
            .get(st.getObject().stringValue(), () -> {  //throws AnyException
              return inThreadTransaction.findNode(RESOURCE, "uri", st.getObject().stringValue());
            });

        // check if the rel is already present. If so, don't recreate.
        // explore the node with the lowest degree
        boolean found = false;
        Relationship theRel = null;
        if (fromNode
            .getDegree(RelationshipType.withName(handleIRI(st.getPredicate(), RELATIONSHIP)),
                Direction.OUTGOING) <
            toNode.getDegree(RelationshipType.withName(handleIRI(st.getPredicate(), RELATIONSHIP)),
                Direction.INCOMING)) {
          for (Relationship rel : fromNode
              .getRelationships(Direction.OUTGOING,
                  RelationshipType.withName(handleIRI(st.getPredicate(), RELATIONSHIP)))) {
            if (rel.getEndNode().equals(toNode)) {
              found = true;
              theRel = rel;
              break;
            }
          }
        } else {
          for (Relationship rel : toNode
              .getRelationships(Direction.INCOMING,
                  RelationshipType.withName(handleIRI(st.getPredicate(), RELATIONSHIP)))) {
            if (rel.getStartNode().equals(fromNode)) {
              found = true;
              theRel = rel;
              break;
            }
          }
        }

        if (!found) {
          theRel = fromNode.createRelationshipTo(
              toNode,
              RelationshipType.withName(handleIRI(st.getPredicate(), RELATIONSHIP)));
        }

        Map<String, Object> relProps = this.relProps.get(st);
        if (relProps!=null){
          for (Entry<String,Object> entry:relProps.entrySet()) {
            theRel.setProperty(entry.getKey(),entry.getValue());
          }
        }

      } catch (ExecutionException e) {
        e.printStackTrace();
      }
    }

    statements.clear();
    resourceLabels.clear();
    resourceProps.clear();
    relProps.clear();
    nodeCache.invalidateAll();
    Integer result = 0;
    if (parserConfig.getGraphConf().getHandleVocabUris() == GRAPHCONF_VOC_URI_SHORTEN) {
      result = namespaces.partialRefresh(inThreadTransaction);
    }

    return result;
  }


  @Override
  protected void periodicOperation() {

    if (parserConfig.getGraphConf().getHandleVocabUris() == GRAPHCONF_VOC_URI_SHORTEN) {
      try (Transaction tempTransaction = graphdb.beginTx()) {
        namespaces.partialRefresh(tempTransaction);
        tempTransaction.commit();
        log.debug("namespace prefixes synced: " + namespaces.toString());
      } catch (Exception e) {
        e.printStackTrace();
      }
    }

    try (Transaction tempTransaction = graphdb.beginTx()) {
      this.runPartialTx(tempTransaction);
      tempTransaction.commit();
      log.debug("partial commit: " + mappedTripleCounter + " triples ingested. Total so far: "
          + totalTriplesMapped);
    } catch (Exception e) {
      e.printStackTrace();
    }

    totalTriplesMapped += mappedTripleCounter;
    mappedTripleCounter = 0;

  }

}
