package semantics;

import static semantics.RDFImport.RELATIONSHIP;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader.InvalidCacheLoadException;
import com.google.common.collect.Iterators;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Callable;
import org.eclipse.rdf4j.model.BNode;
import org.eclipse.rdf4j.model.Statement;
import org.eclipse.rdf4j.rio.RDFHandlerException;
import org.neo4j.graphdb.Direction;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.helpers.collection.Iterables;
import org.neo4j.logging.Log;

/**
 * This class implements an RDF handler to statement-wise delete imported RDF triples.
 *
 * Created on 03/06/2019.
 *
 * @author Emre Arkan
 */
class DirectStatementDeleter extends RDFToLPGStatementProcessor implements Callable<Integer> {

  private static final Label RESOURCE = Label.label("Resource");
  private final Cache<String, Node> nodeCache;
  private long notDeletedStatementCount;
  private long statementsWithBNodeCount;
  private String BNodeInfo;

  DirectStatementDeleter(GraphDatabaseService db, RDFParserConfig conf, Log l) {
    super(db, conf, l);
    nodeCache = CacheBuilder.newBuilder()
        .maximumSize(conf.getNodeCacheSize())
        .build();
    BNodeInfo = "";
    notDeletedStatementCount = 0;
    statementsWithBNodeCount = 0;
  }

  /**
   * Analog to endRDF in {@link DirectStatementLoader}, however modified for deletion.
   *
   * Executed at the end of each commit to inform the user of the current state of the deletion
   * process.
   */
  @Override
  public void endRDF() throws RDFHandlerException {
    Util.inTx(graphdb, this);
    totalTriplesMapped += mappedTripleCounter;
    log.info("Successful (last) partial commit of " + mappedTripleCounter + " triples. " +
        "Total number of triples deleted is " + totalTriplesMapped + " out of "
        + totalTriplesParsed + " parsed.");
  }

  /**
   * Analog to call in {@link DirectStatementLoader}, however strongly modified to delete nodes
   * rather than creating or updating.
   *
   * {@link #resourceLabels}, {@link #resourceProps}, and {@link #statements}, which contain the
   * statements to be deleted, are processed respectively. If a statement does not exist in the
   * database, {@link #notDeletedStatementCount} is increased to inform the user of not deleted
   * statement count.
   *
   * {@link #statementsWithBNodeCount} counts the number of statements, which could not be deleted
   * due to containing a blank node.
   *
   * {@link #deleteNodeIfEmpty(Node)} is called for each {@code Node} processed, to check and delete
   * it, if applicable.
   *
   * @return An obligatory return, which is always 0, since the overridden method must return an
   * Integer
   */
  @Override
  public Integer call() throws Exception {

    for (Map.Entry<String, Set<String>> entry : resourceLabels.entrySet()) {
      if (entry.getKey().startsWith("genid")) {
        //if the node represents a blank node
        statementsWithBNodeCount += entry.getValue().size() + 1;
        continue;
      }
      Node tempNode = null;
      final Node node;
      try {
        tempNode = nodeCache
            .get(entry.getKey(), () -> graphdb.findNode(RESOURCE, "uri", entry.getKey()));
      } catch (InvalidCacheLoadException icle) {
        icle.printStackTrace();
      }
      node = tempNode;
      entry.getValue().forEach(l -> {
        if (node != null && node.hasLabel(Label.label(l))) {
          //if node exist in the database and has the label to be deleted
          node.removeLabel(Label.label(l));
        } else {
          notDeletedStatementCount++;
        }
      });
      resourceProps.get(entry.getKey()).forEach((k, v) -> {
        if (v instanceof List) {
          List valuesToDelete = (List) v;
          if (node != null && node.hasProperty(k)) {
            ArrayList<Object> newProps = new ArrayList<>();
            Object prop = node.getProperty(k);
            if (prop instanceof long[]) {
              long[] props = (long[]) prop;
              for (long currentVal : props) {
                if (!valuesToDelete.contains(currentVal)) {
                  newProps.add(currentVal);
                }
              }
            } else if (prop instanceof double[]) {
              double[] props = (double[]) prop;
              for (double currentVal : props) {
                if (!valuesToDelete.contains(currentVal)) {
                  newProps.add(currentVal);
                }
              }
            } else if (prop instanceof boolean[]) {
              boolean[] props = (boolean[]) prop;
              for (boolean currentVal : props) {
                if (!valuesToDelete.contains(currentVal)) {
                  newProps.add(currentVal);
                }
              }
            } else if (prop instanceof LocalDateTime[]) {
              LocalDateTime[] props = (LocalDateTime[]) prop;
              for (LocalDateTime currentVal : props) {
                if (!valuesToDelete.contains(currentVal)) {
                  newProps.add(currentVal);
                }
              }
            } else if (prop instanceof LocalDate[]) {
              LocalDate[] props = (LocalDate[]) prop;
              for (LocalDate currentVal : props) {
                if (!valuesToDelete.contains(currentVal)) {
                  newProps.add(currentVal);
                }
              }
            } else {
              Object[] props = (Object[]) prop;
              for (Object currentVal : props) {
                if (!valuesToDelete.contains(currentVal)) {
                  newProps.add(currentVal);
                }
              }
            }
            node.removeProperty(k);
            if (!newProps.isEmpty()) {
              node.setProperty(k, toPropertyValue(newProps));
            }
          } else {
            notDeletedStatementCount += valuesToDelete.size();
          }
        } else {
          if (node != null && node.hasProperty(k)) {
            node.removeProperty(k);
          } else {
            notDeletedStatementCount++;
          }
        }
      });
      if (node != null) {
        deleteNodeIfEmpty(node);
      }
    }

    for (Statement st : statements) {
      if (st.getSubject() instanceof BNode != st.getObject() instanceof BNode) {
        statementsWithBNodeCount++;
      }
      if (st.getSubject() instanceof BNode || st.getObject() instanceof BNode) {
        continue;
      }
      Node fromNode = null;
      try {
        fromNode = nodeCache.get(st.getSubject().stringValue(),
            () -> graphdb.findNode(RESOURCE, "uri", st.getSubject().stringValue()));
      } catch (InvalidCacheLoadException icle) {
        icle.printStackTrace();
      }
      Node toNode = null;
      try {
        toNode = nodeCache.get(st.getObject().stringValue(),
            () -> graphdb.findNode(RESOURCE, "uri", st.getObject().stringValue()));
      } catch (InvalidCacheLoadException icle) {
        icle.printStackTrace();
      }
      if (fromNode == null || toNode == null) {
        notDeletedStatementCount++;
        continue;
      }
      // find relationship if it exists
      if (fromNode.getDegree(RelationshipType.withName(handleIRI(st.getPredicate(), RELATIONSHIP)),
          Direction.OUTGOING) <
          toNode.getDegree(RelationshipType.withName(handleIRI(st.getPredicate(), RELATIONSHIP)),
              Direction.INCOMING)) {
        for (Relationship rel : fromNode
            .getRelationships(RelationshipType.withName(handleIRI(st.getPredicate(), RELATIONSHIP)),
                Direction.OUTGOING)) {
          if (rel.getEndNode().equals(toNode)) {
            rel.delete();
            break;
          }
        }
      } else {
        for (Relationship rel : toNode
            .getRelationships(RelationshipType.withName(handleIRI(st.getPredicate(), RELATIONSHIP)),
                Direction.INCOMING)) {
          if (rel.getStartNode().equals(fromNode)) {
            rel.delete();
            break;
          }
        }
      }
      deleteNodeIfEmpty(toNode);
      deleteNodeIfEmpty(fromNode);
    }
    statements.clear();
    resourceLabels.clear();
    resourceProps.clear();
    if (statementsWithBNodeCount > 0) {
      setBNodeInfo(statementsWithBNodeCount
          + " of the statements could not be deleted, due to containing a blank node.");
    }
    return 0;
  }

  /**
   * Analog to periodicOperation in {@link DirectStatementLoader}, however modified for the
   * deletion
   *
   * After each partial commit, a short information about the current state of the deletion process
   * is logged.
   */
  @Override
  protected void periodicOperation() {
    Util.inTx(graphdb, this);
    totalTriplesMapped += mappedTripleCounter;
    log.info("Successful partial commit of " + mappedTripleCounter + " triples. " +
        (totalTriplesMapped - notDeletedStatementCount) + " triples deleted so far...");
    mappedTripleCounter = 0;
  }

  /**
   * @return amount of not deleted statement count and statements with blank node count
   */
  long getNotDeletedStatementCount() {
    return notDeletedStatementCount + statementsWithBNodeCount;
  }

  /**
   * @return information about statement not deleted due to containing a blank node
   */
  String getBNodeInfo() {
    return BNodeInfo;
  }

  /*
   * Called in {@link DirectStatementDeleter#call()} after the deletion process is done
   * @param BNodeInfo information about statement not deleted due to containing a blank node
   */
  private void setBNodeInfo(String BNodeInfo) {
    this.BNodeInfo = BNodeInfo;
  }

  /**
   * Deletes a given {@code node}, if all conditions are met. Call in the {@link #call()} method.
   *
   * @param node to be deleted
   */
  private void deleteNodeIfEmpty(Node node) {
    int nodePropertyCount = node.getAllProperties().size();
    int labelCount = Iterators.size(node.getLabels().iterator());
    if (!node.hasRelationship(Direction.OUTGOING) &&
        !node.hasRelationship(Direction.INCOMING) &&
        node.hasLabel(RESOURCE) && labelCount == 1 &&
        (node.getAllProperties().containsKey("uri") && nodePropertyCount == 1)) {
      node.delete();
    }
  }

  // Adapted from APOC
  private Object toPropertyValue(Object value) {
    if (value instanceof Iterable) {
      Iterable it = (Iterable) value;
      Object first = Iterables.firstOrNull(it);
      return Iterables.asArray(first.getClass(), it);
    }
    return value;
  }
}
