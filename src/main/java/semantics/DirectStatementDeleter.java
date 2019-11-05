package semantics;

import static semantics.RDFImport.RELATIONSHIP;
import static semantics.RDFParserConfig.URL_SHORTEN;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader.InvalidCacheLoadException;
import com.google.common.collect.Iterators;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.HashMap;
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
 * This class implements an RDF handler to statement-wise delete imported RDF data
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
  private String bNodeInfo;

  DirectStatementDeleter(GraphDatabaseService db, RDFParserConfig conf, Log l) {

    super(db, conf, l);
    nodeCache = CacheBuilder.newBuilder()
        .maximumSize(conf.getNodeCacheSize())
        .build();
    bNodeInfo = "";
    notDeletedStatementCount = 0;
    statementsWithBNodeCount = 0;
  }

  @Override
  public void endRDF() throws RDFHandlerException {
    Util.inTx(graphdb, this);
    totalTriplesMapped += mappedTripleCounter;
    if (parserConfig.getHandleVocabUris() == URL_SHORTEN) {
      persistNamespaceNode();
    }

    log.info("Successful (last) partial commit of " + mappedTripleCounter + " triples. " +
        "Total number of triples deleted is " + totalTriplesMapped + " out of "
        + totalTriplesParsed + " parsed.");
  }

  @Override
  public Integer call() throws Exception {

    for (Map.Entry<String, Set<String>> entry : resourceLabels.entrySet()) {
      if (entry.getKey().startsWith("genid")) {
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
        fromNode = nodeCache.get(st.getSubject().stringValue(), () -> {  //throws AnyException
          return graphdb.findNode(RESOURCE, "uri", st.getSubject().stringValue());
        });
      } catch (InvalidCacheLoadException icle) {
        icle.printStackTrace();
      }
      Node toNode = null;
      try {
        toNode = nodeCache.get(st.getObject().stringValue(), () -> {  //throws AnyException
          return graphdb.findNode(RESOURCE, "uri", st.getObject().stringValue());
        });
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
      setbNodeInfo(statementsWithBNodeCount
          + " of the statements could not be deleted, due to containing a blank node.");
    }

    //TODO what to return here? number of nodes and rels?
    return 0;
  }

  @Override
  protected void periodicOperation() {
    Util.inTx(graphdb, this);
    totalTriplesMapped += mappedTripleCounter;
    log.info("Successful partial commit of " + mappedTripleCounter + " triples. " +
        (totalTriplesMapped - notDeletedStatementCount) + " triples deleted so far...");
    mappedTripleCounter = 0;
  }

  long getNotDeletedStatementCount() {
    return notDeletedStatementCount + statementsWithBNodeCount;
  }

  String getbNodeInfo() {
    return bNodeInfo;
  }

  private void setbNodeInfo(String bNodeInfo) {
    this.bNodeInfo = bNodeInfo;
  }

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

  private void persistNamespaceNode() {
    Map<String, Object> params = new HashMap<>();
    params.put("props", namespaces);
    graphdb.execute("MERGE (n:NamespacePrefixDefinition) SET n+={props}", params);
  }

}
