package n10s.quadrdf;

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
import java.util.concurrent.ExecutionException;
import n10s.ContextResource;
import n10s.RDFToLPGStatementProcessor;
import n10s.graphconfig.RDFParserConfig;
import org.eclipse.rdf4j.model.BNode;
import org.eclipse.rdf4j.model.Statement;
import org.eclipse.rdf4j.rio.RDFHandlerException;
import org.neo4j.graphdb.Direction;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.graphdb.Result;
import org.neo4j.graphdb.Transaction;
import org.neo4j.logging.Log;

/**
 * This class implements an RDF handler to statement-wise delete imported RDF data sets
 * <p>
 * Created on 18/06/2019.
 *
 * @author Emre Arkan
 */

public class RDFQuadDirectStatementDeleter extends RDFQuadToLPGStatementProcessor {

  private static final Label RESOURCE = Label.label("Resource");

  private Cache<ContextResource, Node> nodeCache;
  private long notDeletedStatementCount;
  private long statementsWithbNodeCount;
  private String bNodeInfo;

  public RDFQuadDirectStatementDeleter(GraphDatabaseService db, Transaction tx,
      RDFParserConfig conf, Log l) {
    super(db, tx, conf, l);
    nodeCache = CacheBuilder.newBuilder()
        .maximumSize(conf.getNodeCacheSize())
        .build();
    bNodeInfo = "";
    notDeletedStatementCount = 0;
    statementsWithbNodeCount = 0;
  }

  @Override
  public void endRDF() throws RDFHandlerException {

    periodicOperation();

    log.debug("Delete operation  complete: Total number of triples deleted is "
        + totalTriplesMapped + "(out of " + totalTriplesParsed + " parsed)");
  }


  public Integer runPartialTx(Transaction inThreadTransaction) {

    for (Map.Entry<ContextResource, Set<String>> entry : resourceLabels.entrySet()) {
      try {
        if (entry.getKey().getUri().startsWith("genid")) {
          statementsWithbNodeCount += entry.getValue().size() + 1;
          continue;
        }
        Node tempNode = null;
        final Node node;
        try {
          tempNode = nodeCache.get(entry.getKey(), new Callable<Node>() {
            @Override
            public Node call() {
              Node node = null;
              Map<String, Object> params = new HashMap<>();
              String cypher = buildCypher(entry.getKey().getUri(),
                  entry.getKey().getGraphUri(),
                  params);
              Result result = inThreadTransaction.execute(cypher, params);
              if (result.hasNext()) {
                node = (Node) result.next().get("n");
                if (result.hasNext()) {
                  String props =
                      "{uri: " + entry.getKey().getUri() +
                          (entry.getKey().getGraphUri() == null ? "}" :
                              ", graphUri: " + entry.getKey().getGraphUri() + "}");
                  throw new IllegalStateException(
                      "There are multiple matching nodes for the given properties " + props);
                }
              }
              return node;
            }
          });
        } catch (InvalidCacheLoadException | IllegalStateException e) {
          e.printStackTrace();
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
      } catch (ExecutionException e) {
        e.printStackTrace();
      }
    }

    for (Statement st : statements) {
      try {
        if (st.getSubject() instanceof BNode != st.getObject() instanceof BNode) {
          statementsWithbNodeCount++;
        }
        if (st.getSubject() instanceof BNode || st.getObject() instanceof BNode) {
          continue;
        }
        ContextResource from = new ContextResource(st.getSubject().stringValue(),
            st.getContext() != null ? st.getContext().stringValue() : null);
        Node fromNode = null;
        try {
          fromNode = nodeCache.get(from, new Callable<Node>() {
            @Override
            public Node call() {  //throws AnyException
              Node node = null;
              Map<String, Object> params = new HashMap<>();
              String cypher = buildCypher(st.getSubject().stringValue(),
                  st.getContext() != null ? st.getContext().stringValue() : null,
                  params);
              Result result = inThreadTransaction.execute(cypher, params);
              if (result.hasNext()) {
                node = (Node) result.next().get("n");
                if (result.hasNext()) {
                  String props =
                      "{uri: " + st.getSubject().stringValue() +
                          (st.getContext() == null ? "}" :
                              ", graphUri: " + st.getContext().stringValue() + "}");
                  throw new IllegalStateException(
                      "There are multiple matching nodes for the given properties " + props);
                }
              }
              return node;
            }
          });
        } catch (InvalidCacheLoadException | IllegalStateException e) {
          e.printStackTrace();
        }
        ContextResource to = new ContextResource(st.getObject().stringValue(),
            st.getContext() != null ? st.getContext().stringValue() : null);
        Node toNode = null;
        try {
          toNode = nodeCache.get(to, new Callable<Node>() {
            @Override
            public Node call() {  //throws AnyException
              Node node = null;
              Map<String, Object> params = new HashMap<>();
              String cypher = buildCypher(st.getObject().stringValue(),
                  st.getContext() != null ? st.getContext().stringValue() : null,
                  params);
              Result result = inThreadTransaction.execute(cypher, params);
              if (result.hasNext()) {
                node = (Node) result.next().get("n");
                if (result.hasNext()) {
                  String props =
                      "{uri: " + st.getObject().stringValue() +
                          (st.getContext() == null ? "}" :
                              ", graphUri: " + st.getContext().stringValue() + "}");
                  throw new IllegalStateException(
                      "There are multiple matching nodes for the given properties " + props);
                }
              }
              return node;
            }
          });
        } catch (InvalidCacheLoadException | IllegalStateException e) {
          e.printStackTrace();
        }
        if (fromNode == null || toNode == null) {
          notDeletedStatementCount++;
          continue;
        }

        // find relationship if it exists
        if (fromNode.getDegree(RelationshipType
                .withName(handleIRI(st.getPredicate(), RDFToLPGStatementProcessor.RELATIONSHIP)),
            Direction.OUTGOING) <
            toNode.getDegree(RelationshipType
                    .withName(handleIRI(st.getPredicate(), RDFToLPGStatementProcessor.RELATIONSHIP)),
                Direction.INCOMING)) {
          for (Relationship rel : fromNode
              .getRelationships(Direction.OUTGOING,
                  RelationshipType.withName(
                      handleIRI(st.getPredicate(), RDFToLPGStatementProcessor.RELATIONSHIP)))) {
            if (rel.getEndNode().equals(toNode)) {
              rel.delete();
              break;
            }
          }
        } else {
          for (Relationship rel : toNode
              .getRelationships(Direction.INCOMING,
                  RelationshipType.withName(
                      handleIRI(st.getPredicate(), RDFToLPGStatementProcessor.RELATIONSHIP)))) {
            if (rel.getStartNode().equals(fromNode)) {
              rel.delete();
              break;
            }
          }
        }
        deleteNodeIfEmpty(toNode);
        deleteNodeIfEmpty(fromNode);
      } catch (ExecutionException e) {
        e.printStackTrace();
      }
    }

    statements.clear();
    resourceLabels.clear();
    resourceProps.clear();
    nodeCache.invalidateAll();
    if (statementsWithbNodeCount > 0) {
      setbNodeInfo(statementsWithbNodeCount
          + " of the statements could not be deleted, due to containing a blank node.");
    }

    //TODO what to return here? number of nodes and rels?
    return 0;
  }

  @Override
  protected void periodicOperation() {
    try (Transaction tempTransaction = graphdb.beginTx()) {
      this.runPartialTx(tempTransaction);
      tempTransaction.commit();
      log.debug("partial commit: " + mappedTripleCounter + " triples deleted. Total so far: "
          + totalTriplesMapped);
    }
    totalTriplesMapped += mappedTripleCounter;
    mappedTripleCounter = 0;
  }

  public long getNotDeletedStatementCount() {
    return notDeletedStatementCount + statementsWithbNodeCount;
  }

  public String getbNodeInfo() {
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
        node.getAllProperties().containsKey("uri") &&
        ((node.getAllProperties().containsKey("graphUri") && nodePropertyCount == 2) ||
            nodePropertyCount == 1)) {
      node.delete();
    }
  }

}
