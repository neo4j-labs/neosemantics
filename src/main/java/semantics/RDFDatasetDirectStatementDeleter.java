package semantics;

import static semantics.RDFImport.RELATIONSHIP;
import static semantics.RDFParserConfig.URL_SHORTEN;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader.InvalidCacheLoadException;
import com.google.common.collect.Iterators;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
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
import org.neo4j.graphdb.Result;
import org.neo4j.helpers.collection.Iterables;
import org.neo4j.logging.Log;

/**
 * This class implements an RDF handler to statement-wise delete imported RDF data sets
 *
 * Created on 18/06/2019.
 *
 * @author Emre Arkan
 */

class RDFDatasetDirectStatementDeleter extends RDFDatasetToLPGStatementProcessor implements
    Callable<Integer> {

  private static final Label RESOURCE = Label.label("Resource");

  private Cache<ContextResource, Node> nodeCache;
  private long notDeletedStatementCount;
  private long bNodeCount;
  private String bNodeInfo;

  RDFDatasetDirectStatementDeleter(GraphDatabaseService db, RDFParserConfig conf, Log l) {
    super(db, conf, l);
    nodeCache = CacheBuilder.newBuilder()
        .maximumSize(conf.getNodeCacheSize())
        .build();
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

    for (Map.Entry<ContextResource, Set<String>> entry : resourceLabels.entrySet()) {
      if (entry.getKey().getUri().startsWith("genid")) {
        bNodeCount++;
        notDeletedStatementCount++;
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
            Result result = graphdb.execute(cypher, params);
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
      if (node == null) {
        notDeletedStatementCount++;
      }
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
          if (node == null) {
            notDeletedStatementCount += valuesToDelete.size();
            return;
          }
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
          if (node == null) {
            notDeletedStatementCount++;
            return;
          }
          node.removeProperty(k);
        }
      });
      if (node != null) {
        deleteNodeIfEmpty(node);
      }
    }

    for (Statement st : statements) {
      if (st.getSubject() instanceof BNode || st.getObject() instanceof BNode) {
        bNodeCount++;
        notDeletedStatementCount++;
        continue;
      }
      ContextResource from = new ContextResource(st.getSubject().stringValue(),
          st.getContext() != null ? st.getContext().stringValue() : null);
      Node fromNode = null;
      try {
        fromNode = nodeCache.get(from, new Callable<Node>() {
          @Override
          public Node call() {  //throws AnyException
            Node node;
            Map<String, Object> params = new HashMap<>();
            String cypher = buildCypher(st.getSubject().stringValue(),
                st.getContext() != null ? st.getContext().stringValue() : null,
                params);
            Result result = graphdb.execute(cypher, params);
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
            } else {
              throw new NoSuchElementException(
                  "There exists no node with \"uri\": " + st.getSubject().stringValue()
                      + " and \"graphUri\": " + st.getContext().stringValue());
            }
            return node;
          }
        });
      } catch (InvalidCacheLoadException | NoSuchElementException e) {
        e.printStackTrace();
      }
      if (fromNode == null) {
        notDeletedStatementCount++;
        continue;
      }
      ContextResource to = new ContextResource(st.getObject().stringValue(),
          st.getContext() != null ? st.getContext().stringValue() : null);
      Node toNode = null;
      try {
        toNode = nodeCache.get(to, new Callable<Node>() {
          @Override
          public Node call() {  //throws AnyException
            Node node;
            Map<String, Object> params = new HashMap<>();
            String cypher = buildCypher(st.getObject().stringValue(),
                st.getContext() != null ? st.getContext().stringValue() : null,
                params);
            Result result = graphdb.execute(cypher, params);
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
            } else {
              throw new NoSuchElementException(
                  "There exists no node with \"uri\": " + st.getSubject().stringValue()
                      + " and \"graphUri\": " + st.getContext().stringValue());
            }
            return node;
          }
        });
      } catch (InvalidCacheLoadException | NoSuchElementException e) {
        e.printStackTrace();
      }
      if (toNode == null) {
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
    if (notDeletedStatementCount > 0) {
      setbNodeInfo(bNodeCount
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
    return notDeletedStatementCount;
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
        node.getAllProperties().containsKey("uri") &&
        ((node.getAllProperties().containsKey("graphUri") && nodePropertyCount == 2) ||
            nodePropertyCount == 1)) {
      node.delete();
    }
  }

  private void persistNamespaceNode() {
    Map<String, Object> params = new HashMap<>();
    params.put("props", namespaces);
    graphdb.execute("MERGE (n:NamespacePrefixDefinition) SET n+={props}", params);
  }

  // Adapted from APOC :)
  private Object toPropertyValue(Object value) {
    if (value instanceof Iterable) {
      Iterable it = (Iterable) value;
      Object first = Iterables.firstOrNull(it);
      return Iterables.asArray(first.getClass(), it);
    }
    return value;
  }

}
