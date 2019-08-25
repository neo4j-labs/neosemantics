package semantics;

import static semantics.RDFImport.RELATIONSHIP;
import static semantics.RDFParserConfig.URL_SHORTEN;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.stream.Collectors;
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
 * This class implements an RDF handler to statement-wise imported RDF quadruples.
 *
 * Created on 06/06/2019.
 *
 * @author Emre Arkan
 */
class RDFQuadDirectStatementLoader extends RDFQuadToLPGStatementProcessor implements
    Callable<Integer> {

  private static final Label RESOURCE = Label.label("Resource");
  private static final String[] EMPTY_ARRAY = new String[0];
  private Cache<ContextResource, Node> nodeCache;

  RDFQuadDirectStatementLoader(GraphDatabaseService db, RDFParserConfig conf, Log l) {
    super(db, conf, l);
    nodeCache = CacheBuilder.newBuilder()
        .maximumSize(conf.getNodeCacheSize())
        .build();
  }

  /**
   * Analog to endRDF in {@link DirectStatementLoader}
   *
   * Executed at the end of each commit to inform the user of the current state of the import
   * process.
   */
  @Override
  public void endRDF() throws RDFHandlerException {
    Util.inTx(graphdb, this);
    totalTriplesMapped += mappedTripleCounter;
    if (parserConfig.getHandleVocabUris() == URL_SHORTEN) {
      persistNamespaceNode();
    }
    log.info("Import complete: " + totalTriplesMapped + "  triples ingested out of "
        + totalTriplesParsed + " parsed");
  }

  /**
   * Analog to persistNamespaceNode in {@link DirectStatementLoader}
   */
  private void persistNamespaceNode() {
    Map<String, Object> params = new HashMap<>();
    params.put("props", namespaces);
    graphdb.execute("MERGE (n:NamespacePrefixDefinition) SET n+={props}", params);
  }

  /**
   * Analog to call in {@link DirectStatementLoader}, however strongly modified to process
   * quadruples rather than triples.
   *
   * {@link #resourceLabels}, {@link #resourceProps}, and {@link #statements}, which contain the
   * statements to be imported are processed respectively. If a statement already exist in the
   * database, it is ignored.
   *
   * @return An obligatory return, which is always 0, since the overridden method must return an
   * Integer
   */
  @Override
  public Integer call() throws Exception {
    for (Map.Entry<ContextResource, Set<String>> entry : resourceLabels.entrySet()) {

      final Node node = nodeCache.get(entry.getKey(), () -> {
        Node searched_node = null;
        Map<String, Object> params = new HashMap<>();
        String cypher = buildCypher(entry.getKey().getUri(),
            entry.getKey().getGraphUri(),
            params);
        Result result = graphdb.execute(cypher, params);
        if (result.hasNext()) {
          searched_node = (Node) result.next().get("n");
          if (result.hasNext()) {
            String props =
                "{uri: " + entry.getKey().getUri() +
                    (entry.getKey().getGraphUri() == null ? "}" :
                        ", graphUri: " + entry.getKey().getGraphUri() + "}");
            throw new IllegalStateException(
                "There are multiple matching nodes for the given properties " + props);
          }
        }
        if (searched_node == null) {
          searched_node = graphdb.createNode(RESOURCE);
          searched_node.setProperty("uri", entry.getKey().getUri());
          if (entry.getKey().getGraphUri() != null) {
            searched_node.setProperty("graphUri", entry.getKey().getGraphUri());
          }
        }
        return searched_node;
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
              for (int i = 0; i < properties.length; i++) {
                ((List) v).add(properties[i]);
              }
            } else {
              ((List) v).add(node.getProperty(k));
            }
            node.setProperty(k, toPropertyValue(((List) v).stream().collect(Collectors.toSet())));
          }
        } else {
          node.setProperty(k, v);
        }
      });
    }

    for (Statement st : statements) {
      ContextResource from = new ContextResource(st.getSubject().stringValue(),
          st.getContext() != null ? st.getContext().stringValue() : null);
      final Node fromNode = nodeCache.get(from, () -> {
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
      });
      ContextResource to = new ContextResource(st.getObject().stringValue(),
          st.getContext() != null ? st.getContext().stringValue() : null);
      final Node toNode = nodeCache.get(to, () -> {
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
      });

      // Check if the relationship is already present. If so, don't recreate.
      boolean found = false;
      if (fromNode.getDegree(RelationshipType.withName(handleIRI(st.getPredicate(), RELATIONSHIP)),
          Direction.OUTGOING) <
          toNode.getDegree(RelationshipType.withName(handleIRI(st.getPredicate(), RELATIONSHIP)),
              Direction.INCOMING)) {
        for (Relationship rel : fromNode
            .getRelationships(RelationshipType.withName(handleIRI(st.getPredicate(), RELATIONSHIP)),
                Direction.OUTGOING)) {
          if (rel.getEndNode().equals(toNode)) {
            found = true;
            break;
          }
        }
      } else {
        for (Relationship rel : toNode
            .getRelationships(RelationshipType.withName(handleIRI(st.getPredicate(), RELATIONSHIP)),
                Direction.INCOMING)) {
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
    return 0;
  }

  /**
   * Analog to periodicOperation in {@link DirectStatementLoader}
   */
  @Override
  protected void periodicOperation() {
    Util.inTx(graphdb, this);
    totalTriplesMapped += mappedTripleCounter;
    mappedTripleCounter = 0;
    persistNamespaceNode();
  }

  // Adapted from APOC
  private Object toPropertyValue(Object value) {
    Iterable it = (Iterable) value;
    Object first = Iterables.firstOrNull(it);
    if (first == null) {
      return EMPTY_ARRAY;
    }
    return Iterables.asArray(first.getClass(), it);
  }
}
