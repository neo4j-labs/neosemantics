package semantics;

import static semantics.RDFImport.RELATIONSHIP;

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
import org.neo4j.graphdb.*;
import org.neo4j.logging.Log;
import semantics.config.GraphConfig;
import semantics.config.RDFParserConfig;

/**
 * Created on 06/06/2019.
 *
 * @author Emre Arkan
 */

class RDFQuadDirectStatementLoader extends RDFQuadToLPGStatementProcessor implements
    Callable<Integer> {

  private static final Label RESOURCE = Label.label("Resource");
  private static final String[] EMPTY_ARRAY = new String[0];
  private Cache<ContextResource, Node> nodeCache;

  RDFQuadDirectStatementLoader(GraphDatabaseService db, Transaction tx, RDFParserConfig conf, Log l) {

    super(db, tx, conf, l);
    nodeCache = CacheBuilder.newBuilder()
        .maximumSize(conf.getNodeCacheSize())
        .build();
  }

  @Override
  public void endRDF() throws RDFHandlerException {
    Util.inTx(graphdb, this);
    totalTriplesMapped += mappedTripleCounter;
    if (parserConfig.getGraphConf().getHandleVocabUris() == GraphConfig.GRAPHCONF_VOC_URI_SHORTEN) {
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
    tx.execute("MERGE (n:NamespacePrefixDefinition) SET n+={props}", params);
  }

  @Override
  public Integer call() throws Exception {
    int count = 0;

    for (Map.Entry<ContextResource, Set<String>> entry : resourceLabels.entrySet()) {

      final Node node = nodeCache.get(entry.getKey(), new Callable<Node>() {
        @Override
        public Node call() {
          Node node = null;
          Map<String, Object> params = new HashMap<>();
          String cypher = buildCypher(entry.getKey().getUri(),
              entry.getKey().getGraphUri(),
              params);
          Result result = tx.execute(cypher, params);
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
          if (node == null) {
            node = tx.createNode(RESOURCE);
            node.setProperty("uri", entry.getKey().getUri());
            if (entry.getKey().getGraphUri() != null) {
              node.setProperty("graphUri", entry.getKey().getGraphUri());
            }
          }
          return node;
        }
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
                //here an exception can be raised if types are conflicting
              }
            } else {
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
      ContextResource from = new ContextResource(st.getSubject().stringValue(),
          st.getContext() != null ? st.getContext().stringValue() : null);
      final Node fromNode = nodeCache.get(from, new Callable<Node>() {
        @Override
        public Node call() {  //throws AnyException
          Node node;
          Map<String, Object> params = new HashMap<>();
          String cypher = buildCypher(st.getSubject().stringValue(),
              st.getContext() != null ? st.getContext().stringValue() : null,
              params);
          Result result = tx.execute(cypher, params);
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
      ContextResource to = new ContextResource(st.getObject().stringValue(),
          st.getContext() != null ? st.getContext().stringValue() : null);
      final Node toNode = nodeCache.get(to, new Callable<Node>() {
        @Override
        public Node call() {  //throws AnyException
          Node node;
          Map<String, Object> params = new HashMap<>();
          String cypher = buildCypher(st.getObject().stringValue(),
              st.getContext() != null ? st.getContext().stringValue() : null,
              params);
          Result result = tx.execute(cypher, params);
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
