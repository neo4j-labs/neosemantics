package n10s.rdf.load;

import static n10s.graphconfig.GraphConfig.GRAPHCONF_VOC_URI_SHORTEN;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;

import java.lang.reflect.Array;
import java.util.ArrayList;
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
import org.neo4j.graphdb.*;
import org.neo4j.logging.Log;

/**
 * Created by jbarrasa on 09/11/2016.
 */

public class DirectStatementLoader extends RDFToLPGStatementProcessor {

  private static final Label RESOURCE = Label.label("Resource");
  private Cache<String, Node> nodeCache;

  public DirectStatementLoader(GraphDatabaseService db, Transaction tx, RDFParserConfig conf,
      Log l, boolean reuseTx ) {

    super(db, tx, conf, l, reuseTx);
    nodeCache = CacheBuilder.newBuilder()
        .maximumSize(conf.getNodeCacheSize())
        .build();
  }

  @Override
  public void endRDF() throws RDFHandlerException {
    if(reuseTx){
      namespaces.partialRefresh(tx);
      log.debug("namespace prefixes synced: " + namespaces.toString());
      this.runPartialTx(tx);
      log.debug("rdf import commit: " + mappedTripleCounter + " triples ingested.");
      //not sure this is needed here
      totalTriplesMapped += mappedTripleCounter;
    } else {
      periodicOperation();
    }
    log.debug("Import complete: " + totalTriplesMapped + "  triples ingested out of "
        + totalTriplesParsed + " parsed");
  }

  public Integer runPartialTx(Transaction inThreadTransaction) {

    try {
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
          resourceProps.get(entry.getKey()).forEach((k, v) -> setProperty(node,k,v));
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
          if (relProps != null) {
            for (Entry<String, Object> entry : relProps.entrySet()) {

              setProperty(theRel, entry.getKey(), entry.getValue());
            }
          }

        } catch (ExecutionException e) {
          e.printStackTrace();
        }
      }


      Integer result = 0;
      if (parserConfig.getGraphConf().getHandleVocabUris() == GRAPHCONF_VOC_URI_SHORTEN) {
        result = namespaces.partialRefresh(inThreadTransaction);
      }

      return result;

    } finally {
      statements.clear();
      resourceLabels.clear();
      resourceProps.clear();
      relProps.clear();
      nodeCache.invalidateAll();
    }
  }

  private void setProperty(Entity node, String k, Object v) {
    if (v instanceof List) {
      Object currentValue = node.getProperty(k, null);
      List<Object> newList = new ArrayList<>();
      if (currentValue != null) {
        //initialise with existing values
        if (currentValue.getClass().isArray()) {
          int length = Array.getLength(currentValue);
          for (int i = 0; i < length; i ++) {
            Object atomicValue = Array.get(currentValue, i);
            newList.add(atomicValue);
          }
        } else {
          //TODO: this logic could go because now it's not possible to change
          // from atomic to multival without emptying the DB
          newList.add(node.getProperty(k));
        }
      }

      Class<?> currentDatatype = newList.isEmpty()?((List) v).get(0).getClass():newList.get(0).getClass();

      List<Object> discardedItems = new ArrayList<>();

      for(Object x:(List)v) {
        if (x.getClass().equals(currentDatatype)){
          newList.add(x);
        }  else {
          discardedItems.add(x);
        }
      }

      if(!discardedItems.isEmpty()){
        this.datatypeConflictFound |= true;
        if (getParserConfig().isStrictDataTypeCheck()){
          this.mappedTripleCounter-= discardedItems.size();
          log.warn("The following values for property '" + k + "' have been discarded because of datatype heterogeneity (previously stored values are of type " + currentDatatype + ") : " + discardedItems );
          node.setProperty(k, toPropertyValue(newList.stream().collect(Collectors.toSet())));
        } else {
          //default all to string if they're not already return defaultToString(it.iterator());
          newList.addAll(discardedItems);
          node.setProperty(k, toPropertyValue(defaultToString(newList.iterator()).stream().collect(Collectors.toSet())));
        }
      } else {
        //no discarded elements. all good, newlist contains all the values. nothing to do
        node.setProperty(k, toPropertyValue(newList.stream().collect(Collectors.toSet())));
      }

    } else {
      node.setProperty(k, v);
    }
  }


  @Override
  protected void periodicOperation() {

    if (parserConfig.getGraphConf().getHandleVocabUris() == GRAPHCONF_VOC_URI_SHORTEN) {
      try (Transaction tempTransaction = graphdb.beginTx()) {
        namespaces.partialRefresh(tempTransaction);
        tempTransaction.commit();
        log.debug("namespace prefixes synced: " + namespaces.toString());
      } catch (Exception e) {
        log.error("Problems syncing up namespace prefixes in partial commit. ", e);
        if (getParserConfig().isAbortOnError()){
          throw new NamespacePrefixConflict("Problems syncing up namespace prefixes in partial commit. ", e);
        }
      }
    }

    try (Transaction tempTransaction = graphdb.beginTx()) {
      this.runPartialTx(tempTransaction);
      tempTransaction.commit();
      log.debug("partial commit: " + mappedTripleCounter + " triples ingested. Total so far: "
          + totalTriplesMapped);
      totalTriplesMapped += mappedTripleCounter;
    } catch (Exception e) {
      log.error("Problems when running partial commit. Partial transaction rolled back. "  + mappedTripleCounter + " triples lost.", e);
      if (getParserConfig().isAbortOnError()){
        throw new PartialCommitException("Problems when running partial commit. Partial transaction rolled back. " , e);
      }
    }

    mappedTripleCounter = 0;

  }

}
