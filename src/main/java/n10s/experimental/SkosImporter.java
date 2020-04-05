package n10s.experimental;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.stream.Collectors;
import n10s.RDFToLPGStatementProcessor;
import n10s.Util;
import n10s.graphconfig.RDFParserConfig;
import org.eclipse.rdf4j.model.IRI;
import org.eclipse.rdf4j.model.Literal;
import org.eclipse.rdf4j.model.Statement;
import org.eclipse.rdf4j.model.impl.SimpleValueFactory;
import org.eclipse.rdf4j.model.vocabulary.RDF;
import org.eclipse.rdf4j.model.vocabulary.SKOS;
import org.eclipse.rdf4j.model.vocabulary.SKOSXL;
import org.eclipse.rdf4j.rio.RDFHandlerException;
import org.neo4j.graphdb.Direction;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.graphdb.Transaction;
import org.neo4j.logging.Log;

public class SkosImporter extends RDFToLPGStatementProcessor implements Callable<Integer> {

  protected Map<String, String> resourceIndirectAltProps = new HashMap<>();
  protected Map<String, String> resourceIndirectPrefProps = new HashMap<>();
  protected Map<String, String> resourceIndirectHiddenProps = new HashMap<>();
  protected Map<String, Literal> pendingLabels = new HashMap<>();
  public static final Label RESOURCE = Label.label("Resource");
  Cache<String, Node> nodeCache;

  protected SkosImporter(GraphDatabaseService db, Transaction tx,
      RDFParserConfig conf, Log l) {
    super(db, tx, conf, l);
    nodeCache = CacheBuilder.newBuilder()
        .maximumSize(conf.getNodeCacheSize())
        .build();
  }

  @Override
  protected void periodicOperation() {
    Util.inTx(graphdb, this);
    totalTriplesMapped += mappedTripleCounter;
    mappedTripleCounter = 0;
  }

  @Override
  public void endRDF() throws RDFHandlerException {

    Util.inTx(graphdb, this);
    totalTriplesMapped += mappedTripleCounter;
    // take away the unused extra triples (maybe too much for just returning a counter?)
    int unusedExtra = 0;
    for (Entry<String, Map<String, Object>> entry : resourceProps.entrySet()) {
      for (Entry<String, Object> values : entry.getValue().entrySet()) {
        if (values.getValue() instanceof List) {
          unusedExtra += ((List) values.getValue()).size();
        } else {
          unusedExtra++;
        }
      }
    }
    totalTriplesMapped -= unusedExtra;

    statements.clear();
    resourceLabels.clear();
    resourceProps.clear();

  }

  @Override
  public void handleStatement(Statement st) {

    if (parserConfig.getPredicateExclusionList() == null || !parserConfig
        .getPredicateExclusionList()
        .contains(st.getPredicate().stringValue())) {
      if (st.getPredicate().equals(RDF.TYPE) && (st.getObject().equals(SKOS.CONCEPT)) && st
          .getSubject() instanceof IRI) {
        instantiate(parserConfig.getGraphConf().getClassLabelName(),
            (IRI) st.getSubject());
      } else if ((st.getPredicate().equals(SKOS.BROADER) || st.getPredicate().equals(SKOS.RELATED))
          && st.getObject() instanceof IRI && st
          .getSubject() instanceof IRI) {
        instantiatePair("Resource", (IRI) st.getSubject(), "Resource", (IRI) st.getObject());
        addStatement(st);
      } else if (st.getPredicate().equals(SKOS.NARROWER) && st.getObject() instanceof IRI && st
          .getSubject() instanceof IRI) {
        instantiatePair("Resource", (IRI) st.getSubject(), "Resource", (IRI) st.getObject());
        //we invert the order to make it a 'broader'
        addStatement(SimpleValueFactory.getInstance()
            .createStatement((IRI) st.getObject(), SKOS.BROADER, st.getSubject()));
      } else if (
          (st.getPredicate().equals(SKOS.PREF_LABEL) || st.getPredicate().equals(SKOS.ALT_LABEL) ||
              st.getPredicate().equals(SKOS.HIDDEN_LABEL)) && st.getSubject() instanceof IRI) {
        //we also instantiate when we get a label property
        instantiate(parserConfig.getGraphConf().getClassLabelName(),
            (IRI) st.getSubject());
        setProp(st.getSubject().stringValue(), st.getPredicate(), (Literal) st.getObject());
        mappedTripleCounter++;
      } else if (
          (st.getPredicate().equals(SKOSXL.PREF_LABEL) || st.getPredicate().equals(SKOSXL.ALT_LABEL)
              ||
              st.getPredicate().equals(SKOSXL.HIDDEN_LABEL)) && st.getSubject() instanceof IRI) {
        //instantiate when we get a label property
        instantiate(parserConfig.getGraphConf().getClassLabelName(),
            (IRI) st.getSubject());
        //set the indirect reference
        setIndirectPropFirstLeg(st.getSubject().stringValue(), st.getPredicate(),
            st.getObject().stringValue());
        mappedTripleCounter++;
      } else if (st.getPredicate().equals(SKOSXL.LITERAL_FORM) && st
          .getObject() instanceof Literal) {
        // complete the indirect reference
        setIndirectPropSecondLeg(st.getSubject().stringValue(), (Literal) st.getObject());
        mappedTripleCounter++;
      }
    }
    totalTriplesParsed++;

    if (parserConfig.getCommitSize() != Long.MAX_VALUE
        && mappedTripleCounter != 0
        && mappedTripleCounter % parserConfig.getCommitSize() == 0) {
      periodicOperation();
    }

  }

  private void instantiate(String label, IRI iri) {
    setLabel(iri.stringValue(), label);
    resourceProps.get(iri.stringValue()).put("name", iri.getLocalName());
    mappedTripleCounter++;
  }

  private void instantiatePair(String label1, IRI iri1, String label2, IRI iri2) {
    setLabel(iri1.stringValue(), label1);
    resourceProps.get(iri1.stringValue()).put("name", iri1.getLocalName());
    setLabel(iri2.stringValue(), label2);
    resourceProps.get(iri2.stringValue()).put("name", iri2.getLocalName());
    mappedTripleCounter++;
  }

  private void setIndirectPropSecondLeg(String subject, Literal object) {
    if (resourceIndirectPrefProps.containsKey(subject)) {
      setProp(resourceIndirectPrefProps.get(subject), SKOS.PREF_LABEL, object);
    } else if (resourceIndirectAltProps.containsKey(subject)) {
      setProp(resourceIndirectAltProps.get(subject), SKOS.ALT_LABEL, object);
    } else if (resourceIndirectHiddenProps.containsKey(subject)) {
      setProp(resourceIndirectHiddenProps.get(subject), SKOS.HIDDEN_LABEL, object);
    } else {
      //first leg not parsed yet
      pendingLabels.put(subject, object);
    }

  }

  private void setIndirectPropFirstLeg(String subject, IRI predicate, String object) {
    if (pendingLabels.containsKey(object)) {
      setProp(subject, predicate, pendingLabels.get(object));
    } else {
      if (predicate.equals(SKOSXL.PREF_LABEL)) {
        resourceIndirectPrefProps.put(object, subject);
      } else if (predicate.equals(SKOSXL.ALT_LABEL)) {
        resourceIndirectAltProps.put(object, subject);
      } else if (predicate.equals(SKOSXL.HIDDEN_LABEL)) {
        resourceIndirectHiddenProps.put(object, subject);
      }
    }
  }


  @Override
  public Integer call() throws Exception {

    for (Entry<String, Set<String>> entry : resourceLabels.entrySet()) {

      if (!entry.getValue().isEmpty()) {
        // if the uri is for an element for which we have not parsed the
        // onto element type (class, property, rel) then it's an extra-statement
        // and should be processed when the element in question is parsed

        final Node node = nodeCache.get(entry.getKey(), new Callable<Node>() {
          @Override
          public Node call() {
            Node node = tx.findNode(RESOURCE, "uri", entry.getKey());
            if (node == null) {
              node = tx.createNode(RESOURCE);
              node.setProperty("uri", entry.getKey());
            }
            return node;
          }
        });

        entry.getValue().forEach(l -> node.addLabel(Label.label(l)));

        resourceProps.get(entry.getKey()).forEach((k, v) -> {
          //node.setProperty(k, v);
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
                ((List) v).add(node.getProperty(k));
              }
              //we make it a set to remove duplicates. Semantics of multivalued props in RDF.
              node.setProperty(k, toPropertyValue(((List) v).stream().collect(Collectors.toSet())));
            }
          } else {
            node.setProperty(k, v);
          }
        });
        //and after processing the props for all uris, then we clear them from resourceProps
        resourceProps.remove(entry.getKey());

      }
    }

    for (Statement st : statements) {

      final Node fromNode = nodeCache.get(st.getSubject().stringValue(), new Callable<Node>() {
        @Override
        public Node call() {  //throws AnyException
          return tx.findNode(RESOURCE, "uri", st.getSubject().stringValue());
        }
      });

      final Node toNode = nodeCache.get(st.getObject().stringValue(), new Callable<Node>() {
        @Override
        public Node call() {  //throws AnyException
          return tx.findNode(RESOURCE, "uri", st.getObject().stringValue());
        }
      });

      // check if the rel is already present. If so, don't recreate.
      // explore the node with the lowest degree
      boolean found = false;
      if (fromNode.getDegree(RelationshipType.withName(translateRelName(st.getPredicate())),
          Direction.OUTGOING) <
          toNode.getDegree(RelationshipType.withName(translateRelName(st.getPredicate())),
              Direction.INCOMING)) {
        for (Relationship rel : fromNode
            .getRelationships(Direction.OUTGOING,
                RelationshipType.withName(translateRelName(st.getPredicate())))) {
          if (rel.getEndNode().equals(toNode)) {
            found = true;
            break;
          }
        }
      } else {
        for (Relationship rel : toNode
            .getRelationships(Direction.INCOMING,
                RelationshipType.withName(translateRelName(st.getPredicate())))) {
          if (rel.getStartNode().equals(fromNode)) {
            found = true;
            break;
          }
        }
      }

      if (!found) {
        fromNode.createRelationshipTo(
            toNode,
            RelationshipType.withName(translateRelName(st.getPredicate())));
      }
    }

    statements.clear();
    resourceLabels.clear();

    return 0;
  }

  private String translateRelName(IRI iri) {
    if (iri.equals(SKOS.BROADER)) {
      return parserConfig.getGraphConf().getSubClassOfRelName();
    } else if (iri.equals(SKOS.RELATED)) {
      return parserConfig.getGraphConf().getRelatedConceptRelName();
    } else {
      //Not valid. Should not happen.
      return "REL";
    }
  }

}
