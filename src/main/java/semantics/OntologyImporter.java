package semantics;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Callable;
import org.eclipse.rdf4j.model.IRI;
import org.eclipse.rdf4j.model.Statement;
import org.eclipse.rdf4j.model.vocabulary.OWL;
import org.eclipse.rdf4j.model.vocabulary.RDF;
import org.eclipse.rdf4j.model.vocabulary.RDFS;
import org.eclipse.rdf4j.rio.RDFHandlerException;
import org.neo4j.graphdb.Direction;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.logging.Log;

public class OntologyImporter extends RDFToLPGStatementProcessor implements Callable<Integer> {

  protected Set<Statement> extraStatements = new HashSet<>();
  public static final Label RESOURCE = Label.label("Resource");
  Cache<String, Node> nodeCache;

  protected OntologyImporter(GraphDatabaseService db,
      OntologyLoaderConfig conf, Log l) {
    super(db, conf, l);
    nodeCache = CacheBuilder.newBuilder()
        .maximumSize(conf.getNodeCacheSize())
        .build();
  }

  @Override
  public void startRDF() throws RDFHandlerException {
    //do nothing
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
    // take away the unused extra triples
    totalTriplesMapped -= extraStatements.size();

    statements.clear();
    resourceLabels.clear();
    resourceProps.clear();
    extraStatements.clear();

  }

  @Override
  public void handleStatement(Statement st) {

    if (parserConfig.getPredicateExclusionList() == null || !parserConfig
        .getPredicateExclusionList()
        .contains(st.getPredicate().stringValue())) {
      if (st.getPredicate().equals(RDF.TYPE) && (st.getObject().equals(RDFS.CLASS) || st.getObject()
          .equals(OWL.CLASS)) && st.getSubject() instanceof IRI) {
        instantiate(((OntologyLoaderConfig) parserConfig).getClassLabelName(),
            (IRI) st.getSubject());
      } else if (st.getPredicate().equals(RDF.TYPE) && (st.getObject().equals(RDF.PROPERTY) || st
          .getObject().equals(OWL.OBJECTPROPERTY)) && st.getSubject() instanceof IRI) {
        instantiate(((OntologyLoaderConfig) parserConfig).getObjectPropertyLabelName(),
            (IRI) st.getSubject());
      } else if (st.getPredicate().equals(RDF.TYPE) && st.getObject().equals(OWL.DATATYPEPROPERTY)
          && st.getSubject() instanceof IRI) {
        instantiate(((OntologyLoaderConfig) parserConfig).getDataTypePropertyLabelName(),
            (IRI) st.getSubject());
      } else if (st.getPredicate().equals(RDFS.SUBCLASSOF) && st.getObject() instanceof IRI && st
          .getSubject() instanceof IRI) {
        instantiatePair("Resource", (IRI) st.getSubject(), "Resource", (IRI) st.getObject());
        addStatement(st);
      } else if (st.getPredicate().equals(RDFS.SUBPROPERTYOF) && st.getObject() instanceof IRI && st
          .getSubject() instanceof IRI) {
        instantiatePair("Resource", (IRI) st.getSubject(), "Resource", (IRI) st.getObject());
        addStatement(st);
      } else if (st.getPredicate().equals(RDFS.DOMAIN) && st.getObject() instanceof IRI && st
          .getSubject() instanceof IRI) {
        instantiatePair("Resource", (IRI) st.getSubject(), "Resource", (IRI) st.getObject());
        addStatement(st);
      } else if (st.getPredicate().equals(RDFS.RANGE) && st.getObject() instanceof IRI && st
          .getSubject() instanceof IRI) {
        instantiatePair("Resource", (IRI) st.getSubject(), "Resource", (IRI) st.getObject());
        addStatement(st);
      } else if ((st.getPredicate().equals(RDFS.LABEL) || st.getPredicate().equals(RDFS.COMMENT))
          && st.getSubject() instanceof IRI) {
        extraStatements.add(st);
        mappedTripleCounter++;
      }
    }
    totalTriplesParsed++;

    if (parserConfig.getCommitSize() != Long.MAX_VALUE
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


  @Override
  public Integer call() throws Exception {
    for (Map.Entry<String, Set<String>> entry : resourceLabels.entrySet()) {

      final Node node = nodeCache.get(entry.getKey(), new Callable<Node>() {
        @Override
        public Node call() {
          Node node = graphdb.findNode(RESOURCE, "uri", entry.getKey());
          if (node == null) {
            node = graphdb.createNode(RESOURCE);
            node.setProperty("uri", entry.getKey());
          }
          return node;
        }
      });

      entry.getValue().forEach(l -> node.addLabel(Label.label(l)));

      resourceProps.get(entry.getKey()).forEach((k, v) -> {
        node.setProperty(k, v);
      });
    }

    for (Statement st : statements) {

      final Node fromNode = nodeCache.get(st.getSubject().stringValue(), new Callable<Node>() {
        @Override
        public Node call() {  //throws AnyException
          return graphdb.findNode(RESOURCE, "uri", st.getSubject().stringValue());
        }
      });

      final Node toNode = nodeCache.get(st.getObject().stringValue(), new Callable<Node>() {
        @Override
        public Node call() {  //throws AnyException
          return graphdb.findNode(RESOURCE, "uri", st.getObject().stringValue());
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
            .getRelationships(RelationshipType.withName(translateRelName(st.getPredicate())),
                Direction.OUTGOING)) {
          if (rel.getEndNode().equals(toNode)) {
            found = true;
            break;
          }
        }
      } else {
        for (Relationship rel : toNode
            .getRelationships(RelationshipType.withName(translateRelName(st.getPredicate())),
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
            RelationshipType.withName(translateRelName(st.getPredicate())));
      }
    }

    extraStatements.removeIf(s -> persistedStatementCanBeRemoved(s));

    statements.clear();
    resourceLabels.clear();
    resourceProps.clear();

    return 0;
  }


  private boolean persistedStatementCanBeRemoved(Statement st) {
    final Node fromNode = graphdb.findNode(RESOURCE, "uri", st.getSubject().stringValue());

    if (fromNode == null) {
      //resource not loaded, keep in list just in case
      return false;
    } else {
      //add property and delete from list
      fromNode.setProperty(st.getPredicate().getLocalName(), st.getObject().stringValue());
      return true;
    }

  }

  private String translateRelName(IRI iri) {
    if (iri.equals(RDFS.SUBCLASSOF)) {
      return ((OntologyLoaderConfig) parserConfig).getSubClassOfRelName();
    } else if (iri.equals(RDFS.SUBPROPERTYOF)) {
      return ((OntologyLoaderConfig) parserConfig).getSubPropertyOfRelName();
    } else if (iri.equals(RDFS.DOMAIN)) {
      return ((OntologyLoaderConfig) parserConfig).getDomainRelName();
    } else if (iri.equals(RDFS.RANGE)) {
      return ((OntologyLoaderConfig) parserConfig).getRangeRelName();
    } else {
      //Not valid
      return "REL";
    }
  }

}
