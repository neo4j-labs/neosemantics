package n10s.quadrdf;

import static n10s.graphconfig.GraphConfig.GRAPHCONF_RDFTYPES_AS_LABELS;
import static n10s.graphconfig.GraphConfig.GRAPHCONF_RDFTYPES_AS_LABELS_AND_NODES;

import com.google.common.base.Preconditions;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import n10s.RDFToLPGStatementProcessor;
import n10s.graphconfig.GraphConfig;
import n10s.graphconfig.RDFParserConfig;
import org.eclipse.rdf4j.model.BNode;
import org.eclipse.rdf4j.model.IRI;
import org.eclipse.rdf4j.model.Literal;
import org.eclipse.rdf4j.model.Resource;
import org.eclipse.rdf4j.model.Statement;
import org.eclipse.rdf4j.model.Value;
import org.eclipse.rdf4j.model.vocabulary.RDF;
import org.eclipse.rdf4j.rio.RDFHandler;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Transaction;
import org.neo4j.logging.Log;


/**
 * Created on 06/06/2019.
 *
 * @author Emre Arkan
 */

abstract class RDFQuadToLPGStatementProcessor extends RDFToLPGStatementProcessor implements
    RDFHandler {

  Map<ContextResource, Map<String, Object>> resourceProps;
  Map<ContextResource, Set<String>> resourceLabels;

  RDFQuadToLPGStatementProcessor(GraphDatabaseService db, Transaction tx, RDFParserConfig conf,
      Log l) {
    super(db, tx, conf, l);
    resourceProps = new HashMap<>();
    resourceLabels = new HashMap<>();
  }

  @Override
  public void handleStatement(Statement st) {
    Resource context = st.getContext();
    IRI predicate = st.getPredicate();
    Resource subject = st.getSubject();
    Value object = st.getObject();
    ContextResource sub = new ContextResource(subject.stringValue(),
        context != null ? context.stringValue() : null);
    ContextResource obj = new ContextResource(object.stringValue(),
        context != null ? context.stringValue() : null);

    if (parserConfig.getPredicateExclusionList() == null || !parserConfig
        .getPredicateExclusionList()
        .contains(predicate.stringValue())) {
      if (object instanceof Literal) {
        if (setProp(sub, predicate, (Literal) object)) {
          // property may be filtered because of lang filter hence the conditional increment.
          mappedTripleCounter++;
        }
      } else if ((parserConfig.getGraphConf().getHandleRDFTypes() == GRAPHCONF_RDFTYPES_AS_LABELS
          && predicate.equals(RDF.TYPE) ||
          parserConfig.getGraphConf().getHandleRDFTypes() == GRAPHCONF_RDFTYPES_AS_LABELS_AND_NODES)
          && !(object instanceof BNode)) {

        setLabel(sub, handleIRI((IRI) object, LABEL));

        if (parserConfig.getGraphConf().getHandleRDFTypes()
            == GRAPHCONF_RDFTYPES_AS_LABELS_AND_NODES) {
          addResource(sub);
          addResource(obj);
          addStatement(st);
        }

        mappedTripleCounter++;
      } else {
        addResource(sub);
        addResource(obj);
        addStatement(st);
        mappedTripleCounter++;
      }
    }
    totalTriplesParsed++;

    if (parserConfig.getCommitSize() != Long.MAX_VALUE
        && mappedTripleCounter % parserConfig.getCommitSize() == 0) {
      periodicOperation();
    }
  }

  protected abstract void periodicOperation();

  String buildCypher(String uri, String graphUri, Map<String, Object> params) {
    Preconditions.checkNotNull(uri);
    StringBuilder cypher = new StringBuilder();
    params.put("uri", uri);
    cypher.append("MATCH (n:Resource) ");
    cypher.append("WHERE n.uri = $uri ");
    if (graphUri != null) {
      cypher.append("AND n.graphUri = $graphUri ");
      params.put("graphUri", graphUri);
    } else {
      cypher.append("AND n.graphUri is null ");
    }
    cypher.append("RETURN n");
    return cypher.toString();
  }

  private boolean setProp(ContextResource contextResource, IRI propertyIRI,
      Literal propValueRaw) {
    Map<String, Object> props;

    String propName = handleIRI(propertyIRI, PROPERTY);

    Object propValue = getObjectValue(propertyIRI, propValueRaw);

    if (propValue != null) {
      if (!resourceProps.containsKey(contextResource)) {
        props = initialiseProps(contextResource);
        initialiseLabels(contextResource);
      } else {
        props = resourceProps.get(contextResource);
      }
      if (parserConfig.getGraphConf().getHandleMultival()
          == GraphConfig.GRAPHCONF_MULTIVAL_PROP_OVERWRITE) {
        // Ok for single valued props. If applied to multivalued ones
        // only the last value read is kept.
        props.put(propName, propValue);
      } else if (parserConfig.getGraphConf().getHandleMultival()
          == GraphConfig.GRAPHCONF_MULTIVAL_PROP_ARRAY) {
        if (parserConfig.getGraphConf().getMultivalPropList() == null || parserConfig.getGraphConf()
            .getMultivalPropList()
            .contains(propertyIRI.stringValue())) {
          if (props.containsKey(propName)) {
            List<Object> propVals = (List<Object>) props.get(propName);
            propVals.add(propValue);

            // If multiple datatypes are tried to be stored in the same List, a java.lang
            // .ArrayStoreException arises
          } else {
            List<Object> propVals = new ArrayList<>();
            propVals.add(propValue);
            props.put(propName, propVals);
          }
        } else {
          //if handleMultival set to ARRAY but prop not in list, then default to overwrite.
          props.put(propName, propValue);
        }
      }
    }
    return propValue != null;
  }

  private void setLabel(ContextResource contextResource, String label) {
    Set<String> labels;
    if (!resourceLabels.containsKey(contextResource)) {
      initialiseProps(contextResource);
      labels = initialiseLabels(contextResource);
    } else {
      labels = resourceLabels.get(contextResource);
    }
    labels.add(label);
  }

  private void addResource(ContextResource contextResource) {
    if (!resourceLabels.containsKey(contextResource)) {
      initialise(contextResource);
    }
  }

  private void initialise(ContextResource contextResource) {
    initialiseProps(contextResource);
    initialiseLabels(contextResource);
  }

  private Set<String> initialiseLabels(ContextResource contextResource) {
    Set<String> labels = new HashSet<>();
    resourceLabels.put(contextResource, labels);
    return labels;
  }

  private HashMap<String, Object> initialiseProps(ContextResource contextResource) {
    HashMap<String, Object> props = new HashMap<>();
    resourceProps.put(contextResource, props);
    return props;
  }

}
