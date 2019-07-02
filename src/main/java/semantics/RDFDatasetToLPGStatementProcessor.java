package semantics;

import static semantics.RDFImport.LABEL;
import static semantics.RDFImport.PROPERTY;
import static semantics.RDFParserConfig.PROP_ARRAY;
import static semantics.RDFParserConfig.PROP_OVERWRITE;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.eclipse.rdf4j.model.BNode;
import org.eclipse.rdf4j.model.IRI;
import org.eclipse.rdf4j.model.Literal;
import org.eclipse.rdf4j.model.Resource;
import org.eclipse.rdf4j.model.Statement;
import org.eclipse.rdf4j.model.Value;
import org.eclipse.rdf4j.model.vocabulary.RDF;
import org.eclipse.rdf4j.rio.RDFHandler;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.logging.Log;


/**
 * Created on 06/06/2019.
 *
 * @author Emre Arkan
 */

abstract class RDFDatasetToLPGStatementProcessor extends RDFToLPGStatementProcessor implements
    RDFHandler {

  Map<ContextResource, Map<String, Object>> resourceProps;
  Map<ContextResource, Set<String>> resourceLabels;

  RDFDatasetToLPGStatementProcessor(GraphDatabaseService db, RDFParserConfig conf, Log l) {
    super(db, conf, l);
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
      } else if (parserConfig.isTypesToLabels() && predicate.equals(RDF.TYPE)
          && !(object instanceof BNode)) {
        setLabel(sub, handleIRI((IRI) object, LABEL));
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
      if (parserConfig.getHandleMultival() == PROP_OVERWRITE) {
        // Ok for single valued props. If applied to multivalued ones
        // only the last value read is kept.
        props.put(propName, propValue);
      } else if (parserConfig.getHandleMultival() == PROP_ARRAY) {
        if (parserConfig.getMultivalPropList() == null || parserConfig.getMultivalPropList()
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
