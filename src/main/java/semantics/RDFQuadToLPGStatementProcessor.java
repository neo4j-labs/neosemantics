package semantics;

import static semantics.RDFImport.LABEL;
import static semantics.RDFImport.PROPERTY;
import static semantics.RDFParserConfig.PROP_ARRAY;
import static semantics.RDFParserConfig.PROP_OVERWRITE;

import com.google.common.base.Preconditions;
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
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.logging.Log;


/**
 * This class extends {@link RDFToLPGStatementProcessor} to process RDF quadruples.
 *
 * Created on 06/06/2019.
 *
 * @author Emre Arkan
 */
abstract class RDFQuadToLPGStatementProcessor extends RDFToLPGStatementProcessor {

  Map<ContextResource, Map<String, Object>> resourceProps;
  Map<ContextResource, Set<String>> resourceLabels;

  RDFQuadToLPGStatementProcessor(GraphDatabaseService db, RDFParserConfig conf, Log l) {
    super(db, conf, l);
    resourceProps = new HashMap<>();
    resourceLabels = new HashMap<>();
  }

  /**
   * Analog to handleStatement in {@link RDFToLPGStatementProcessor}, however modified for {@link
   * ContextResource}.
   *
   * @param st statement to be handled
   */
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

  /**
   * Constructs a Cypher query to find a specific node defined by its uri and optionally graphUri
   *
   * @param uri of the node that is searched
   * @param graphUri of the node that is searched, can be {@code null}
   * @param params parameters of the Cypher query
   * @return constructed Cypher query
   */
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
      cypher.append("AND NOT EXISTS(n.graphUri) ");
    }
    cypher.append("RETURN n");
    return cypher.toString();
  }

  /**
   * Analog to setProp in {@link RDFToLPGStatementProcessor}, however modified for {@link
   * ContextResource}.
   *
   * Adds a given predicate-literal pair to {@link #resourceProps} of a given {@link
   * ContextResource}
   *
   * @param contextResource, to which the property belongs
   * @param propertyIRI, predicate IRI of the statement
   * @param propValueRaw literal value
   * @return true if property value is not {@code null}, false otherwise
   */
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
          props.put(propName, propValue);
        }
      }
    }
    return propValue != null;
  }

  /**
   * Analog to setLabel in {@link RDFToLPGStatementProcessor}, however modified for {@link
   * ContextResource}.
   *
   * Adds a given label to {@link #resourceLabels} of a given {@link ContextResource}.
   *
   * @param contextResource, to which the label belongs
   * @param label to be added to {@link #resourceLabels} of the given {@link ContextResource}
   */
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

  /**
   * Analog to addResource in {@link RDFToLPGStatementProcessor}, however modified for {@link
   * ContextResource}.
   *
   * Adds a given {@link ContextResource} as a resource.
   *
   * @param contextResource which is to be added as a resource, if not already existent
   */
  private void addResource(ContextResource contextResource) {
    if (!resourceLabels.containsKey(contextResource)) {
      initialise(contextResource);
    }
  }

  /**
   * Analog to initialise in {@link RDFToLPGStatementProcessor}, however modified for {@link
   * ContextResource}.
   *
   * Calls {@link #initialiseProps(ContextResource)} and {@link #initialiseLabels(ContextResource)}
   * for the given {@link ContextResource} to initialize these.
   *
   * @param contextResource whose {@link #resourceProps} and {@link #resourceLabels} are to be
   * initialized
   */
  private void initialise(ContextResource contextResource) {
    initialiseProps(contextResource);
    initialiseLabels(contextResource);
  }

  /**
   * Analog to initialiseLabels in {@link RDFToLPGStatementProcessor}, however modified for {@link
   * ContextResource}.
   *
   * Initializes the {@link Set<String>} of a {@link ContextResource} that contains the labels
   * (Object IRI) of the given {@link ContextResource}.
   *
   * @param contextResource whose label set is to be initialized
   * @return {@link Set<String>} for the label {@link Set} of the current {@link ContextResource}
   */
  private Set<String> initialiseLabels(ContextResource contextResource) {
    Set<String> labels = new HashSet<>();
    resourceLabels.put(contextResource, labels);
    return labels;
  }

  /**
   * Analog to initialiseProps in {@link RDFToLPGStatementProcessor}, however modified for {@link
   * ContextResource}.
   *
   * Initializes the {@link HashMap} of the given {@link ContextResource} that contains {@link
   * String} as key (predicate IRI) and {@link Object} as value (literal).
   *
   * @param contextResource whose property map is to be initialized
   * @return {@link HashMap} for the predicate-literal pairs of the given {@link ContextResource}
   */
  private HashMap<String, Object> initialiseProps(ContextResource contextResource) {
    HashMap<String, Object> props = new HashMap<>();
    resourceProps.put(contextResource, props);
    return props;
  }

}
