package semantics;

import static semantics.RDFImport.CUSTOM_DATA_TYPE_SEPERATOR;
import static semantics.RDFImport.DATATYPE;
import static semantics.RDFImport.LABEL;
import static semantics.RDFImport.PREFIX_SEPARATOR;
import static semantics.RDFImport.PROPERTY;
import static semantics.RDFImport.PROP_ARRAY;
import static semantics.RDFImport.PROP_OVERWRITE;
import static semantics.RDFImport.RELATIONSHIP;
import static semantics.RDFImport.URL_IGNORE;
import static semantics.RDFImport.URL_MAP;
import static semantics.RDFImport.URL_SHORTEN;
import static semantics.mapping.MappingUtils.getImportMappingsFromDB;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import org.eclipse.rdf4j.model.BNode;
import org.eclipse.rdf4j.model.IRI;
import org.eclipse.rdf4j.model.Literal;
import org.eclipse.rdf4j.model.Resource;
import org.eclipse.rdf4j.model.Statement;
import org.eclipse.rdf4j.model.Value;
import org.eclipse.rdf4j.model.vocabulary.RDF;
import org.eclipse.rdf4j.model.vocabulary.XMLSchema;
import org.eclipse.rdf4j.rio.RDFHandler;
import org.eclipse.rdf4j.rio.RDFHandlerException;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Result;
import org.neo4j.logging.Log;


/**
 * Created by jbarrasa on 15/04/2019.
 */

abstract class RDFToLPGStatementProcessor implements RDFHandler {

  protected final Map<String, String> vocMappings;
  protected final String langFilter;
  protected final int handleUris;
  protected final int handleMultival;
  protected final boolean labellise;
  protected final boolean keepLangTag;
  protected final boolean keepCustomDataTypes;
  protected final Set<String> multivalPropList;
  protected final Set<String> excludedPredicatesList;
  protected final Set<String> customDataTypedPropList;
  protected final long commitFreq;
  protected GraphDatabaseService graphdb;
  protected Log log;
  protected Map<String, String> namespaces = new HashMap<>();
  protected Set<Statement> statements = new HashSet<>();
  protected Map<String, Map<String, Object>> resourceProps = new HashMap<>();
  protected Map<String, Set<String>> resourceLabels = new HashMap<>();
  protected int totalTriplesParsed = 0;
  protected int totalTriplesMapped = 0;
  protected int mappedTripleCounter = 0;

  protected RDFToLPGStatementProcessor(GraphDatabaseService db, String langFilter, int handleUrls,
      int handleMultival,
      Set<String> multivalPropUriList, boolean keepCustomDataTypes,
      Set<String> customDataTypedPropList, Set<String> predicateExclList,
      boolean klt,
      boolean labellise, long commitFreq) {
    this.graphdb = db;
    this.langFilter = langFilter;
    this.handleUris = handleUrls;
    if (this.handleUris == URL_MAP) {
      Map<String, String> mappingsTemp = getImportMappingsFromDB(this.graphdb);
      if (mappingsTemp.containsKey(RDF.TYPE.stringValue())) {
        //a mapping on RDF.TYPE is illegal
        mappingsTemp.remove(RDF.TYPE.stringValue());
        log.info(
            "Mapping on rdf:type property is not applicable in RDF import and will be discarded");
      }
      this.vocMappings = mappingsTemp;
    } else {
      this.vocMappings = null;
    }
    ;
    this.handleMultival = handleMultival;
    this.labellise = labellise;
    this.commitFreq = commitFreq;
    this.multivalPropList = multivalPropUriList;
    this.customDataTypedPropList = customDataTypedPropList;
    this.excludedPredicatesList = predicateExclList;
    this.keepLangTag = klt;
    this.keepCustomDataTypes = keepCustomDataTypes;
  }


  protected void getExistingNamespaces() {
    Result nslist = graphdb.execute("MATCH (n:NamespacePrefixDefinition) \n" +
        "UNWIND keys(n) AS namespace\n" +
        "RETURN namespace, n[namespace] AS prefix");
    if (!nslist.hasNext()) {
      namespaces.putAll(getPopularNamespaces());
    }
    while (nslist.hasNext()) {
      Map<String, Object> ns = nslist.next();
      namespaces.put((String) ns.get("namespace"), (String) ns.get("prefix"));
    }
  }

  protected abstract Map<String, String> getPopularNamespaces();

  protected Map<String, String> namespaceList() {
    Map<String, String> ns = new HashMap<>();
    ns.put("http://schema.org/", "sch");
    ns.put("http://purl.org/dc/elements/1.1/", "dc");
    ns.put("http://purl.org/dc/terms/", "dct");
    ns.put("http://www.w3.org/2004/02/skos/core#", "skos");
    ns.put("http://www.w3.org/2000/01/rdf-schema#", "rdfs");
    ns.put("http://www.w3.org/2002/07/owl#", "owl");
    ns.put("http://www.w3.org/1999/02/22-rdf-syntax-ns#", "rdf");
    return ns;
  }


  protected String getPrefix(String namespace) {
    if (namespaces.containsKey(namespace)) {
      return namespaces.get(namespace);
    } else {
      namespaces.put(namespace, nextPrefix());
      return namespaces.get(namespace);
    }
  }

  String nextPrefix() {

    return "ns" + namespaces.values().stream().filter(x -> x.startsWith("ns")).count();
  }

  /**
   * Processing for literals as follows
   * Mapping according to this figure: https://www.w3.org/TR/xmlschema11-2/#built-in-datatypes
   * Each sub-category of integer -> long
   * decimal, float, and double -> double
   * boolean -> boolean
   * Custom data type -> String (value + CUSTOM_DATA_TYPE_SEPERATOR + custom DT IRI)
   * String -> String
   * @return processed literal
   */
  protected Object getObjectValue(IRI propertyIRI, Literal object, boolean keepLangTag,
      boolean keepCustomDataTypes, int handleUris) {
    IRI datatype = object.getDatatype();
    if (datatype.equals(XMLSchema.INTEGER) || datatype.equals(XMLSchema.LONG) || datatype
        .equals(XMLSchema.INT) ||
        datatype.equals(XMLSchema.SHORT) || datatype.equals(XMLSchema.BYTE) ||
        datatype.equals(XMLSchema.NON_NEGATIVE_INTEGER) || datatype
        .equals(XMLSchema.POSITIVE_INTEGER) ||
        datatype.equals(XMLSchema.UNSIGNED_LONG) || datatype.equals(XMLSchema.UNSIGNED_INT) ||
        datatype.equals(XMLSchema.UNSIGNED_SHORT) || datatype.equals(XMLSchema.UNSIGNED_BYTE) ||
        datatype.equals(XMLSchema.NON_POSITIVE_INTEGER) || datatype
        .equals(XMLSchema.NEGATIVE_INTEGER)) {
      return object.longValue();
    } else if (datatype.equals(XMLSchema.DECIMAL) || datatype.equals(XMLSchema.DOUBLE) ||
        datatype.equals(XMLSchema.FLOAT)) {
      return object.doubleValue();
    } else if (datatype.equals(XMLSchema.BOOLEAN)) {
      return object.booleanValue();
    } else if (object.getLanguage().isPresent()) {
      final Optional<String> language = object.getLanguage();
      if (langFilter == null || !language.isPresent() ||
          (language.isPresent() && langFilter.equals(language.get()))) {
        return object.stringValue() + (keepLangTag && language.isPresent() ? "@" + language.get()
            : "");
      }
      return null;
    } else if (keepCustomDataTypes && !(handleUris == URL_IGNORE || handleUris == URL_MAP)
        && !datatype.equals(XMLSchema.STRING)) {
      //Custom Datatype
      String value = object.stringValue();
      if (customDataTypedPropList == null || customDataTypedPropList
          .contains(propertyIRI.stringValue())) {
        String datatypeString;
        if (handleUris == URL_SHORTEN) {
          datatypeString = handleIRI(datatype, DATATYPE);
        } else {
          datatypeString = datatype.stringValue();
        }
        value = value.concat(CUSTOM_DATA_TYPE_SEPERATOR + datatypeString);
      }
      return value;
    } else {
      //String or no datatype => String
      return object.stringValue();
    }
  }

  @Override
  public void handleComment(String comment) throws RDFHandlerException {

  }


  protected String handleIRI(IRI iri, int elementType) {
    //TODO: would caching this improve perf? It's kind of cached in getPrefix()
    if (handleUris == URL_SHORTEN) {
      String localName = iri.getLocalName();
      String prefix = getPrefix(iri.getNamespace());
      return prefix + PREFIX_SEPARATOR + localName;
    } else if (handleUris == URL_IGNORE) {
      return neo4jCapitalisation(iri.getLocalName(), elementType);
    } else if (handleUris == URL_MAP) {
      return mapElement(iri, elementType, null);
    } else { //if (handleUris  ==  URL_KEEP){
      return iri.stringValue();
    }
  }

  private String neo4jCapitalisation(String name, int element) {
    if (element == RELATIONSHIP) {
      return name.toUpperCase();
    } else if (element == LABEL) {
      return name.substring(0, 1).toUpperCase() + name.substring(1);
    } else {
      return name;
    }
  }


  private String mapElement(IRI iri, int elementType, String mappingId) {
    //Placeholder for mapping based data load
    //if mappingId is null use default mapping
    if (this.vocMappings.containsKey(iri.stringValue())) {
      return this.vocMappings.get(iri.stringValue());
    } else {
      //if no mapping defined, default to 'IGNORE'
      return neo4jCapitalisation(iri.getLocalName(), elementType);
    }
  }


  @Override
  public void startRDF() throws RDFHandlerException {
    if (handleUris != URL_IGNORE) {
      //differentiate between map/shorten and keep_long urls?
      getExistingNamespaces();
      log.info("Found " + namespaces.size() + " namespaces in the DB: " + namespaces);
    } else {
      log.info("URIs will be ignored. Only local names will be kept.");
    }

  }

  @Override
  public void handleNamespace(String prefix, String uri) throws RDFHandlerException {

  }

  protected void addStatement(Statement st) {
    statements.add(st);
  }


  protected void initialise(String subjectUri) {
    initialiseProps(subjectUri);
    initialiseLabels(subjectUri);
  }

  protected Set<String> initialiseLabels(String subjectUri) {
    Set<String> labels = new HashSet<>();
    //        labels.add("Resource");  this was in the preview version (praaopt)
    resourceLabels.put(subjectUri, labels);
    return labels;
  }

  protected HashMap<String, Object> initialiseProps(String subjectUri) {
    HashMap<String, Object> props = new HashMap<>();
    //props.put("uri", subjectUri); this was in the preview version probably removed as an optimisation
    resourceProps.put(subjectUri, props);
    return props;
  }

  protected boolean setProp(String subjectUri, IRI propertyIRI, Literal propValueRaw) {
    Map<String, Object> props;

    String propName = handleIRI(propertyIRI, PROPERTY);

    Object propValue = getObjectValue(propertyIRI, propValueRaw, keepLangTag, keepCustomDataTypes,
        handleUris); //this will
    // come from config var

    if (propValue != null) {
      if (!resourceProps.containsKey(subjectUri)) {
        props = initialiseProps(subjectUri);
        initialiseLabels(subjectUri);
      } else {
        props = resourceProps.get(subjectUri);
      }
      if (handleMultival == PROP_OVERWRITE) {
        // Ok for single valued props. If applied to multivalued ones
        // only the last value read is kept.
        props.put(propName, propValue);
      } else if (handleMultival == PROP_ARRAY) {
        if (multivalPropList == null || multivalPropList.contains(propertyIRI.stringValue())) {
          if (props.containsKey(propName)) {
            // TODO We're assuming it's an array. If we run this load on a pre-existing dataset
            // potentially with data that's not an array, we'd have to check datatypes
            // and deal with it.
            List<Object> propVals = (List<Object>) props.get(propName);
            propVals.add(propValue);

            // If multiple datatypes are tried to be stored in the same List, a java.lang
            // .ArrayStoreException arises
            // TODO: wrap the exception to provide a better (from user perspective more informative) error message
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
      //  An option to reify multivalued property vals (literal nodes?)
      //  else if (handleMultival == PROP_REIFY) {
      //      //do reify
      //  }
    }
    return propValue != null;
  }

  protected void setLabel(String subjectUri, String label) {
    Set<String> labels;

    if (!resourceLabels.containsKey(subjectUri)) {
      initialiseProps(subjectUri);
      labels = initialiseLabels(subjectUri);
    } else {
      labels = resourceLabels.get(subjectUri);
    }

    labels.add(label);
  }

  protected void addResource(String subjectUri) {

    if (!resourceLabels.containsKey(subjectUri)) {
      initialise(subjectUri);
    }
  }

  @Override
  public void handleStatement(Statement st) {
    IRI predicate = st.getPredicate();
    Resource subject = st.getSubject();
    Value object = st.getObject();

    if (excludedPredicatesList == null || !excludedPredicatesList
        .contains(predicate.stringValue())) {
      if (object instanceof Literal) {
        if (setProp(subject.stringValue(), predicate, (Literal) object)) {
          // property may be filtered because of lang filter hence the conditional increment.
          mappedTripleCounter++;
        }
      } else if (labellise && predicate.equals(RDF.TYPE) && !(object instanceof BNode)) {
        setLabel(subject.stringValue(), handleIRI((IRI) object, LABEL));
        mappedTripleCounter++;
      } else {
        addResource(subject.stringValue());
        addResource(object.stringValue());
        addStatement(st);
        mappedTripleCounter++;
      }
    }
    totalTriplesParsed++;

    if (commitFreq != Integer.MAX_VALUE && mappedTripleCounter % commitFreq == 0) {
      periodicOperation();
    }
  }

  protected abstract void periodicOperation();

}
