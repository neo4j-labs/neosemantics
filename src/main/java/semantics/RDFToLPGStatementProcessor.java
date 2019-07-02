package semantics;

import static semantics.RDFImport.CUSTOM_DATA_TYPE_SEPERATOR;
import static semantics.RDFImport.DATATYPE;
import static semantics.RDFImport.LABEL;
import static semantics.RDFImport.PREFIX_SEPARATOR;
import static semantics.RDFImport.PROPERTY;
import static semantics.RDFImport.RELATIONSHIP;
import static semantics.RDFParserConfig.PROP_ARRAY;
import static semantics.RDFParserConfig.PROP_OVERWRITE;
import static semantics.RDFParserConfig.URL_IGNORE;
import static semantics.RDFParserConfig.URL_MAP;
import static semantics.RDFParserConfig.URL_SHORTEN;
import static semantics.mapping.MappingUtils.getImportMappingsFromDB;

import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.format.DateTimeParseException;
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
import org.eclipse.rdf4j.rio.RDFHandlerException;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Result;
import org.neo4j.logging.Log;


/**
 * Created by jbarrasa on 15/04/2019.
 */

abstract class RDFToLPGStatementProcessor extends ConfiguredStatementHandler {

  protected final Map<String, String> vocMappings;
  protected final RDFParserConfig parserConfig;
  protected GraphDatabaseService graphdb;
  protected final Log log;
  protected Map<String, String> namespaces = new HashMap<>();
  protected Set<Statement> statements = new HashSet<>();
  protected Map<String, Map<String, Object>> resourceProps = new HashMap<>();
  protected Map<String, Set<String>> resourceLabels = new HashMap<>();
  protected long totalTriplesParsed = 0;
  protected long totalTriplesMapped = 0;
  protected long mappedTripleCounter = 0;

  protected RDFToLPGStatementProcessor(GraphDatabaseService db, RDFParserConfig conf, Log l) {
    this.graphdb = db;
    this.parserConfig = conf;
    log = l;
    if (this.parserConfig.getHandleVocabUris() == URL_MAP) {
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
  }


  protected void loadNamespaces() {
    Result nslist = graphdb.execute("MATCH (n:NamespacePrefixDefinition) \n" +
        "UNWIND keys(n) AS namespace\n" +
        "RETURN namespace, n[namespace] AS prefix");
    if (!nslist.hasNext()) {
      //no namespace definition, initialise with popular ones
      namespaces.putAll(popularNamespaceList());
    } else {
      while (nslist.hasNext()) {
        Map<String, Object> ns = nslist.next();
        namespaces.put((String) ns.get("namespace"), (String) ns.get("prefix"));
      }
      popularNamespaceList().forEach((k, v) -> {
        if (!namespaces.containsKey(k)) {
          namespaces.put(k, v);
        }
      });
    }

  }

  protected Map<String, String> popularNamespaceList() {
    Map<String, String> ns = new HashMap<>();
    ns.put("http://schema.org/", "sch");
    ns.put("http://purl.org/dc/elements/1.1/", "dc");
    ns.put("http://purl.org/dc/terms/", "dct");
    ns.put("http://www.w3.org/2004/02/skos/core#", "skos");
    ns.put("http://www.w3.org/2000/01/rdf-schema#", "rdfs");
    ns.put("http://www.w3.org/2002/07/owl#", "owl");
    ns.put("http://www.w3.org/1999/02/22-rdf-syntax-ns#", "rdf");
    ns.put("http://www.w3.org/ns/shacl#", "sh");
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
   * Processing for literals as follows Mapping according to this figure:
   * https://www.w3.org/TR/xmlschema11-2/#built-in-datatypes String -> String Each sub-category of
   * integer -> long decimal, float, and double -> double boolean -> boolean Custom data type ->
   * String (value + CUSTOM_DATA_TYPE_SEPERATOR + custom DT IRI)
   *
   * @return processed literal
   */
  protected Object getObjectValue(IRI propertyIRI, Literal object) {
    IRI datatype = object.getDatatype();
    if (datatype.equals(XMLSchema.STRING) || datatype.equals(RDF.LANGSTRING)) {
      final Optional<String> language = object.getLanguage();
      if (parserConfig.getLanguageFilter() == null || !language.isPresent() || parserConfig
          .getLanguageFilter().equals(language.get())) {
        return object.stringValue() + (parserConfig.isKeepLangTag() && language.isPresent() ? "@"
            + language.get()
            : "");
      } else {
        //filtered by lang
        return null;
      }
    } else if (typeMapsToLongType(datatype)) {
      return object.longValue();
    } else if (typeMapsToDouble(datatype)) {
      return object.doubleValue();
    } else if (datatype.equals(XMLSchema.BOOLEAN)) {
      return object.booleanValue();
    } else if (datatype.equals(XMLSchema.DATETIME)) {
      try {
        return LocalDateTime.parse(object.stringValue());
      } catch (DateTimeParseException e) {
        //if date cannot be parsed we return string value
        return object.stringValue();
      }
    } else if (datatype.equals(XMLSchema.DATE)) {
      try {
        return LocalDate.parse(object.stringValue());
      } catch (DateTimeParseException e) {
        //if date cannot be parsed we return string value
        return object.stringValue();
      }
    } else {
      //it's a custom data type
      if (parserConfig.isKeepCustomDataTypes() && !(parserConfig.getHandleVocabUris() == URL_IGNORE
          || parserConfig.getHandleVocabUris() == URL_MAP)) {
        //keep custom type
        String value = object.stringValue();
        if (parserConfig.getCustomDataTypedPropList() == null || parserConfig
            .getCustomDataTypedPropList()
            .contains(propertyIRI.stringValue())) {
          String datatypeString;
          if (parserConfig.getHandleVocabUris() == URL_SHORTEN) {
            datatypeString = handleIRI(datatype, DATATYPE);
          } else {
            datatypeString = datatype.stringValue();
          }
          value = value.concat(CUSTOM_DATA_TYPE_SEPERATOR + datatypeString);
        }
        return value;
      }
    }
    // default
    return object.stringValue();
  }

  private boolean typeMapsToDouble(IRI datatype) {
    return datatype.equals(XMLSchema.DECIMAL) || datatype.equals(XMLSchema.DOUBLE) ||
        datatype.equals(XMLSchema.FLOAT);
  }

  private boolean typeMapsToLongType(IRI datatype) {
    return datatype.equals(XMLSchema.INTEGER) || datatype.equals(XMLSchema.LONG) || datatype
        .equals(XMLSchema.INT) ||
        datatype.equals(XMLSchema.SHORT) || datatype.equals(XMLSchema.BYTE) ||
        datatype.equals(XMLSchema.NON_NEGATIVE_INTEGER) || datatype
        .equals(XMLSchema.POSITIVE_INTEGER) ||
        datatype.equals(XMLSchema.UNSIGNED_LONG) || datatype.equals(XMLSchema.UNSIGNED_INT) ||
        datatype.equals(XMLSchema.UNSIGNED_SHORT) || datatype.equals(XMLSchema.UNSIGNED_BYTE) ||
        datatype.equals(XMLSchema.NON_POSITIVE_INTEGER) || datatype
        .equals(XMLSchema.NEGATIVE_INTEGER);
  }

  @Override
  public void handleComment(String comment) throws RDFHandlerException {

  }


  protected String handleIRI(IRI iri, int elementType) {
    //TODO: would caching this improve perf? It's kind of cached in getPrefix()
    if (parserConfig.getHandleVocabUris() == URL_SHORTEN) {
      String localName = iri.getLocalName();
      String prefix = getPrefix(iri.getNamespace());
      return prefix + PREFIX_SEPARATOR + localName;
    } else if (parserConfig.getHandleVocabUris() == URL_IGNORE) {
      return applyCapitalisation(iri.getLocalName(), elementType);
    } else if (parserConfig.getHandleVocabUris() == URL_MAP) {
      return mapElement(iri, elementType, null);
    } else { //if (handleUris  ==  URL_KEEP){
      return iri.stringValue();
    }
  }

  private String applyCapitalisation(String name, int element) {
    if (parserConfig.isApplyNeo4jNaming()) {
      //apply Neo4j naming recommendations
      if (element == RELATIONSHIP) {
        return name.toUpperCase();
      } else if (element == LABEL) {
        return name.substring(0, 1).toUpperCase() + name.substring(1);
      } else if (element == PROPERTY) {
        return name.substring(0, 1).toLowerCase() + name.substring(1);
      } else {
        //should not happen
        return name;
      }
    } else {
      //keep capitalisation as is
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
      return applyCapitalisation(iri.getLocalName(), elementType);
    }
  }


  @Override
  public void startRDF() throws RDFHandlerException {
    if (parserConfig.getHandleVocabUris() == URL_SHORTEN) {
      //differentiate between map/shorten and keep_long urls?
      loadNamespaces();
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

    Object propValue = getObjectValue(propertyIRI, propValueRaw);

    if (propValue != null) {
      if (!resourceProps.containsKey(subjectUri)) {
        props = initialiseProps(subjectUri);
        initialiseLabels(subjectUri);
      } else {
        props = resourceProps.get(subjectUri);
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

            // If multiple datatypes are tried to be stored in the same List,
            // a java.lang.ArrayStoreException arises
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
      //  For future? An option to reify multivalued property vals (literal nodes?)
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

    if (parserConfig.getPredicateExclusionList() == null || !parserConfig
        .getPredicateExclusionList()
        .contains(predicate.stringValue()))
    // filter by predicate
    {
      if (object instanceof Literal) {
        // DataType property
        if (setProp(subject.stringValue(), predicate, (Literal) object)) {
          // property may be filtered because of lang filter hence the conditional increment.
          mappedTripleCounter++;
        }
      } else if (parserConfig.isTypesToLabels() && predicate.equals(RDF.TYPE)
          && !(object instanceof BNode)) {
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

    if (parserConfig.getCommitSize() != Long.MAX_VALUE
        && mappedTripleCounter % parserConfig.getCommitSize() == 0) {
      periodicOperation();
    }
  }

  @Override
  RDFParserConfig getParserConfig() {
    return parserConfig;
  }

  Map<String, String> getNamespaces() {
    return namespaces;
  }

  protected abstract void periodicOperation();

}
