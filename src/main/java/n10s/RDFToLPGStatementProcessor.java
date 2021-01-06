package n10s;

import n10s.graphconfig.GraphConfig;
import n10s.graphconfig.RDFParserConfig;
import n10s.utils.DateUtils;
import n10s.utils.InvalidNamespacePrefixDefinitionInDB;
import n10s.utils.NsPrefixMap;
import org.eclipse.rdf4j.model.*;
import org.eclipse.rdf4j.model.impl.SimpleValueFactory;
import org.eclipse.rdf4j.model.vocabulary.RDF;
import org.eclipse.rdf4j.model.vocabulary.XMLSchema;
import org.eclipse.rdf4j.rio.RDFHandlerException;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Transaction;
import org.neo4j.internal.helpers.collection.Iterables;
import org.neo4j.logging.Log;

import java.time.DateTimeException;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.format.DateTimeParseException;
import java.util.*;

import javax.xml.bind.DatatypeConverter;

import static n10s.graphconfig.GraphConfig.*;
import static n10s.graphconfig.Params.CUSTOM_DATA_TYPE_SEPERATOR;
import static n10s.graphconfig.Params.PREFIX_SEPARATOR;
import static n10s.mapping.MappingUtils.getImportMappingsFromDB;


/**
 * Created by jbarrasa on 15/04/2019.
 */

public abstract class RDFToLPGStatementProcessor extends ConfiguredStatementHandler {

  public static final int RELATIONSHIP = 0;
  public static final int LABEL = 1;
  public static final int PROPERTY = 2;
  public static final int DATATYPE = 3;
  protected final Log log;
  private static final String[] EMPTY_ARRAY = new String[0];
  protected Transaction tx;
  protected final RDFParserConfig parserConfig;
  private final Map<String, String> vocMappings;
  protected GraphDatabaseService graphdb;
  protected NsPrefixMap namespaces;
  protected Set<Statement> statements = new HashSet<>();
  protected Map<String, Map<String, Object>> resourceProps = new HashMap<>();
  protected Map<Statement, Map<String, Object>> relProps = new HashMap<>();
  protected Map<String, Set<String>> resourceLabels = new HashMap<>();
  public long totalTriplesParsed = 0;
  public long totalTriplesMapped = 0;
  public long mappedTripleCounter = 0;
  private final ValueFactory vf = SimpleValueFactory.getInstance();
  protected StringBuilder loadWarnings = new StringBuilder();
  protected boolean datatypeConflictFound = false;


  public RDFToLPGStatementProcessor(GraphDatabaseService db, Transaction tx, RDFParserConfig conf,
      Log l) {
    this.graphdb = db;
    this.tx = tx;
    this.parserConfig = conf;
    log = l;
    //initialise vocMappings  if needed
    if (this.parserConfig.getGraphConf().getHandleVocabUris()
        == GraphConfig.GRAPHCONF_VOC_URI_MAP) {
      Map<String, String> mappingsTemp = getImportMappingsFromDB(this.graphdb);
      if (mappingsTemp.containsKey(RDF.TYPE.stringValue())) {
        //a mapping on RDF.TYPE is illegal
        mappingsTemp.remove(RDF.TYPE.stringValue());
        log.debug(
            "Mapping on rdf:type property is not applicable in RDF import and will be discarded");
      }
      this.vocMappings = mappingsTemp;
    } else {
      this.vocMappings = null;
    }
  }

  protected void appendWarning(String warningMessage){
    loadWarnings.append(warningMessage);
  }
  private void loadNamespaces() throws InvalidNamespacePrefixDefinitionInDB {
    namespaces = new NsPrefixMap(tx, false);
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
        return object.stringValue() + (
            parserConfig.getGraphConf().isKeepLangTag() && language.isPresent() ? "@"
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
        return DateUtils.parseDateTime(object.stringValue());
      } catch (IllegalArgumentException e) {
        //if date cannot be parsed we return string value
        return object.stringValue();
      }
    } else if (datatype.equals(XMLSchema.DATE)) {
      try {
        return DateUtils.parseDate(object.stringValue());
      } catch (IllegalArgumentException e) {
        //if date cannot be parsed we return string value
        return object.stringValue();
      }
    } else {
      //it's a custom data type
      if (parserConfig.getGraphConf().isKeepCustomDataTypes() && !(
          parserConfig.getGraphConf().getHandleVocabUris() == GRAPHCONF_VOC_URI_IGNORE
              || parserConfig.getGraphConf().getHandleVocabUris() == GRAPHCONF_VOC_URI_MAP)) {
        //keep custom type as long as property is not absent from customDT list
        if (parserConfig.getGraphConf().getCustomDataTypePropList() == null || parserConfig
            .getGraphConf().getCustomDataTypePropList()
            .contains(propertyIRI.stringValue())) {
          return getValueWithDatatype(datatype, object.stringValue());
        } else {
          return object.stringValue();
        }
      }
    }
    // default
    return object.stringValue();
  }
  
  

  protected String getValueWithDatatype(IRI datatype, String value) {
    StringBuilder result = new StringBuilder(value);
    result.append(CUSTOM_DATA_TYPE_SEPERATOR);
    if (parserConfig.getGraphConf().getHandleVocabUris() == GRAPHCONF_VOC_URI_SHORTEN) {
      result.append(handleIRI(datatype, DATATYPE));
    } else {
      result.append(datatype.stringValue());
    }
    return result.toString();
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
    if (parserConfig.getGraphConf().getHandleVocabUris() == GRAPHCONF_VOC_URI_SHORTEN ||
        parserConfig.getGraphConf().getHandleVocabUris() == GRAPHCONF_VOC_URI_SHORTEN_STRICT) {
      String localName = iri.getLocalName();
      String prefix = namespaces.getPrefixOrAdd(iri.getNamespace(),
          parserConfig.getGraphConf().getHandleVocabUris() == GRAPHCONF_VOC_URI_SHORTEN_STRICT);
      return prefix + PREFIX_SEPARATOR + localName;
    } else if (parserConfig.getGraphConf().getHandleVocabUris() == GRAPHCONF_VOC_URI_IGNORE) {
      return applyCapitalisation(iri.getLocalName(), elementType);
    } else if (parserConfig.getGraphConf().getHandleVocabUris() == GRAPHCONF_VOC_URI_MAP) {
      return mapElement(iri, elementType, null);
    } else { //if (handleUris  ==  URL_KEEP){
      return iri.stringValue();
    }
  }

  private String applyCapitalisation(String name, int element) {
    if (parserConfig.getGraphConf().isApplyNeo4jNaming()) {
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
    if (parserConfig.getGraphConf().getHandleVocabUris() == GRAPHCONF_VOC_URI_SHORTEN ||
        parserConfig.getGraphConf().getHandleVocabUris() == GRAPHCONF_VOC_URI_SHORTEN_STRICT) {
      //differentiate between map/shorten and keep_long urls?
      try {
        loadNamespaces();
      } catch (InvalidNamespacePrefixDefinitionInDB e) {
        throw new RDFHandlerException(e.getMessage());
      }
      log.debug(
          "Found " + namespaces.getPrefixes().size() + " namespaces in the DB: " + namespaces);
    }
  }

  @Override
  public void handleNamespace(String prefix, String uri) throws RDFHandlerException {

  }

  protected void addStatement(Statement st) {
    statements.add(st);
  }


  private void initialise(String subjectUri) {
    initialiseResourceProps(resourceProps, subjectUri);
    initialiseLabels(subjectUri);
  }

  private Set<String> initialiseLabels(String subjectUri) {
    Set<String> labels = new HashSet<>();
    //        labels.add("Resource");  this was in the preview version (praaopt)
    resourceLabels.put(subjectUri, labels);
    return labels;
  }

  private Map<String, Object> initialiseResourceProps(Map<String,Map<String, Object>> m, String subjectUri) {
    Map<String, Object> props = new HashMap<>();
    //props.put("uri", subjectUri); this was in the preview version probably removed as an optimisation
    m.put(subjectUri, props);
    return props;
  }

  private Map<String, Object> initialiseRelProps(Map<Statement,Map<String, Object>> m, Statement stmt) {
    HashMap<String, Object> props = new HashMap<>();
    //props.put("uri", subjectUri); this was in the preview version probably removed as an optimisation
    m.put(stmt, props);
    return props;
  }

  protected boolean setProp(String subjectUri, IRI propertyIRI, Literal propValueRaw) {
    Map<String, Object> props;
    Object propValue = getObjectValue(propertyIRI, propValueRaw);
    if (propValue != null) {
      if (!resourceProps.containsKey(subjectUri)) {
        props = initialiseResourceProps(resourceProps,subjectUri);
        initialiseLabels(subjectUri);
      } else {
        props = resourceProps.get(subjectUri);
      }
      addPropertyValueToElementProps(propertyIRI, props, propValue);
      //  For future? An option to reify multivalued property vals (literal nodes?)
      //  else if (handleMultival == PROP_REIFY) {
      //      //do reify
      //  }
    }
    return propValue != null;
  }

  private void addPropertyValueToElementProps(IRI propertyIRI, Map<String, Object> props, Object propValue) {

    String propName = handleIRI(propertyIRI, PROPERTY);

    if (parserConfig.getGraphConf().getHandleMultival() == GRAPHCONF_MULTIVAL_PROP_OVERWRITE) {
      // Ok for single valued props. If applied to multivalued ones
      // only the last value read is kept.
      props.put(propName, propValue);
    } else if (parserConfig.getGraphConf().getHandleMultival() == GRAPHCONF_MULTIVAL_PROP_ARRAY) {
      if (parserConfig.getGraphConf().getMultivalPropList() == null || parserConfig.getGraphConf()
          .getMultivalPropList()
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
  }

  protected void setLabel(String subjectUri, String label) {
    Set<String> labels;

    if (!resourceLabels.containsKey(subjectUri)) {
      initialiseResourceProps(resourceProps,subjectUri);
      labels = initialiseLabels(subjectUri);
    } else {
      labels = resourceLabels.get(subjectUri);
    }

    labels.add(label);
  }

  private void addResource(String subjectUri) {

    if (!resourceLabels.containsKey(subjectUri)) {
      initialise(subjectUri);
    }
  }

  @Override
  public void handleStatement(Statement st) {
    IRI predicate = st.getPredicate();
    Resource subject = st.getSubject();
    Value object = st.getObject();


    String subjectUri = (subject instanceof BNode? "bnode://" + subject.stringValue(): subject.stringValue());

    if (parserConfig.getPredicateExclusionList() == null || !parserConfig
        .getPredicateExclusionList()
        .contains(predicate.stringValue()))
    // filter by predicate
    {
      if(subject instanceof Triple){
        //RDF* only parsed when triples used as subject (i.e. to store relationship properties)
        Triple reifiedStatement = (Triple) subject;
        if (!(reifiedStatement.getObject() instanceof Literal ||
            reifiedStatement.getPredicate().equals(RDF.TYPE)) && object instanceof Literal){
            //reified datatype property statements cannot be easily mapped to a PG. Ignore
          String subjectUri1 = reifiedStatement.getSubject() instanceof BNode? "bnode://" +
                  reifiedStatement.getSubject().stringValue(): reifiedStatement.getSubject().stringValue();
          String objectUri1 = reifiedStatement.getObject() instanceof BNode? "bnode://" +
                  reifiedStatement.getObject().stringValue(): reifiedStatement.getObject().stringValue();

          addResource(subjectUri1);
          addResource(objectUri1);
          Statement stmt = vf.createStatement(vf.createIRI(subjectUri1),
              reifiedStatement.getPredicate(), vf.createIRI(objectUri1));
          addStatement(stmt);
          addRelProp(stmt, predicate, (Literal)object);
          mappedTripleCounter++;
        }
      } else if (object instanceof Literal) {
        // DataType property
        if (setProp(subjectUri, predicate, (Literal) object)) {
          // property may be filtered because of lang filter hence the conditional increment.
          mappedTripleCounter++;
        }
      } else if ((parserConfig.getGraphConf().getHandleRDFTypes() == GRAPHCONF_RDFTYPES_AS_LABELS ||
          parserConfig.getGraphConf().getHandleRDFTypes() == GRAPHCONF_RDFTYPES_AS_LABELS_AND_NODES)
          && predicate.equals(RDF.TYPE)
          && !(object instanceof BNode)) {

        setLabel(subjectUri, handleIRI((IRI) object, LABEL));

        if (parserConfig.getGraphConf().getHandleRDFTypes()
            == GRAPHCONF_RDFTYPES_AS_LABELS_AND_NODES) {
          addResource(subjectUri);
          addResource(object.stringValue());
          //addStatement(st);
          addStatement(vf.createStatement(vf.createIRI(subjectUri),
                  predicate, object));
        }

        mappedTripleCounter++;

      } else if (object instanceof Triple) {
        //ignore RDF* statements with triples as object
      } else {
        addResource(subjectUri);
        String objectUri = object instanceof BNode? "bnode://" + object.stringValue(): object.stringValue();
        addResource(objectUri);
        addStatement(vf.createStatement(vf.createIRI(subjectUri),
                predicate, vf.createIRI(objectUri)));
        mappedTripleCounter++;
      }
    }
    totalTriplesParsed++;

    if (parserConfig.getCommitSize() != Long.MAX_VALUE && mappedTripleCounter != 0
        && mappedTripleCounter % parserConfig.getCommitSize() == 0) {
      periodicOperation();
    }
  }

  protected boolean addRelProp(Statement stmt, IRI predicate, Literal propValueRaw){

    Map<String, Object> props;
    Object propValue = getObjectValue(predicate, propValueRaw);
    if (propValue != null) {
      if (!relProps.containsKey(stmt)) {
        props = initialiseRelProps(relProps,stmt);
      } else {
        props = relProps.get(stmt);
      }
      addPropertyValueToElementProps(predicate, props, propValue);
      //  For future? An option to reify multivalued property vals (literal nodes?)
      //  else if (handleMultival == PROP_REIFY) {
      //      //do reify
      //  }
    }
    return propValue != null;
  }


  @Override
  public RDFParserConfig getParserConfig() {
    return parserConfig;
  }

  public Map<String, String> getNamespaces() {
    return (namespaces == null ? null : namespaces.getPrefixToNs());
  }

  // Stolen from APOC ;)
  protected Object toPropertyValue(Object value) {
    Iterable it = (Iterable) value;
    Object first = Iterables.firstOrNull(it);
    if (first == null) {
      return EMPTY_ARRAY;
    }
    return Iterables.asArray(first.getClass(), it);
  }

  protected List<String> defaultToString(Iterator it) {
      List<String> list = new ArrayList<>();
      while(it.hasNext()) {
        Object next = it.next();
        list.add(next instanceof String? next.toString(): getValueWithDatatype(getBestGuessDatatype(next.getClass()),next.toString()));
      }
    return list;

  }

  private IRI getBestGuessDatatype(Class<?> c) {
    if (c.equals(Double.class)){
      return XMLSchema.DOUBLE;
    } else if (c.equals(Long.class)) {
      return XMLSchema.LONG;
    } else if (c.equals(Boolean.class)) {
      return XMLSchema.BOOLEAN;
    } else if (c.equals(LocalDate.class)) {
      return XMLSchema.DATE;
    } else if (c.equals(LocalDateTime.class)) {
      return XMLSchema.DATETIME;
    } else {
      return XMLSchema.STRING;
    }
  }

  protected abstract void periodicOperation();

  public String getWarnings() {
    return loadWarnings.toString() + (datatypeConflictFound?datatypeConflictMessage():"");
  }

  private String datatypeConflictMessage() {
    if(this.getParserConfig().isStrictDataTypeCheck()){
      return "Some triples were discarded because of heterogeneous data typing of values for the same property. " +
              "Check logs  for details.";
    } else {
      return "Some heterogeneous data typing of values for the same property was found. Values were imported as typed strings. " +
              "Check logs  for details.";
    }
  }

  protected class NamespacePrefixConflict extends RDFHandlerException {
    public NamespacePrefixConflict(String s, Exception  e) {
      super(s,e);
    }
  }

  protected class PartialCommitException extends RDFHandlerException {
    public PartialCommitException(String s, Exception  e) {
      super(s,e);
    }
  }


  protected class HeterogeneousDataTyping extends RDFHandlerException {
    public HeterogeneousDataTyping(){ super("Heterogeneous Datatyping");}
  }
}
