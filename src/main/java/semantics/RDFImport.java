package semantics;

import java.io.BufferedWriter;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStreamWriter;
import java.net.HttpURLConnection;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLConnection;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Stream;
import org.eclipse.rdf4j.model.IRI;
import org.eclipse.rdf4j.model.impl.SimpleValueFactory;
import org.eclipse.rdf4j.model.util.URIUtil;
import org.eclipse.rdf4j.model.vocabulary.XMLSchema;
import org.eclipse.rdf4j.rio.RDFFormat;
import org.eclipse.rdf4j.rio.RDFHandlerException;
import org.eclipse.rdf4j.rio.RDFParseException;
import org.eclipse.rdf4j.rio.RDFParser;
import org.eclipse.rdf4j.rio.Rio;
import org.eclipse.rdf4j.rio.helpers.BasicParserSettings;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.QueryExecutionException;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.ResourceIterator;
import org.neo4j.graphdb.schema.IndexDefinition;
import org.neo4j.logging.Log;
import org.neo4j.procedure.Context;
import org.neo4j.procedure.Mode;
import org.neo4j.procedure.Name;
import org.neo4j.procedure.Procedure;
import org.neo4j.procedure.UserFunction;
import semantics.result.GraphResult;
import semantics.result.NamespacePrefixesResult;
import semantics.result.StreamedStatement;

/**
 * Created by jbarrasa on 21/03/2016. <p> RDF importer based on: 1. Instancdes of DatatypeProperties
 * become node attributes 2. rdf:type relationships are transformed either into labels or
 * relationships to nodes representing the class 3. Instances of ObjectProperties become
 * relationships ( See https://jbarrasa.com/2016/06/07/importing-rdf-data-into-neo4j/ )
 */
public class RDFImport {

  private static final boolean DEFAULT_TYPES_TO_LABELS = true;

  private static final boolean DEFAULT_KEEP_CUSTOM_DATA_TYPES = false;

  private static final long DEFAULT_COMMIT_SIZE = 25000;
  private static final long DEFAULT_NODE_CACHE_SIZE = 10000;
  public static final String PREFIX_SEPARATOR = "__";
  public static final String CUSTOM_DATA_TYPE_SEPERATOR = "^^";
  static final int URL_SHORTEN = 0;
  static final int URL_IGNORE = 1;
  static final int URL_MAP = 2;
  static final int URL_KEEP = 3;
  static final int PROP_OVERWRITE = 0;
  static final int PROP_ARRAY = 1;
  static final int PROP_REIFY = 2;

  static final int RELATIONSHIP = 0;
  static final int LABEL = 1;
  static final int PROPERTY = 2;
  static final int DATATYPE = 3;


  private static final Pattern DATATYPE_SHORTENED_PATTERN = Pattern.compile(
      "(.+)" + Pattern.quote(CUSTOM_DATA_TYPE_SEPERATOR) + "((\\w+)" +
          Pattern.quote(PREFIX_SEPARATOR) + "(.+))$");
  private static final Pattern DATATYPE_REGULAR_PATTERN = Pattern.compile(
      "(.+?)" + Pattern.quote(CUSTOM_DATA_TYPE_SEPERATOR) + "([a-zA-Z]+:(.+))");

  private static final Pattern SHORTENED_URI_PATTERN =
      Pattern.compile("^(\\w+)__(\\w+)$");

  private static final Pattern LANGUAGE_TAGGED_VALUE_PATTERN =
      Pattern.compile("^(.*)@([a-z,\\-]+)$");

  @Context
  public GraphDatabaseService db;
  @Context
  public Log log;

  public static RDFFormat[] availableParsers = new RDFFormat[]{RDFFormat.RDFXML, RDFFormat.JSONLD,
      RDFFormat.TURTLE,
      RDFFormat.NTRIPLES, RDFFormat.TRIG};

  @Procedure(mode = Mode.WRITE)
  public Stream<ImportResults> importRDF(@Name("url") String url, @Name("format") String format,
      @Name(value = "params", defaultValue = "{}") Map<String, Object> props) {

    final int handleVocabUris = (props.containsKey("handleVocabUris") ? getHandleVocabUrisAsInt(
        (String) props.get("handleVocabUris")) : 0);
    final boolean applyNeo4jNaming = (props.containsKey("applyNeo4jNaming") ? (boolean) props
        .get("applyNeo4jNaming") : false);
    final int handleMultival = (props.containsKey("handleMultival") ? getHandleMultivalAsInt(
        (String) props.get("handleMultival")) : 0);
    final List<String> multivalPropList = (props.containsKey("multivalPropList")
        ? (List<String>) props.get("multivalPropList") : null);
    final List<String> predicateExclusionList = (props.containsKey("predicateExclusionList")
        ? (List<String>) props.get("predicateExclusionList") : null);
    final List<String> customDataTypedPropList = (props.containsKey("customDataTypedPropList")
        ? (List<String>) props.get("customDataTypedPropList") : null);
    final boolean typesToLabels = (props.containsKey("typesToLabels") ? (boolean) props
        .get("typesToLabels") : DEFAULT_TYPES_TO_LABELS);
    final boolean keepLangTag = (props.containsKey("keepLangTag") ? (boolean) props
        .get("keepLangTag") : false);
    final boolean keepCustomDataTypes = (props.containsKey("keepCustomDataTypes") ? (boolean) props
        .get("keepCustomDataTypes") : DEFAULT_KEEP_CUSTOM_DATA_TYPES);
    final long commitSize = (props.containsKey("commitSize") ? (long) props.get("commitSize")
        : DEFAULT_COMMIT_SIZE);
    final long nodeCacheSize = (props.containsKey("nodeCacheSize") ? (long) props
        .get("nodeCacheSize") : DEFAULT_NODE_CACHE_SIZE);
    final String languageFilter = (props.containsKey("languageFilter") ? (String) props
        .get("languageFilter") : null);
    final boolean verifyUriSyntax = (props.containsKey("verifyUriSyntax") ? (Boolean) props
        .get("verifyUriSyntax") : true);

    ImportResults importResults = new ImportResults();

    DirectStatementLoader statementLoader = new DirectStatementLoader(db,
        (commitSize > 0 ? commitSize : 5000),
        nodeCacheSize, handleVocabUris, handleMultival,
        (multivalPropList == null ? null : new HashSet<String>(multivalPropList)),
        keepCustomDataTypes,
        (customDataTypedPropList == null ? null : new HashSet<String>(customDataTypedPropList)),
        (predicateExclusionList == null ? null : new HashSet<String>(predicateExclusionList)),
        typesToLabels, keepLangTag,
        languageFilter, applyNeo4jNaming, log);
    try {
      checkIndexesExist();

      InputStream inputStream = getInputStream(url, props);
      RDFParser rdfParser = Rio.createParser(getFormat(format));
      rdfParser.set(BasicParserSettings.VERIFY_URI_SYNTAX, verifyUriSyntax);
      rdfParser.setRDFHandler(statementLoader);
      rdfParser.parse(inputStream, url);
    } catch (MalformedURLException e) {
      e.printStackTrace();
    } catch (IOException | RDFHandlerException | QueryExecutionException | RDFParseException | RDFImportPreRequisitesNotMet e) {
      importResults.setTerminationKO(e.getMessage());
      e.printStackTrace();
    } finally {
      importResults.setTriplesLoaded(statementLoader.totalTriplesMapped);
      importResults.setNamespaces(statementLoader.getNamespaces());
    }
    return Stream.of(importResults);
  }

  private InputStream getInputStream(String url, Map<String, Object> props) throws IOException {
    URLConnection urlConn;
    //This should be delegated to APOC to do handle different protocols, deal with redirection, etc.
    urlConn = new URL(url).openConnection();
    if (props.containsKey("headerParams")) {
      Map<String, String> headerParams = (Map<String, String>) props.get("headerParams");
      Object method = headerParams.get("method");
      if (method != null && urlConn instanceof HttpURLConnection) {
        HttpURLConnection http = (HttpURLConnection) urlConn;
        http.setRequestMethod(method.toString());
      }
      headerParams.forEach((k, v) -> urlConn.setRequestProperty(k, v));
      if (props.containsKey("payload")) {
        urlConn.setDoOutput(true);
        BufferedWriter writer = new BufferedWriter(
            new OutputStreamWriter(urlConn.getOutputStream(), "UTF-8"));
        writer.write(props.get("payload").toString());
        writer.close();
      }
    }
    return urlConn.getInputStream();
  }

  private int getHandleVocabUrisAsInt(String handleVocUrisAsText) {
    if (handleVocUrisAsText.equals("SHORTEN")) {
      return 0;
    } else if (handleVocUrisAsText.equals("IGNORE")) {
      return 1;
    } else if (handleVocUrisAsText.equals("MAP")) {
      return 2;
    } else { //KEEP
      return 3;
    }
  }

  private int getHandleMultivalAsInt(String ignoreUrlsAsText) {
    if (ignoreUrlsAsText.equals("OVERWRITE")) {
      return 0;
    } else if (ignoreUrlsAsText.equals("ARRAY")) {
      return 1;
    } else if (ignoreUrlsAsText.equals("REIFY")) {
      return 2;
    } else { //HYBRID
      return 3;
    }
  }

  @Procedure(mode = Mode.READ)
  public Stream<GraphResult> previewRDF(@Name("url") String url, @Name("format") String format,
      @Name(value = "params", defaultValue = "{}") Map<String, Object> props) {

    final int handleVocabUris = (props.containsKey("handleVocabUris") ? getHandleVocabUrisAsInt(
        (String) props.get("handleVocabUris")) : 0);
    final boolean applyNeo4jNaming = (props.containsKey("applyNeo4jNaming") ? (boolean) props
        .get("applyNeo4jNaming") : false);
    final int handleMultival = (props.containsKey("handleMultival") ? getHandleMultivalAsInt(
        (String) props.get("handleMultival")) : 0);
    final List<String> multivalPropList = (props.containsKey("multivalPropList")
        ? (List<String>) props.get("multivalPropList") : null);
    final List<String> predicateExclusionList = (props.containsKey("predicateExclusionList")
        ? (List<String>) props.get("predicateExclusionList") : null);
    final List<String> customDataTypedPropList = (props.containsKey("customDataTypesList")
        ? (List<String>) props.get("customDataTypesList") : null);
    final boolean typesToLabels = (props.containsKey("typesToLabels") ? (boolean) props
        .get("typesToLabels") : DEFAULT_TYPES_TO_LABELS);
    final boolean keepCustomDataTypes = (props.containsKey("keepCustomDataTypes") ? (boolean) props
        .get("keepCustomDataTypes") : DEFAULT_KEEP_CUSTOM_DATA_TYPES);
    final boolean keepLangTag = (props.containsKey("keepLangTag") ? (boolean) props
        .get("keepLangTag") : false);
    final String languageFilter = (props.containsKey("languageFilter") ? (String) props
        .get("languageFilter") : null);
    final boolean verifyUriSyntax = (props.containsKey("verifyUriSyntax") ? (Boolean) props
        .get("verifyUriSyntax") : true);

    Map<String, Node> virtualNodes = new HashMap<>();
    List<Relationship> virtualRels = new ArrayList<>();

    StatementPreviewer statementViewer = new StatementPreviewer(db, handleVocabUris, handleMultival,
        (multivalPropList == null ? null : new HashSet<String>(multivalPropList)),
        keepCustomDataTypes,
        (customDataTypedPropList == null ? null : new HashSet<String>(customDataTypedPropList)),
        (predicateExclusionList == null ? null : new HashSet<String>(predicateExclusionList)),
        typesToLabels, virtualNodes,
        virtualRels, keepLangTag, languageFilter, applyNeo4jNaming, log);
    try {

      InputStream inputStream = getInputStream(url, props);
      RDFFormat rdfFormat = getFormat(format);
      log.info("Data set to be parsed as " + rdfFormat);
      RDFParser rdfParser = Rio.createParser(rdfFormat);
      rdfParser.set(BasicParserSettings.VERIFY_URI_SYNTAX, verifyUriSyntax);
      rdfParser.setRDFHandler(statementViewer);
      rdfParser.parse(inputStream, "http://neo4j.com/base/");
    } catch (MalformedURLException e) {
      e.printStackTrace();
    } catch (IOException | RDFHandlerException | QueryExecutionException | RDFParseException | RDFImportPreRequisitesNotMet e) {
      e.printStackTrace();
    }

    GraphResult graphResult = new GraphResult(new ArrayList<>(virtualNodes.values()), virtualRels);
    return Stream.of(graphResult);


  }

  @Procedure(mode = Mode.READ)
  public Stream<StreamedStatement> streamRDF(@Name("url") String url, @Name("format") String format,
      @Name(value = "params", defaultValue = "{}") Map<String, Object> props) {
    final boolean verifyUriSyntax = (props.containsKey("verifyUriSyntax") ? (Boolean) props
        .get("verifyUriSyntax") : true);

    StatementStreamer statementStreamer = new StatementStreamer();
    try {

      InputStream inputStream = getInputStream(url, props);
      RDFFormat rdfFormat = getFormat(format);
      log.info("Data set to be parsed as " + rdfFormat);
      RDFParser rdfParser = Rio.createParser(rdfFormat);
      rdfParser.set(BasicParserSettings.VERIFY_URI_SYNTAX, verifyUriSyntax);
      rdfParser.setRDFHandler(statementStreamer);
      rdfParser.parse(inputStream, "http://neo4j.com/base/");
    } catch (MalformedURLException e) {
      e.printStackTrace();
    } catch (IOException | RDFHandlerException | QueryExecutionException | RDFParseException | RDFImportPreRequisitesNotMet e) {
      e.printStackTrace();
    }

    return statementStreamer.getStatements().stream();


  }

  @Procedure(mode = Mode.READ)
  public Stream<GraphResult> previewRDFSnippet(@Name("rdf") String rdfFragment,
      @Name("format") String format,
      @Name(value = "params", defaultValue = "{}") Map<String, Object> props) {

    final int handleVocabUris = (props.containsKey("handleVocabUris") ? getHandleVocabUrisAsInt(
        (String) props.get("handleVocabUris")) : 0);
    final boolean applyNeo4jNaming = (props.containsKey("applyNeo4jNaming") ? (boolean) props
        .get("applyNeo4jNaming") : false);
    final int handleMultival = (props.containsKey("handleMultival") ? getHandleMultivalAsInt(
        (String) props.get("handleMultival")) : 0);
    final List<String> multivalPropList = (props.containsKey("multivalPropList")
        ? (List<String>) props.get("multivalPropList") : null);
    final List<String> predicateExclusionList = (props.containsKey("predicateExclusionList")
        ? (List<String>) props.get("predicateExclusionList") : null);
    final List<String> customDataTypedPropList = (props.containsKey("customDataTypesList")
        ? (List<String>) props.get("customDataTypesList") : null);
    final boolean typesToLabels = (props.containsKey("typesToLabels") ? (boolean) props
        .get("typesToLabels") : DEFAULT_TYPES_TO_LABELS);
    final boolean keepLangTag = (props.containsKey("keepLangTag") ? (boolean) props
        .get("keepLangTag") : false);
    final boolean keepCustomDataTypes = (props.containsKey("keepCustomDataTypes") ? (boolean) props
        .get("keepCustomDataTypes") : DEFAULT_KEEP_CUSTOM_DATA_TYPES);
    final String languageFilter = (props.containsKey("languageFilter") ? (String) props
        .get("languageFilter") : null);
    final boolean verifyUriSyntax = (props.containsKey("verifyUriSyntax") ? (Boolean) props
        .get("verifyUriSyntax") : true);

    Map<String, Node> virtualNodes = new HashMap<>();
    List<Relationship> virtualRels = new ArrayList<>();

    StatementPreviewer statementViewer = new StatementPreviewer(db, handleVocabUris, handleMultival,
        (multivalPropList == null ? null : new HashSet<String>(multivalPropList)),
        keepCustomDataTypes,
        (customDataTypedPropList == null ? null : new HashSet<String>(customDataTypedPropList)),
        (predicateExclusionList == null ? null : new HashSet<String>(predicateExclusionList)),
        typesToLabels, virtualNodes, virtualRels, keepLangTag, languageFilter, applyNeo4jNaming,
        log);
    try {
      InputStream inputStream = new ByteArrayInputStream(
          rdfFragment.getBytes(Charset.defaultCharset())); //rdfFragment.openStream();
      RDFFormat rdfFormat = getFormat(format);
      log.info("Data set to be parsed as " + rdfFormat);
      RDFParser rdfParser = Rio.createParser(rdfFormat);
      rdfParser.set(BasicParserSettings.VERIFY_URI_SYNTAX, verifyUriSyntax);
      rdfParser.setRDFHandler(statementViewer);
      rdfParser.parse(inputStream, "http://neo4j.com/base/");
    } catch (MalformedURLException e) {
      e.printStackTrace();
    } catch (IOException | RDFHandlerException | QueryExecutionException | RDFParseException | RDFImportPreRequisitesNotMet e) {
      e.printStackTrace();
    }

    GraphResult graphResult = new GraphResult(new ArrayList<>(virtualNodes.values()), virtualRels);
    return Stream.of(graphResult);


  }

  @UserFunction
  public String getDataType(@Name("literal") Object literal) {

    String result;

    if (literal instanceof String) {
      Matcher matcherShortened = DATATYPE_SHORTENED_PATTERN.matcher((String) literal);
      Matcher matcherRegular = DATATYPE_REGULAR_PATTERN.matcher((String) literal);
      if (matcherShortened.matches()) {
        result = matcherShortened.group(2);
      } else if (matcherRegular.matches()) {
        result = matcherRegular.group(2);
      } else {
        result = XMLSchema.STRING.stringValue();
      }
    } else if (literal instanceof Long) {
      result = XMLSchema.LONG.stringValue();
    } else if (literal instanceof Double) {
      result = XMLSchema.DOUBLE.stringValue();
    } else if (literal instanceof Boolean) {
      result = XMLSchema.BOOLEAN.stringValue();
    } else {
      result = null;
    }

    return result;
  }

  @UserFunction
  public String getValue(@Name("literal") String literal) {

    Matcher matcherShortened = DATATYPE_SHORTENED_PATTERN.matcher(literal);
    Matcher matcherRegular = DATATYPE_REGULAR_PATTERN.matcher(literal);
    String result = literal;
    if (matcherShortened.matches()) {
      result = matcherShortened.group(1);
    } else if (matcherRegular.matches()) {
      result = matcherRegular.group(1);
    }
    return result;
  }

  @UserFunction
  public String getIRILocalName(@Name("url") String url) {
    return url.substring(URIUtil.getLocalNameIndex(url));
  }

  @UserFunction
  public String getIRINamespace(@Name("url") String url) {
    return url.substring(0, URIUtil.getLocalNameIndex(url));
  }

  @UserFunction
  public String getLangValue(@Name("lang") String lang, @Name("values") Object values) {

    if (values instanceof List) {
      if (((List) values).get(0) instanceof String) {
        for (Object val : (List<String>) values) {
          Matcher m = LANGUAGE_TAGGED_VALUE_PATTERN.matcher((String) val);
          if (m.matches() && m.group(2).equals(lang)) {
            return m.group(1);
          }
        }
      }
    } else if (values instanceof String[]) {
      String[] valuesAsArray = (String[]) values;
      for (int i = 0; i < valuesAsArray.length; i++) {
        Matcher m = LANGUAGE_TAGGED_VALUE_PATTERN.matcher(valuesAsArray[i]);
        if (m.matches() && m.group(2).equals(lang)) {
          return m.group(1);
        }
      }
    } else if (values instanceof String) {
      Matcher m = LANGUAGE_TAGGED_VALUE_PATTERN.matcher((String) values);
      if (m.matches() && m.group(2).equals(lang)) {
        return m.group(1);
      }
    }
    return null;
  }

  @UserFunction
  public String uriFromShort(@Name("short") String str) {

    Matcher m = SHORTENED_URI_PATTERN.matcher(str);
    if (m.matches()) {
      ResourceIterator<Node> nspd = db.findNodes(Label.label("NamespacePrefixDefinition"));
      if (nspd.hasNext()) {
        Map<String, Object> namespaces = nspd.next().getAllProperties();
        Iterator<Map.Entry<String, Object>> nsIterator = namespaces.entrySet().iterator();
        while (nsIterator.hasNext()) {
          Map.Entry<String, Object> kv = nsIterator.next();
          if (m.group(1).equals(kv.getValue())) {
            return kv.getKey() + m.group(2);
          }
        }
      }
    }
    //default return original value
    return str;
  }

  @UserFunction
  public String shortFromUri(@Name("uri") String str) {
    try {
      IRI iri = SimpleValueFactory.getInstance().createIRI(str);
      ResourceIterator<Node> nspd = db.findNodes(Label.label("NamespacePrefixDefinition"));
      if (nspd.hasNext()) {
        Map<String, Object> namespaces = nspd.next().getAllProperties();
        Iterator<Map.Entry<String, Object>> nsIterator = namespaces.entrySet().iterator();
        while (nsIterator.hasNext()) {
          Map.Entry<String, Object> kv = nsIterator.next();
          if (kv.getKey().equals(iri.getNamespace())) {
            return kv.getValue() + PREFIX_SEPARATOR + iri.getLocalName();
          }
        }
      }
      return str;
    } catch (Exception e) {
      return str;
    }
  }

  @Procedure(mode = Mode.WRITE)
  public Stream<NamespacePrefixesResult> addNamespacePrefix(@Name("prefix") String prefix,
      @Name("ns") String ns) {

    Map<String, Object> params = new HashMap<>();
    params.put("prefix", prefix);

    return db
        .execute(String.format("MERGE (n:NamespacePrefixDefinition) SET n.`%s` = $prefix "
            + "WITH n UNWIND keys(n) as ns\n"
            + "RETURN n[ns] as prefix, ns as namespace", ns), params).stream().map(
            n -> new NamespacePrefixesResult((String) n.get("prefix"),
                (String) n.get("namespace")));

  }

  @Procedure(mode = Mode.READ)
  public Stream<NamespacePrefixesResult> listNamespacePrefixes() {

    return db
        .execute("MATCH (n:NamespacePrefixDefinition) \n" +
            "UNWIND keys(n) AS namespace\n" +
            "RETURN namespace, n[namespace] AS prefix").stream().map(
            n -> new NamespacePrefixesResult((String) n.get("prefix"),
                (String) n.get("namespace")));

  }

  private void checkIndexesExist() throws RDFImportPreRequisitesNotMet {
    Iterable<IndexDefinition> indexes = db.schema().getIndexes();
    if (missing(indexes.iterator(), "Resource")) {
      throw new RDFImportPreRequisitesNotMet(
          "The required index on :Resource(uri) could not be found");
    }
  }

  private boolean missing(Iterator<IndexDefinition> iterator, String indexLabel) {
    while (iterator.hasNext()) {
      IndexDefinition indexDef = iterator.next();
      if (indexDef.getLabel().name().equals(indexLabel) &&
          indexDef.getPropertyKeys().iterator().next().equals("uri")) {
        return false;
      }
    }
    return true;
  }

  private RDFFormat getFormat(String format) throws RDFImportPreRequisitesNotMet {
    if (format != null) {
      for (RDFFormat parser : availableParsers) {
        if (parser.getName().equals(format)) {
          return parser;
        }
      }
    }
    throw new RDFImportPreRequisitesNotMet("Unrecognized serialization format: " + format);
  }


  public static class ImportResults {

    public String terminationStatus = "OK";
    public long triplesLoaded = 0;
    public Map<String, String> namespaces;
    public String extraInfo = "";

    public void setTriplesLoaded(long triplesLoaded) {
      this.triplesLoaded = triplesLoaded;
    }

    public void setNamespaces(Map<String, String> namespaces) {
      this.namespaces = namespaces;
    }

    public void setTerminationKO(String message) {
      this.terminationStatus = "KO";
      this.extraInfo = message;
    }

  }

  private class RDFImportPreRequisitesNotMet extends Exception {

    String message;

    public RDFImportPreRequisitesNotMet(String s) {
      message = s;
    }

    @Override
    public String getMessage() {
      return message;
    }
  }
}
